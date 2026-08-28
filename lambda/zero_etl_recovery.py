"""Recreate an Aurora zero-ETL integration after a Global Database transition.

Amazon EventBridge invokes this function when an Aurora global database finishes
a switchover (RDS-EVENT-0185) or failover (RDS-EVENT-0238 or
RDS-EVENT-0519). The function reconciles the local zero-ETL integration so it
points to the current writer and to an Amazon Redshift target in the same Region.

Design notes
------------
1. The global cluster is the source of truth. The function calls
   DescribeGlobalClusters and reads which member reports IsWriter. Duplicate,
   retried, and out-of-order events converge on the same topology.

2. Region affinity and reserved concurrency select one actor. Each regional
   copy acts only when the current writer is in its Region. The CloudFormation
   template reserves concurrency of one, serializing duplicate deliveries in
   that Region.

3. Every possible primary Region has its own Amazon Redshift target. Aurora
   zero-ETL requires the source and target to be in the same Region. TARGET_ARN
   is therefore local to this function's Region.

4. Stale cleanup is cross-Region and tag-scoped. The function searches every
   member Region and deletes only integrations carrying both ownership tags.
   Integrations managed elsewhere are untouched.

5. A promoted cluster with no writer DB instance is not ready. The function
   raises an error before cleanup, which activates Lambda asynchronous retries.
   If retries expire, operators create an instance and replay the event or
   invoke the function again from the dead-letter queue runbook.

Environment variables
---------------------
GLOBAL_CLUSTER_ID    Aurora global cluster identifier.
SOURCE_CLUSTER_ARNS  Comma-separated allowlist of member DB cluster ARNs.
TARGET_ARN           Amazon Redshift namespace ARN in this function's Region.
INTEGRATION_NAME     Name applied to the managed integration.
DATA_FILTER          Optional data filter. Required for Aurora PostgreSQL.
KMS_KEY_ID           Optional KMS key ARN in this function's Region.
SNS_TOPIC_ARN        Optional topic for outcome notifications.
"""

import json
import logging
import os

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GLOBAL_CLUSTER_ID = os.environ["GLOBAL_CLUSTER_ID"]
SOURCE_CLUSTER_ARNS = frozenset(
    arn.strip()
    for arn in os.environ["SOURCE_CLUSTER_ARNS"].split(",")
    if arn.strip()
)
TARGET_ARN = os.environ["TARGET_ARN"]
INTEGRATION_NAME = os.environ["INTEGRATION_NAME"]
DATA_FILTER = os.environ.get("DATA_FILTER")
KMS_KEY_ID = os.environ.get("KMS_KEY_ID")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
THIS_REGION = os.environ["AWS_REGION"]

MANAGED_BY_TAG = "ManagedBy"
MANAGED_BY_VALUE = "zero-etl-globaldb-recovery"
GLOBAL_CLUSTER_TAG = "GlobalCluster"
LIVE_STATES = frozenset(
    {"creating", "active", "modifying", "syncing", "needs_attention"}
)

_BOTO_CONFIG = Config(
    retries={"max_attempts": 5, "mode": "standard"},
    connect_timeout=5,
    read_timeout=20,
)

rds = boto3.client("rds", config=_BOTO_CONFIG)
sns = boto3.client("sns", config=_BOTO_CONFIG) if SNS_TOPIC_ARN else None
_regional_rds = {THIS_REGION: rds}


def _rds_for(region):
    """Return an RDS client bound to region, creating it on first use."""
    if region not in _regional_rds:
        _regional_rds[region] = boto3.client(
            "rds", region_name=region, config=_BOTO_CONFIG
        )
    return _regional_rds[region]


def _region_of(arn):
    """Return the Region component of an ARN."""
    return arn.split(":")[3]


def _notify(subject, message):
    """Log and optionally publish an operational notification."""
    logger.info("%s | %s", subject, message)
    if not sns:
        return
    try:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
    except (ClientError, BotoCoreError) as exc:
        logger.warning("Could not publish notification: %s", exc)


def _global_members():
    """Return the member list of the configured global cluster."""
    response = rds.describe_global_clusters(
        GlobalClusterIdentifier=GLOBAL_CLUSTER_ID
    )
    return response["GlobalClusters"][0]["GlobalClusterMembers"]


def _writer_arn(members):
    """Return the DB cluster ARN of the current writer, or None."""
    for member in members:
        if member.get("IsWriter"):
            return member["DBClusterArn"]
    return None


def _writer_cluster_ready(writer_arn):
    """Return True when the writer cluster is available with a writer instance."""
    response = _rds_for(_region_of(writer_arn)).describe_db_clusters(
        DBClusterIdentifier=writer_arn
    )
    cluster = response["DBClusters"][0]
    has_writer_instance = any(
        member.get("IsClusterWriter")
        for member in cluster.get("DBClusterMembers", [])
    )
    return cluster.get("Status") == "available" and has_writer_instance


def _managed_integrations(region):
    """Return integrations in region owned by this global-cluster automation.

    TargetArn is intentionally not used for discovery. Every Region has a local
    Redshift target, so cleanup must find the old integration even though its
    target differs from the target configured in the newly promoted Region.
    """
    owned = []
    client = _rds_for(region)
    for page in client.get_paginator("describe_integrations").paginate():
        for integration in page["Integrations"]:
            tags = {tag["Key"]: tag["Value"] for tag in integration.get("Tags", [])}
            if (
                tags.get(MANAGED_BY_TAG) == MANAGED_BY_VALUE
                and tags.get(GLOBAL_CLUSTER_TAG) == GLOBAL_CLUSTER_ID
            ):
                owned.append(integration)
    return owned


def _delete_stale(regions, keep_source_arn):
    """Delete owned integrations that don't point at keep_source_arn.

    Returns (deleted_arns, inaccessible_regions). Listing or deletion failures
    are reported rather than raised so recovery can proceed when API calls to a
    member Region don't succeed.
    """
    deleted = []
    inaccessible = []

    for region in regions:
        try:
            candidates = _managed_integrations(region)
        except (ClientError, BotoCoreError) as exc:
            logger.warning("Could not list integrations in %s: %s", region, exc)
            inaccessible.append(region)
            continue

        for integration in candidates:
            if integration["SourceArn"] == keep_source_arn:
                continue
            if integration["Status"] == "deleting":
                continue
            arn = integration["IntegrationArn"]
            logger.info(
                "Deleting stale integration %s in %s (source %s, status %s)",
                arn,
                region,
                integration["SourceArn"],
                integration["Status"],
            )
            try:
                _rds_for(region).delete_integration(IntegrationIdentifier=arn)
                deleted.append(arn)
            except (ClientError, BotoCoreError) as exc:
                logger.warning("Could not delete %s in %s: %s", arn, region, exc)
                inaccessible.append(region)

    return deleted, inaccessible


def _live_integration_for(integrations, source_arn):
    """Return an owned live integration for source_arn, if one exists."""
    for integration in integrations:
        if (
            integration["SourceArn"] == source_arn
            and integration["Status"] in LIVE_STATES
        ):
            return integration
    return None


def _create_integration(source_arn):
    """Create the locally targeted integration with ownership tags."""
    kwargs = {
        "SourceArn": source_arn,
        "TargetArn": TARGET_ARN,
        "IntegrationName": INTEGRATION_NAME,
        "Tags": [
            {"Key": MANAGED_BY_TAG, "Value": MANAGED_BY_VALUE},
            {"Key": GLOBAL_CLUSTER_TAG, "Value": GLOBAL_CLUSTER_ID},
        ],
    }
    if DATA_FILTER:
        kwargs["DataFilter"] = DATA_FILTER
    if KMS_KEY_ID:
        kwargs["KMSKeyId"] = KMS_KEY_ID
    return rds.create_integration(**kwargs)


def _validate_regional_configuration(writer_arn):
    """Fail before cleanup if allowlist or local-target configuration is invalid."""
    if writer_arn not in SOURCE_CLUSTER_ARNS:
        raise RuntimeError(
            f"Writer {writer_arn} isn't in SOURCE_CLUSTER_ARNS; update the stack "
            "before retrying recovery."
        )
    if _region_of(TARGET_ARN) != THIS_REGION:
        raise RuntimeError(
            f"Target {TARGET_ARN} must be in {THIS_REGION}, the writer's Region."
        )
    if KMS_KEY_ID and _region_of(KMS_KEY_ID) != THIS_REGION:
        raise RuntimeError(
            f"Integration KMS key {KMS_KEY_ID} must be in {THIS_REGION}."
        )


def handler(event, _context):
    """Reconcile the local integration to the current global writer."""
    logger.info("Received event: %s", json.dumps(event))
    event_id = event.get("detail", {}).get("EventID", "unknown")

    members = _global_members()
    writer_arn = _writer_arn(members)
    if not writer_arn:
        message = (
            f"No writer is currently reported for {GLOBAL_CLUSTER_ID}. "
            "Lambda will retry this asynchronous invocation."
        )
        _notify("Zero-ETL recovery deferred: no writer", message)
        raise RuntimeError(message)

    if _region_of(writer_arn) != THIS_REGION:
        logger.info(
            "Writer is in %s, this function runs in %s. Exiting.",
            _region_of(writer_arn),
            THIS_REGION,
        )
        return {"status": "skipped", "reason": "not-writer-region"}

    try:
        _validate_regional_configuration(writer_arn)
    except RuntimeError as exc:
        _notify("Zero-ETL recovery blocked: configuration", str(exc))
        raise

    if not _writer_cluster_ready(writer_arn):
        message = (
            f"Writer cluster {writer_arn} isn't available with a writer DB "
            "instance. Create or restore a writer instance, then replay the "
            "failed event or invoke this function again. No integration was deleted."
        )
        _notify("Zero-ETL recovery blocked: writer instance required", message)
        raise RuntimeError(message)

    member_regions = sorted(
        {_region_of(member["DBClusterArn"]) for member in members} | {THIS_REGION}
    )
    local = _managed_integrations(THIS_REGION)
    existing = _live_integration_for(local, writer_arn)

    deleted, inaccessible = _delete_stale(
        member_regions, keep_source_arn=writer_arn
    )

    if existing:
        logger.info(
            "Integration %s already targets the writer (status %s).",
            existing["IntegrationArn"],
            existing["Status"],
        )
        return {
            "status": "noop",
            "integrationArn": existing["IntegrationArn"],
            "staleDeleted": deleted,
            "inaccessibleRegions": inaccessible,
        }

    try:
        result = _create_integration(writer_arn)
    except rds.exceptions.IntegrationAlreadyExistsFault:
        # Defensive fallback. Reserved concurrency normally serializes duplicate
        # deliveries, but a manually created concurrent request can still win.
        existing = _live_integration_for(
            _managed_integrations(THIS_REGION), writer_arn
        )
        if not existing:
            _notify(
                "Zero-ETL recovery failed: conflicting integration",
                "CreateIntegration reported an existing integration, but no "
                "owned live integration was found for the current writer.",
            )
            raise
        return {
            "status": "exists",
            "integrationArn": existing["IntegrationArn"],
            "staleDeleted": deleted,
        }
    except rds.exceptions.IntegrationConflictOperationFault as exc:
        _notify(
            "Zero-ETL recovery deferred",
            f"Conflict recreating the integration for {GLOBAL_CLUSTER_ID}: "
            f"{exc}. Lambda retries this asynchronous invocation.",
        )
        raise
    except rds.exceptions.IntegrationQuotaExceededFault as exc:
        _notify(
            "Zero-ETL recovery failed: quota exceeded",
            f"Reached an integration quota while recovering {GLOBAL_CLUSTER_ID}: "
            f"{exc}. Correct the quota or stale-integration condition, then replay "
            "the event or invoke this function again.",
        )
        raise
    except rds.exceptions.KMSKeyNotAccessibleFault as exc:
        _notify(
            "Zero-ETL recovery failed: KMS key not accessible",
            f"The local integration KMS key isn't usable in {THIS_REGION}: {exc}. "
            "Correct the key policy or regional key parameter, then retry.",
        )
        raise
    except (ClientError, BotoCoreError) as exc:
        _notify(
            "Zero-ETL recovery failed",
            f"CreateIntegration failed for {GLOBAL_CLUSTER_ID}: {exc}. Correct "
            "the reported condition, then replay the event or invoke this "
            "function again.",
        )
        raise

    summary = [
        f"Event {event_id} on global cluster {GLOBAL_CLUSTER_ID}.",
        f"Created integration {result['IntegrationArn']} against writer "
        f"{writer_arn} and local target {TARGET_ARN}.",
        "Initial data seeding must finish before freshness-sensitive queries resume.",
    ]
    if deleted:
        summary.append(f"Deleted stale integrations: {', '.join(deleted)}.")
    if inaccessible:
        summary.append(
            "Could not confirm cleanup in: "
            f"{', '.join(sorted(set(inaccessible)))}. Check for a leftover "
            "integration after API access is restored."
        )

    _notify("Zero-ETL integration recreated", " ".join(summary))

    return {
        "status": "created",
        "integrationArn": result["IntegrationArn"],
        "sourceArn": writer_arn,
        "targetArn": TARGET_ARN,
        "staleDeleted": deleted,
        "inaccessibleRegions": sorted(set(inaccessible)),
    }
