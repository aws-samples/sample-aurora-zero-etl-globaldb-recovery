"""Recreate an Aurora zero-ETL integration after a Global Database transition.

Amazon EventBridge invokes this function when an Aurora global database finishes
a switchover (RDS-EVENT-0185) or a failover (RDS-EVENT-0238, RDS-EVENT-0519).
The function reconciles the zero-ETL integration so that it points at whichever
DB cluster is currently the writer of the global cluster.

Design notes
------------
1. The global cluster is the source of truth, not the event. The function calls
   DescribeGlobalClusters and reads which member reports IsWriter, instead of
   trusting a single event payload. Duplicate, retried, or out-of-order events
   therefore converge on the same result.

2. Region affinity picks a single actor. CreateIntegration is a Regional call
   and the integration is created in the Region of the source cluster. Deploy
   this function in every Region that can host the primary. Each copy acts only
   when the current writer is in its own Region and exits otherwise, so exactly
   one Region performs the recreate without any locking.

3. Stale cleanup is cross-Region. The integration bound to the former primary
   lives in the former primary's Region, so a Regional DescribeIntegrations
   call in the new primary's Region cannot see it. The function walks every
   member Region of the global cluster with a Regional client to find and delete
   integrations it owns. Deletion is best effort: during a service impairment
   API calls to the former primary's Region may not succeed, and the new
   integration must still be created.

4. Ownership is tag-scoped. The function only ever deletes integrations that
   carry both of its own tags, so integrations managed elsewhere are untouched.

Environment variables
---------------------
GLOBAL_CLUSTER_ID  (required) Aurora global cluster identifier.
TARGET_ARN         (required) Amazon Redshift namespace ARN (provisioned or
                   Serverless) that receives the replicated data.
INTEGRATION_NAME   (required) Name applied to the managed integration.
DATA_FILTER        (optional) Data filter expression. Required for Aurora
                   PostgreSQL sources, for example "include: salesdb.*.*".
KMS_KEY_ID         (optional) Customer managed AWS KMS key for the integration.
SNS_TOPIC_ARN      (optional) Topic that receives outcome notifications.
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
TARGET_ARN = os.environ["TARGET_ARN"]
INTEGRATION_NAME = os.environ["INTEGRATION_NAME"]
DATA_FILTER = os.environ.get("DATA_FILTER")
KMS_KEY_ID = os.environ.get("KMS_KEY_ID")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

# Lambda populates AWS_REGION with the Region the function runs in.
THIS_REGION = os.environ["AWS_REGION"]

# Tag values that mark an integration as owned by this automation.
MANAGED_BY_TAG = "ManagedBy"
MANAGED_BY_VALUE = "zero-etl-globaldb-recovery"
GLOBAL_CLUSTER_TAG = "GlobalCluster"

# Integration states that mean "an integration already exists here, do not
# create a second one".
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
    return arn.split(":")[3]


def _notify(subject, message):
    logger.info("%s | %s", subject, message)
    if not sns:
        return
    try:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
    except (ClientError, BotoCoreError) as exc:
        # A failed notification must not mask the recovery result.
        logger.warning("Could not publish notification: %s", exc)


def _global_members():
    """Return the member list of the global cluster."""
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


def _managed_integrations(region):
    """Return integrations in region that this automation owns."""
    owned = []
    client = _rds_for(region)
    for page in client.get_paginator("describe_integrations").paginate():
        for integration in page["Integrations"]:
            if integration.get("TargetArn") != TARGET_ARN:
                continue
            tags = {t["Key"]: t["Value"] for t in integration.get("Tags", [])}
            if (
                tags.get(MANAGED_BY_TAG) == MANAGED_BY_VALUE
                and tags.get(GLOBAL_CLUSTER_TAG) == GLOBAL_CLUSTER_ID
            ):
                owned.append(integration)
    return owned


def _delete_stale(regions, keep_source_arn):
    """Delete owned integrations that do not point at keep_source_arn.

    Walks every member Region because the stale integration lives in the former
    primary's Region. Returns (deleted_arns, unreachable_regions). Failures are
    reported rather than raised so that recovery proceeds when a Region is
    unavailable.
    """
    deleted = []
    unreachable = []

    for region in regions:
        try:
            candidates = _managed_integrations(region)
        except (ClientError, BotoCoreError) as exc:
            logger.warning("Could not list integrations in %s: %s", region, exc)
            unreachable.append(region)
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
                unreachable.append(region)

    return deleted, unreachable


def _live_integration_for(integrations, source_arn):
    for integration in integrations:
        if (
            integration["SourceArn"] == source_arn
            and integration["Status"] in LIVE_STATES
        ):
            return integration
    return None


def _create_integration(source_arn):
    kwargs = {
        "SourceArn": source_arn,
        "TargetArn": TARGET_ARN,
        "IntegrationName": INTEGRATION_NAME,
        "Tags": [
            {"Key": MANAGED_BY_TAG, "Value": MANAGED_BY_VALUE},
            {"Key": GLOBAL_CLUSTER_TAG, "Value": GLOBAL_CLUSTER_ID},
        ],
    }
    # Aurora PostgreSQL sources require at least one data filter pattern.
    if DATA_FILTER:
        kwargs["DataFilter"] = DATA_FILTER
    if KMS_KEY_ID:
        kwargs["KMSKeyId"] = KMS_KEY_ID
    return rds.create_integration(**kwargs)


def handler(event, _context):
    logger.info("Received event: %s", json.dumps(event))
    event_id = event.get("detail", {}).get("EventID", "unknown")

    members = _global_members()
    writer_arn = _writer_arn(members)
    if not writer_arn:
        # A transition can be mid-flight. A later event, or the EventBridge
        # retry, reconciles once a writer is elected.
        logger.info("No writer yet for %s. Exiting.", GLOBAL_CLUSTER_ID)
        return {"status": "skipped", "reason": "no-writer"}

    if _region_of(writer_arn) != THIS_REGION:
        logger.info(
            "Writer is in %s, this function runs in %s. Exiting.",
            _region_of(writer_arn),
            THIS_REGION,
        )
        return {"status": "skipped", "reason": "not-writer-region"}

    member_regions = sorted(
        {_region_of(m["DBClusterArn"]) for m in members} | {THIS_REGION}
    )

    local = _managed_integrations(THIS_REGION)
    existing = _live_integration_for(local, writer_arn)

    deleted, unreachable = _delete_stale(member_regions, keep_source_arn=writer_arn)

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
            "unreachableRegions": unreachable,
        }

    try:
        result = _create_integration(writer_arn)
    except rds.exceptions.IntegrationAlreadyExistsFault:
        logger.info("Integration already exists for this source and target.")
        return {"status": "exists", "staleDeleted": deleted}
    except rds.exceptions.IntegrationConflictOperationFault as exc:
        # Usually a delete that has not finished. Raise so EventBridge retries.
        _notify(
            "Zero-ETL recovery deferred",
            f"Conflict recreating the integration for {GLOBAL_CLUSTER_ID}: {exc}. "
            "EventBridge retries this invocation.",
        )
        raise
    except rds.exceptions.IntegrationQuotaExceededFault as exc:
        _notify(
            "Zero-ETL recovery failed: quota exceeded",
            f"Reached an integration quota while recovering {GLOBAL_CLUSTER_ID}: "
            f"{exc}. Stale integrations may still exist in {unreachable or 'another Region'}. "
            "Review integration quotas for the source cluster, target, and account.",
        )
        raise
    except rds.exceptions.KMSKeyNotAccessibleFault as exc:
        _notify(
            "Zero-ETL recovery failed: KMS key not accessible",
            f"The KMS key for {GLOBAL_CLUSTER_ID} is not usable in {THIS_REGION}: "
            f"{exc}. A customer managed key must be usable in every Region that "
            "can host the primary.",
        )
        raise

    summary = [
        f"Event {event_id} on global cluster {GLOBAL_CLUSTER_ID}.",
        f"Created integration {result['IntegrationArn']} against the new primary "
        f"{writer_arn}.",
        "Initial data seeding runs before the target is queryable again.",
    ]
    if deleted:
        summary.append(f"Deleted stale integrations: {', '.join(deleted)}.")
    if unreachable:
        summary.append(
            "Could not confirm cleanup in: "
            f"{', '.join(sorted(set(unreachable)))}. Check for a leftover "
            "integration once those Regions recover."
        )

    _notify("Zero-ETL integration recreated", " ".join(summary))

    return {
        "status": "created",
        "integrationArn": result["IntegrationArn"],
        "sourceArn": writer_arn,
        "staleDeleted": deleted,
        "unreachableRegions": sorted(set(unreachable)),
    }
