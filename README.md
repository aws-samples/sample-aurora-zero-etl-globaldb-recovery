# Automating Aurora zero-ETL recovery after global failover

Event-driven automation that recreates an Amazon Aurora zero-ETL integration
with Amazon Redshift after an Amazon Aurora Global Database switchover or
failover. Accompanies the AWS Database Blog post of the same name.

This is sample code, for non-production usage. You should work with your
security and legal teams to meet your organizational security, regulatory and
compliance requirements before deployment.

## How the recovery works

A zero-ETL integration identifies its source by the ARN of one DB cluster, and
each cluster in a global database has its own ARN. A switchover or failover
promotes a cluster with a different ARN to primary, so the integration stops
replicating. The documented recovery is to delete the integration and create a
new one against the new primary. This automation performs that recovery for
you: it detects the Aurora Global Database transition event and recreates the
zero-ETL integration against the new primary.

## What this deploys

![Architecture diagram: an Aurora global database switchover or failover emits an RDS event; an EventBridge rule in each Region invokes a Lambda function; the function queries the global cluster for the current writer, deletes the previous integration in the former primary's Region, and creates a new zero-ETL integration against the new primary; SNS notifies operators and CloudWatch alarms cover function errors and the dead-letter queue.](docs/architecture-zero-etl-recovery.png)

1. Aurora emits `RDS-EVENT-0185` (global switchover finished),
   `RDS-EVENT-0238` (global failover completed), or `RDS-EVENT-0519` (global
   failover completed, promoted cluster has no instances).
2. An Amazon EventBridge rule matches the event and invokes an AWS Lambda
   function.
3. The function calls `DescribeGlobalClusters` to find the member that reports
   `IsWriter`, rather than trusting the event payload. Duplicate and
   out-of-order events converge on the same result.
4. The function acts only if the writer is in its own Region, so exactly one
   Region performs the recreate without any locking.
5. It deletes the previous integration it owns, searching every member Region
   because that integration lives in the former primary's Region, and then
   creates a new integration against the new primary.
6. Amazon SNS reports the outcome. Amazon CloudWatch alarms cover function
   errors and a non-empty dead-letter queue.

## Repository layout

| Path | Description |
|---|---|
| `cloudformation/zero-etl-recovery.yaml` | Full stack: KMS key, SNS topic, SQS dead-letter queue, IAM role, Lambda function, EventBridge rule, CloudWatch alarms, log group. |
| `lambda/zero_etl_recovery.py` | Recovery function source. The readable, testable copy. |
| `scripts/sync_lambda_into_template.py` | Guard that keeps the template's inline code identical to the function source. |
| `iam/lambda-least-privilege-policy.json` | Reference copy of the execution role policy for non-CloudFormation deployments. |
| `docs/architecture-zero-etl-recovery.drawio` | Editable architecture diagram source. |

The template embeds the function source in the Lambda `Code.ZipFile` block so
the stack deploys with one command and no S3 bucket. Edit
`lambda/zero_etl_recovery.py`, then run:

```bash
python3 scripts/sync_lambda_into_template.py --write
```

## Prerequisites

1. An Aurora PostgreSQL global database with a primary cluster and at least one
   secondary cluster, on an engine version that supports
   [zero-ETL integrations with Amazon Redshift](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/zero-etl.html).
2. A custom DB cluster parameter group applied to **every** cluster in the
   global database, with enhanced logical replication enabled:

   ```
   rds.logical_replication=1
   aurora.enhanced_logical_replication=1
   aurora.logical_replication_backup=0
   aurora.logical_replication_globaldb=0
   ```

   Applying it to the secondaries as well means the source is ready the moment
   a secondary is promoted.
3. An Amazon Redshift target (provisioned namespace or Redshift Serverless) with
   `enable_case_sensitive_identifier` set to `true` and the source clusters
   configured as authorized integration sources.
4. All databases in the source cluster using UTF-8 encoding, and a primary key
   on every table matched by the data filter. Tables without one are placed in a
   failed state.
5. Permissions to create AWS CloudFormation, AWS KMS, Amazon SNS, Amazon SQS,
   AWS Lambda, Amazon EventBridge, AWS IAM, and Amazon CloudWatch resources.
6. Python 3.9 or later locally if you plan to run the sync guard or Checkov.

## Deploy

Deploy the stack in **every** Region that can host the primary cluster. For a
primary in `us-east-1` and a secondary in `us-west-2`:

```bash
for region in us-east-1 us-west-2; do
  aws cloudformation deploy \
    --region "$region" \
    --stack-name zero-etl-globaldb-recovery \
    --template-file cloudformation/zero-etl-recovery.yaml \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides \
      GlobalClusterId=aurora-postgres-global \
      TargetArn=arn:aws:redshift-serverless:us-east-1:111122223333:namespace/REPLACE-WITH-NAMESPACE-UUID \
      IntegrationName=globaldb-analytics \
      DataFilter="include: salesdb.*.*" \
      NotificationEmail=dba-oncall@example.com
done
```

`DataFilter` is required for Aurora PostgreSQL sources and uses
`database.schema.table` patterns. Confirm the SNS subscription from your inbox
after the stack completes.

## Test

Validate with a planned switchover, which is non-destructive and loses no data:

```bash
aws rds switchover-global-cluster \
  --global-cluster-identifier aurora-postgres-global \
  --target-db-cluster-identifier \
    arn:aws:rds:us-west-2:111122223333:cluster:aurora-postgres-secondary
```

Then confirm the new integration exists and is healthy:

```bash
aws rds describe-integrations --region us-west-2 \
  --query 'Integrations[?Tags[?Key==`ManagedBy` && Value==`zero-etl-globaldb-recovery`]].[IntegrationName,Status,SourceArn]' \
  --output table
```

And that the old one is gone:

```bash
aws rds describe-integrations --region us-east-1 \
  --query 'length(Integrations[?Tags[?Key==`ManagedBy` && Value==`zero-etl-globaldb-recovery`]])'
```

Switch back to validate the reverse direction.

## Monitor

- The function publishes its outcome to the SNS topic on every recreate.
- `<IntegrationName>-zero-etl-recovery-failed` alarms on Lambda `Errors`, which
  is the signal that the integration was **not** recreated.
- `<IntegrationName>-zero-etl-recovery-dlq-not-empty` alarms when an invocation
  exhausts its retries.
- Amazon Redshift publishes integration lag and table counts as CloudWatch
  metrics; see
  [Metrics for zero-ETL integrations](https://docs.aws.amazon.com/redshift/latest/mgmt/zero-etl-using.metrics.html).
  These carry an `IntegrationId` dimension, which is not known until the
  integration is created, so this stack does not pre-create an alarm on them.
- Amazon Redshift also emits
  [zero-ETL integration events to EventBridge](https://docs.aws.amazon.com/redshift/latest/mgmt/integration-event-notifications.html)
  with `INFO`, `WARNING`, and `ERROR` severities.
- Query state directly in the target warehouse:

  ```sql
  SELECT * FROM SVV_INTEGRATION;
  SELECT * FROM SVV_INTEGRATION_TABLE_STATE;
  ```

## Considerations and limitations

- **Data seeding follows the recreate.** A new integration reseeds from the
  source before the target is fully queryable again. The automation restores the
  pipeline promptly; data continuity in Amazon Redshift follows after the
  reseed. Prefer planned switchovers so you can schedule that window.
- **Cleanup is best effort during a Regional outage.** If the former primary's
  Region is unreachable, the previous integration cannot be deleted. The function
  still creates the new integration and reports the Regions it could not reach
  in its SNS notification and return value. Reconcile once those Regions
  recover.
- **Tag-scoped ownership.** The function only deletes integrations carrying both
  its `ManagedBy` and `GlobalCluster` tags. Integrations managed elsewhere are
  never touched. Conversely, an integration created by hand will not be cleaned
  up by this automation.
- **Quotas apply.** Integrations are limited per source cluster, per target, and
  per account per Region. See
  [Aurora zero-ETL quotas](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/zero-etl.html).
- **Customer managed keys must be multi-Region-usable.** If you pass
  `KmsKeyId`, the key must be usable in every Region that can host the primary.
- **Blue/Green deployments.** Delete the integration before switching over an
  Amazon RDS Blue/Green environment, then recreate it.
- **Changing `aurora.enhanced_logical_replication` invalidates logical
  replication slots.** Keep it stable outside planned maintenance.

## Clean up

```bash
for region in us-east-1 us-west-2; do
  aws cloudformation delete-stack --region "$region" \
    --stack-name zero-etl-globaldb-recovery
done
```

The stack does not create or delete the Aurora clusters, the Redshift
warehouse, or the zero-ETL integration itself. Delete any test integration and
the test clusters and warehouse separately.

## Verify before you commit

```bash
python3 -m py_compile lambda/zero_etl_recovery.py
python3 scripts/sync_lambda_into_template.py --check
python3 -m checkov.main -f cloudformation/zero-etl-recovery.yaml --compact
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md) for how to report a security issue.

## License

Licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
