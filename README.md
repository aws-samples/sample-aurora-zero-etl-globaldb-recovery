# Automate Aurora zero-ETL recovery after global failover

Event-driven automation that recreates an Amazon Aurora zero-ETL integration
with a same-Region Amazon Redshift target after an Amazon Aurora Global Database
switchover or failover. Accompanies the AWS Database Blog post of the same name.

This is sample code, for non-production usage. You should work with your
security and legal teams to meet your organizational security, regulatory and
compliance requirements before deployment.

## Architecture constraint: one Redshift target per Region

![Architecture diagram showing an Aurora Global Database across two Regions. EventBridge invokes a Lambda recovery function after switchover or failover. The function recreates the zero-ETL integration against the active logical Amazon Redshift target, while Amazon CloudWatch and Amazon SNS report health and failures. Each deployed source uses a Redshift target in the same Region.](docs/architecture-zero-etl-recovery.png)

Amazon Redshift requires a zero-ETL target to be in the same AWS Region as the
integration source. Provide a Redshift namespace in every Region that can host
the Aurora primary. Each regional stack receives its local namespace ARN. After
a transition, the function in the writer Region creates the integration against
that Region's target. Analytics clients must use their own failover or routing
mechanism to select the active regional Redshift target; this sample does not
route queries.

## How the recovery works

1. EventBridge matches `RDS-EVENT-0185` (switchover finished),
   `RDS-EVENT-0238` (failover completed), or `RDS-EVENT-0519` (failover
   completed but the promoted cluster has no DB instances).
2. Lambda calls `DescribeGlobalClusters` to find the current writer.
3. Region affinity selects the function in the writer Region. Reserved
   concurrency of one serializes duplicate deliveries in that Region.
4. Before cleanup, the function validates that the writer is allowlisted, the
   Redshift target and optional integration KMS key are local, and the cluster
   is available with a writer DB instance.
5. The function finds old integrations by ownership tags across all member
   Regions, deletes them, and creates an integration against the local target.
6. The function creates or updates two replication alarms for the new
   integration ID and sends an SNS notification that includes that ID.
7. An operator creates the Redshift destination database from the new
   integration ID. The sample doesn't automate this step.
8. If the handler raises an error, Lambda retries it as configured by the
   template. If retries expire, the event reaches the dead-letter queue and
   alarms notify operators.

For event `RDS-EVENT-0519`, the readiness check fails before cleanup. The
template's retry budget is short (Lambda waits one minute, then two minutes,
between attempts), so plan to create a writer DB instance and then replay the
dead-letter event or invoke the function again. For this event, the replay is
the expected recovery path. The automation does not poll indefinitely for an
instance.

## Repository layout

| Path | Description |
|---|---|
| `cloudformation/zero-etl-recovery.yaml` | KMS, SNS, SQS DLQ, IAM, Lambda, EventBridge, alarms, and log group. |
| `.github/workflows/validate.yml` | CI checks: compile, inline-code sync, IAM JSON, and Checkov. |
| `lambda/zero_etl_recovery.py` | Canonical recovery function source. |
| `scripts/sync_lambda_into_template.py` | Keeps inline Lambda code identical to the canonical source. |
| `iam/lambda-least-privilege-policy.json` | Reference execution-role policy. |
| `docs/architecture-zero-etl-recovery.drawio` | Editable architecture diagram. |

## Prerequisites

1. An Aurora PostgreSQL global database. Zero-ETL requires Aurora PostgreSQL
   16.4 or later, 17.4 or later, or 18.3 or later, and Region support varies.
   Check the
   [zero-ETL support matrix](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Concepts.Aurora_Fea_Regions_DB-eng.Feature.Zero-ETL.html).
2. A same-Region Redshift target in every Region that can host the writer.
3. Every member cluster authorized as an integration source on its local target.
4. A custom cluster parameter group on every member:

   ```text
   rds.logical_replication=1
   aurora.enhanced_logical_replication=1
   aurora.logical_replication_backup=0
   aurora.logical_replication_globaldb=0
   ```

5. All databases use UTF-8. Every table selected by the filter has a primary
   key. Aurora Global Database replicates schema, but after an unplanned
   transition verify that the promoted state contains the selected database,
   schema, and tables.
6. Case sensitivity enabled on each Redshift target with
   `enable_case_sensitive_identifier=true`.
7. Optional integration encryption keys in each Region. Use independent
   regional keys or local replicas of a multi-Region key. Pass the local key ARN
   to each stack. The stack separately creates a local notification key.

## Deploy

The example uses a local Redshift namespace and local integration KMS key in
each Region. Remove `IntegrationKmsKeyArn` if you want the AWS owned integration
key.

```bash
regions=(us-east-1 us-west-2)
target_arns=(
  arn:aws:redshift-serverless:us-east-1:111122223333:namespace/EAST-NAMESPACE-UUID
  arn:aws:redshift-serverless:us-west-2:111122223333:namespace/WEST-NAMESPACE-UUID
)
integration_key_arns=(
  arn:aws:kms:us-east-1:111122223333:key/EAST-KEY-ID
  arn:aws:kms:us-west-2:111122223333:key/WEST-KEY-ID
)
source_cluster_arns="arn:aws:rds:us-east-1:111122223333:cluster:aurora-postgres-primary,arn:aws:rds:us-west-2:111122223333:cluster:aurora-postgres-secondary"

for i in "${!regions[@]}"; do
  aws cloudformation deploy \
    --region "${regions[$i]}" \
    --stack-name zero-etl-globaldb-recovery \
    --template-file cloudformation/zero-etl-recovery.yaml \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides \
      GlobalClusterId=aurora-postgres-global \
      SourceClusterArns="$source_cluster_arns" \
      TargetArn="${target_arns[$i]}" \
      IntegrationKmsKeyArn="${integration_key_arns[$i]}" \
      IntegrationName=globaldb-analytics \
      DataFilter="include: salesdb.*.*" \
      IntegrationLagThresholdSeconds=900 \
      NotificationEmail=dba-oncall@example.com
done
```

`CAPABILITY_IAM` is sufficient because the template lets CloudFormation name the
IAM role. Use `CAPABILITY_NAMED_IAM` only if you add a custom `RoleName`.

The function timeout is 300 seconds. RDS clients use a five-second connection
timeout, a 20-second read timeout, and standard SDK retries. The template
configures EventBridge to retry target delivery up to four times within one
hour, and configures Lambda asynchronous invocation to retry a handler error
twice within a maximum event age of one hour. These are template settings, not
service defaults.

`IntegrationLagThresholdSeconds` (default 900, minimum 60) sets the lag alarm
threshold. Replace the example value with the threshold your rehearsal and
workload require.

## Concurrency and idempotency

Each regional function has reserved concurrency of one. Duplicate events queue
behind the active invocation. The later invocation re-reads the global writer
and owned integrations and returns `noop` when recovery is already underway.
`IntegrationAlreadyExistsFault` is retained as a defensive fallback for a
concurrent request made outside this function; the handler re-reads and
validates the owned integration before returning success.

## Validate

Run a planned switchover, then inspect the integration in the promoted Region:

```bash
aws rds switchover-global-cluster \
  --global-cluster-identifier aurora-postgres-global \
  --target-db-cluster-identifier \
    arn:aws:rds:us-west-2:111122223333:cluster:aurora-postgres-secondary

aws rds describe-integrations --region us-west-2 \
  --query 'Integrations[?Tags[?Key==`ManagedBy` && Value==`zero-etl-globaldb-recovery`]].[IntegrationName,Status,SourceArn,TargetArn]' \
  --output table
```

In the former primary Region, the old integration can report `deleting` for a
while. Check it by its recorded ARN rather than by counting integrations.

Wait for the new integration to become `active`. Then create the destination
database on the local Redshift target from the integration ID in the SNS
notification. Superusers can also read it from `SVV_INTEGRATION`. Aurora
PostgreSQL requires the source database name:

```sql
CREATE DATABASE salesdb_analytics_west_01
FROM INTEGRATION '<integration-id>'
DATABASE 'salesdb';
```

The principal needs superuser privileges or both `CREATEDB` and `CREATEUSER`.
Then validate the selected tables:

```sql
SELECT * FROM SVV_INTEGRATION;
SELECT * FROM SVV_INTEGRATION_TABLE_STATE;
```

Each transition creates a new integration ID, so use a new versioned database
name each time (for example, `salesdb_analytics_east_02` after switching back).
The automation never drops Redshift databases; retain old ones until your
retention and rollback requirements allow deletion. A switchover and switch-back
rehearsal performs two full reseeds of the selected data.

## Failure recovery

The former integration points to the old primary and no longer follows the new
writer. If deletion succeeds but replacement creation fails, the function
raises the error for Lambda retry. Every retry re-reads the current topology. If
retries expire, the DLQ alarm fires. Correct the reported readiness,
authorization, KMS, or quota issue, then replay the DLQ event or invoke the
function again. Do not recreate an integration to the former primary as a
rollback.

## Queries during reseeding

This sample does not route analytics queries. Until the replacement integration
is `active` and the selected tables have completed synchronization:

- Pause freshness-sensitive reports and jobs, or route them through your own
  regional analytics failover mechanism.
- If the business permits querying previously replicated data, label results
  with the last successful replication timestamp and a stale-data warning.
- Resume normal processing only after `SVV_INTEGRATION` and
  `SVV_INTEGRATION_TABLE_STATE` confirm readiness.
- Keep the destination database name in client configuration or your routing
  layer, because each recovery uses a new versioned database name.

## Monitoring

The template creates alarms for Lambda errors and a non-empty DLQ. Because every
recreated integration has a new `IntegrationId`, the function itself calls
`PutMetricAlarm` after each create, `noop`, or race path to point two stable
alarms at the newest ID:

- `<IntegrationName>-zero-etl-integration-lag` (`IntegrationLag`)
- `<IntegrationName>-zero-etl-tables-failed`
  (`IntegrationNumTablesFailedReplication`)

Both use the `AWS/Redshift` namespace and treat missing data as not breaching.
CloudFormation doesn't own these two alarms; delete them separately.

There is no universal reseeding duration. It varies with selected data volume,
table count, source activity, and target capacity. Measure from integration
creation until the integration is active and selected tables are synchronized
using a rehearsal with production-representative data. Lambda duration is not
reseed duration because the function returns after `CreateIntegration` is
accepted.

## IAM scope

- `DescribeGlobalClusters` is scoped to the configured global-cluster ARN.
- `DescribeDBClusters` and `CreateIntegration` are scoped to
  `SourceClusterArns`.
- `DescribeIntegrations`, `DeleteIntegration`, and tagging use the
  `arn:aws:rds:*:<account>:integration:*` pattern because integration IDs are
  service generated and cleanup spans member Regions. Request-tag and
  resource-tag conditions limit creation and deletion to integrations owned by
  this automation.
- Redshift permissions are scoped to the local target ARN.
- Only `cloudwatch:PutMetricAlarm` and the X-Ray write APIs use
  `Resource: "*"`.

## Cost considerations

This design needs a Redshift target in every Region that can become primary.
Every recreated integration performs a full initial synchronization, which uses
source I/O and target compute. Measure synchronization time and cost in a
non-production rehearsal. See
[Amazon Aurora pricing](https://aws.amazon.com/rds/aurora/pricing/),
[Amazon Redshift pricing](https://aws.amazon.com/redshift/pricing/), and
[Amazon CloudWatch pricing](https://aws.amazon.com/cloudwatch/pricing/).

## Aurora MySQL

Apply a custom `aurora-mysql8.0` cluster parameter group to every member:

```text
aurora_enhanced_binlog=1
binlog_backup=0
binlog_format=ROW
binlog_replication_globaldb=0
binlog_row_image=full
binlog_row_metadata=full
```

Also make sure `binlog_transaction_compression` isn't `ON` and
`binlog_row_value_options` isn't `PARTIAL_JSON`. Aurora MySQL data filters use
`database.table` patterns.

## Clean up

```bash
for region in us-east-1 us-west-2; do
  aws cloudformation delete-stack --region "$region" \
    --stack-name zero-etl-globaldb-recovery
  aws cloudwatch delete-alarms --region "$region" --alarm-names \
    globaldb-analytics-zero-etl-integration-lag \
    globaldb-analytics-zero-etl-tables-failed
done
```

The stacks don't own the Aurora clusters, Redshift targets, zero-ETL
integrations, or destination databases. Delete test integrations and retained
destination databases separately, after your retention requirements allow it.

## Verify before committing

```bash
python3 -m py_compile lambda/zero_etl_recovery.py
python3 scripts/sync_lambda_into_template.py --check
python3 -m json.tool iam/lambda-least-privilege-policy.json > /dev/null
cfn-lint cloudformation/zero-etl-recovery.yaml
python3 -m checkov.main -f cloudformation/zero-etl-recovery.yaml --compact
```

The same checks, except `cfn-lint`, run in CI on every push and pull request.
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md) for security reporting guidance.

## License

Licensed under the MIT-0 License. See [LICENSE](LICENSE).
