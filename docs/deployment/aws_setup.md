# AWS Setup

## 1. S3 bucket

The project uses a single bucket:

```text
s3://binance-market-data-lake
```

S3 is the system-of-record landing zone. Databricks reads from it with `s3a://` URIs; local Python and Spark jobs read/write with boto3 and S3A.

### Prefix inventory

| Prefix | Writer | Notes |
|---|---|---|
| `landing/historical/klines/` | Historical batch | Hive-style `symbol/interval/year/month` partitions |
| `landing/rest/klines/` | REST batch | `symbol/interval/date` partitions |
| `landing/streaming/trades_recovery/` | Spark streaming | `symbol/event_date` partitions; active source for Bronze |
| `landing/streaming/trades/` | Legacy stream | Retained; not the active Bronze source |
| `quarantine/` | REST batch | Failed payloads |
| `metadata/audit/historical/` | Historical batch | Per-archive audit JSON |
| `metadata/ingestion_audit/` | REST batch | Per-symbol audit JSON |
| `metadata/checkpoints/rest/klines/` | REST batch | REST last-processed timestamps |
| `metadata/checkpoints/binance_trades_streaming_v2/` | Spark streaming | Spark commits/offsets |
| `metadata/databricks/schemas/` | Bronze Auto Loader | Schema inference state |
| `metadata/databricks/checkpoints/` | Bronze Auto Loader | Ingestion checkpoint state |

## 2. Required IAM permissions

Least-privilege_actions for the identities used by the pipeline:

| Area | Actions |
|---|---|
| Landing writes | `s3:PutObject`, `s3:AbortMultipartUpload`, `s3:ListMultipartUploadParts` on `landing/*`, `quarantine/*` |
| Read for ingestion | `s3:GetObject`, `s3:ListBucket` on landing prefixes |
| Checkpoints/state | `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on `metadata/*` |
| Monitoring | `s3:ListBucket`, `s3:GetObject` on streaming prefix and `_spark_metadata/` |
| Idempotency | `s3:HeadObject` (covered by `s3:GetObject` in most policies) |

Example scope for a Databricks instance profile (adjust to your account):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::binance-market-data-lake"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"],
      "Resource": ["arn:aws:s3:::binance-market-data-lake/*"]
    }
  ]
}
```

## 3. Credential wiring

| Consumer | Mechanism |
|---|---|
| Local Python scripts | Standard AWS credential provider chain (env vars or `~/.aws/credentials`); `S3_BUCKET` and `AWS_REGION` from `.env` |
| Spark containers | `${USERPROFILE}/.aws` mounted read-only at `/home/spark/.aws`; `DefaultAWSCredentialsProviderChain` set in `spark_to_s3_recovery.py` |
| Airflow containers | `${USERPROFILE}/.aws` mounted read-only at `/home/airflow/.aws`; boto3 uses default chain |
| Databricks | Prefer an instance profile / service principal with the policy above; avoid embedding keys in notebooks |

`spark_to_s3_recovery.py` also sets:

```python
fs.s3a.aws.credentials.provider = com.amazonaws.auth.DefaultAWSCredentialsProviderChain
fs.s3a.endpoint = s3.<AWS_REGION>.amazonaws.com
```

Never commit AWS keys. `credentials.txt`, `.env`, and `ai_serving/.env` are local-only files.

## 4. Bucket settings

Recommended:

- Enable **bucket versioning** before any destructive cleanup or output-prefix change. The recovery runbook notes versioning was not enabled at review time.
- Consider a lifecycle rule for `quarantine/` and old `_spark_metadata` compact files after retention requirements are met.
- Do not enable public access.

## 5. Region consistency warning

The repository contains inconsistent region settings:

| Location | Value |
|---|---|
| Root `.env` | `AWS_REGION=ap-south-1` |
| `airflow/.env` | `AWS_DEFAULT_REGION=ap-south-2` |
| `docker-compose.yml` `spark` service | Defaults to `ap-south-2` |
| `docker-compose.yml` `spark-streaming` service | Defaults to `ap-south-1` and hardcodes `s3.ap-south-1.amazonaws.com` in the non-recovery Spark script |
| `src/pipeline/rest_pipeline.py` | Hardcodes `ap-south-2` |
| `src/historical/run_historical_batch.py` | Defaults to `ap-south-2` |
| `src/historical/run_historical_ingestion.py` | Defaults to `ap-south-1` |
| `src/streaming/spark_to_s3_recovery.py` | Defaults to `ap-south-1` |

Actions before production use:

1. Pick the actual bucket region.
2. Set that region consistently in root `.env`, `airflow/.env`, and compose environment variables.
3. Update hard-coded defaults in `rest_pipeline.py` and the historical entry points to read from env.
4. Verify the S3A endpoint in the Spark job matches the bucket region.

## 6. Verification commands

```powershell
# Bucket exists and is reachable
aws s3 ls s3://binance-market-data-lake/

# Landing prefixes
aws s3 ls s3://binance-market-data-lake/landing/ --recursive | Select-Object -First 20

# Streaming checkpoint
aws s3 ls s3://binance-market-data-lake/metadata/checkpoints/binance_trades_streaming_v2/ --recursive

# Versioning status
aws s3api get-bucket-versioning --bucket binance-market-data-lake
```

## 7. Related documents

- [Airflow Setup](./airflow_setup.md)
- [Databricks Setup](./databricks_setup.md)
- [Ingestion](../pipeline/ingestion.md)
- [Streaming](../pipeline/streaming.md)
