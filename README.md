# parquet2hive
Hive import statement generator for Parquet datasets.

## Installation
```bash
pip install parquet2hive
```

## Example usage
```bash
➜  ~  parquet2hive -d s3://telemetry-parquet/churn/telemetry-2/
drop table if exists churn; create external table churn(clientId string, sampleId int, channel string, normalizedChannel string, country string, profileCreationDate int, submissionDate string, syncConfigured boolean, syncCountDesktop int, syncCountMobile int, version string, timestamp bigint) partitioned by (submission_date_s3 string) stored as parquet location 's3://telemetry-parquet/churn/telemetry-2/'; msck repair table churn;
```
