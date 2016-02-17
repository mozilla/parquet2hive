# parquet2hive
Hive import statement generator for Parquet datasets.

## Installation
```bash
pip install parquet2hive
```

## Example usage
```bash
➜  ~ parquet2hive -d s3://telemetry-parquet/executive_stream
Analyzing dataset executive_stream, v3
drop table if exists executive_stream_v3; create external table executive_stream_v3(docType string, submissionDate string, activityTimestamp double, profileCreationTimestamp double, clientId string, documentId string, country string, channel string, os string, osVersion string, default boolean, buildId string, app string, version string, vendor string, reason string, hours double, google int, yahoo int, bing int, other int, pluginHangs int) partitioned by (submission_date_s3 string, channel_s3 string) stored as parquet location 's3://telemetry-parquet/executive_stream/v3'; msck repair table executive_stream;

parquet2hive -d s3://telemetry-parquet/executive_stream | xargs -0 hive -e
Analyzing dataset executive_stream, v3

```
