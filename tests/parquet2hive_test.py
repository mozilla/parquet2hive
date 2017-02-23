from time import sleep

import boto3
from moto import mock_s3
from parquet2hive_modules import parquet2hivelib as lib
import pytest


def _setup_module():
    global s3
    global bucket_name
    global s3_client
    global bucket
    global dataset_file
    global new_dataset_file
    global complex_file

    s3 = boto3.resource('s3')
    bucket_name = 'test-bucket'
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)
    bucket = s3.Bucket(bucket_name)
    dataset_file = 'tests/dataset.parquet'
    new_dataset_file = 'tests/dataset-new.parquet'
    complex_file = 'tests/complex.parquet'
    lib.check_success_exists.cache_clear()


class TestLoadBucket(object):

    @mock_s3
    def test_load_prefix_no_prefix(self):
        _setup_module()

        objects = ['churn/v1/parquet', 'frank/v1/parquet']
        for o in objects:
            s3_client.put_object(Bucket=bucket_name, Key=o, Body=open(dataset_file, 'rb'))

        bash_cmd = lib.load_prefix('s3://' + bucket_name)

        assert 'drop table if exists churn' in bash_cmd
        assert 'create external table churn' in bash_cmd
        assert 'create external table churn_v1' in bash_cmd
        assert 'drop table if exists frank' in bash_cmd
        assert 'create external table frank' in bash_cmd
        assert 'create external table frank_v1' in bash_cmd

    @mock_s3
    def test_load_prefix_incorrect_layout(self):
        _setup_module()

        objects = ['temp/churn/v1/parquet', 'temp/frank/v1/partquet']
        for o in objects:
            s3_client.put_object(Bucket=bucket_name, Key=o, Body=open(dataset_file, 'rb'))

        bash_cmd = lib.load_prefix('s3://' + bucket_name)
        assert not bash_cmd

    @mock_s3
    def test_load_prefix_with_prefix(self):
        _setup_module()

        objects = ['temp/churn/v1/parquet', 'temp/frank/v1/parquet']
        for o in objects:
            s3_client.put_object(Bucket=bucket_name, Key=o, Body=open(dataset_file, 'rb'))

        bash_cmd = lib.load_prefix('s3://{}/{}'.format(bucket_name, 'temp'))

        assert 'drop table if exists churn' in bash_cmd
        assert 'create external table churn' in bash_cmd
        assert 'create external table churn_v1' in bash_cmd
        assert 'drop table if exists frank' in bash_cmd
        assert 'create external table frank' in bash_cmd
        assert 'create external table frank_v1' in bash_cmd

    @mock_s3
    def test_load_prefix_ignore_dir(self):
        _setup_module()

        objects = ['temp/churn/v1/parquet', 'temp/frank/v1/parquet', 'tester/v1/parquet']
        for o in objects:
            s3_client.put_object(Bucket=bucket_name, Key=o, Body=open(dataset_file, 'rb'))

        bash_cmd = lib.load_prefix('s3://{}/{}'.format(bucket_name, 'temp'))
        assert 'drop table if exists tester' not in bash_cmd
        assert 'create external table churn' in bash_cmd
        assert 'create external table frank' in bash_cmd


class TestGetBashCmd(object):

    @mock_s3
    def test_with_single_file(self):
        _setup_module()

        prefix, version, objectname = 'churn', 'v2', 'parquet'
        s3_client.put_object(Bucket=bucket_name, Key='/'.join((prefix, version, objectname)), Body=open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset)

        assert bash_cmd.startswith('hive'), 'Should be a valid hive command'
        assert 'drop table if exists churn' in bash_cmd, 'Should drop table without version'
        assert 'create external table churn' in bash_cmd, 'Should create table without version'
        assert 'drop table if exists churn_v2' in bash_cmd, 'Should drop table with version'
        assert 'create external table churn_v2' in bash_cmd, 'Should create table with version'

    @mock_s3
    def test_with_single_file_end_in_slash(self):
        _setup_module()

        prefix, version, objectname = 'churn', 'v2', 'parquet'
        s3_client.put_object(Bucket=bucket_name, Key='/'.join((prefix, version, objectname)), Body=open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset + '/')

        assert bash_cmd.startswith('hive'), 'Should be a valid hive command'

    @mock_s3
    def test_dataset_version(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, version='v2')

        assert 'v3' not in bash_cmd, 'Should only process v2, but found v3'
        assert 'v2' in bash_cmd, 'Should process v2, but didn\'t'

    @mock_s3
    def test_version_order(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset)

        assert bash_cmd.find('v3') < bash_cmd.find('v2'), 'v3 should process before v2, but didn\'t'

    @mock_s3
    def test_success_only(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=open(dataset_file, 'rb'))
        s3_client.put_object(Bucket=bucket_name, Key='/'.join((prefix, 'v3', '_SUCCESS')), Body=b'SUCCESS')

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, success_only=True)

        assert 'v3' in bash_cmd, 'Should process v3 but didn\'t'
        assert 'v2' not in bash_cmd, 'Should not process v2 since _SUCCESS is missing, but did'

    @mock_s3
    def test_use_last_version(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, recent_versions=1)

        assert 'v3' in bash_cmd, 'Should process v3, but didn\'t'
        assert 'v2' not in bash_cmd, 'Should only process v3 and not v2, but didn\'t'

    @mock_s3
    def test_use_last_version_success_only(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=open(dataset_file, 'rb'))
        s3_client.put_object(Bucket=bucket_name, Key='/'.join((prefix, 'v2', '_SUCCESS')), Body=b'SUCCESS')

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, recent_versions=1, success_only=True)

        assert 'v2' in bash_cmd, 'Should process v2, but didn\'t'
        assert 'v3' not in bash_cmd, 'Should only process v2 and not v3, but didn\'t'

    @mock_s3
    def test_use_last_versions(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v1', 'v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, recent_versions=2)

        assert 'v3' in bash_cmd, 'Should process v3, but didn\'t'
        assert 'v2' in bash_cmd, 'Should process v2, but didn\'t'
        assert 'v1' not in bash_cmd, 'Should only process v3 and v2 and not v1, but didn\'t'

    @mock_s3
    def test_use_last_versions_success_only(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v1', 'v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=open(dataset_file, 'rb'))

        for v in versions[:2]:
            s3_client.put_object(Bucket=bucket_name, Key='/'.join((prefix, v, '_SUCCESS')), Body=b'SUCCESS')

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, recent_versions=2, success_only=True)

        assert 'v1' in bash_cmd, 'Should process v1, but didn\'t'
        assert 'v2' in bash_cmd, 'Should process v2, but didn\'t'
        assert bash_cmd.find('churn_v2;') < bash_cmd.find('churn;') < bash_cmd.find('churn_v1;'), 'Should process v2 as both churn_v2 and churn before processing v1'
        assert 'v3' not in bash_cmd, 'Should only process v1 and v2 and not v3, but didn\'t'

    @mock_s3
    def test_use_most_recent_file(self):
        _setup_module()

        # new_dataset_file has column 'id', dataset_file does not
        prefix, version, objects = 'churn', 'v1', ['dataset_file', 'new_dataset_file']
        filenames = {'dataset_file': dataset_file, 'new_dataset_file': new_dataset_file}
        for _object in objects:
            key = '/'.join((prefix, version, _object))
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=open(filenames[_object], 'rb'))
            sleep(0.1)

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset)

        assert '`id`' in bash_cmd, 'Column from newer file should be in schema, but is not'
        assert '`country`' not in bash_cmd, 'Column from older file should not be in schema'

    @mock_s3
    def test_nested_dataset(self):
        _setup_module()

        prefix, version, objects = 'prod/churn', 'v1', ['dataset_file']
        filenames = {'dataset_file': dataset_file, 'new_dataset_file': new_dataset_file}
        for _object in objects:
            key = '/'.join((prefix, version, _object))
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=open(filenames[_object], 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset)

        assert 'table prod/churn' not in bash_cmd
        assert 'table churn' in bash_cmd

    @mock_s3
    def test_alias(self):
        _setup_module()

        prefix, versions, _object = 'churn', ['v1', 'v2'], 'dataset_file'
        for v in versions:
            key = '/'.join((prefix, v, _object))
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, alias='burn')

        assert 'table churn' not in bash_cmd
        assert 'table burn_v1' in bash_cmd
        assert 'table burn_v2' in bash_cmd
        assert 'table burn' in bash_cmd

    @mock_s3
    def test_regex_exclude(self):
        _setup_module()

        prefix, version_objects = 'churn', ['v1/file', 'v2/DEV_something']
        for _object in version_objects:
            key = '/'.join((prefix, _object))
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, exclude_regex=['.*DEV.*'])

        assert 'table churn' in bash_cmd
        assert 'table churn_v1' in bash_cmd
        assert 'table churn_v2' not in bash_cmd

    @mock_s3
    def test_regex_exclude_all(self):
        _setup_module()

        prefix, version_objects = 'churn', ['v1/file', 'v2/DEV_something']
        for _object in version_objects:
            key = '/'.join((prefix, _object))
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, exclude_regex=['.*'])

        assert not bash_cmd


class TestGetVersions(object):

    @mock_s3
    def test_incorrect_version(self):
        _setup_module()

        prefix = 'prefix'
        version = '24'
        keys = ['/'.join((prefix, version, key)) for key in ('p1', 'p2', 'p3')]
        for k in keys:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=b'teststring')

        assert lib.get_versions(bucket, prefix) == [], 'incorrect version should have been ignored'

    @mock_s3
    def test_missing_prefix(self):
        _setup_module()

        prefix = 'v1'
        keys = ['/'.join((prefix, key)) for key in ('p1', 'p2', 'p3')]
        for k in keys:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=b'teststring')

        assert lib.get_versions(bucket, prefix) == [], 'missing prefix should have been ignored'

    @mock_s3
    def test_in_order(self):
        _setup_module()

        prefix = 'prefix'
        versions = ['v1', 'v2']
        keys = ['/'.join((prefix, version, key)) for version in versions for key in ('p1', 'p2', 'p3')]
        for k in keys:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=b'teststring')

        assert lib.get_versions(bucket, prefix) == ['v2', 'v1'], 'versions not returned in descending order'

    @mock_s3
    def test_ignore_nested_dataset(self):
        _setup_module()

        prefix, version, objects = 'prod/churn', 'v1', ['dataset_file']
        for _object in objects:
            key = '/'.join((prefix, version, _object))
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=open(dataset_file, 'rb'))

        assert lib.get_versions(bucket, 'prod') == [], 'Should ignore nested dataset that is not explicitly identified'


class TestSuccessExists(object):

    @mock_s3
    def test_exists_within_partition(self):
        _setup_module()

        prefix = 'longitudinal'
        version = 'v1'
        partition = 'sample_id=1'
        keys = ['/'.join((prefix, version, partition, fname)) for fname in ('p1', 'p2', '_SUCCESS')]
        for k in keys:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=b'teststring')

        assert lib.check_success_exists(s3, bucket_name, '/'.join([prefix, version, partition])), '_SUCCESS not found in partition when it exists'

    @mock_s3
    def test_exists_within_version(self):
        _setup_module()

        prefix = 'longitudinal'
        version = 'v1'
        keys = ['/'.join((prefix, version, fname)) for fname in ('p1', 'p2', '_SUCCESS')]
        for k in keys:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=b'teststring')

        assert lib.check_success_exists(s3, bucket_name, '/'.join([prefix, version])), '_SUCCESS not found in dir when it exists'

    @mock_s3
    def test_missing_from_partition(self):
        _setup_module()

        prefix = 'longitudinal'
        version = 'v1'
        partition = 'sample_id=1'
        keys = ['/'.join((prefix, version, partition, fname)) for fname in ('p1', 'p2', 'p3')]
        for k in keys:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=b'teststring')

        assert not lib.check_success_exists(s3, bucket_name, '/'.join([prefix, version, partition])), '_SUCCESS found when actually missing from partition'

    @mock_s3
    def test_missing_from_version(self):
        _setup_module()

        prefix = 'longitudinal'
        version = 'v1'
        keys = ['/'.join((prefix, version, fname)) for fname in ('p1', 'p2', 'p3')]
        for k in keys:
            s3_client.put_object(Bucket=bucket_name, Key=k, Body=b'teststring')

        assert not lib.check_success_exists(s3, bucket_name, '/'.join([prefix, version])), '_SUCCESS found when actually missing from directory'


class TestIgnoreKey(object):

    def test_ignore_real_file_temp_dir(self):
        assert lib.ignore_key('mobile/android_events/v1/channel=aurora/submission=20160919/_temporary/0_$folder$'), "Did not ignore temporary dir file"

    def test_ignore_metadata_file(self):
        assert lib.ignore_key('mobile/android_events/v1/channel=aurora/submission=20160919_$folder$'), "Did not ignore metadata file"

    def test_no_ignore(self):
        assert not lib.ignore_key('directory1/directory2/partition=1/file'), "Ignored correct directory"

    def test_ignore_temp_dir(self):
        assert lib.ignore_key('directory1/directory2/partition=1/_temp/file'), "Did not ignore temporary directory"

    def test_ignore_temp_file(self):
        assert lib.ignore_key('directory1/directory2/partition=2/_tempfile'), "Did not ignore temporary file"

    def test_ignore_dir(self):
        assert lib.ignore_key('directory1/directory2/partition=1/'), "Did not ignore directory"


class TestGetPartitioningFields(object):

    def test_finds_partitions(self):
        assert lib.get_partitioning_fields('sample_id=1/test_id=3/obj') == ['sample_id', 'test_id']


DATASET_TREE = [
    ('optional', 'byte_array', 'clientId', 'utf8'),
    ('optional', 'int32', 'sampleId', None),
    ('optional', 'byte_array', 'channel', 'utf8'),
    ('optional', 'byte_array', 'normalizedChannel', 'utf8'),
    ('optional', 'byte_array', 'country', 'utf8'),
    ('optional', 'int32', 'profileCreationDate', None),
    ('optional', 'byte_array', 'subsessionStartDate', 'utf8'),
    ('optional', 'int32', 'subsessionLength', None),
    ('optional', 'byte_array', 'distributionId', 'utf8'),
    ('optional', 'byte_array', 'submissionDate', 'utf8'),
    ('optional', 'boolean', 'syncConfigured', None),
    ('optional', 'int32', 'syncCountDesktop', None),
    ('optional', 'int32', 'syncCountMobile', None),
    ('optional', 'byte_array', 'version', 'utf8'),
    ('optional', 'int64', 'timestamp', None),
    ('optional', 'boolean', 'e10sEnabled', None),
    ('optional', 'byte_array', 'e10sCohort', 'utf8'),
]

NEW_DATASET_TREE = [
    ('optional', 'int64', 'id', None),
]


class TestBuildTree(object):

    @mock_s3
    def test_dataset_schema(self):
        _setup_module()

        obj = bucket.Object('my/dataset')
        with open(dataset_file, 'rb') as fileobj:
            obj.upload_fileobj(fileobj)

        schema = lib.read_schema(obj)
        assert lib.build_tree(schema[1:], schema[0].num_children) == DATASET_TREE

    @mock_s3
    def test_new_dataset_schema(self):
        _setup_module()

        obj = bucket.Object('my/new-dataset')
        with open(new_dataset_file, 'rb') as fileobj:
            obj.upload_fileobj(fileobj)

        schema = lib.read_schema(obj)
        assert lib.build_tree(schema[1:], schema[0].num_children) == NEW_DATASET_TREE


DATASET_SQL = "drop table if exists dataset_table; " \
            + "create external table dataset_table(" \
                + "`clientId` string, " \
                + "`sampleId` int, " \
                + "`channel` string, " \
                + "`normalizedChannel` string, " \
                + "`country` string, " \
                + "`profileCreationDate` int, " \
                + "`subsessionStartDate` string, " \
                + "`subsessionLength` int, " \
                + "`distributionId` string, " \
                + "`submissionDate` string, " \
                + "`syncConfigured` boolean, " \
                + "`syncCountDesktop` int, " \
                + "`syncCountMobile` int, " \
                + "`version` string, " \
                + "`timestamp` bigint, " \
                + "`e10sEnabled` boolean, " \
                + "`e10sCohort` string" \
            + ") stored as parquet location '\"'s3://test-bucket/dataset.parquet'\"'; " \
            + "msck repair table dataset_table;"

NEW_DATASET_SQL = "drop table if exists new_dataset_table; " \
                + "create external table new_dataset_table(" \
                    + "`id` bigint" \
                + ") stored as parquet location '\"'s3://test-bucket/new-dataset.parquet'\"'; " \
                + "msck repair table new_dataset_table;"

COMPLEX_SQL = "drop table if exists complex_table; " \
            + "create external table complex_table(" \
                + "`application` struct<" \
                    + "`addons`: struct<" \
                        + "`active_addons`: map<string,string>, " \
                        + "`active_experiment`: struct<`id`: string, `branch`: string>, " \
                        + "`active_gmplugins`: map<string,string>, " \
                        + "`active_plugins`: string, " \
                        + "`persona`: string, " \
                        + "`theme`: struct<" \
                            + "`id`: string, " \
                            + "`blocklisted`: boolean, " \
                            + "`description`: string, " \
                            + "`name`: string, " \
                            + "`user_disabled`: boolean, " \
                            + "`app_disabled`: boolean, " \
                            + "`version`: string, " \
                            + "`scope`: bigint, " \
                            + "`foreign_install`: string, " \
                            + "`has_binary_components`: boolean, " \
                            + "`install_day`: string, " \
                            + "`update_day`: bigint" \
                        + ">" \
                    + ">, " \
                    + "`architecture`: string, " \
                    + "`build_id`: string, " \
                    + "`channel`: string, " \
                    + "`name`: string, " \
                    + "`platform_version`: string, " \
                    + "`version`: string" \
                + ">, " \
                + "`client_id` string, " \
                + "`creation_date` string, " \
                + "`environment` struct<" \
                    + "`system`: struct<" \
                        + "`os`: struct<" \
                            + "`name`: string, " \
                            + "`version`: string, " \
                            + "`locale`: string" \
                        + ">" \
                    + ">, " \
                    + "`profile`: struct<" \
                        + "`creation_date`: bigint, " \
                        + "`reset_date`: bigint" \
                    + ">, " \
                    + "`settings`: struct<" \
                        + "`blocklist_enabled`: boolean, " \
                        + "`is_default_browser`: boolean, " \
                        + "`default_search_engine`: string, " \
                        + "`default_search_engine_data`: struct<" \
                            + "`name`: string, " \
                            + "`load_path`: string, " \
                            + "`submission_url`: string, " \
                            + "`origin`: string" \
                        + ">, " \
                        + "`e10s_enabled`: boolean, " \
                        + "`e10s_cohort`: string, " \
                        + "`locale`: string, " \
                        + "`telemetry_enabled`: boolean, " \
                        + "`update`: struct<" \
                            + "`auto_download`: boolean, " \
                            + "`channel`: string, " \
                            + "`enabled`: boolean" \
                        + ">" \
                    + ">" \
                + ">, " \
                + "`id` string, " \
                + "`type` string, " \
                + "`version` double, " \
                + "`payload` struct<" \
                    + "`version`: bigint, " \
                    + "`study_name`: string, " \
                    + "`branch`: string, " \
                    + "`addon_version`: string, " \
                    + "`shield_version`: string, " \
                    + "`testing`: boolean, " \
                    + "`data`: struct<" \
                        + "`study_state`: string, " \
                        + "`study_state_fullname`: string, " \
                        + "`attributes`: map<string,string>" \
                    + ">, " \
                    + "`type`: string" \
                + ">, " \
                + "`metadata` struct<" \
                    + "`timestamp`: bigint, " \
                    + "`submission_date`: string, " \
                    + "`date`: string, " \
                    + "`normalized_channel`: string, " \
                    + "`geo_country`: string, " \
                    + "`geo_city`: string" \
                + ">" \
            + ") stored as parquet location '\"'s3://test-bucket/complex.parquet'\"'; " \
            + "msck repair table complex_table;"


class TestParquet2Sql(object):

    @mock_s3
    def test_dataset(self):
        _setup_module()

        obj = bucket.Object('my/dataset')
        with open(dataset_file, 'rb') as fileobj:
            obj.upload_fileobj(fileobj)

        schema = lib.read_schema(obj)
        assert lib.parquet2sql(schema, 'dataset_table', 's3://test-bucket/dataset.parquet', []) == DATASET_SQL

    @mock_s3
    def test_new_dataset(self):
        _setup_module()

        obj = bucket.Object('my/new-dataset')
        with open(new_dataset_file, 'rb') as fileobj:
            obj.upload_fileobj(fileobj)

        schema = lib.read_schema(obj)
        assert lib.parquet2sql(schema, 'new_dataset_table', 's3://test-bucket/new-dataset.parquet', []) == NEW_DATASET_SQL

    @mock_s3
    def test_complex(self):
        _setup_module()

        obj = bucket.Object('my/complex')
        with open(complex_file, 'rb') as fileobj:
            obj.upload_fileobj(fileobj)

        schema = lib.read_schema(obj)
        assert lib.parquet2sql(schema, 'complex_table', 's3://test-bucket/complex.parquet', []) == COMPLEX_SQL


class TestSqlType(object):

    def test_list(self):
        fields = [
            ('required', 'group', 'my_list', 'list', [
                ('repeated', 'group', 'list', None, [
                    ('optional', 'byte_array', 'element', 'utf8'),
                ])
            ]),
            ('optional', 'group', 'my_list', 'list', [
                ('repeated', 'group', 'list', None, [
                    ('required', 'byte_array', 'element', 'utf8'),
                ]),
            ]),
            ('optional', 'group', 'array_of_arrays', 'list', [
                ('repeated', 'group', 'list', None, [
                    ('required', 'group', 'element', 'list', [
                        ('repeated', 'group', 'list', None, [
                            ('required', 'int32', 'element', None),
                        ]),
                    ]),
                ]),
            ]),
            ('optional', 'group', 'my_list', 'list', [
                ('repeated', 'group', 'element', None, [
                    ('required', 'byte_array', 'str', 'utf8'),
                ]),
            ]),
            ('optional', 'group', 'my_list', 'list', [
                ('repeated', 'int32', 'element', None),
            ]),
            ('optional', 'group', 'my_list', 'list', [
                ('repeated', 'group', 'element', None, [
                    ('required', 'byte_array', 'str', 'utf8'),
                    ('required', 'int32', 'num', None),
                ]),
            ]),
            ('optional', 'group', 'my_list', 'list', [
                ('repeated', 'group', 'array', None, [
                    ('required', 'byte_array', 'str', 'utf8'),
                ])
            ]),
            ('optional', 'group', 'my_list', 'list', [
                ('repeated', 'group', 'my_list_tuple', None, [
                    ('required', 'byte_array', 'str', 'utf8'),
                ])
            ]),
            ('repeated', 'int32', 'num', None),
        ]

        assert lib.sql_type(fields[0]) == 'array<string>'
        assert lib.sql_type(fields[1]) == 'array<string>'
        assert lib.sql_type(fields[2]) == 'array<array<int>>'
        assert lib.sql_type(fields[3]) == 'array<string>'
        assert lib.sql_type(fields[4]) == 'array<int>'
        assert lib.sql_type(fields[5]) == 'array<struct<`str`: string, `num`: int>>'
        assert lib.sql_type(fields[6]) == 'array<struct<`str`: string>>'
        assert lib.sql_type(fields[7]) == 'array<struct<`str`: string>>'
        assert lib.sql_type(fields[8]) == 'array<int>'

    def test_map(self):
        fields = [
            ('required', 'group', 'my_map', 'map', [
                ('repeated', 'group', 'key_value', None, [
                    ('required', 'byte_array', 'key', 'utf8'),
                    ('optional', 'int32', 'value', None),
                ]),
            ]),
            ('optional', 'group', 'my_map', 'map', [
                ('repeated', 'group', 'map', None, [
                    ('required', 'byte_array', 'str', 'utf8'),
                    ('required', 'int32', 'num', None),
                ]),
            ]),
            ('optional', 'group', 'my_map', 'map_key_value', [
                ('repeated', 'group', 'map', None, [
                    ('required', 'byte_array', 'key', 'utf8'),
                    ('optional', 'int32', 'value', None),
                ]),
            ]),
        ]

        assert lib.sql_type(fields[0]) == 'map<string,int>'
        assert lib.sql_type(fields[1]) == 'map<string,int>'
        assert lib.sql_type(fields[2]) == 'map<string,int>'

    def test_complex(self):
        fields = [
            ('optional', 'group', 'fx_startup_migration_data_recency', 'map', [
                ('repeated', 'group', 'map', 'map_key_value', [
                    ('required', 'byte_array', 'key', 'utf8'),
                    ('required', 'group', 'value', 'list', [
                        ('repeated', 'group', 'array', None, [
                            ('required', 'group', 'values', 'list', [
                                ('repeated', 'int32', 'array', None),
                            ]),
                            ('required', 'int64', 'sum', None),
                        ]),
                    ]),
                ]),
            ]),
            ('optional', 'group', 'fx_migration_entry_point', 'list', [
                ('repeated', 'group', 'array', 'list', [
                    ('repeated', 'int32', 'array', None),
                ]),
            ]),
            ('optional', 'group', 'default_search_engine_data', None, [
                ('optional', 'byte_array', 'name', 'utf8'),
                ('optional', 'byte_array', 'load_path', 'utf8'),
                ('optional', 'byte_array', 'submission_url', 'utf8'),
            ]),
            ('optional', 'group', 'active_addons', 'list', [
                ('repeated', 'group', 'array', 'map', [
                    ('repeated', 'group', 'map', 'map_key_value', [
                        ('required', 'byte_array', 'key', 'utf8'),
                        ('required', 'group', 'value', None, [
                            ('optional', 'boolean', 'blocklisted', None),
                            ('optional', 'byte_array', 'description', 'utf8'),
                            ('optional', 'byte_array', 'name', 'utf8'),
                            ('optional', 'boolean', 'user_disabled', None),
                            ('optional', 'boolean', 'app_disabled', None),
                            ('optional', 'byte_array', 'version', 'utf8'),
                            ('optional', 'int32', 'scope', None),
                            ('optional', 'byte_array', 'type', 'utf8'),
                            ('optional', 'boolean', 'foreign_install', None),
                            ('optional', 'boolean', 'has_byte_array_components', None),
                            ('optional', 'int64', 'install_day', None),
                            ('optional', 'int64', 'update_day', None),
                            ('optional', 'int32', 'signed_state', None),
                            ('optional', 'boolean', 'is_system', None),
                        ]),
                    ]),
                ]),
            ]),
        ]

        assert lib.sql_type(fields[0]) == 'map<string,array<struct<`values`: array<int>, `sum`: bigint>>>'
        assert lib.sql_type(fields[1]) == 'array<array<int>>'
        assert lib.sql_type(fields[2]) == 'struct<`name`: string, `load_path`: string, `submission_url`: string>'
        assert lib.sql_type(fields[3]) == 'array<map<string,struct<`blocklisted`: boolean, `description`: string, `name`: string, `user_disabled`: boolean, `app_disabled`: boolean, `version`: string, `scope`: int, `type`: string, `foreign_install`: boolean, `has_byte_array_components`: boolean, `install_day`: bigint, `update_day`: bigint, `signed_state`: int, `is_system`: boolean>>>'



class TestReadSchema(object):

    @mock_s3
    def test_fail_on_bad_magic_number(self):
        _setup_module()

        obj = bucket.Object('not-parquet')
        obj.put(Body=b'dootdoot\x04\x00\x00\x00FAIL')

        with pytest.raises(lib.ParquetFormatError) as exc:
            lib.read_schema(obj)
        assert 'magic number is invalid' in str(exc.value)

    @mock_s3
    def test_fail_on_too_small(self):
        _setup_module()

        obj = bucket.Object('not-parquet')

        obj.put(Body=b'\x00\x00PAR1')
        with pytest.raises(lib.ParquetFormatError) as exc:
            lib.read_schema(obj)
        assert 'file is too small' in str(exc.value)

        obj.put(Body=b'doo\x04\x00\x00\x00PAR1')
        with pytest.raises(lib.ParquetFormatError) as exc:
            lib.read_schema(obj)
        assert 'file is too small' in str(exc.value)
