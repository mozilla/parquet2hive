import boto3
from moto import mock_s3
from parquet2hive_modules import parquet2hivelib as lib
from time import sleep

def _setup_module():
    global s3
    global bucket_name
    global s3_client
    global bucket
    global dataset_file
    global new_dataset_file

    s3 = boto3.resource('s3')
    bucket_name = 'test-bucket'
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket = bucket_name)
    bucket = s3.Bucket(bucket_name)
    dataset_file = 'tests/dataset.parquet'
    new_dataset_file = 'tests/dataset-new.parquet'

class TestGetBashCmd:

    @mock_s3
    def test_with_single_file(self):
        _setup_module()
        
        prefix, version, objectname = 'churn', 'v2', 'parquet'
        s3_client.put_object(Bucket = bucket_name, Key = '/'.join((prefix, version, objectname)), Body = open(dataset_file, 'rb'))
        
        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset)

        assert bash_cmd.startswith('hive'), 'Should be a valid hive command'
        assert 'drop table if exists churn' in bash_cmd, 'Should drop table without version'
        assert 'create external table churn' in bash_cmd, 'Should create table without version' 
        assert 'drop table if exists churn_v2' in bash_cmd, 'Should drop table with version'
        assert 'create external table churn_v2' in bash_cmd, 'Should create table with version'

    @mock_s3
    def test_dataset_version(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, version='v2')

        assert 'v3' not in bash_cmd, 'Should only process v2, but found v3'
        assert 'v2' in bash_cmd, 'Should process v2, but didn\'t'


    @mock_s3
    def test_version_order(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset)

        assert bash_cmd.find('v3') < bash_cmd.find('v2'), 'v3 should process before v2, but didn\'t'


    @mock_s3
    def test_success_only(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = open(dataset_file, 'rb'))
        s3_client.put_object(Bucket = bucket_name, Key = '/'.join((prefix, 'v3', '_SUCCESS')), Body = b'SUCCESS')

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, success_only = True)

        assert 'v3' in bash_cmd, 'Should process v3 but didn\'t'
        assert 'v2' not in bash_cmd, 'Should not process v2 since _SUCCESS is missing, but did'

    @mock_s3
    def test_use_last_version(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, recent_versions = 1)

        assert 'v3' in bash_cmd, 'Should process v3, but didn\'t'
        assert 'v2' not in bash_cmd, 'Should only process v3 and not v2, but didn\'t'

    @mock_s3
    def test_use_last_version_success_only(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = open(dataset_file, 'rb'))
        s3_client.put_object(Bucket = bucket_name, Key = '/'.join((prefix, 'v2', '_SUCCESS')), Body = b'SUCCESS')

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, recent_versions = 1, success_only = True)

        assert 'v2' in bash_cmd, 'Should process v2, but didn\'t'
        assert 'v3' not in bash_cmd, 'Should only process v2 and not v3, but didn\'t'

    @mock_s3
    def test_use_last_versions(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v1', 'v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = open(dataset_file, 'rb'))

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, recent_versions = 2)

        assert 'v3' in bash_cmd, 'Should process v3, but didn\'t'
        assert 'v2' in bash_cmd, 'Should process v2, but didn\'t'
        assert 'v1' not in bash_cmd, 'Should only process v3 and v2 and not v1, but didn\'t'

    @mock_s3
    def test_use_last_versions_success_only(self):
        _setup_module()

        prefix, versions, objectname = 'churn', ['v1', 'v2', 'v3'], 'parquet'
        for k in ['/'.join((prefix, v, objectname)) for v in versions]:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = open(dataset_file, 'rb'))

        for v in versions[:2]:
            s3_client.put_object(Bucket = bucket_name, Key = '/'.join((prefix, v, '_SUCCESS')), Body = b'SUCCESS')

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset, recent_versions = 2, success_only = True)

        assert 'v1' in bash_cmd, 'Should process v1, but didn\'t'
        assert 'v2' in bash_cmd, 'Should process v2, but didn\'t'
        assert bash_cmd.find('churn_v2;') < bash_cmd.find('churn;') < bash_cmd.find('churn_v1;'), 'Should process v2 as both churn_v2 and churn before processing v1'
        assert 'v3' not in bash_cmd, 'Should only process v1 and v2 and not v3, but didn\'t'

    @mock_s3
    def test_use_most_recent_file(self):
        _setup_module()

        #new_dataset_file has column 'id', dataset_file does not
        prefix, version, objects = 'churn', 'v1', ['dataset_file', 'new_dataset_file']
        filenames = {'dataset_file' : dataset_file, 'new_dataset_file' : new_dataset_file}
        for _object in objects:
            key = '/'.join((prefix, version, _object))
            s3_client.put_object(Bucket = bucket_name, Key = key, Body = open(filenames[_object], 'rb'))
            sleep(0.1)

        dataset = 's3://' + '/'.join((bucket_name, prefix))
        bash_cmd = lib.get_bash_cmd(dataset)

        assert '`id`' in bash_cmd, 'Column from newer file should be in schema, but is not'
        assert '`country`' not in bash_cmd, 'Column from older file should not be in schema'

        


class TestGetVersions:

    @mock_s3
    def test_incorrect_version(self):
        _setup_module()
        
        prefix = 'prefix'
        version = '24'
        keys = [ '/'.join((prefix, version, key)) for key in ('p1', 'p2', 'p3')]
        for k in keys:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = b'teststring')

        assert lib.get_versions(bucket, prefix) == [], 'incorrect version should have been ignored'

    @mock_s3
    def test_missing_prefix(self):
        _setup_module()
      
        prefix = 'v1'
        keys = [ '/'.join((prefix, key)) for key in ('p1', 'p2', 'p3')]
        for k in keys:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = b'teststring')

        assert lib.get_versions(bucket, prefix) == [], 'missing prefix should have been ignored'

    @mock_s3
    def test_in_order(self):
        _setup_module()

        prefix = 'prefix'
        versions = ['v1', 'v2'] 
        keys = [ '/'.join((prefix, version, key)) for version in versions for key in ('p1', 'p2', 'p3')]
        for k in keys:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = b'teststring')

        assert [v[2] for v in lib.get_versions(bucket, prefix)] == ['v2', 'v1'], 'versions not returned in descending order'

class TestSuccessExists:

    @mock_s3
    def test_exists_within_partition(self):
        _setup_module()

        prefix = 'longitudinal'
        version = 'v1'
        partition = 'sample_id=1'
        keys = [ '/'.join((prefix, version, partition, fname))  for fname in ('p1', 'p2', '_SUCCESS')]
        for k in keys:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = b'teststring')

        assert lib.check_success_exists(s3, bucket_name, '/'.join([prefix, version, partition])), '_SUCCESS not found in partition when it exists'

    @mock_s3
    def test_exists_within_version(self):
        _setup_module()

        prefix = 'longitudinal'
        version = 'v1'
        keys = [ '/'.join((prefix, version, fname))  for fname in ('p1', 'p2', '_SUCCESS')]
        for k in keys:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = b'teststring')

        assert lib.check_success_exists(s3, bucket_name, '/'.join([prefix, version])), '_SUCCESS not found in dir when it exists'

    @mock_s3
    def test_missing_from_partition(self):
        _setup_module()

        prefix = 'longitudinal'
        version = 'v1'
        partition = 'sample_id=1'
        keys = [ '/'.join((prefix, version, partition, fname))  for fname in ('p1', 'p2', 'p3')]
        for k in keys:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = b'teststring')

        assert not lib.check_success_exists(s3, bucket_name, '/'.join([prefix, version, partition])), '_SUCCESS found when actually missing from partition'

    @mock_s3
    def test_missing_from_version(self):
        _setup_module()

        prefix = 'longitudinal'
        version = 'v1'
        keys = [ '/'.join((prefix, version, fname))  for fname in ('p1', 'p2', 'p3')]
        for k in keys:
            s3_client.put_object(Bucket = bucket_name, Key = k, Body = b'teststring')

        assert not lib.check_success_exists(s3, bucket_name, '/'.join([prefix, version])), '_SUCCESS found when actually missing from directory'


class TestGetPartitioningFields:

    def test_finds_partitions(self):
        assert lib.get_partitioning_fields('sample_id=1/test_id=3/obj') == ['sample_id', 'test_id']


class TestAvro2Sql:

    def setup(self):
        self.avro = {'fields': [{'metadata': {}, 'type': 'string', 'name': 'clientId', 'nullable': True},
                           {'metadata': {}, 'type': 'integer', 'name': 'sampleId', 'nullable': True}],
                'type': 'struct'}
        self.name = "churn"
        self.version = "v3"
        self.location = "localhost"
        self.partitions = []

        self.create_table_sql = 'drop table if exists {0}; '
        self.create_table_sql += 'create external table {0}(`clientId` string, `sampleId` int) stored as parquet location \'"\'localhost/v3\'"\'; '
        self.create_table_sql += 'msck repair table {0};'

    def test_churn_transform(self):
        assert lib.avro2sql(self.avro, self.name, self.version, self.location, self.partitions) == self.create_table_sql.format('churn_v3'), 'SQL Schema is not correct'

    def test_churn_transform_no_version(self):
        assert lib.avro2sql(self.avro, self.name, self.version, self.location, self.partitions, False) == self.create_table_sql.format('churn'), 'SQL Schema is not correct'


class TestTransformType:

    def test_unchanged_type(self):
        assert lib.transform_type('string') == 'string', 'Unchanged type is changed when it should not be'

    def test_mapped_type(self):
        assert lib.transform_type('integer') == 'int', 'Mapped type is not mapping to correct value'

    def test_map_type(self):
        avro = {'type' : 'map',
                'values' : 'string'}
        assert lib.transform_type(avro) == 'map<string,string>', 'Map of string to string did not return correct schema'

    def test_nested_map(self):
        avro = {'type' : 'map',
                'values' : {'type' : 'map', 'values' : 'string'}}
        assert lib.transform_type(avro) == 'map<string,map<string,string>>', 'Nested map of string-string did not return correct schema'

    def test_array_type(self):
        avro = {'type' : 'array',
                'items' : 'integer'}
        assert lib.transform_type(avro) == 'array<int>', 'Array of ints did not return correct schema'

    def test_record_type(self):
        avro = {'type' : 'record',
                'name' : 'udf1',
                'fields' : [{'name' : 'field1', 'type' : 'integer'},
                            {'name' : 'field2', 'type' : 'string'}]}
        assert lib.transform_type(avro) == 'struct<`field1`: int, `field2`: string>', 'Record with two fields did not return correct schema'
        assert 'udf1' in lib.udf, 'New udf not inserted into udf dict' 

    def test_struct_type(self):
        avro = {'type' : 'struct',
                'fields' : [{'name' : 'field1', 'type' : 'long'},
                            {'name' : 'field2', 'type' : 'timestamp'}]}
        assert lib.transform_type(avro) == 'struct<`field1`: bigint, `field2`: timestamp>', 'Struct with two fields did not return correct schema'



class TestJarFile:

    def test_find_jar(self):
        assert lib.find_jar_path(), 'Did not find jar!'

