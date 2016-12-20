import boto3
import botocore
import re
import json
import sys
import struct

from functools32 import lru_cache

from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport
from parquet_format.ttypes import FileMetaData

ignore_patterns = [
    r'.*/$',  # dirs
    r'.*/_',  # temp dirs and files
    r'.*/[^/]*\$folder\$/?'  # metadata dirs and files
]

udf = {}

class ParquetFormatError(Exception):
    pass

def load_prefix(s3_loc, success_only=None, recent_versions=None, exclude_regex=None):
    """Get a bash command which will load every dataset in a bucket at a prefix.

    For this to work, all datasets must be of the form `s3://$BUCKET_NAME/$PREFIX/$DATASET_NAME/v$VERSION/$PARTITIONS`.
    Any other formats will be ignored.

    :param bucket_name
    :param prefix
    """
    bucket_name, prefix = _get_bucket_and_prefix(s3_loc)
    datasets = _get_common_prefixes(bucket_name, prefix)
    bash_cmd = ''

    for dataset in datasets:
        try:
            bash_cmd += get_bash_cmd('s3://{}/{}'.format(bucket_name, dataset),
                                     success_only=success_only, recent_versions=recent_versions, exclude_regex=exclude_regex)
        except Exception as e:
            sys.stderr.write('Failed to process {}, {}\n'.format(dataset, str(e)))
    return bash_cmd

def get_bash_cmd(dataset, success_only=False, recent_versions=None, version=None, alias=None, exclude_regex=None):
    bucket_name, prefix = _get_bucket_and_prefix(dataset)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    versions = get_versions(bucket, prefix)

    if version is not None:
        versions = [v for v in versions if v == version]
        if not versions:
            sys.stderr.write("No schemas available with that version")

    bash_cmd, versions_loaded = "", 0
    for version in versions:
        success_exists = False
        version_prefix = prefix + '/' + version + '/'
        dataset_name = prefix.split('/')[-1] if alias is None else alias

        keys = sorted(bucket.objects.filter(Prefix=version_prefix), key=lambda obj: obj.last_modified, reverse=True)

        for key in keys:
            if ignore_key(key.key, exclude_regex=exclude_regex):
                continue

            partition = "/".join(key.key.split("/")[:-1])
            if success_only:
                if check_success_exists(s3, bucket.name, partition):
                    success_exists = True
                else:
                    continue
            break

        else:
            if success_only and not success_exists:
                sys.stderr.write("Ignoring dataset missing _SUCCESS file\n")
            else:
                sys.stderr.write("Ignoring empty dataset\n")
            continue

        sys.stderr.write("Analyzing dataset {}, {}\n".format(dataset_name, version))

        schema = read_schema(key.Object())

        partitions = get_partitioning_fields(key.key[len(prefix):])

        bash_cmd += "hive -hiveconf hive.support.sql11.reserved.keywords=false -e '{}'".format(avro2sql(schema, dataset_name, version, dataset, partitions)) + '\n'
        if versions_loaded == 0:  # Most recent version
            bash_cmd += "hive -e '{}'".format(avro2sql(schema, dataset_name, version, dataset, partitions, with_version=False)) + '\n'

        versions_loaded += 1
        if recent_versions is not None and versions_loaded >= recent_versions:
            break

    return bash_cmd


def read_schema(s3obj):
    # get object size
    object_size = s3obj.content_length

    # raise error if object is too small
    if object_size < 8:
        raise ParquetFormatError('file is too small')

    # get footer size
    response = s3obj.get(Range='bytes={}-'.format(object_size - 8))
    footer_size = struct.unpack('<i', response['Body'].read(4))[0]
    magic_number = response['Body'].read(4)

    # raise error if object is too small
    if object_size < (8 + footer_size):
        raise ParquetFormatError('file is too small')

    # raise error if magic number is bad
    if magic_number != 'PAR1':
        raise ParquetFormatError('magic number is invalid')

    # read footer
    response = s3obj.get(Range='bytes={}-'.format(object_size - 8 - footer_size))
    footer = response['Body']

    # read metadata from footer
    transport = TTransport.TFileObjectTransport(footer)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    metadata = FileMetaData()
    metadata.read(protocol)

    # parse as json and return
    return json.loads(metadata.key_value_metadata[0].value)


def get_versions(bucket, prefix):
    prefix = _remove_trailing_backslash(prefix) + '/'

    xs = bucket.meta.client.list_objects(Bucket=bucket.name, Delimiter='/', Prefix=prefix)
    tentative = [o.get('Prefix') for o in xs.get('CommonPrefixes', [])]

    versions = []
    for version_prefix in tentative:
        tmp = filter(bool, version_prefix.split("/"))
        if len(tmp) < 2:
            sys.stderr.write("Ignoring incompatible versioning scheme\n")
            continue

        # we don't yet support importing multiple datasets with a single command
        dataset_prefix = '/'.join(tmp[:-1])
        if dataset_prefix != prefix[:-1]:
            sys.stderr.write("Ignoring dataset nested within prefix. To load this dataset, call p2h on it directly: `parquet2hive s3://{}`\n".format(dataset_prefix))
            continue

        version = tmp[-1]
        if not re.match("^v[0-9]+$", version):
            sys.stderr.write("Ignoring incompatible versioning scheme: version must be an integer prefixed with a 'v'\n")
            continue

        versions.append(version)

    return sorted(versions, key=lambda x: int(x[1:]), reverse=True)


@lru_cache(maxsize=64)
def check_success_exists(s3, bucket, prefix):
    if not prefix.endswith('/'):
        prefix = prefix + '/'

    success_obj_loc = prefix + '_SUCCESS'
    exists = False

    try:
        s3.Object(bucket, success_obj_loc).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            exists = False
        else:
            raise e
    else:
        exists = True

    return exists


def ignore_key(key, exclude_regex=None):
    if exclude_regex is None:
        exclude_regex = []
    return any([re.match(pat, key) for pat in ignore_patterns + exclude_regex])


def get_partitioning_fields(prefix):
    return re.findall("([^=/]+)=[^=/]+", prefix)


def avro2sql(avro, name, version, location, partitions, with_version=True):
    fields = [avro2sql_column(field) for field in avro["fields"]]
    fields_decl = ", ".join(fields)

    if partitions:
        columns = ", ".join(["{} string".format(p) for p in partitions])
        partition_decl = " partitioned by ({})".format(columns)
    else:
        partition_decl = ""

    # check for duplicated fields
    field_names = [field["name"] for field in avro["fields"]]
    duplicate_columns = set(field_names) & set(partitions)
    assert not duplicate_columns, "Columns {} are in both the table columns and the partitioning columns; they should only be in one or another".format(", ".join(duplicate_columns))
    table_name = name + "_" + version if with_version else name
    return "drop table if exists {0}; create external table {0}({1}){2} stored as parquet location '\"'{3}/{4}'\"'; msck repair table {0};".format(table_name, fields_decl, partition_decl, location, version)


def avro2sql_column(avro):
    return "`{}` {}".format(avro["name"], transform_type(avro["type"]))


def transform_type(avro):
    is_dict, is_list, is_str = isinstance(avro, dict), isinstance(avro, list), isinstance(avro, str) or isinstance(avro, unicode)

    unchanged_types = ['string', 'int', 'float', 'double', 'boolean', 'date', 'timestamp', 'binary']
    mapped_types = {'integer': 'int', 'long': 'bigint'}

    if is_str and avro in unchanged_types:
        sql_type = avro
    elif is_str and avro in mapped_types:
        sql_type = mapped_types[avro]
    elif is_dict and avro["type"] == "map":
        value_type = avro.get("values", avro.get("valueType"))  # this can differ depending on the Avro schema version
        sql_type = "map<string,{}>".format(transform_type(value_type))
    elif is_dict and avro["type"] == "array":
        item_type = avro.get("items", avro.get("elementType"))  # this can differ depending on the Avro schema version
        sql_type = "array<{}>".format(transform_type(item_type))
    elif is_dict and avro["type"] in ("record", "struct"):
        fields_decl = ", ".join(["`{}`: {}".format(field["name"], transform_type(field["type"])) for field in avro["fields"]])
        sql_type = "struct<{}>".format(fields_decl)
        if avro["type"] == "record":
            udf[avro["name"]] = sql_type
    elif is_list:
        sql_type = transform_type(avro[0] if avro[1] == "null" else avro[1])
    elif avro in udf:
        sql_type = udf[avro]
    else:
        raise Exception("Unknown type {}".format(avro))

    return sql_type

def _remove_trailing_backslash(location):
    if location.endswith('/'):
        return location[:-1]
    return location

def _get_bucket_and_prefix(s3_loc):
    m = re.search("s3://([^/]*)/?(.*)", _remove_trailing_backslash(s3_loc))
    bucket_name = m.group(1)
    prefix = m.group(2)
    return bucket_name, prefix

def _get_common_prefixes(bucket, prefix=''):
    if prefix:
        prefix = _remove_trailing_backslash(prefix) + '/'
    client = boto3.client('s3')
    result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
    return [prefix.get('Prefix') for prefix in result.get('CommonPrefixes', [])]

