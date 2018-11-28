import re
import sys
import struct

import boto3
import botocore

from functools32 import lru_cache

from six.moves import shlex_quote

from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport
from .parquet_format.ttypes import FileMetaData, Type, ConvertedType, FieldRepetitionType

CONVERSIONS = {
    'boolean': 'boolean',
    'int32': 'int',
    'int64': 'bigint',
    'int96': 'timestamp',
    'float': 'float',
    'double': 'double',
    'byte_array': 'binary',
    'fixed_len_byte_array': 'binary',
}

ignore_patterns = [
    r'.*/$',  # dirs
    r'.*/_[^=/]*/',  # temp dirs
    r'.*/_[^/]*$',  # temp files
    r'.*/[^/]*\$folder\$/?'  # metadata dirs and files
]

udf = {}

class UnknownParquetTypeError(Exception):
    pass

class ParquetFormatError(Exception):
    pass

def load_prefix(s3_loc, success_only=None, recent_versions=None, exclude_regex=None, just_sql=False):
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
        dataset = _remove_trailing_backslash(dataset)
        try:
            bash_cmd += get_bash_cmd('s3://{}/{}'.format(bucket_name, dataset),
                                     success_only=success_only, recent_versions=recent_versions,
                                     exclude_regex=exclude_regex, just_sql=just_sql)
        except Exception as e:
            sys.stderr.write('Failed to process {}, {}\n'.format(dataset, str(e)))
    return bash_cmd


def get_bash_cmd(location, success_only=False, recent_versions=None, version=None, alias=None, exclude_regex=None, just_sql=False):
    bucket_name, prefix = _get_bucket_and_prefix(location)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    versions = get_versions(bucket, prefix)

    client = botocore.session.get_session().create_client('s3')
    paginator = client.get_paginator('list_objects_v2')

    if version is not None:
        versions = [v for v in versions if v == version]
        if not versions:
            sys.stderr.write("No schemas available with that version")

    output, versions_loaded = "", 0
    for version in versions:
        success_exists = False
        version_prefix = prefix + '/' + version + '/'
        version_location = location + '/' + version
        dataset_name = prefix.split('/')[-1] if alias is None else alias

        latest_summary = None

        iterator = paginator.paginate(Bucket=bucket_name, Prefix=version_prefix)

        for summary in iterator.search('Contents[]'):
            if summary is None:
                continue

            if ignore_key(summary['Key'], exclude_regex=exclude_regex):
                continue

            if success_only:
                success_prefix = '/'.join(summary['Key'].split('/')[:-1])

                if check_success_exists(s3, bucket_name, success_prefix):
                    success_exists = True
                else:
                    continue

            if latest_summary is None or summary['LastModified'] > latest_summary['LastModified']:
                latest_summary = summary

        if success_only and not success_exists:
            sys.stderr.write("Ignoring dataset missing _SUCCESS file\n")
            continue

        if latest_summary is None:
            sys.stderr.write("Ignoring empty dataset\n")
            continue

        sys.stderr.write("Analyzing dataset {}, {}\n".format(dataset_name, version))

        schema = read_schema(s3.Object(bucket_name, latest_summary['Key']))

        partitions = get_partitioning_fields(latest_summary['Key'][len(prefix):])

        version_table_name = _normalize_table_name(dataset_name + "_" + version)
        version_sql = parquet2sql(schema, version_table_name, version_location, partitions)
        output += _format_sql(version_sql, just_sql)

        if versions_loaded == 0:  # Most recent version
            default_table_name = _normalize_table_name(dataset_name)
            default_sql = parquet2sql(schema, default_table_name, version_location, partitions)
            output += _format_sql(default_sql, just_sql)

        versions_loaded += 1
        if recent_versions is not None and versions_loaded >= recent_versions:
            break

    return output


def _format_sql(sql, just_sql=False):
    if just_sql:
        return sql + "\n"
    else:
        return "hive -e {} --hiveconf hive.msck.path.validation=skip\n".format(shlex_quote(sql))


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

    # return schema
    return metadata.schema


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


def parquet2sql(schema, table_name, location, partitions):
    fields = build_tree(schema[1:], schema[0].num_children)
    stmts = ["`{}` {}".format(field['name'], sql_type(field)) for field in fields]
    fields_decl = ", ".join(stmts)

    if partitions:
        columns = ", ".join(["`{}` string".format(p) for p in partitions])
        partition_decl = " partitioned by ({})".format(columns)
    else:
        partition_decl = ""

    # check for duplicated fields
    field_names = [field['name'] for field in fields]
    duplicate_columns = set(field_names) & set(partitions)
    assert not duplicate_columns, "Columns {} are in both the table columns and the partitioning columns; they should only be in one or another".format(", ".join(duplicate_columns))
    return "drop table if exists `{0}`; create external table `{0}`({1}){2} stored as parquet location '{3}'; msck repair table `{0}`;".format(table_name, fields_decl, partition_decl, location)


def build_tree(schema, children):
    retval = []

    for _ in range(children):
        elem = schema.pop(0)

        elem_type = 'group' if elem.type is None else Type._VALUES_TO_NAMES[elem.type].lower()
        converted_type = None if elem.converted_type is None else ConvertedType._VALUES_TO_NAMES[elem.converted_type].lower()
        repetition_type = FieldRepetitionType._VALUES_TO_NAMES[elem.repetition_type].lower()

        if elem_type == 'group':
            children = build_tree(schema, elem.num_children)
        else:
            children = None

        retval.append({
            'type': elem_type,
            'repetition_type': repetition_type,
            'name': elem.name,
            'children': children,
            'converted_type': converted_type,
            'scale': elem.scale,
            'precision': elem.precision,
        })

    return retval


def sql_type(elem):
    # list type
    if elem['type'] == 'group' and elem['converted_type'] == 'list':
        child = elem['children'][0]

        # if the repeated field is not a group, then its type is the element type and elements are required
        if child['type'] != 'group':
            child['repetition_type'] = 'required'
            return 'array<{}>'.format(sql_type(child))

        # if the repeated field is a group with multiple fields, then its type is the element type and elements are required
        if child['type'] == 'group' and len(child['children']) > 1:
            child['repetition_type'] = 'required'
            return 'array<{}>'.format(sql_type(child))

        # if the repeated field is a group with one field and is named either array or uses the LIST-annotated group's
        # name with _tuple appended then the repeated type is the element type and elements are required
        if child['type'] == 'group' and len(child['children']) == 1 and child['name'] in ('array', elem['name'] + '_tuple'):
            child['repetition_type'] = 'required'
            return 'array<{}>'.format(sql_type(child))

        return 'array<{}>'.format(sql_type(child['children'][0]))

    # map type
    if elem['type'] == 'group' and elem['converted_type'] in ('map', 'map_key_value'):
        key, val = elem['children'][0]['children']
        return 'map<{},{}>'.format(sql_type(key), sql_type(val))

    # struct type
    if elem['type'] == 'group' and elem['converted_type'] is None:
        subs = ['`{}`: {}'.format(sub['name'], sql_type(sub)) for sub in elem['children']]
        return 'struct<{}>'.format(', '.join(subs))

    # unannotated repeated type
    if elem['repetition_type'] == 'repeated':
        elem['repetition_type'] = 'required'
        return 'array<{}>'.format(sql_type(elem))

    # byte_array type + utf8 converted_type = string
    if elem['type'] == 'byte_array' and elem['converted_type'] == 'utf8':
        return 'string'

    # decimal type
    if elem['type'] == 'fixed_len_byte_array' and elem['converted_type'] == 'decimal':
        return 'decimal({},{})'.format(elem['precision'], elem['scale'])

    # conversion map
    if elem['type'] in CONVERSIONS:
        return CONVERSIONS[elem['type']]

    raise UnknownParquetTypeError('Unknown type ' + elem['type'])


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


def _normalize_table_name(table_name):
    table_name = re.sub(r"([A-Z]+)([A-Z][a-z])", r'\1_\2', table_name)
    table_name = re.sub(r"([a-z\d])([A-Z])", r'\1_\2', table_name)
    table_name = table_name.replace("-", "_")
    return table_name.lower()
