import boto3
import re
import singer

import singer_encodings.csv as csv
from tap_intacct import conversion

LOGGER = singer.get_logger()

def get_exported_tables(bucket, company_name, path=None):
    prefix = str.join('/', [path, company_name]) if path else company_name
    s3_objects = list_files_in_bucket(bucket, prefix)
    exported_tables = {o['Key'].split('/')[-1].split('.')[0] for o in s3_objects}
    return exported_tables

def list_files_in_bucket(bucket, search_prefix=None):
    s3_client = boto3.client('s3')

    s3_objects = []

    max_results = 1000
    args = {
        'Bucket': bucket,
        'MaxKeys': max_results,
    }

    if search_prefix is not None:
        args['Prefix'] = search_prefix

    result = s3_client.list_objects_v2(**args)

    next_continuation_token = None
    if result['KeyCount'] > 0:
        s3_objects += result['Contents']
        next_continuation_token = result.get('NextContinuationToken')

    while next_continuation_token is not None:
        LOGGER.info('Continuing pagination with token "%s".', next_continuation_token)

        continuation_args = args.copy()
        continuation_args['ContinuationToken'] = next_continuation_token

        result = s3_client.list_objects_v2(**continuation_args)

        s3_objects += result['Contents']
        next_continuation_token = result.get('NextContinuationToken')

    if s3_objects:
        LOGGER.info("Found %s files.", len(s3_objects))
    else:
        LOGGER.warning('Found no files for bucket "%s" that match prefix "%s"', bucket, search_prefix)

    return s3_objects

def get_sampled_schema_for_table(config, table_name):
    LOGGER.info('Sampling records to determine table schema "%s".', table_name)


    s3_files = get_input_files_for_table(config, table_name)

    if not s3_files:
        return {}

    samples = sample_files(config, table_name, s3_files)

    metadata_schema = {
        '_s3_source_bucket': {'type': 'string'},
        '_s3_source_file': {'type': 'string'},
        '_s3_source_lineno': {'type': 'integer'},
        '_s3_extra': {'type': 'array', 'items': {'type': 'string'}},
    }

    data_schema = conversion.generate_schema(samples)

    return {
        'type': 'object',
        'properties': merge_dicts(data_schema, metadata_schema)
    }

def merge_dicts(first, second):
    to_return = first.copy()

    for key in second:
        if key in first:
            if isinstance(first[key], dict) and isinstance(second[key], dict):
                to_return[key] = merge_dicts(first[key], second[key])
            else:
                to_return[key] = second[key]

        else:
            to_return[key] = second[key]

    return to_return

def get_input_files_for_table(config, table_name, modified_since=None):
    bucket = config['bucket']

    to_return = []

    path = config.get('path')
    company_name = config['company_name']
    prefix = str.join('/', [path, company_name]) if path else company_name
    s3_objects = list_files_in_bucket(bucket, prefix)

    pattern = "^" + prefix + '/' + table_name + "\..*"
    matcher = re.compile(pattern)

    LOGGER.info(
        'Checking bucket "%s" for keys matching "%s"', bucket, pattern)

    matched_files = []
    for s3_object in s3_objects:
        key = s3_object['Key']
        last_modified = s3_object['LastModified']

        if matcher.search(key):
            matched_files.append({'key': key, 'last_modified': last_modified})

    if not matched_files:
        raise Exception("No files found matching pattern {}".format(pattern))

    for matched_file in matched_files:
        if modified_since is None or modified_since < matched_file['last_modified']:
            LOGGER.info('Will download key "%s" as it was last modified %s', matched_file['key'], matched_file['last_modified'])
            to_return.append(matched_file)

    to_return = sorted(to_return, key=lambda item: item['last_modified'])

    if not to_return:
        LOGGER.warning('No files found matching pattern "%s" modified since %s', pattern, modified_since)

    return to_return

def sample_file(config, table_name, s3_path, sample_rate, max_records):
    LOGGER.info('Sampling %s (%s records, every %sth record).', s3_path, max_records, sample_rate)

    samples = []

    file_handle = get_file_handle(config, s3_path)
    iterator = csv.get_row_iterator(file_handle._raw_stream)

    current_row = 0

    for row in iterator:
        if (current_row % sample_rate) == 0:
            samples.append(row)

        current_row += 1

        if len(samples) >= max_records:
            break

    LOGGER.info('Sampled %s records.', len(samples))

    return samples


# pylint: disable=too-many-arguments
def sample_files(config, table_name, s3_files,
                 sample_rate=1, max_records=1000, max_files=5):
    to_return = []

    files_so_far = 0

    for s3_file in s3_files:
        to_return += sample_file(config, table_name, s3_file['key'],
                                 sample_rate, max_records)

        files_so_far += 1

        if files_so_far >= max_files:
            break

    return to_return

def get_file_handle(config, s3_path):
    bucket = config['bucket']
    s3_client = boto3.resource('s3')

    s3_bucket = s3_client.Bucket(bucket)
    s3_object = s3_bucket.Object(s3_path)
    return s3_object.get()['Body']
