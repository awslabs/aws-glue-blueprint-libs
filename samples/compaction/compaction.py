# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3
import concurrent.futures
import sys
import urllib
import math
import json

from pyspark.sql.functions import input_file_name, col
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configure required parameters
params = [
    'JOB_NAME',
    'region',
    'input_database',
    'input_table',
    'input_format',
    'output_path',
    'desired_size_mb',
    'enable_size_control',
    'enable_manifest'
]

# Configure optional parameters
if '--input_format_options' in sys.argv:
    params.append('input_format_options')
if '--manifest_path' in sys.argv:
    params.append('manifest_path')


def str_to_bool(val):
    return val.lower() in ['true', '1', 't', 'y', 'yes']


args = getResolvedOptions(sys.argv, params)
region = args['region']
input_database = args['input_database']
input_table = args['input_table']
input_format = args['input_format']
input_format_options = "{}"
output_path = args['output_path']

enable_size_control = str_to_bool(args['enable_size_control'])
desired_size_in_bytes = float(args['desired_size_mb']) * 1024.0 * 1024.0
enable_manifest = str_to_bool(args['enable_manifest'])

if 'input_format_options' in args:
    input_format_options = args['input_format_options']
if 'manifest_path' in args:
    manifest_path = args['manifest_path']

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

session = boto3.Session(region_name=region)
glue = session.client('glue')
s3 = session.resource('s3')
s3_client = session.client('s3')


def get_partition_num_and_size(s3_url, exclude_s3_paths):
    o = urllib.parse.urlparse(s3_url)
    bucket = o.netloc
    prefix = urllib.parse.unquote(o.path)[1:]

    keys = []
    list_objects_v2_paginator = s3_client.get_paginator('list_objects_v2')
    for s3_list_page in list_objects_v2_paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(s3_list_page['Contents'])

    partition_size_in_bytes = 0
    partition_file_num = 0
    for key in keys:
        if key in exclude_s3_paths:
            continue
        file_size_in_bytes = int(key['Size'])
        partition_size_in_bytes += file_size_in_bytes
        partition_file_num += 1

    return partition_file_num, partition_size_in_bytes


# run compaction ETL on partition
def process(part, base_location):
    s3_url = part['StorageDescriptor']['Location']
    base_location = f"{base_location.rstrip('/')}/"
    partition_location = s3_url.replace(base_location, "")
    print('Processing location: ' + s3_url)
    print('Base location: ' + base_location)
    print('Partition location: ' + partition_location)

    # Create DataFrame from options per Hive partition instead of DynamicFrame
    # in order to use full functionality of input_file_name()
    dfr = spark.read.format(input_format)
    for k, v in json.loads(input_format_options):
        dfr = dfr.option(k, v)

    df = dfr.load(s3_url)
    # Drop rows where all records are null
    df = df.na.drop(how="all")

    updated_manifest_body = ""

    # Manage state based on manifest files
    manifest_key = ""
    s3_object = None

    files_in_manifest = []
    if enable_manifest:
        manifest_key = f"{manifest_path.rstrip('/')}/{input_database}/{input_table}/{partition_location}manifest.txt"
        # Retrieve manifest file for the target partition
        print('Manifest location: ' + manifest_key)

        o = urllib.parse.urlparse(manifest_key)
        manifest_bucket = o.netloc
        manifest_prefix = urllib.parse.unquote(o.path)[1:]

        s3_object = s3.Object(manifest_bucket, manifest_prefix)

        # Check manifest file to see if new files come
        is_new_file = False
        try:
            files_in_manifest = s3_object.get()['Body'].read().decode('utf-8').splitlines()
        except s3.meta.client.exceptions.NoSuchKey:
            print(f"manifest file {manifest_key} does not exist. All the files are new.")
            is_new_file = True
        except Exception as e:
            print(f'Unexpected error: {e}')
            raise

        file_rows = df.withColumn('filepath', input_file_name()).select(col('filepath')).distinct().collect()
        print(f"Partition {partition_location} has {len(file_rows)} files.")
        for file_row in file_rows:
            print(f"Partition {partition_location}: adding {file_row.filepath} to manifest.")
            updated_manifest_body = updated_manifest_body + file_row.filepath + '\n'
            if files_in_manifest:
                if file_row.filepath not in files_in_manifest:
                    print(f"{file_row.filepath} is a new file, which is not included in the manifest.")
                    is_new_file = True
                else:
                    print(f"{file_row.filepath} is an already-compacted file, which is included in the manifest. "
                          "Excluding it for current compaction.")
                    df = df.withColumn('filepath', input_file_name()).filter(col('filepath') != file_row.filepath).drop(col('filepath'))

        if not is_new_file:
            print("There are not any new files. Skipping this partition for compaction.")
            return

    # Calculate optimal file number per partition
    skip_compaction = False
    if enable_size_control:
        partition_file_num, partition_size_in_bytes = get_partition_num_and_size(s3_url, files_in_manifest)
        average_file_size_in_bytes = partition_size_in_bytes / partition_file_num
        if partition_size_in_bytes < desired_size_in_bytes:
            if enable_manifest:
                print('Skipping compaction since currently input data is not enough large for compaction.')
                return
            optimal_file_num = 1
        elif average_file_size_in_bytes < desired_size_in_bytes:
            optimal_file_num = math.floor(float(partition_size_in_bytes) / desired_size_in_bytes)
        else:
            optimal_file_num = partition_file_num
            skip_compaction = True
            print('Partition ' + s3_url
                  + ', file num = ' + str(partition_file_num)
                  + ', entire partition size = ' + str(partition_size_in_bytes)
                  + ', average file size = ' + str(average_file_size_in_bytes)
                  + ', calculated optimal file num = ' + str(optimal_file_num))
    else:
        optimal_file_num = 1

    # Compaction into output path
    output_partition_path = f"{output_path.rstrip('/')}/{partition_location}"
    print(f"Writing into the output path: {output_partition_path}")
    if not skip_compaction:
        df = df.coalesce(optimal_file_num)

    if enable_manifest:
        df.write.mode('append').format(input_format).save(output_partition_path)
    else:
        df.write.mode('overwrite').format(input_format).save(output_partition_path)

    # Update a manifest file per partition
    if enable_manifest and len(updated_manifest_body) > 0:
        try:
            print(f"Updating manifest file: {manifest_key} with body={updated_manifest_body}")
            s3_object.put(
                Body=updated_manifest_body.encode('utf-8'),
                ContentEncoding='utf-8',
                ContentType='text/plane'
            )
        except Exception as e:
            print(f'Unexpected error: {e}')
            raise


# Get partition information for table
partitions = []
get_partitions_paginator = glue.get_paginator('get_partitions')
for page in get_partitions_paginator.paginate(DatabaseName=input_database, TableName=input_table):
    partitions.extend(page['Partitions'])
print('Partition num = ' + str(len(partitions)))

table = glue.get_table(DatabaseName=input_database, Name=input_table)
table_base_location = table['Table']['StorageDescriptor']['Location']

# If the table is not partitioned, call the process method to compact the base table location.
if len(partitions) == 0:
    process(table['Table'], table_base_location)
# If the table is partitioned, starts threads to compact each partition.
else:
    # Compacting partitions in multi-thread
    with concurrent.futures.ThreadPoolExecutor(max_workers=300) as executor:
        future_to_partition = {executor.submit(
            process, partition, table_base_location): partition for partition in partitions}
        for future in concurrent.futures.as_completed(future_to_partition):
            partition = future_to_partition[future]
            data = future.result()

job.commit()
