# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, col
from pyspark.context import SparkContext
from awsglue.transforms import ResolveChoice, DropNullFields
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

# Configure required parameters
params = [
    'JOB_NAME',
    'output_database',
    'tmp_table',
    'output_table',
    'output_path'
]
# Configure optional parameters
if '--partition_keys' in sys.argv:
    params.append('partition_keys')
if '--timestamp_column_name' in sys.argv:
    params.append('timestamp_column_name')
if '--timestamp_column_granularity' in sys.argv:
    params.append('timestamp_column_granularity')

args = getResolvedOptions(sys.argv, params)
output_database = args['output_database']
tmp_table = args['tmp_table']
output_table = args['output_table']
output_path = args['output_path']
partition_keys = []
timestamp_column_name = ""
if 'partition_keys' in args and args['partition_keys'] != "":
    partition_keys = [x.strip() for x in args['partition_keys'].split(',')]
if 'timestamp_column_name' in args:
    timestamp_column_name = args['timestamp_column_name']
if 'timestamp_column_granularity' in args:
    timestamp_column_granularity = args['timestamp_column_granularity']

if timestamp_column_name == "":
    pass
elif timestamp_column_granularity == 'year':
    partition_keys = ['year']
elif timestamp_column_granularity == 'month':
    partition_keys = ['year', 'month']
elif timestamp_column_granularity == 'day':
    partition_keys = ['year', 'month', 'day']
elif timestamp_column_granularity == 'hour':
    partition_keys = ['year', 'month', 'day', 'hour']
elif timestamp_column_granularity == 'minute':
    partition_keys = ['year', 'month', 'day', 'hour', 'minute']

# getOrCreate allows this to run as a job or in a notebook.
glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Create DynamicFrame from Data Catalog
dyf = glue_context.create_dynamic_frame.from_catalog(
    database=output_database,
    table_name=tmp_table,
    transformation_ctx='dyf'
)

# Resolve choice type with make_struct
dyf = ResolveChoice.apply(
    frame=dyf,
    choice='make_struct',
    transformation_ctx='resolvechoice'
)

# Drop null fields
dyf = DropNullFields.apply(
    frame=dyf,
    transformation_ctx='dropnullfields'
)

# Apply mapping into Timestamp based on specified timestamp column
if timestamp_column_name:
    # Add temp column from specified timestamp column
    tmp_timestamp_column_name = f'tmp_${timestamp_column_name}'
    df = dyf.toDF()
    df.withColumn(tmp_timestamp_column_name, col(timestamp_column_name))
    dyf = DynamicFrame.fromDF(df, glue_context, "add_tmp_column")

    # Perform apply_mapping to convert the temp column to timestamp data type
    mapping = []
    for field in dyf.schema():
        if field.name == tmp_timestamp_column_name:
            mapping.append((field.name, field.dataType.typeName(), field.name, 'timestamp'))
        else:
            mapping.append((field.name, field.dataType.typeName(), field.name, field.dataType.typeName()))
    dyf = dyf.apply_mapping(mapping)

    # Add partition columns
    df = dyf.toDF()
    if 'year' in partition_keys:
        df = df.withColumn('year', year(timestamp_column_name))
    if 'month' in partition_keys:
        df = df.withColumn('month', month(timestamp_column_name))
    if 'day' in partition_keys:
        df = df.withColumn('day', dayofmonth(timestamp_column_name))
    if 'hour' in partition_keys:
        df = df.withColumn('hour', hour(timestamp_column_name))
    if 'minute' in partition_keys:
        df = df.withColumn('minute', minute(timestamp_column_name))

    df.drop(col(tmp_timestamp_column_name))
    dyf = DynamicFrame.fromDF(df, glue_context, "add_partitions")

# Write DynamicFrame to S3 in glueparquet format
sink = glue_context.getSink(
    connection_type="s3",
    path=output_path,
    enableUpdateCatalog=True,
    partitionKeys=partition_keys
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(
    catalogDatabase=output_database,
    catalogTableName=output_table
)
sink.writeFrame(dyf)

job.commit()
