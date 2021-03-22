# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
from pyspark.context import SparkContext
from awsglue.transforms import ResolveChoice, DropNullFields
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import boto3

# Configure required parameters
params = [
    'JOB_NAME',
    'output_database',
    'tmp_table',
    'output_table',
    'output_path'
]

args = getResolvedOptions(sys.argv, params)
output_database = args['output_database']
tmp_table = args['tmp_table']
output_table = args['output_table']
output_path = args['output_path']

# Retrieve partition key information from tmp-table
partition_keys = []
glue = boto3.client('glue')
res = glue.get_table(DatabaseName=output_database, Name=tmp_table)
for partition_key in res['Table']['PartitionKeys']:
    partition_keys.append(partition_key['Name'])

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
