# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ResolveChoice, DropNullFields
from awsglue.utils import getResolvedOptions


# Configure required parameters
params = [
    'JOB_NAME',
    'region',
    'input_database',
    'input_table',
    'output_database',
    'output_table',
    'output_path'
]


args = getResolvedOptions(sys.argv, params)
region = args['region']
input_database = args['input_database']
input_table = args['input_table']
output_database = args['output_database']
output_table = args['output_table']
output_path = args['output_path']

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)


# Create DynamicFrame from Data Catalog
dyf = glue_context.create_dynamic_frame.from_catalog(
    database=input_database,
    table_name=input_table,
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

# Retrieve partition keys
glue = boto3.client('glue')
res = glue.get_table(DatabaseName=input_database, Name=input_table)
partition_key_array = res['Table']['PartitionKeys']
partition_key_names = []
for partition_key in partition_key_array:
    partition_key_names.append(partition_key['Name'])

# Create the target table from the source table.
table_to_create = {
    'Name' : output_table,
    'StorageDescriptor' : res['Table']['StorageDescriptor'],
    'PartitionKeys': res['Table']['PartitionKeys'],
    'TableType': 'GOVERNED'
}

table_to_create['StorageDescriptor']['Location'] = output_path
table_to_create['StorageDescriptor']['Parameters']['lakeformation.aso.status'] = 'true'
if 'Description' in res['Table']:
    table_to_create['Description'] = res['Table']['Description']

try:
    glue.create_table(
        DatabaseName=output_database,
        TableInput=table_to_create
    )
except Exception as e:
    print(f"Failed to create destination table. If {output_database}.{output_table} table exists, please remove it.")
    print(e)
    raise

# Begin Lake Formation transaction
tx_id = glue_context.start_transaction(read_only=True)

# Write DynamicFrame into Lake Formation governed table using transaction
sink = glue_context.getSink(
    connection_type="s3",
    path=output_path,
    enableUpdateCatalog=True,
    partitionKeys=partition_key_names,
    transactionId=tx_id
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(
    catalogDatabase=output_database,
    catalogTableName=output_table
)

try:
    sink.writeFrame(dyf)
    glue_context.commit_transaction(tx_id)
except Exception:
    glue_context.cancel_transaction(tx_id)
    raise

job.commit()
