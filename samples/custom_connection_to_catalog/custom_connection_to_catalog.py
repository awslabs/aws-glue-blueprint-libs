# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import boto3
import concurrent.futures
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# Configure required parameters
params = [
    'JOB_NAME',
    'region',
    'source_connection',
    'show_tables_query_string',
    'output_database'
]
# Configure optional parameters
if '--output_table_prefix' in sys.argv:
    params.append('output_table_prefix')


args = getResolvedOptions(sys.argv, params)
region = args['region']
source_connection = args['source_connection']
show_tables_query_string = args['show_tables_query_string']
output_database = args['output_database']
output_table_prefix = ""
if 'output_table_prefix' in args:
    output_table_prefix = args['output_table_prefix']

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

glue = boto3.client('glue')

# Create DynamicFrame for all tables under schema/database configured in the custom connection
dyf = glue_context.create_dynamic_frame.from_options(
    connection_type="custom.jdbc",
    connection_options={
        "query": show_tables_query_string,
        "connectionName": source_connection
    },
    transformation_ctx="dyf"
)


def create_table_definition(table_name):
    print(f"Creating table definition for Table {table_name}.")

    dyf_table = glue_context.create_dynamic_frame.from_options(
        connection_type="custom.jdbc",
        connection_options={
            "query": f"SELECT * FROM {table_name} LIMIT 1",
            "connectionName": source_connection
        },
        transformation_ctx=f"dyf_table_{table_name}"
    )
    table_input = {'Name': f"{output_table_prefix}{table_name}", 'TableType': "EXTERNAL_TABLE"}
    columns = []
    for field in dyf_table.schema():
        column = {'Name': field.name, 'Type': field.dataType.typeName()}
        columns.append(column)
    storage_descriptor = {'Columns': columns, 'Location': table_name}
    table_input['StorageDescriptor'] = storage_descriptor
    try:
        glue.create_table(
            DatabaseName=output_database,
            TableInput=table_input
        )
        print(f"New table {table_name} is created.")
    except glue.exceptions.AlreadyExistsException:
        glue.update_table(
            DatabaseName=output_database,
            TableInput=table_input
        )
        print(f"Existing table {table_name} is updated.")

table_rows = dyf.toDF().select(col('table_name')).collect()
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    future_to_partition = {executor.submit(
        create_table_definition, table_row.table_name): table_row for table_row in table_rows}
    for future in concurrent.futures.as_completed(future_to_partition):
        partition = future_to_partition[future]
        data = future.result()

job.commit()
