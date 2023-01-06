# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import json

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# Configure required parameters
params = [
    'JOB_NAME',
    'TempDir',
    'region',
    'account_id',
    'input_connection_name',
    'input_connection_type',
    'input_database',
    'input_table',
    'output_path',
    'output_format'
]

# Configure optional parameters
if '--redshift_iam_role' in sys.argv:
    params.append('redshift_iam_role_arn')
if '--output_format_options' in sys.argv:
    params.append('output_format_options')
if '--partition_keys' in sys.argv:
    params.append('partition_keys')

args = getResolvedOptions(sys.argv, params)
region = args['region']
account_id = args['account_id']
input_connection_name = args['input_connection_name']
input_connection_type = args['input_connection_type']
input_table = args['input_table']
output_path = args['output_path']
output_format = args['output_format']
output_format_options = "{}"

if 'input_database' in args:
    input_database = args['input_database']

if 'output_format_options' in args:
    output_format_options = args['output_format_options']
output_format_options_dict = json.loads(output_format_options)

if 'redshift_iam_role_arn' in args:
    redshift_iam_role_arn = args['redshift_iam_role_arn']

partition_keys = []
if 'partition_keys' in args and args['partition_keys'] != "":
    partition_keys = [x.strip() for x in args['partition_keys'].split(',')]

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

options = {
    "dbtable": input_table,
    "connectionName": input_connection_name,
    "useConnectionProperties": True,
    "redshiftTmpDir": args["TempDir"]
}
if input_database:
    options["database"] = input_database

if redshift_iam_role_arn:
    options["aws_iam_role"] = redshift_iam_role_arn

dyf = glue_context.create_dynamic_frame.from_options(
    connection_type = input_connection_type,
    connection_options = options
)

glue_context.write_dynamic_frame.from_options(
    frame = dyf,
    connection_type = "s3",
    connection_options = {
        "path": output_path,
        "partitionKeys": partition_keys
    },
    format = output_format,
    format_options = output_format_options_dict
)

job.commit()
