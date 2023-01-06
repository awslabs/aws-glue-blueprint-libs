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
    'input_path',
    'input_format',
    'output_connection',
    'output_database',
    'output_table'
]

# Configure optional parameters
if '--input_format_options' in sys.argv:
    params.append('input_format_options')
if '--redshift_iam_role' in sys.argv:
    params.append('redshift_iam_role_arn')

args = getResolvedOptions(sys.argv, params)
region = args['region']
account_id = args['account_id']
input_path = args['input_path']
input_format = args['input_format']
input_format_options = "{}"
output_connection = args['output_connection']
output_database = args['output_database']
output_table = args['output_table']

if 'input_format_options' in args:
    input_format_options = args['input_format_options']
if 'redshift_iam_role_arn' in args:
    redshift_iam_role_arn = args['redshift_iam_role_arn']

input_format_options_dict = json.loads(input_format_options)

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Currently partitioned location is not supported as input path in this blueprint.
dyf = glue_context.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {
        "paths": [input_path]
    },
    format = input_format,
    format_options = input_format_options_dict
)

options = {
    "database": output_database,
    "dbtable": output_table
}
if redshift_iam_role_arn:
    options["aws_iam_role"] = redshift_iam_role_arn

glue_context.write_dynamic_frame.from_jdbc_conf(
    frame = dyf,
    catalog_connection = output_connection,
    connection_options = options,
    redshift_tmp_dir = args["TempDir"]
)

job.commit()
