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
    'region',
    'input_path',
    'input_format',
    'dynamodb_table',
    'dynamodb_write_throughput_percent'
]

# Configure optional parameters
if '--input_format_options' in sys.argv:
    params.append('input_format_options')

args = getResolvedOptions(sys.argv, params)
region = args['region']
input_path = args['input_path']
input_format = args['input_format']
input_format_options = "{}"
dynamodb_table = args['dynamodb_table']
dynamodb_write_throughput_percent = args['dynamodb_write_throughput_percent']

if 'input_format_options' in args:
    input_format_options = args['input_format_options']

input_format_options = json.loads(input_format_options)

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Currently partitioned location is not supported as input path in this blueprint.
dyf = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [input_path]
    },
    format=input_format,
    format_options=input_format_options
)

glue_context.write_dynamic_frame_from_options(
    frame=dyf,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": dynamodb_table,
        "dynamodb.throughput.write.percent": dynamodb_write_throughput_percent
    }
)

job.commit()
