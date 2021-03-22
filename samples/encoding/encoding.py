# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configure required parameters
params = [
    'JOB_NAME',
    'input_path',
    'input_format',
    'input_encoding',
    'output_path'
]
# Configure optional parameters
if '--partition_keys' in sys.argv:
    params.append('partition_keys')

args = getResolvedOptions(sys.argv, params)
input_path = args['input_path']
input_format = args['input_format']
input_encoding = args['input_encoding']
output_path = args['output_path']
input_format_options = "{}"
partition_keys = []
if 'partition_keys' in args and args['partition_keys'] != "":
    partition_keys = [x.strip() for x in args['partition_keys'].split(',')]

# getOrCreate allows this to run as a job or in a notebook.
glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Create DataFrame with specified character encoding
df = spark.read.format(input_format).load(input_path, encoding=input_encoding)

# Write DataFrame
df.write.mode('overwrite').format(input_format).partitionBy(partition_keys).save(output_path)
