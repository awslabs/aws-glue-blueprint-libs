# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from awsglue.blueprint.workflow import *
from awsglue.blueprint.job import *
from awsglue.blueprint.crawler import *
import boto3
from botocore.client import ClientError


def validate_params(user_params, system_params):
    if user_params['InputDataLocation'] == user_params['OutputDataLocation']:
        err_msg = 'InputDataLocation is same as OutputDataLocation.'
        raise ClientError({"Error": {"Code": "InvalidInputException", "Message": err_msg}}, 'validate_params')


def generate_layout(user_params, system_params):
    file_name = "encoding.py"

    session = boto3.Session(region_name=system_params['region'])
    s3_client = session.client('s3')

    workflow_name = user_params['WorkflowName']

    # Validate params
    validate_params(user_params, system_params)

    location = {'LocationConstraint': system_params['region']}
    # Creating script bucket
    the_script_bucket = f"aws-glue-scripts-{system_params['accountId']}-{system_params['region']}"
    try:
        s3_client.head_bucket(Bucket=the_script_bucket)
        print("Script bucket already exists: ", the_script_bucket)
    except ClientError as ce:
        print(ce)
        print(ce.response['ResponseMetadata'])
        print("Creating script bucket: ", the_script_bucket)
        if system_params['region'] == "us-east-1":
            bucket = s3_client.create_bucket(Bucket=the_script_bucket)
        else:
            bucket = s3_client.create_bucket(Bucket=the_script_bucket, CreateBucketConfiguration=location)

    # Creating temp bucket
    the_temp_bucket = f"aws-glue-temporary-{system_params['accountId']}-{system_params['region']}"
    the_temp_prefix = f"{workflow_name}/"
    the_temp_location = f"s3://{the_temp_bucket}/{the_temp_prefix}"
    try:
        s3_client.head_bucket(Bucket=the_temp_bucket)
        print("Temp bucket already exists: ", the_temp_bucket)
    except ClientError as ce:
        print(ce)
        print(ce.response['ResponseMetadata'])
        print("Creating temp bucket: ", the_temp_bucket)
        if system_params['region'] == "us-east-1":
            bucket = s3_client.create_bucket(Bucket=the_temp_bucket)
        else:
            bucket = s3_client.create_bucket(Bucket=the_temp_bucket, CreateBucketConfiguration=location)

    # Upload job script to script bucket
    the_script_key = f"{workflow_name}/{file_name}"
    the_script_location = f"s3://{the_script_bucket}/{the_script_key}"
    with open("encoding/encoding.py", "rb") as f:
        s3_client.upload_fileobj(f, the_script_bucket, the_script_key)

    # Structure workflow
    jobs = []

    command = {
        "Name": "glueetl",
        "ScriptLocation": the_script_location,
        "PythonVersion": "3"
    }
    arguments = {
        "--TempDir": the_temp_location,
        "--job-bookmark-option": "job-bookmark-enable",
        "--job-language": "python",
        "--enable-metrics": "",
        "--enable-continuous-cloudwatch-log": "true",
        "--input_path": user_params['InputDataLocation'],
        "--input_format": user_params['InputDataFormat'],
        "--input_encoding": user_params['InputDataEncoding'],
        "--output_path": user_params['OutputDataLocation']
    }
    if user_params['PartitionKeys'] and user_params['PartitionKeys'] != [] and user_params['PartitionKeys'] != [""]:
        arguments["--partition_keys"] = ",".join(user_params['PartitionKeys'])

    transform_job = Job(
        Name="{0}_encoding".format(workflow_name),
        Command=command,
        Role=user_params['IAMRole'],
        DefaultArguments=arguments,
        WorkerType="G.1X",
        NumberOfWorkers=user_params['NumberOfWorkers'],
        GlueVersion="2.0"
    )
    jobs.append(transform_job)

    workflow = Workflow(Name=workflow_name, Entities=Entities(Jobs=jobs, Crawlers=[]))

    return workflow
