# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from awsglue.blueprint.workflow import Workflow, Entities
from awsglue.blueprint.job import Job
from awsglue.blueprint.crawler import *
import boto3
from botocore.client import ClientError


def generate_layout(user_params, system_params):
    file_name = f"standard_table_to_governed_{user_params['SourceDatabaseName']}_{user_params['SourceTableName']}.py"

    session = boto3.Session(region_name=system_params['region'])
    glue = session.client('glue')
    s3_client = session.client('s3')

    workflow_name = user_params['WorkflowName']

    # Create Database if it does not exists
    try:
        glue.create_database(
            DatabaseInput={
                'Name': user_params['DestinationDatabaseName']
            }
        )
        print("New database is created.")
    except glue.exceptions.AlreadyExistsException:
        print("Existing database is used.")

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
    with open("standard_table_to_governed/standard_table_to_governed.py", "rb") as f:
        s3_client.upload_fileobj(f, the_script_bucket, the_script_key)

    jobs = []
    crawlers = []

    command = {
        "Name": "glueetl",
        "ScriptLocation": the_script_location,
        "PythonVersion": "3"
    }
    arguments = {
        "--region": system_params['region'],
        "--TempDir": the_temp_location,
        "--job-bookmark-option": "job-bookmark-enable",
        "--job-language": "python",
        "--enable-s3-parquet-optimized-committer": "",
        "--enable-rename-algorithm-v2": "",
        "--enable-metrics": "",
        "--enable-continuous-cloudwatch-log": "true",
        "--input_database": user_params['SourceDatabaseName'],
        "--input_table": user_params['SourceTableName'],
        "--output_database": user_params['DestinationDatabaseName'],
        "--output_table": user_params['DestinationTableName'],
        "--output_path": user_params['OutputDataLocation']
    }

    transform_job = Job(
        Name=f"{workflow_name}_standard_table_to_governed_{user_params['SourceDatabaseName']}_{user_params['SourceTableName']}",
        Command=command,
        Role=user_params['GlueExecutionRole'],
        DefaultArguments=arguments,
        WorkerType="G.1X",
        NumberOfWorkers=user_params['NumberOfWorkers'],
        GlueVersion="2.0"
    )

    jobs.append(transform_job)

    workflow = Workflow(Name=workflow_name, Entities=Entities(Jobs=jobs, Crawlers=crawlers))

    return workflow
