# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from awsglue.blueprint.workflow import Workflow, Entities
from awsglue.blueprint.job import Job
from awsglue.blueprint.crawler import *
import boto3
from botocore.client import ClientError
from logging import getLogger, StreamHandler, INFO


logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(INFO)
logger.setLevel(INFO)
logger.addHandler(handler)
logger.propagate = False


def create_s3_bucket_if_needed(s3_client, bucket_name, region):
    location = {'LocationConstraint': region}
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"S3 Bucket already exists: {bucket_name}")
    except ClientError as ce1:
        if ce1.response['Error']['Code'] == "404": # bucket not found
            logger.info(f"Creating S3 bucket: {bucket_name}")
            try:
                if region == "us-east-1":
                    bucket = s3_client.create_bucket(Bucket=bucket_name)
                else:
                    bucket = s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
                logger.info(f"Created S3 bucket: {bucket_name}")
            except ClientError as ce2:
                logger.error(f"Unexpected error occurred when creating S3 bucket: {bucket_name}, exception: {ce2}")
                raise
            except:
                logger.error(f"Unexpected error occurred when creating S3 bucket: {bucket_name}")
                raise
        else:
            logger.error(f"Unexpected error occurred when heading S3 bucket: {bucket_name}, exception: {ce1}")
            raise
    except:
        logger.error(f"Unexpected error occurred when heading S3 bucket: {bucket_name}")
        raise


def generate_layout(user_params, system_params):
    file_name = f"custom_connection_to_catalog_{user_params['SourceConnectionName']}.py"

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
        logger.info("New database is created.")
    except glue.exceptions.AlreadyExistsException:
        logger.info("Existing database is used.")


    # Creating script bucket
    the_script_bucket = f"aws-glue-scripts-{system_params['accountId']}-{system_params['region']}"
    create_s3_bucket_if_needed(s3_client, the_script_bucket, system_params['region'])

    # Creating temp bucket
    the_temp_bucket = f"aws-glue-temporary-{system_params['accountId']}-{system_params['region']}"
    create_s3_bucket_if_needed(s3_client, the_temp_bucket, system_params['region'])
    the_temp_prefix = f"{workflow_name}/"
    the_temp_location = f"s3://{the_temp_bucket}/{the_temp_prefix}"

    # Upload job script to script bucket
    the_script_key = f"{workflow_name}/{file_name}"
    the_script_location = f"s3://{the_script_bucket}/{the_script_key}"
    with open("custom_connection_to_catalog/custom_connection_to_catalog.py", "rb") as f:
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
        "--job-bookmark-option": "job-bookmark-disable",
        "--job-language": "python",
        "--enable-s3-parquet-optimized-committer": "",
        "--enable-rename-algorithm-v2": "",
        "--enable-metrics": "",
        "--enable-continuous-cloudwatch-log": "true",
        "--source_connection": user_params['SourceConnectionName'],
        "--show_tables_query_string": user_params['ShowTablesQueryString'],
        "--output_database": user_params['DestinationDatabaseName']
    }
    if user_params['DestinationTableNamePrefix'] and user_params['DestinationTableNamePrefix'] != "":
        arguments["--output_table_prefix"] = user_params['DestinationTableNamePrefix']

    transform_job = Job(
        Name=f"{workflow_name}_custom_connection_to_catalog_{user_params['SourceConnectionName']}",
        Command=command,
        Role=user_params['GlueExecutionRole'],
        DefaultArguments=arguments,
        Connections={
            "Connections": [user_params['SourceConnectionName']]
        },
        WorkerType="G.1X",
        NumberOfWorkers=user_params['NumberOfWorkers'],
        GlueVersion="2.0"
    )

    jobs.append(transform_job)

    workflow = Workflow(Name=workflow_name, Entities=Entities(Jobs=jobs, Crawlers=crawlers))

    return workflow
