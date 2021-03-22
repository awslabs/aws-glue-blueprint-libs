# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from awsglue.blueprint.workflow import Workflow, Entities
from awsglue.blueprint.job import Job
from awsglue.blueprint.crawler import *
import boto3
from botocore.client import ClientError
import datetime


def generate_schedule(type):
    now = datetime.datetime.utcnow()
    year = now.year
    number_of_month = now.month
    days = now.day
    hours = now.hour
    minutes = now.minute
    days_of_week = now.weekday()

    if type == 'Hourly':
        return generate_cron_expression(minutes, "0/1", "*", "*", "?", "*")
    elif type == 'Daily':
        return generate_cron_expression(minutes, hours, "*", "*", "?", "*")
    elif type == 'Weekly':
        return generate_cron_expression(minutes, hours, "?", "*", days_of_week, "*")
    elif type == 'Monthly':
        return generate_cron_expression(minutes, hours, days, "*", "?", "*")
    else:
        return generate_cron_expression(minutes, hours, days, number_of_month, "?", year)


def generate_cron_expression(minutes, hours, days, number_of_month, days_of_week, year):
    return "cron({0} {1} {2} {3} {4} {5})".format(minutes, hours, days, number_of_month, days_of_week, year)


def validate_params(user_params, system_params):
    if user_params['InputDataLocation'] and user_params['InputDataLocation'] != "" \
            and user_params['InputDataLocation'] == user_params['OutputDataLocation']:
        err_msg = 'InputDataLocation is same as OutputDataLocation.'
        raise ClientError({"Error": {"Code": "InvalidInputException", "Message": err_msg}}, 'validate_params')


def generate_layout(user_params, system_params):
    file_name = "compaction_{0}_{1}.py".format(user_params['SourceDatabaseName'], user_params['SourceTableName'])

    session = boto3.Session(region_name=system_params['region'])
    glue = session.client('glue')
    s3_client = session.client('s3')

    workflow_name = user_params['WorkflowName']

    # Validate params
    validate_params(user_params, system_params)

    # Create Source Database if it does not exists
    try:
        glue.create_database(
            DatabaseInput={
                'Name': user_params['SourceDatabaseName']
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
        bucket = s3_client.create_bucket(Bucket=the_temp_bucket, CreateBucketConfiguration=location)

    # Creating manifest bucket
    if user_params['EnableManifest']:
        the_manifest_bucket = f"aws-glue-blueprint-compaction-manifest-{system_params['accountId']}-{system_params['region']}"
        the_manifest_prefix = f"{workflow_name}/"
        the_manifest_location = f"s3://{the_manifest_bucket}/{the_manifest_prefix}"
        try:
            s3_client.head_bucket(Bucket=the_manifest_bucket)
            print("Manifest bucket already exists: ", the_manifest_bucket)
        except ClientError as ce:
            print(ce)
            print(ce.response['ResponseMetadata'])
            print("Creating Manifest bucket: ", the_manifest_bucket)
            bucket = s3_client.create_bucket(Bucket=the_manifest_bucket, CreateBucketConfiguration=location)

    # Upload job script to script bucket
    the_script_key = f"{workflow_name}/{file_name}"
    the_script_location = f"s3://{the_script_bucket}/{the_script_key}"
    with open("compaction/compaction.py", "rb") as f:
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
        "--enable_size_control": user_params['EnableSizeControl'],
        "--input_database": user_params['SourceDatabaseName'],
        "--input_table": user_params['SourceTableName'],
        "--input_format": user_params['InputDataFormat'],
        "--output_path": user_params['OutputDataLocation'],
        "--desired_size_mb": user_params['DesiredFileSizeMB'],
        "--enable_manifest": user_params['EnableManifest']
    }
    if user_params['InputDataFormatOptions']:
        arguments["--input_format_options"] = user_params['InputDataFormatOptions']
    if user_params['EnableManifest']:
        arguments["--manifest_path"] = the_manifest_location

    crawler_source = None

    try:
        # Get the source table definition and validate the parameters with it.
        src_table = glue.get_table(
            DatabaseName=user_params['SourceDatabaseName'],
            Name=user_params['SourceTableName']
        )
        if src_table['Table']['StorageDescriptor']['Location'] == user_params['OutputDataLocation']:
            err_msg = 'Location on the source table is same as OutputDataLocation.'
            raise ClientError({"Error": {"Code": "InvalidInputException", "Message": err_msg}}, 'validate_params')
        if user_params['InputDataLocation'] and user_params['InputDataLocation'] != "" \
                and src_table['Table']['StorageDescriptor']['Location'] != user_params['InputDataLocation']:
            err_msg = 'Location on the source table is different from InputDataLocation.'
            raise ClientError({"Error": {"Code": "InvalidInputException", "Message": err_msg}}, 'validate_params')
        print("Existing table is used.")

    except glue.exceptions.EntityNotFoundException:
        if user_params['InputDataLocation'] and user_params['InputDataLocation'] != "":
            # Create a new source table if it does not exist
            glue.create_table(
                DatabaseName=user_params['SourceDatabaseName'],
                TableInput={
                    'Name': user_params['SourceTableName'],
                    'StorageDescriptor': {
                        'Location': user_params['InputDataLocation']
                    }
                }
            )
            print("New table is created.")
        else:
            err_msg = 'Source table does not exist, and input data location is not provided.'
            raise ClientError({"Error": {"Code": "InvalidInputException", "Message": err_msg}}, 'validate_params')

    if user_params['InputDataLocation'] and user_params['InputDataLocation'] != "":
        targets_source = {"CatalogTargets": [{"DatabaseName": user_params['SourceDatabaseName'], "Tables": [user_params['SourceTableName']]}]}
        crawler_source = Crawler(
            Name="{}_crawler_source".format(workflow_name),
            Role=user_params['IAMRole'],
            Grouping={
                "TableGroupingPolicy": "CombineCompatibleSchemas"
            },
            Targets=targets_source,
            SchemaChangePolicy={"DeleteBehavior": "LOG"},
        )
        crawlers.append(crawler_source)

    if crawler_source:
        transform_job = Job(
            Name="{0}_compaction_{1}_{2}".format(workflow_name, user_params['SourceDatabaseName'], user_params['SourceTableName']),
            Command=command,
            Role=user_params['IAMRole'],
            DefaultArguments=arguments,
            WorkerType="G.1X",
            NumberOfWorkers=user_params['NumberOfWorkers'],
            GlueVersion="2.0",
            DependsOn={crawler_source: "SUCCEEDED"}
        )
    else:
        transform_job = Job(
            Name="{0}_compaction_{1}_{2}".format(workflow_name, user_params['SourceDatabaseName'], user_params['SourceTableName']),
            Command=command,
            Role=user_params['IAMRole'],
            DefaultArguments=arguments,
            WorkerType="G.1X",
            NumberOfWorkers=user_params['NumberOfWorkers'],
            GlueVersion="2.0"
        )

    jobs.append(transform_job)

    # Create destination database if it does not exists
    try:
        glue.create_database(
            DatabaseInput={
                'Name': user_params['DestinationDatabaseName']
            }
        )
        print("New database is created.")
    except glue.exceptions.AlreadyExistsException:
        print("Existing database is used.")

    try:
        # Get the destination table and validate the parameters with it.
        dst_table = glue.get_table(
            DatabaseName=user_params['DestinationDatabaseName'],
            Name=user_params['DestinationTableName']
        )
        if dst_table['Table']['StorageDescriptor']['Location'] != user_params['OutputDataLocation']:
            err_msg = 'Location on the destination table is different from the OutputDataLocation.'
            raise ClientError({"Error": {"Code": "InvalidInputException", "Message": err_msg}}, 'validate_params')
        print("Existing table is used.")
    except glue.exceptions.EntityNotFoundException:
        # Create destination table if it does not exist
        glue.create_table(
            DatabaseName=user_params['DestinationDatabaseName'],
            TableInput={
                'Name': user_params['DestinationTableName'],
                'StorageDescriptor': {
                    'Location': user_params['OutputDataLocation']
                }
            }
        )
        print("New table is created.")

    targets_destination = {"CatalogTargets": [{"DatabaseName": user_params['DestinationDatabaseName'], "Tables": [user_params['DestinationTableName']]}]}
    crawler_destination = Crawler(
        Name="{}_crawler_destination".format(workflow_name),
        Role=user_params['IAMRole'],
        Targets=targets_destination,
        SchemaChangePolicy={"DeleteBehavior": "LOG"},
        DependsOn={transform_job: "SUCCEEDED"}
    )
    crawlers.append(crawler_destination)

    if user_params['Frequency']:
        if user_params['Frequency'] == 'Custom':
            schedule = user_params['FrequencyCronFormat']
        else:
            schedule = generate_schedule(user_params['Frequency'])
    else:
        schedule = None
    workflow = Workflow(Name=workflow_name, Entities=Entities(Jobs=jobs, Crawlers=crawlers), OnSchedule=schedule)

    return workflow
