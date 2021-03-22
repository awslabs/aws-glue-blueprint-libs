# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from awsglue.blueprint.workflow import *
from awsglue.blueprint.job import *
from awsglue.blueprint.crawler import *
import boto3
s3_client = boto3.client('s3')


# Ingesting all the S3 paths as Glue table
def generate_layout(user_params, system_params):
    session = boto3.Session(region_name=system_params['region'])
    glue = session.client('glue')

    # Create Database if it does not exists
    try:
        glue.create_database(
            DatabaseInput={
                'Name': user_params['DatabaseName']
            }
        )
        print("New database is created.")
    except glue.exceptions.AlreadyExistsException:
        print("Existing database is used.")

    workflow_name = user_params['WorkflowName']
    crawlers = []
    previous_crawler = None
    for i, path in enumerate(user_params['S3Paths']):
        targets = {"S3Targets": [{"Path": path}]}
        if previous_crawler:
            crawler = Crawler(
                Name="{}_crawler".format(workflow_name) + "_" + str(i),
                Role=user_params['IAMRole'],
                DatabaseName=user_params['DatabaseName'],
                TablePrefix=user_params['TableNamePrefix'],
                Targets=targets,
                DependsOn={previous_crawler: "SUCCEEDED"}
            )
        else:
            crawler = Crawler(
                Name="{}_crawler".format(workflow_name) + "_" + str(i),
                Role=user_params['IAMRole'],
                DatabaseName=user_params['DatabaseName'],
                TablePrefix=user_params['TableNamePrefix'],
                Targets=targets
            )
        crawlers.append(crawler)
        previous_crawler = crawler

    workflow = Workflow(Name=workflow_name, Entities=Entities(Jobs=[], Crawlers=crawlers))
    return workflow
