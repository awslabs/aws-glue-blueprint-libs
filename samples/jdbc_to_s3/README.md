# README: Blueprint - Importing a JDBC table into S3

## Overview

This blueprint imports data from JDBC table located on Amazon RDS, Amazon Redshift, and on-prem databases into a designated Amazon S3 location.

## Resources

```
jdbc_to_s3/
```

## How to use it
### Input/Output

* Input
    * JDBC table
* Output
    * Output files on Amazon S3

### Parameters

* WorkflowName: Name for the workflow.
* IAMRole: IAM role used by the generated job.
* OutputDataLocation: Output data location (Amazon S3 path). Data is written into this location.
* SourceConnectionName: A source connection name.
* SourceConnectionType: A source connection type.
* SourceTableName: A source table name.
* OutputDataFormat: Output file format.
    * Valid values are '`csv`', '`json`', '`parquet`', and '`orc`'.
* (Optional) OutputDataFormatOptions: A JSON string of format options.
    * See details in [Format Options for ETL Inputs and Outputs in AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format.html)
* (Optional) PartitionKeys: Comma-separated column names to use as partition keys.
    * If you do not provide partition keys, the data will be written into tables directly without any partitions.
* NumberOfWorkers: The number of G.1X workers in glue job.

## Prerequisite
You must create a JDBC connection for the source table.

## Tutorial

1. Download the files.
2. Compress the blueprint files into a zip archive.

    $ zip jdbc_to_s3.zip jdbc_to_s3/*
3. Upload `jdbc_to_s3.zip` to an S3 bucket.

    $ aws s3 cp jdbc_to_s3.zip s3://path/to/blueprint/
4. Sign in to the AWS Glue console, and in the navigation pane, choose **Blueprints**.
5. Choose **Add blueprint**.
6. Specify `jdbc_to_s3-tutorial` in **Blueprint name** and `s3://path/to/blueprint/jdbc_to_s3.zip` in **ZIP archive location (S3).**. Then choose **Add blueprint**.
7. Wait for the blueprint to be **ACTIVE**.
8. Select your `jdbc_to_s3-tutorial` blueprint, and choose **Create workflow** from the **Actions** menu.
9. Specify parameters and choose **Submit**.
    1. WorkflowName: `jdbc_to_s3`
    2. IAMRole: `GlueServiceRole`
    3. SourceConnectionName: `redshift`
    4. SourceConnectionType: `redshift`
    5. SourceTableName: `public.covid`
    6. RedshiftIAMRoleArn: `arn:aws:iam::0123456789101:role/service-role/AmazonRedshift-Role`
    7. OutputDataLocation: `s3://path_to_your_data/`
    8. OutputDataFormat: `json`
    9. NumberOfWorkers: `5` (Use default value)
    10. IAM role: `GlueServiceRole`
        Note: This role is used to create the entities in workflow.
10. Wait for the blueprint run to be **SUCCEEDED**.
11. Choose **Workflows** in the navigation pane.
12. Select the `jdbc_to_s3` workflow and choose **Run** from the **Actions** menu.