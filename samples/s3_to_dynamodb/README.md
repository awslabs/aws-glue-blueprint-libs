# README: Blueprint - Importing S3 data into a DynamoDB table
 
## Overview

This blueprint imports data from a designated Amazon S3 location into a DynamoDB table.

## Resources

```
s3_to_dynamodb/
```

## How to use it
### Input/Output

* Input
    * Input files on Amazon S3
* Output
    * Data imported into DynamoDB table.

### Parameters

* WorkflowName: Name for the workflow.
* IAMRole: IAM role used by the generated job.
* InputDataLocation: Input data location (Amazon S3 path). Data is read from this location.
* InputDataFormat: Input file format. 
    * Valid values are '`csv`', '`json`', '`parquet`', and '`orc`'.
* (Optional) InputDataFormatOptions: A JSON string of format options.
    * See details in [Format Options for ETL Inputs and Outputs in AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format.html)
* DynamoDbTableName: Name of DynamoDB table to import data into. Must already exist.
* DynamoDbWriteThroughputPercent: DynamoDB write throughput percent. 
   * Valid values are from "0.1" to "1.5", inclusive. Default value is 0.5.
* NumberOfWorkers: The number of G.1X workers in glue job.

## Prerequisite
You must create a destination table with primary key definitions.

Create a new DynamoDB table with the following configuration:
  - Table name: `covid`
  - Partition key: `iso_code`
  - Sort key: `date` 
  - Settings: default

## Tutorial

1. Download the files.
2. Compress the blueprint files into a zip archive.
    
    $ zip s3_to_dynamodb.zip s3_to_dynamodb/*
3. Upload `s3_to_dynamodb.zip` to an S3 bucket.
    
    $ aws s3 cp s3_to_dynamodb.zip s3://path/to/blueprint/
4. Sign in to the AWS Glue console, and in the navigation pane, choose **Blueprints**.
5. Choose **Add blueprint**.
6. Specify `s3_to_dynamodb-tutorial` in **Blueprint name** and `s3://path/to/blueprint/s3_to_dynamodb.zip` in **ZIP archive location (S3).**. Then choose **Add blueprint**.
7. Wait for the blueprint to be **ACTIVE**.
8. Select your `s3_to_dynamodb-tutorial` blueprint, and choose **Create workflow** from the **Actions** menu.
9. Specify parameters and choose **Submit**.
    1. WorkflowName: `s3_to_dynamodb`
    2. IAMRole: `GlueServiceRole`
    3. InputDataLocation: `s3://covid19-lake/rearc-covid-19-world-cases-deaths-testing/json/`
    4. InputDataFormat: `json`
    5. DynamoDbTableName: `covid`
    5. DynamoDbWriteThroughputPercent: `0.5` (Use default value)
    6. NumberOfWorkers: `5` (Use default value)
    7. IAM role: `GlueServiceRole`
        
        Note: This role is used to create the entities in workflow.
10. Wait for the blueprint run to be **SUCCEEDED**.
11. Choose **Workflows** in the navigation pane.
12. Select the `s3_to_dynamodb` workflow and choose **Run** from the **Actions** menu.