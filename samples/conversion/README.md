# README: Blueprint - File Format Conversion

## Overview

A very common request is to make the data lake faster and more useful. To achieve this, it's important to perform the appropriate ETL against your data lake. We created this blueprint based on our experience with typical ETL workloads.

A common ETL best practice is to convert csv/json files into a columnar format like Parquet.
Another best practice is to compress files using an appropriate compression codec.
Both are essential to optimize common analytic workloads.

With this blueprint, you can achieve both best practices by using a simple wizard, without writing complex ETL code and wrangling data.
It converts input files from various kind of standard file formats into parquet format which is applicable for analytic workload.

## Use cases

In a typical batch processing use case---for example, periodic uploads to Amazon S3---there might be files in json or csv format that are not optimized for analytic workloads.
Sometimes they are not compressed or are compressed in non-splittable format (like gzip).
With this blueprint, you can convert data into an appropriate format with a preferred compression codec.

## Resources

```
conversion/
```

## How to use it

### Input/Output

* Input
    * Input files (json, csv, etc.)
* Output
    * Converted files (parquet)
    * Table definition

### Parameters

* WorkflowName: Name for the workflow.
* IAMRole: IAM role used for crawler and job.
* InputDataLocation: Input data location (Amazon S3 path). Data is read from this location.
* DestinationDatabaseName: A destination database name used to store a new table.
* DestinationTableName: A destination table name in the Data Catalog.
* OutputDataLocation: Output data location (S3 path). Data is written into this location.
* Frequency: The frequency to trigger this ETL workflow with.
* FrequencyCronFormat: A custom cron format as 'Custom' frequency setting. 
* NumberOfWorkers: The number of G.1X workers in AWS Glue job.

### Considerations

* This blueprint is for a single source s3 path.
* Whether there are multiple files or multiple folders under the source, they should all have the same schema.
* Data in the source is converted to parquet format using the Snappy codec and written to the destination S3 path.
* If data is partitioned in the source, the partitioned structure is retained in the destination.

### Limitations

* This blueprint supports append-only input files. Running the blueprint with updates to data that was already processed results in duplicate data in the destination due to the use of job bookmarks.
* If you want to process the entire data set as a snapshot instead of processing the data set incrementally, you will need to clean up the output data location (or specify a new output data location), reset the job bookmark, and run the workflow.
* The current implementation doesn’t guarantee to preserve the order of the columns in the destination. This can break automated queries in downstream processes like Athena queries, where the schema is expected to have ordering. (i.e when customers use queries with SELECT * or column identifiers like SELECT 1, COUNT(*) FROM t GROUP BY 1, etc.)

## Tutorial

1. Download the files
2. Compress the blueprint files into zip
    1. $ zip conversion.zip conversion/*
3. Upload `conversion.zip` to your S3 bucket
    1. $ aws s3 cp conversion.zip s3://path/to/blueprint/
4. Open the AWS Glue console and choose **Blueprints**
5. Choose **Add blueprint**
6. Specify `conversion-tutorial` in **Blueprint name**, and `s3://path/to/blueprint/conversion.zip` in **ZIP archive location (S3).** Then choose **Add blueprint**.
7. Wait for the blueprint to be **ACTIVE**
8. Select your `conversion-tutorial` blueprint, and choose **Create workflow** from the **Actions** menu
9. Specify parameters and choose **Submit**
    1. WorkflowName: `conversion`
    2. IAMRole: `GlueServiceRole`
    3. InputDataLocation: `s3://covid19-lake/covidcast/json/data/fb-survey/`
    4. DestinationDatabaseName: `blueprint_tutorial`
    5. DestinationTableName: `conversion`
    6. OutputDataLocation: `s3://path/to/output/data/location/`
    7. NumberOfWorkers: `5` (Use default value)
    8. IAM role: `GlueServiceRole`
        1. Note: This role is to create the entities in workflows.
10. Wait for the blueprint run to be **CREATED**
11. Choose **Workflows** in the navigation pane.
12. Select `conversion` workflow and choose **Run** from the **Actions** menu
