# README: Blueprint - Compaction
 
## Overview

A very common request is to make data lakes faster and more useful. 
To achieve this, it's important to perform appropriate ETL against your data lake. 
We created this blueprint based on our experience with typical ETL workloads.

In a typical ETL use case, a common best practice is to compact a large number of small files into a smaller number of larger files.
With this blueprint, you can achieve this by using a simple wizard, without writing complex ETL code and wrangling data.

This blueprint creates a typical compaction job that compacts input files into larger chunks based on desired file size. 


## Resources

```
compaction/
```

## How to use it

### Input/Output

* Input
    * Input files
* Output
    * Compacted files

### Parameters

* WorkflowName: Name for the workflow.
* IAMRole: IAM role used for the generated crawlers and jobs.
* SourceDatabaseName: A source database name in the Data Catalog.
* SourceTableName: A source table name in the Data Catalog.
* (Optional) InputDataLocation: Input data location (Amazon S3 path). Data is read from this location if this parameter is set.
* InputDataFormat: Format of the input files. 
    * Valid values are '`csv`', '`json`', '`parquet`', and '`orc`'.
* (Optional) InputDataFormatOptions: A JSON string of format option.
    * See details in [Format Options for ETL Inputs and Outputs in AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format.html)
* DestinationDatabaseName: A destination database name in the Data Catalog. The blueprint creates the database if it doens't exist.
* DestinationTableName: A destination table name in the Data Catalog.
* OutputDataLocation: Output data location (S3 path). Data is written into this location.
* EnableSizeControl: Whether you would like to control file size or not. When you enable it, each Hive partition will have files with the desired size, but it might cause performance overhead. When you disable it, each Hive partition will have a single file. Possible values are true and false.
* DesiredFileSize: A desired file size in mega bytes. It is only effective when you EnableSizeControl.
* EnableManifest: Whether to use the manifest to maintain state. If you enable the manifest, this blueprint will append the data into each partition locations. If you disable the manifest, this blueprint will overwrite all the partition locations. It is faster but it can cause data loss at the destination. Possible values are true and false.
* Frequency: The frequency with which to trigger this ETL workflow. 
* FrequencyCronFormat: A custom cron format as a 'Custom' frequency setting. 
* NumberOfWorkers: The number of G.1X workers in glue job.

### Considerations

* This blueprint is for a single source S3 path.
* Whether there are multiple files or multiple folders under the source, they should all have the same schema.

### Limitations

* Compacted data is written to a different S3 location. In-place compaction is not supported so you cannot use the same location for input and output.
* When you disable manifest, during compaction, partial data can be visible.
* When you enable size control with manifest, compaction only happens when the new data has more than desired size. When the new input data is smaller than this size, the data will be skipped so that next job run can process with larger input data.
* When you enable manifest, append-only input files are supported. Running the blueprint with updates to data that was already processed results in losing updates in the destination.

## Tutorial

1. Download the files.
2. Compress the blueprint files into a zip archive.
    
    $ zip compaction.zip compaction/*
3. Upload `compaction.zip` to an Amazon S3 bucket.
    
    $ aws S3 cp compaction.zip s3://path/to/blueprint/
4. Sign in to the AWS Glue console and in the navigation pane, choose **Blueprints**.
5. Choose **Add blueprint**.
6. Specify `compaction-tutorial` in **Blueprint name**, and `s3://path/to/blueprint/compaction.zip` in **ZIP archive location (S3).**
7. Choose **Add blueprint**.
8. Wait for the blueprint to be **ACTIVE**.
9. Select your `compaction-tutorial` blueprint, and choose **Create workflow** from the **Actions** menu.
10. Specify parameters and choose **Submit**.
    1. WorkflowName: `compaction`
    2. IAMRole: `GlueServiceRole`
    3. SourceDatabaseName: `blueprint_tutorial`
    4. SourceTableName: `conversion` (In this tutorial, the `conversion` table is used as a sample data. This table can be created by the tutorial written in the conversion blueprint.)
    5. InputDataLocation: (blank)
    6. InputDataFormat: `parquet`
    7. DestinationDatabaseName: `blueprint_tutorial`
    8. DestinationTableName: `compaction`
    9. OutputDataLocation: `s3://path/to/output/data/location/`
    10. EnableSizeControl: `false` (Use default value)
    11. DesiredFileSizeMB: `128`  (Use default value)
    12. EnableManifest: `true` (Use default value)
    13. NumberOfWorkers: `5` (Use default value)
    14. IAM role: `GlueServiceRole`
        
        Note: This role is used to create the entities in the workflow.
11. Wait for the blueprint run to be **SUCCEEDED**
12. Choose **Workflows** in the navigation pane.
13. Select the `compaction` workflow and choose **Run** from the **Actions** menu