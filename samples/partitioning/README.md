# README: Blueprint - Partitioning

## Overview    

A very common request is to make data lakes faster and more useful. 
To achieve this, it's important to perform appropriate ETL against your data lake. 
We created this blueprint based on our experience with typical ETL workloads.

In a typical ETL use case, a common best practice is to partition data based on specific fields in the existing records.
With a well-designed partition schema, you can reduce unneeded data scans and accelerate your analytic workloads.
With this blueprint, you can achieve this by using a simple wizard, without writing complex ETL code and wrangling data.

This blueprint enables you to create a typical partitioning job that places output files into Hive-style partitions based on specific partition keys.
It also converts input files into parquet format at the same time.
You can optionally generate common partition keys like year/month/day based on your timestamp column.


## Use cases

### Basic partitioning

With this blueprint, you can partition data based on specific fields that exist in the records.
The output data is written into the destination using a Hive-style partition schema.

### Timestamp-based partitioning

Apache Hive supports Hive-style partition schemas like `path_to_dataset/year=2020/month=6/day=1/`. 

It's important to move data into locations that follow Hive-compatible partition schemas before performing analytic/ML workloads. 
Apache Hive can treat any column as a partition key. However, there are common styles like `dt=yyyyMMdd/` or `year=yyyy/month=MM/day=dd/`. 
One of the reasons is that common analytic queries tend to be performed within a date range. 
To make existing schemas into Hive-compatible schemas based on these styles, a timestamp column is usually used to generate partition keys. 
With this blueprint, you can perform Hive-style partitioning based on auto-generated partition keys from your timestamp column (e.g. `year=yyyy/month=MM/day=dd/)`.

## Resources

```
partitioning/
```

## How to use it

### Input/Output

* Input
    * Input files (json, csv, etc.)
* Output
    * Converted files (parquet)
    * Table definition in the AWS Glue data catalog

### Parameters

* WorkflowName: Name for the workflow.
* IAMRole: IAM role used for the generated crawlers and jobs.
* InputDataLocation: Input data location (Amazon S3 path). Data is read from this location.
* DestinationDatabaseName: Name of a destination database used to store the new table.
* DestinationTableName: A destination table name in the Data Catalog.
* OutputDataLocation: Output data location (Amazon S3 path). Data is written into this location.
* (Optional) PartitionKeys: Comma-separated column names to use as partition keys. 
    * If you do not provide partition keys, the data will be written into tables directly without any partitions. 
    * If you provide both `PartitionKeys` and `TimestampColumnName`, then this option is ignored.
* (Optional) TimestampColumnName: A timestamp column name used to partition data based on a timestamp value.
    * `TimestampColumnName` needs to be able to be cast to the Timestamp type by the DynamicFrame ApplyMapping class. (e.g. 'yyyy-mm-dd hh:mm:ss', 'yyyy-mm-dd', etc.)
* (Optional) TimestampColumnGranularity: A timestamp granularity. 
    * Valid values are '`year`', '`month`', '`day`', '`hour`', and '`minute`'. 
    * For example, if you choose '`day`' here, '`year`', '`month`', and '`day`' will be used as partition keys. 
    * If you choose '`hour`' here, '`year`', '`month`', '`day`', and '`hour`' will be used as partition keys. 
    * If you do not choose `TimestampColumnName`, then this option is ignored.
* Frequency: The frequency with which to trigger this ETL workflow. 
* FrequencyCronFormat: A custom cron format as a 'Custom' frequency setting. 
* NumberOfWorkers: The number of G.1X workers in the AWS Glue job.

### Considerations

* This blueprint is for a single source Amazon S3 path.
* Whether there are multiple files or multiple folders under the source, they should all have the same schema.
* Data in the source is written to the destination S3 path in a partitioned way. Partitioning is performed based on the keys provided by user as an input.


### Limitations

* This blueprint supports append-only input files. Running the blueprint with updates to data that was already processed results in duplicate data in the destination due to the use of job bookmarks.
* If you want to process the entire data set as a snapshot instead of processing the data set incrementally, you will need to clean up the output data location (or specify a new output data location), reset the job bookmark, and run the workflow.
* The current implementation doesn’t guarantee to preserve the order of columns in the destination. This can break downstream automated queries, such as with Amazon Athena, where the schema is expected to have ordering. (i.e when customers use queries with `SELECT *` or column identifiers like `SELECT 1, COUNT(*) FROM t GROUP BY 1`, etc.)

## Tutorial

1. Download the files.
2. Compress the blueprint files into a zip archive.
    
    $ zip partitioning.zip partitioning/*
3. Upload `partitioning.zip` to an Amazon S3 bucket.
    
    $ aws s3 cp partitioning.zip s3://path/to/blueprint/.
4. Sign in to the AWS Glue console and choose **Blueprints**.
5. Choose **Add blueprint**.
6. Specify `partitioning-tutorial` in **Blueprint name**, and `s3://path/to/blueprint/partitioning.zip` in **ZIP archive location (S3).**
7. Choose **Add blueprint**.
8. Wait for the blueprint to be **ACTIVE**
9. Select your `partitioning-tutorial` blueprint, and choose **Create workflow** from the **Actions** menu.
10. Specify the parameters and choose **Submit**.
    1. WorkflowName: `partitioning`
    2. IAMRole: `GlueServiceRole`
    3. InputDataLocation: `s3://covid19-lake/enigma-aggregation/json/global/`
    4. DestinationDatabaseName: `blueprint_tutorial`
    5. DestinationTableName: `partitioning`
    6. OutputDataLocation: `s3://path/to/output/data/location/`
    7. PartitionKeys: ``
    8. TimestampColumnName: `date`
    9. TimestampColumnGranularity: `day`
    10. NumberOfWorkers: `5` (Use default value)
    11. IAM role: `GlueServiceRole`
         
         Note: This role is used to create the entities in the workflow.
11. Wait for the blueprint run to be **SUCCEEDED**.
12. Choose **Workflows** in the navigation pane.
13. Select the `partitioning` workflow and choose **Run** from the **Actions** menu.