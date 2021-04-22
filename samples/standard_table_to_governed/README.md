# README: Blueprint - Importing standard table into Lake Formation governed table
 
## Overview

This blueprint imports a standard Glue table into a Lake Formation governed table.

## Resources

```
standard_table_to_governed/
```

## How to use it
### Input/Output

* Input
    * A standard Glue table.
* Output
    * A Lake Formation governed table.

### Parameters

* WorkflowName: Name for the workflow.
* GlueExecutionRole: IAM role used for the generated jobs.
* SourceDatabaseName: A source database name in the Data Catalog.
* SourceTableName: A source table name in the Data Catalog.
* DestinationDatabaseName: A destination database name in the Data Catalog. The blueprint creates the database if it doens't exist.
* DestinationTableName: A destination table name in the Data Catalog.
* OutputDataLocation: Output data location (S3 path). Data is written into this location.
* NumberOfWorkers: The number of G.1X workers in glue job.

## Prerequisite
* You need to have a Glue Catalog table referring to their s3 path.
* You need to register your S3 path as a Lake Formation data lake location.
* You need to configure Lake Formation permissions on your IAM role `GlueExecutionRole`.
  * Database permissions
    * `Create table`, `Describe` and `Alter` permission for the database specified in `DestinationDatabaseName`.
  * Table permissions
    * `Select`, `Describe`, `Alter` and `Insert` permission for the table specified in `DestinationTableName`.

## Tutorial

1. Download the files.
2. Compress the blueprint files into a zip archive.
    
    $ zip standard_table_to_governed.zip standard_table_to_governed/*
3. Upload `standard_table_to_governed.zip` to an S3 bucket.
    
    $ aws s3 cp standard_table_to_governed.zip s3://path/to/blueprint/
4. Sign in to the AWS Glue console, and in the navigation pane, choose **Blueprints**.
5. Choose **Add blueprint**.
6. Specify `standard_table_to_governed-tutorial` in **Blueprint name** and `s3://path/to/blueprint/standard_table_to_governed.zip` in **ZIP archive location (S3).**. Then choose **Add blueprint**.
7. Wait for the blueprint to be **ACTIVE**.
8. Select your `standard_table_to_governed-tutorial` blueprint, and choose **Create workflow** from the **Actions** menu.
9. Specify parameters and choose **Submit**.
    1. WorkflowName: `standard_table_to_governed`
    2. GlueExecutionRole: `GlueServiceRole`
    3. SourceDatabaseName: `blueprint_tutorial`
    4. SourceTableName: `conversion` (In this tutorial, the `conversion` table is used as a sample data. This table can be created by the tutorial written in the conversion blueprint.)
    5. DestinationDatabaseName: `blueprint_tutorial`
    6. DestinationTableName: `governed`
    7. OutputDataLocation: `s3://path/to/output/data/location/`
    8. NumberOfWorkers: `5` (Use default value)
    9. IAM role: `GlueServiceRole`
        
        Note: This role is used to create the entities in workflow.
10. Wait for the blueprint run to be **SUCCEEDED**.
11. Choose **Workflows** in the navigation pane.
12. Select the `standard_table_to_governed` workflow and choose **Run** from the **Actions** menu.