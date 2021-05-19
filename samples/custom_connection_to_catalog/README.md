# README: Blueprint - Creating table definitions from Glue Custom Connection
 
## Overview

This blueprint creates Glue tables from a Glue custom connection.

This blueprint has been tested with following custom connections.
* Snowflake

## Resources

```

custom_connection_to_catalog/

```

## How to use it
### Input/Output

* Input
    * A Glue custom connection.
* Output
    * Table definitions.

### Parameters

* WorkflowName: Name for the workflow.
* GlueExecutionRole: IAM role used for the generated jobs.
* SourceConnectionName: A source custom connection name.
* ShowTablesQueryString: A query to show tables in the source custom connection.
* DestinationDatabaseName: A destination database name in the Data Catalog. The blueprint creates the database if it doens't exist.
* DestinationTableNamePrefix: (Optional) A destination table nam prefix in the Data Catalog.
* NumberOfWorkers: The number of G.1X workers in glue job.

## Prerequisite
* You must create a Glue custom connection specifying database name.

* For Snowflake, see detailed instruction here. https://aws.amazon.com/blogs/big-data/performing-data-transformations-using-snowflake-and-aws-glue/
  * JDBC string: `jdbc:snowflake://awspartner.snowflakecomputing.com/?user=${Username}&password=${Password}&warehouse=wsName&schema=public&db=dbName`

### Limitations

* As a source table name, only letters, underscores, decimal digits (0-9), and dollar signs (“$”) are allowed.
* Double-quoted identifiers in database name, table names, and column names are not supported.
* If the source tables have no record, the table schema registered in Glue Data Catalog will be empty regardless of the schema in the source tables.
* This blueprint supports the data store which can run the following two SQLs.
  * List all tables (e.g. `SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE'`).
  * Select one record (e.g. `SELECT * FROM {table_name} LIMIT 1`).

## Tutorial

1. Download the files.
2. Compress the blueprint files into a zip archive.
    
    $ zip custom_connection_to_catalog.zip custom_connection_to_catalog/*
3. Upload `custom_connection_to_catalog.zip` to an S3 bucket.
    
    $ aws s3 cp custom_connection_to_catalog.zip s3://path/to/blueprint/
4. Sign in to the AWS Glue console, and in the navigation pane, choose **Blueprints**.
5. Choose **Add blueprint**.
6. Specify `custom_connection_to_catalog-tutorial` in **Blueprint name** and `s3://path/to/blueprint/custom_connection_to_catalog.zip` in **ZIP archive location (S3).**. Then choose **Add blueprint**.
7. Wait for the blueprint to be **ACTIVE**.
8. Select your `custom_connection_to_catalog-tutorial` blueprint, and choose **Create workflow** from the **Actions** menu.
9. Specify parameters and choose **Submit**.
    1. WorkflowName: `custom_connection_to_catalog`
    2. GlueExecutionRole: `GlueServiceRole`
    3. SourceConnection: `custom_connection_name`
    4. ShowTablesQueryString: `SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE'` (Use default value)
    4. DestinationDatabaseName: `snowflake`
    5. DestinationTableNamePrefix: `snow_`
    6. NumberOfWorkers: `5` (Use default value)
    7. IAM role: `GlueServiceRole`
        
        Note: This role is used to create the entities in workflow.
10. Wait for the blueprint run to be **SUCCEEDED**.
11. Choose **Workflows** in the navigation pane.
12. Select the `custom_connection_to_catalog` workflow and choose **Run** from the **Actions** menu.