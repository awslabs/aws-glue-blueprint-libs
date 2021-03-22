# README: Blueprint - Crawling Amazon S3 locations

## Overview

If your data is cataloged, analytics tools can discover it. If you have data in your Amazon S3 buckets, the first step to build your data lake is to use AWS Glue crawlers
to add metadata to the Glue Data Catalog. Amazon Athena, Amazon EMR, Amazon Redshift, and other services can then query the data.
With this blueprint, you can create a simple wizard to crawl multiple S3 locations add add metadata tables to the Glue Data Catalog.

## Resources

```
crawl_s3_locations/
```

## How to use it

### Input/Output

* Input
    * Amazon S3 paths
* Output
    * Table definitions in the AWS Glue Data Catalog

### Parameters

* WorkflowName: Name for the workflow
* IAMRole: IAM role to be used by the crawlers
* S3Paths: List of Amazon S3 paths for data ingestion
* Database: Name of database to contain new tables
* TablePrefix: Table prefix for the new tables

### Considerations

* The user provides a list of Amazon S3 paths. The blueprint creates one crawler per S3 path and runs them sequentially.
* The user can provide a table prefix and choose a destination database. The user has no control over table names.

### Limitations

* The current implementation doesn’t guarantee to preserve the order of columns in the created tabes.
* This can break automated queries in downstream processes like Athena queries where the schema is expected to have ordering. (i.e when customers use queries with `SELECT *` or column identifiers like `SELECT 1, COUNT(*) FROM t GROUP BY 1`, etc.)

## Tutorial

1. Download the files.
2. Compress the blueprint files into a zip archive.
    
    $ zip crawl_s3_locations.zip crawl_s3_locations/*
3. Upload `crawl_s3_locations.zip` to an S3 bucket.
    
    $ aws s3 cp crawl_s3_locations.zip s3://path/to/blueprint/
4. Sign in to the AWS Glue console, and in the navigation pane, choose **Blueprints**.
5. Choose **Add blueprint**.
6. Specify `crawlers-tutorial` in **Blueprint name** and `s3://path/to/blueprint/crawl_s3_locations.zip` in **ZIP archive location (S3).**. Then choose **Add blueprint**.
7. Wait for the blueprint to be **ACTIVE**.
8. Select the “crawlers-tutorial” blueprint, and choose **Create workflow** from the **Actions** menu.
9. Specify parameters and choose **Submit**.
    1. WorkflowName: `crawl_s3_locations`
    2. IAMRole: `GlueServiceRole`
        
        Note: The role is used by the crawlers.
    3. S3Paths
        1. Enter `s3://covid19-lake/enigma-aggregation/json/global/` and choose **Add**
        2. Enter `s3://covid19-lake/enigma-aggregation/json/global_countries/` and choose **Add**
        3. Enter `s3://covid19-lake/enigma-aggregation/json/us_counties/` and choose **Add**
        4. Enter `s3://covid19-lake/enigma-aggregation/json/us_states/` and choose **Add**
    4. Database:  `blueprint_tutorial`
    5. TablePrefix: (blank)
    6. IAM role: `GlueServiceRole`
        
       Note: This role is used to create the entities in the workflow.
10. Wait for the blueprint run to be **SUCCEEDED**.
11. Choose **Workflows** in the navigation pane.
12. Select the `crawl_s3_locations` workflow and choose **Run** from **Actions** menu.