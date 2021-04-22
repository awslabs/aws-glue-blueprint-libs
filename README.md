# aws-glue-blueprint-libs

This repository provides a Python library to build and test AWS Glue Custom Blueprints locally. It also provides sample blueprints addressing common use-cases in ETL.

### Sample Blueprints
* Crawling Amazon S3 locations: This blueprint crawls multiple Amazon S3 locations to add metadata tables to the Data Catalog.

* Importing Amazon S3 data into a DynamoDB table: This blueprint imports data from Amazon S3 into a DynamoDB table.

* Conversion: This blueprint converts input files in various standard file formats into Apache Parquet format, which is optimized for analytic workloads.

* Converting character encoding: This blueprint converts your non-UTF files into UTF encoded files.

* Compaction: This blueprint creates a job that compacts input files into larger chunks based on desired file size.

* Partitioning: This blueprint creates a partitioning job that places output files into partitions based on specific partition keys.

* Importing an AWS Glue table into a Lake Formation governed table: This blueprint imports a Glue Catalog table into a Lake Formation governed table.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.