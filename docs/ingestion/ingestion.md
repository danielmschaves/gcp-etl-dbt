# Ecommerce Data Pipeline Documentation

## Overview

This project implements a data pipeline that ingests data from the BigQuery public dataset "thelook_ecommerce". The pipeline extracts data from specified tables, validates it using Pydantic models, and loads it into DuckDB. From there, the data can be written to local CSV files, Amazon S3, or MotherDuck.

## Requirements

- Python 
- Poetry
- Google Cloud Account

## Module Descriptions

### bigquery.py

This module handles interactions with Google BigQuery.

#### Functions:

1. `build_ecommerce_query(params: EcommerceJobParameters, ecom_public_dataset: str) -> List[str]`
   - Generates SQL queries for specified tables in the ecommerce dataset.

2. `get_bigquery_client(project_name: str) -> bigquery.Client`
   - Creates and returns a BigQuery client.

3. `get_bigquery_results(queries: List[str], table_names: List[str], bigquery_client: bigquery.Client) -> dict`
   - Executes BigQuery queries and returns results as PyArrow tables.

### duck.py

This module handles interactions with DuckDB and data writing operations.

#### Functions:

1. `create_table_from_pyarrow_tables(duckdb_con, pyarrow_tables: dict)`
   - Creates tables in DuckDB from PyArrow tables.

2. `connect_to_md(duckdb_con, motherduck_token: str)`
   - Connects to MotherDuck database.

3. `load_aws_credentials(duckdb_con, profile: str)`
   - Loads AWS credentials for a specified profile.

4. `write_to_s3_from_duckdb(duckdb_con, tables: List[str], s3_path: str)`
   - Writes specified tables from DuckDB to S3.

5. `write_to_md_from_duckdb(duckdb_con, table: str, local_database: str, remote_database: str)`
   - Writes data from a DuckDB table to MotherDuck.

### models.py

This module defines Pydantic models for data validation and job parameters.

#### Classes:

- `DistributionCenters`, `Events`, `InventoryItems`, `OrderItems`, `Orders`, `Products`, `Users`
  - Pydantic models for each table in the dataset.

- `EcommerceJobParameters`
  - Defines parameters for the Ecommerce job.

- `TableValidationError`
  - Custom exception for DataFrame validation errors.

#### Functions:

- `validate_table(table: pa.Table, table_name: str)`
  - Validates data in a table using the corresponding Pydantic model.

### pipeline.py

This module orchestrates the entire ETL pipeline.

#### Functions:

- `main(params: EcommerceJobParameters)`
  - Executes the main ETL pipeline for the Ecommerce job.

## Data Flow

1. Extract: Data is queried from BigQuery using the specified table names.
2. Transform: Data is validated using Pydantic models.
3. Load: Validated data is loaded into DuckDB.
4. Sink: Data can be written to local CSV files, Amazon S3, or MotherDuck based on the specified destination(s).

## Usage

The pipeline can be run from the command line using the `fire` library. Example:

```bash
make data-ignestion --table_names=table_name --gcp_project=my_gcp_project --destination=local,s3 --s3_path=s3://my-bucket/data --aws_profile=default
```