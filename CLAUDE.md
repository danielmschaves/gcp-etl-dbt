# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

E-commerce data pipeline ingesting from the BigQuery public dataset `thelook_ecommerce`. Data flows: BigQuery → DuckDB → S3/MotherDuck → dbt transforms → Streamlit dashboard.

## Commands

All commands assume a `.env` file is present at the project root (the Makefile auto-exports its contents).

```bash
# Install dependencies
make install          # poetry install

# Ingestion
make data-ingestion   # Extract from BigQuery, validate, load to DuckDB, sink to destination(s)
make pipeline-test    # pytest ingestion/tests

# dbt (must be run from transform/gcp_etl_dbt/ or via Makefile)
make dbt-transform    # dbt run in $DBT_FOLDER
make dbt-debug        # dbt debug
make dbt-compile      # dbt compile

# Run a single dbt model
cd transform/gcp_etl_dbt && dbt run --select <model_name>

# Run dbt tests
cd transform/gcp_etl_dbt && dbt test

# Dashboard
make dashboard        # streamlit run dashboard/app.py

# Formatting
make format           # ruff format .
```

## Architecture

### Ingestion (`ingestion/`)

Python ETL orchestrated by `pipeline.py` using the `fire` CLI library. Run as a module: `python -m ingestion.pipeline`. It:
1. Loads watermarks from `logs/watermarks.json` and passes them to `bigquery.py`, which generates incremental `WHERE created_at > {last_ts}` queries for tables that support it (`TABLES_WITH_TIMESTAMP` in `models.py`). First run is always a full load.
2. Fetches from BigQuery with 3-attempt exponential backoff retry (`bigquery.py:_execute_with_retry`).
3. Validates each PyArrow table using vectorised column-level null checks — O(columns), not O(rows) (`models.py:validate_table`). Tables failing validation are skipped (logged), not abort the run.
4. Loads validated tables into an in-memory DuckDB connection via `duck.py`.
5. Sinks based on `DESTINATION` env var: `local` (CSV), `s3` (Parquet), or `md` (MotherDuck upsert keyed on primary key — no duplicate rows on rerun).
6. Advances and saves watermarks for the next run.

Tests live in `ingestion/tests/` and load sample data from `data/` CSV files.

### Transform (`transform/gcp_etl_dbt/`)

dbt project (`gcp_etl_dbt` profile) backed by `dbt-duckdb` with two targets:

| Target | Backend | Use |
|--------|---------|-----|
| `dev`  | Local `dbt.duckdb` file | Development |
| `prod` | MotherDuck (`md:ecommerce`) | Production |

Both targets require AWS credentials because **sources are external parquet files read from S3** via the `httpfs` DuckDB extension. The source location is `TRANSFORM_S3_PATH_INPUT/{table_name}.parquet`.

**Model layers:**
- `staging/` — views (`+materialized: view`). One model per raw source table (`stg_distribution_centers`, `stg_events`, `stg_inventory_items`, `stg_order_items`, `stg_orders`, `stg_products`, `stg_users`).
- `marts/` — tables (`+materialized: table`), materialized into the `gold` DuckDB schema:
  - Dimensions: `dim_date`, `dim_orders`, `dim_products`, `dim_users`
  - Fact: `fact_order_items` (joins order items → products → orders, uses `format_date_key` macro to produce integer date keys)
  - Analytics marts: `mart_revenue_over_time`, `mart_top_selling_products`, `mart_sales_by_category`, `mart_sales_by_country`, `mart_customer_demographics`, `mart_top_customers`, `mart_average_time_to_ship`, `mart_order_status_distribution`

**Macros:** `format_date_key(date_column)` converts a timestamp to a `YYYYMMDD` integer key used by `fact_order_items` and `dim_date`.

**Packages:** `dbt_utils` (≥0.8.0) and `dbt-unit-testing` (v0.4.12).

### Dashboard (`dashboard/app.py`)

Streamlit app that connects directly to `transform/gcp_etl_dbt/dbt.duckdb` and queries the `main_gold` schema (e.g. `SELECT * FROM main_gold.mart_revenue_over_time`). Requires dbt to have been run first with the `dev` target.

## Environment Variables

Copy `.env.example` (or create `.env`) with:

```
GOOGLE_APPLICATION_CREDENTIALS=   # path to GCP service account JSON
GCP_PROJECT=                       # GCP project name
TABLE_NAMES=                       # comma-separated list (e.g. users,orders)
DESTINATION=                       # local, s3, or md (comma-separated)
S3_PATH=                           # s3://bucket-name
TRANSFORM_S3_PATH_INPUT=           # s3://bucket/path (parquet source for dbt)
TRANSFORM_S3_PATH_OUTPUT=          # s3://bucket/path
AWS_PROFILE=                       # named AWS profile
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=
MOTHERDUCK_TOKEN=                  # required for prod dbt target or md destination
DBT_FOLDER=transform/gcp_etl_dbt  # used by Makefile dbt targets
DBT_TARGET=dev                     # dev or prod
```
