import os
from typing import Dict, List, Optional

import pyarrow as pa
from loguru import logger

# Primary keys used for upsert deduplication in MotherDuck
_TABLE_PRIMARY_KEYS: Dict[str, str] = {
    "distribution_centers": "id",
    "events":               "id",
    "inventory_items":      "id",
    "order_items":          "id",
    "orders":               "order_id",
    "products":             "id",
    "users":                "id",
}


def create_table_from_pyarrow_tables(duckdb_con, pyarrow_tables: Dict[str, pa.Table]) -> None:
    """Load PyArrow tables into DuckDB, replacing any existing table of the same name."""
    for table_name, arrow_table in pyarrow_tables.items():
        temp_name = f"_tmp_{table_name}"
        try:
            duckdb_con.register(temp_name, arrow_table)
            duckdb_con.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {temp_name}"
            )
            duckdb_con.unregister(temp_name)
            logger.info(f"Loaded '{table_name}' into DuckDB ({arrow_table.num_rows:,} rows)")
        except Exception as exc:
            logger.error(f"Failed to load '{table_name}' into DuckDB: {exc}")
            raise


def connect_to_md(duckdb_con, motherduck_token: str) -> None:
    """Attach a MotherDuck session. Token is passed via environment to avoid SQL injection."""
    os.environ.setdefault("motherduck_token", motherduck_token)
    duckdb_con.sql("INSTALL md;")
    duckdb_con.sql("LOAD md;")
    duckdb_con.sql("ATTACH 'md:'")


def load_aws_credentials(duckdb_con, profile: str) -> None:
    """Load named AWS profile credentials into a DuckDB session."""
    duckdb_con.sql(f"CALL load_aws_credentials('{profile}');")


def write_to_s3_from_duckdb(duckdb_con, tables: List[str], s3_path: str) -> None:
    """Write DuckDB tables to S3 as Parquet files."""
    for table in tables:
        dest = f"{s3_path}/{table}.parquet"
        logger.info(f"Writing '{table}' to {dest}")
        try:
            duckdb_con.execute(
                f"COPY (SELECT * FROM {table}) TO '{dest}' (FORMAT PARQUET);"
            )
            logger.info(f"Written '{table}' to {dest}")
        except Exception as exc:
            logger.error(f"Failed to write '{table}' to S3: {exc}")
            raise


def write_to_md_from_duckdb(
    duckdb_con,
    table: str,
    remote_database: str,
    primary_key: Optional[str] = None,
) -> None:
    """
    Upsert a DuckDB table into MotherDuck.

    Uses a delete-then-insert pattern keyed on primary_key when provided,
    ensuring reruns do not accumulate duplicate rows.
    """
    pk = primary_key or _TABLE_PRIMARY_KEYS.get(table)
    remote_table = f"{remote_database}.main.{table}"

    try:
        logger.info(f"Upserting '{table}' → {remote_table}")
        duckdb_con.execute(f"CREATE DATABASE IF NOT EXISTS {remote_database}")
        duckdb_con.execute(
            f"CREATE TABLE IF NOT EXISTS {remote_table} AS SELECT * FROM {table} LIMIT 0"
        )

        if pk:
            duckdb_con.execute(
                f"DELETE FROM {remote_table} WHERE {pk} IN (SELECT {pk} FROM {table})"
            )

        duckdb_con.execute(f"INSERT INTO {remote_table} SELECT * FROM {table}")
        logger.info(f"Upserted '{table}' into {remote_table}")
    except Exception as exc:
        logger.error(f"Failed to upsert '{table}' into MotherDuck: {exc}")
        raise
