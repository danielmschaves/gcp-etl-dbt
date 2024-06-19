from typing import List
from loguru import logger


def create_table_from_pyarrow_tables(duckdb_con, pyarrow_tables: dict):
    """
    Create tables from a dictionary of PyArrow Table objects in DuckDB.

    Parameters:
    - duckdb_con: The DuckDB connection object.
    - pyarrow_tables: A dictionary containing table names as keys and PyArrow Table objects as values.

    Returns:
    None

    Raises:
    - Exception: If there is an error while creating a table in DuckDB.
    """
    for table_name, arrow_table in pyarrow_tables.items():
        try:
            # Temporarily register the PyArrow table to make it available for SQL operations
            duckdb_con.register('temp_arrow_table', arrow_table)
            # Create table in DuckDB from the registered PyArrow table
            duckdb_con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_arrow_table")
            # Unregister the temporary table to clean up
            duckdb_con.unregister('temp_arrow_table')
            logger.info(f"Table {table_name} created successfully in DuckDB from PyArrow Table")
        except Exception as e:
            logger.error(f"Error while creating table {table_name} in DuckDB from PyArrow Table: {e}")
            raise


def connect_to_md(duckdb_con, motherduck_token: str):
    """
    Connects to the Mother Duck database using the provided DuckDB connection and Mother Duck token.

    Args:
        duckdb_con (DuckDBConnection): The DuckDB connection object.
        motherduck_token (str): The token for authenticating with the Mother Duck database.

    Returns:
        None
    """
    duckdb_con.sql(f"INSTALL md;")
    duckdb_con.sql(f"LOAD md;")
    duckdb_con.sql(f"SET motherduck_token='{motherduck_token}';")
    duckdb_con.sql(f"ATTACH 'md:'")


def load_aws_credentials(duckdb_con, profile: str):
    """
    Loads AWS credentials for the specified profile into the DuckDB connection.

    Parameters:
    - duckdb_con: DuckDB connection object
    - profile: AWS profile name

    Returns:
    None
    """
    duckdb_con.sql(f"CALL load_aws_credentials('{profile}');")


def write_to_s3_from_duckdb(
    duckdb_con, tables: List[str], s3_path: str
):
    """
    Writes specified tables from DuckDB to S3.

    Args:
        duckdb_con: The DuckDB connection object.
        tables (List[str]): The names of the tables to write.
        s3_path (str): The S3 path to write the data to.

    Returns:
        None
    """
    for table in tables:
        logger.info(f"Writing data to S3 {s3_path}/{table}")
        try:
            duckdb_con.execute(
                f"""
                COPY (
                    SELECT *
                    FROM {table}
                ) 
                TO '{s3_path}/{table}.parquet' 
                (FORMAT PARQUET);
                """
            )
            logger.info(f"Successfully wrote {table} to S3 at {s3_path}/{table}.parquet")
        except Exception as e:
            logger.error(f"Error writing {table} to S3: {e}")
            raise


def write_to_md_from_duckdb(
    duckdb_con,
    table: str,
    local_database: str,
    remote_database: str
):
    """
    Writes data from a DuckDB table to Motherduck.

    Args:
        duckdb_con: The DuckDB connection object.
        table (str): The name of the table to write data from.
        local_database (str): The name of the local database.
        remote_database (str): The name of the remote database.

    Returns:
        None
    """
    try:
        logger.info(f"Writing data to motherduck {remote_database}.main.{table}")
        duckdb_con.execute(f"CREATE DATABASE IF NOT EXISTS {remote_database}")
        duckdb_con.execute(
            f"CREATE TABLE IF NOT EXISTS {remote_database}.{table} AS SELECT * FROM {local_database}.{table} LIMIT 0"
        )
        # Insert new data
        duckdb_con.execute(
            f"""
            INSERT INTO {remote_database}.main.{table}
            SELECT *
            FROM {local_database}.{table}
            """
        )
    except Exception as e:
        logger.error(f"Failed to write data to motherduck: {e}")
        raise