from typing import List
from loguru import logger


def create_table_from_dataframes(duckdb_con, dataframes: dict):
    """
    Create tables from a dictionary of DataFrame objects in DuckDB.

    Parameters:
    - duckdb_con: The DuckDB connection object.
    - dataframes: A dictionary containing table names as keys and DataFrame objects as values.

    Returns:
    None

    Raises:
    - Exception: If there is an error while creating a table in DuckDB.

    """
    for table_name, dataframe in dataframes.items():
        try:
            # Create table in DuckDB
            duckdb_con.sql(
                f"""
                CREATE TABLE {table_name} AS 
                    SELECT * 
                    FROM dataframe
                    """, {'dataframe': dataframe}
                    )
            logger.info(f"Table {table_name} created successfully in DuckDB")
        except Exception as e:
            logger.error(f"Error while creating table {table_name} in DuckDB: {e}")
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
    duckdb_con, table: str, s3_path: str
):
    """
    Writes data from DuckDB to S3.

    Args:
        duckdb_con: The DuckDB connection object.
        table (str): The name of the table to write.
        s3_path (str): The S3 path to write the data to.

    Returns:
        None
    """
    logger.info(f"Writing data to S3 {s3_path}/{table}")
    duckdb_con.sql(
        f"""
        COPY (
            SELECT *
            FROM {table}
        ) 
        TO '{s3_path}/{table}' 
        (FORMAT PARQUET);
    """
    )


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
    logger.info(f"Writing data to motherduck {remote_database}.main.{table}")
    duckdb_con.sql(f"CREATE DATABASE IF NOT EXISTS {remote_database}")
    duckdb_con.sql(
        f"CREATE TABLE IF NOT EXISTS {remote_database}.{table} AS SELECT * FROM {local_database}.{table} limit 0"
    )
    # Insert new data
    duckdb_con.sql(
        f"""
    INSERT INTO {remote_database}.main.{table}
    SELECT *
        FROM {local_database}.{table}"""
    )