from datetime import datetime
from loguru import logger
import fire
import os
import duckdb
import argparse

from bigquery import (
    get_bigquery_client,
    get_bigquery_results,
    build_ecommerce_query,
)
from duck import (
    create_table_from_pyarrow_tables,
    load_aws_credentials,
    write_to_s3_from_duckdb,
    write_to_md_from_duckdb,
    connect_to_md,
)
from models import (
    TableValidationError,
    EcommerceJobParameters,
    validate_table
)

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='ETL pipeline for Ecommerce data.')
    parser.add_argument('--gcp_project', required=True, help='GCP project name')
    parser.add_argument('--table_names', nargs='+', help='List of table names', required=True)
    parser.add_argument('--destination', required=True, help='Destination for the data')
    parser.add_argument('--s3_path', help='S3 path for data storage')
    parser.add_argument('--aws_profile', help='AWS profile for S3 access')
    args = parser.parse_args()
    return EcommerceJobParameters(
        gcp_project=args.gcp_project,
        table_names=args.table_names,
        destination=args.destination,
        s3_path=args.s3_path,
        aws_profile=args.aws_profile
    )


def main(params: EcommerceJobParameters):
    """
    Executes the main ETL pipeline for the Ecommerce job.

    Args:
        params (EcommerceJobParameters): The parameters for the Ecommerce job.

    Returns:
        None
    """
    start_time = datetime.now()
    bigquery_client = get_bigquery_client(project_name=params.gcp_project)
    conn = duckdb.connect()

    queries = build_ecommerce_query(params)  

    pyarrow_tables = get_bigquery_results(
        queries=queries,
        table_names=params.table_names,
        bigquery_client=bigquery_client,
    )

    # Iterate through the returned dictionary of PyArrow tables
    for table_name, pa_tbl in pyarrow_tables.items():
        # Validate the PyArrow table with the respective model
        try:
            logger.info(f"Validating table: {table_name}")
            validate_table(pa_tbl, table_name)
            logger.info(f"Validation successful for table: {table_name}")
        except TableValidationError as e:
            logger.error(f"Validation failed for table: {table_name} with error: {e}")
            continue  # Or handle the error as needed

        # Add the PyArrow table to the dictionary with the table name as the key
        pyarrow_tables[table_name] = pa_tbl

    # Loading to DuckDB
    create_table_from_pyarrow_tables(
        duckdb_con=conn,
        pyarrow_tables=pyarrow_tables,
    )

    for table_name in params.table_names:
        logger.info(f"Sinking data to {params.destination}")
        if "local" in params.destination:
            conn.execute(f"COPY {table_name} TO '{table_name}.csv';")

        if "s3" in params.destination:
            load_aws_credentials(conn, params.aws_profile)
            write_to_s3_from_duckdb(
                duckdb_con=conn, tables=[table_name], s3_path=params.s3_path
            )

        if "md" in params.destination:
            connect_to_md(conn, os.environ["motherduck_token"])
            write_to_md_from_duckdb(
                duckdb_con=conn,
                table=table_name,
                local_database="local",
                remote_database="ecommerce",
            )

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    logger.info(
        f"Total job completed in {elapsed // 60} minutes and {elapsed % 60:.2f} seconds."
    )

if __name__ == "__main__":
    params = parse_args()
    main(params)