from ingestion.bigquery import (
    get_bigquery_client,
    get_bigquery_result,
    build_ecommerce_query,
)
import duckdb
from loguru import logger
from ingestion.duck import (
    create_table_from_dataframe,
    load_aws_credentials,
    write_to_s3_from_duckdb,
    write_to_md_from_duckdb,
    connect_to_md,
)
import fire
from ingestion.models import validate_dataframe, EcommerceJobParameters
import os
from loguru import logger

def main(params: EcommerceJobParameters):
    bigquery_client = get_bigquery_client(project_name=params.gcp_project)

    # Connection to DuckDB for further processing
    conn = duckdb.connect()

    for table_name in params.table_names:
        logger.info(f"Processing table: {table_name}")
        query_str = build_ecommerce_query(params, table_name)

        if query_str is None:
            logger.warning(f"No valid query could be constructed for table: {table_name}")
            continue

        df = get_bigquery_result(query_str, bigquery_client)
        if df.empty:
            logger.warning(f"No data found for table: {table_name}")
            continue

        # Load data to DuckDB for further processing
        create_table_from_dataframe(conn, table_name, df)
        logger.info(f"Data for {table_name} loaded into DuckDB successfully.")

        # Depending on the `destination`, further actions are taken
        if "local" in params.destination:
            local_file_path = f"{table_name}.csv"
            df.to_csv(local_file_path)
            logger.info(f"Data for {table_name} exported to {local_file_path}.")

        if "s3" in params.destination:
            # Assume s3_path includes the base path and aws_profile is configured properly
            s3_file_path = os.path.join(params.s3_path, f"{table_name}.csv")
            df.to_csv(s3_file_path)  # This is illustrative; actual S3 upload would require boto3
            logger.info(f"Data for {table_name} uploaded to S3 at {s3_file_path}.")

        if "md" in params.destination:
            # This is a placeholder for Motherduck or similar system export
            # Implementation would depend on the specific system's API
            logger.info(f"Data for {table_name} ready for export to Motherduck or similar system.")

if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(EcommerceJobParameters(**kwargs)))