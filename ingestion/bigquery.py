import os
import time
from typing import Dict, List, Optional

import pyarrow as pa
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery
from google.oauth2 import service_account
from loguru import logger

from ingestion.models import EcommerceJobParameters, TABLES_WITH_TIMESTAMP

ECOMMERCE_PUBLIC_DATASET = "bigquery-public-data.thelook_ecommerce"

_MAX_RETRIES = 3


def build_ecommerce_query(
    params: EcommerceJobParameters,
    ecom_public_dataset: str = ECOMMERCE_PUBLIC_DATASET,
    watermarks: Optional[Dict[str, str]] = None,
) -> List[str]:
    """
    Generate incremental SQL queries for each requested table.
    When a watermark exists for a table, only rows newer than the
    last recorded created_at are fetched.
    """
    watermarks = watermarks or {}
    queries: List[str] = []

    for table_name in params.table_names:
        if not table_name:
            logger.warning(f"Skipping empty table name")
            continue

        query = f"SELECT * FROM `{ecom_public_dataset}.{table_name}`"

        if table_name in TABLES_WITH_TIMESTAMP and table_name in watermarks:
            last_ts = watermarks[table_name]
            query += f" WHERE created_at > TIMESTAMP('{last_ts}')"
            logger.info(f"Incremental load for '{table_name}' since {last_ts}")
        else:
            logger.info(f"Full load for '{table_name}'")

        queries.append(query)

    return queries


def get_bigquery_client(project_name: str) -> bigquery.Client:
    """Build a BigQuery client from a service account file or ambient credentials."""
    try:
        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if service_account_path:
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path
            )
            return bigquery.Client(project=project_name, credentials=credentials)
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS is not set.")
    except DefaultCredentialsError as e:
        raise e


def _execute_with_retry(
    bigquery_client: bigquery.Client, query: str, table_name: str
) -> pa.Table:
    """Execute a BigQuery query and return a PyArrow table, with exponential backoff retry."""
    for attempt in range(_MAX_RETRIES):
        try:
            start = time.time()
            result = bigquery_client.query(query).to_arrow()
            elapsed = time.time() - start
            logger.info(
                f"'{table_name}' fetched {result.num_rows:,} rows in {elapsed:.2f}s"
            )
            return result
        except Exception as exc:
            if attempt == _MAX_RETRIES - 1:
                logger.error(f"All {_MAX_RETRIES} attempts failed for '{table_name}': {exc}")
                raise
            wait = 2 ** attempt
            logger.warning(
                f"Attempt {attempt + 1}/{_MAX_RETRIES} failed for '{table_name}': {exc}. "
                f"Retrying in {wait}s..."
            )
            time.sleep(wait)


def get_bigquery_results(
    queries: List[str],
    table_names: List[str],
    bigquery_client: bigquery.Client,
) -> Dict[str, pa.Table]:
    """Execute queries and return results keyed by table name."""
    tables: Dict[str, pa.Table] = {}
    for query, table_name in zip(queries, table_names):
        tables[table_name] = _execute_with_retry(bigquery_client, query, table_name)
    return tables
