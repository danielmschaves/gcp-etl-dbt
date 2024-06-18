import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from loguru import logger
import time
from models import EcommerceJobParameters  # Make sure this import points to the right location
import pandas as pd

ECOMMERCE_PUBLIC_DATASET = "bigquery-public-data.thelook_ecommerce"

def build_ecommerce_query(params: EcommerceJobParameters, table_name: str, ecom_public_dataset: str = ECOMMERCE_PUBLIC_DATASET) -> str:
    """Generate SQL to query a specific table based on provided parameters."""
    if table_name in params.table_names:
        return f"SELECT * FROM `{ecom_public_dataset}.{table_name}`"
    else:
        return None  # Return None if the table name is not in the list or invali

def get_bigquery_client(project_name: str) -> bigquery.Client:
    """Get BigQuery client"""
    try:
        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if service_account_path:
            credentials = service_account.Credentials.from_service_account_file(service_account_path)
            return bigquery.Client(project=project_name, credentials=credentials)
        raise EnvironmentError("No valid credentials found for BigQuery authentication.")
    except DefaultCredentialsError as creds_error:
        raise creds_error
    
def get_bigquery_result(query_str: str, bigquery_client: bigquery.Client) -> pd.DataFrame:
    """Execute the query and return the results as a pandas DataFrame."""
    try:
        logger.info(f"Running query: {query_str}")
        start_time = time.time()
        dataframe = bigquery_client.query(query_str).to_dataframe()
        elapsed_time = time.time() - start_time
        logger.info(f"Query executed and data loaded in {elapsed_time:.2f} seconds")
        return dataframe
    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise
