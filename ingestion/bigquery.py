import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from loguru import logger
from typing import List
import time
from models import EcommerceJobParameters 
import pandas as pd

ECOMMERCE_PUBLIC_DATASET = "bigquery-public-data.thelook_ecommerce"

def build_ecommerce_query(params: EcommerceJobParameters, ecom_public_dataset: str = ECOMMERCE_PUBLIC_DATASET) -> List[str]:
    """
    Generate SQL queries to query specific tables based on provided parameters.

    Args:
        params (EcommerceJobParameters): The parameters for the Ecommerce job.
        ecom_public_dataset (str, optional): The name of the Ecommerce public dataset. Defaults to ECOMMERCE_PUBLIC_DATASET.

    Returns:
        List[str]: A list of SQL queries.

    """
    queries = []
    for table_name in params.table_names:
        if table_name:
            query = f"SELECT * FROM `{ecom_public_dataset}.{table_name}`"
            queries.append(query)
        else:
            logger.warning(f"Invalid table name provided: {table_name}")
    return queries

def get_bigquery_client(project_name: str) -> bigquery.Client:
    """
    Get BigQuery client.

    Args:
        project_name (str): The name of the BigQuery project.

    Returns:
        bigquery.Client: The BigQuery client object.

    Raises:
        EnvironmentError: If no valid credentials are found for BigQuery authentication.
        DefaultCredentialsError: If there is an error with the default credentials.

    """
    try:
        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if service_account_path:
            credentials = service_account.Credentials.from_service_account_file(service_account_path)
            return bigquery.Client(project=project_name, credentials=credentials)
        raise EnvironmentError("No valid credentials found for BigQuery authentication.")
    except DefaultCredentialsError as creds_error:
        raise creds_error
    
def get_bigquery_results(queries: List[str], table_names: List[str], bigquery_client: bigquery.Client) -> dict:
    """
    Execute queries and return results as a dictionary of pandas DataFrames.

    Args:
        queries (List[str]): A list of SQL queries to execute.
        table_names (List[str]): A list of table names corresponding to each query.
        bigquery_client (bigquery.Client): The BigQuery client object used to execute the queries.

    Returns:
        dict: A dictionary where the keys are table names and the values are pandas DataFrames containing the query results.
    """
    dataframes = {}
    for query, table_name in zip(queries, table_names):
        try:
            logger.info(f"Running query for table: {table_name}")
            start_time = time.time()
            df = bigquery_client.query(query).to_dataframe()
            elapsed_time = time.time() - start_time
            logger.info(f"Query for {table_name} executed and data loaded in {elapsed_time:.2f} seconds")
            dataframes[table_name] = df 
        except Exception as e:
            logger.error(f"Error running query for {table_name}: {e}")
            raise
    return dataframes  
