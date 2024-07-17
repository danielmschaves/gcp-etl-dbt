import pandas as pd
import pytest
from pydantic import BaseModel, ValidationError
import duckdb
import os
from ingestion.models import (
    DistributionCenters,
    Events,
    InventoryItems,
    OrderItems,
    Orders,
    Products,
    Users
)

CSV_PATH = 'data/'

# Define a simple model for query testing
class MyModel(BaseModel):
    column1: int
    column2: str
    column3: float

def convert_nan_to_none(row):
    """Convert NaN values in a row to None."""
    return {k: (None if pd.isna(v) else v) for k, v in row.items()}

def parse_datetime_columns(df, columns):
    """Parse datetime columns to ensure compatibility with Pydantic."""
    for column in columns:
        df[column] = pd.to_datetime(df[column], errors='coerce', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

# Define fixtures for each table
@pytest.fixture
def distribution_centers():
    df = pd.read_csv(os.path.join(CSV_PATH, 'distribution_centers.csv')).head(3)
    # Validate data using Pydantic
    validated_data = [DistributionCenters(**convert_nan_to_none(row.to_dict())) for index, row in df.iterrows()]
    return validated_data

@pytest.fixture
def events():
    df = pd.read_csv(os.path.join(CSV_PATH,'events.csv')).head(3)
    # Parse datetime columns
    df = parse_datetime_columns(df, ['created_at'])
    # Validate data using Pydantic
    validated_data = [Events(**convert_nan_to_none(row.to_dict())) for index, row in df.iterrows()]
    return validated_data

@pytest.fixture
def inventory_items():
    df = pd.read_csv(os.path.join(CSV_PATH, 'inventory_items.csv')).head(3)
    # Parse datetime columns
    df = parse_datetime_columns(df, ['created_at', 'sold_at'])
    # Validate data using Pydantic
    validated_data = [InventoryItems(**convert_nan_to_none(row.to_dict())) for index, row in df.iterrows()]
    return validated_data

@pytest.fixture
def order_items():
    df = pd.read_csv(os.path.join(CSV_PATH, 'order_items.csv')).head(3)
    # Parse datetime columns
    df = parse_datetime_columns(df, ['created_at', 'shipped_at', 'delivered_at', 'returned_at'])
    # Validate data using Pydantic
    validated_data = [OrderItems(**convert_nan_to_none(row.to_dict())) for index, row in df.iterrows()]
    return validated_data

@pytest.fixture
def orders():
    df = pd.read_csv(os.path.join(CSV_PATH, 'orders.csv')).head(3)
    # Parse datetime columns
    df = parse_datetime_columns(df, ['created_at', 'shipped_at', 'delivered_at', 'returned_at'])
    # Validate data using Pydantic
    validated_data = [Orders(**convert_nan_to_none(row.to_dict())) for index, row in df.iterrows()]
    return validated_data

@pytest.fixture
def products():
    df = pd.read_csv(os.path.join(CSV_PATH, 'products.csv')).head(3)
    # Validate data using Pydantic
    validated_data = [Products(**convert_nan_to_none(row.to_dict())) for index, row in df.iterrows()]
    return validated_data

@pytest.fixture
def users():
    df = pd.read_csv(os.path.join(CSV_PATH, 'users.csv')).head(3)
    # Parse datetime columns
    df = parse_datetime_columns(df, ['created_at'])
    # Validate data using Pydantic
    validated_data = [Users(**convert_nan_to_none(row.to_dict())) for index, row in df.iterrows()]
    return validated_data

# Integration with DuckDB
@pytest.fixture
def duckdb_connection():
    conn = duckdb.connect(database=":memory:", read_only=False)
    yield conn
    conn.close()

@pytest.fixture
def load_data_into_duckdb(duckdb_connection):
    conn = duckdb_connection
    csv_files = {
        'distribution_centers': 'data/distribution_centers.csv',
        'events': 'data/events.csv',
        'inventory_items': 'data/inventory_items.csv',
        'order_items': 'data/order_items.csv',
        'orders': 'data/orders.csv',
        'products': 'data/products.csv',
        'users': 'data/users.csv'
    }
    for table, csv_file in csv_files.items():
        if table in ['orders', 'users']:
            conn.execute(f"""
                CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{csv_file}', header=True, types={{'gender': 'VARCHAR'}})
            """)
        else:
            conn.execute(f"""
                CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{csv_file}', header=True)
            """)
    return conn

# Test cases for each table
def test_distribution_centers(distribution_centers):
    assert len(distribution_centers) > 0
    assert distribution_centers[0].name == 'Memphis TN'

def test_events(events):
    assert len(events) > 0
    assert events[0].city == 'São Paulo'

def test_inventory_items(inventory_items):
    assert len(inventory_items) > 0
    assert inventory_items[0].product_id == 13844

def test_order_items(order_items):
    assert len(order_items) > 0
    assert order_items[0].order_id == 63170

def test_orders(orders):
    assert len(orders) > 0
    assert orders[0].user_id == 32

def test_products(products):
    assert len(products) > 0
    assert products[0].category == 'Accessories'

def test_users(users):
    assert len(users) > 0
    assert users[0].email == 'angelamckenzie@example.com'

# Example query testing
def test_query_on_duckdb(load_data_into_duckdb):
    conn = load_data_into_duckdb
    result = conn.execute("SELECT COUNT(*) FROM orders").fetchone()
    assert result[0] > 0

# Example validation function
def validate_table(table, model):
    errors = []
    for _, row in table.iterrows():
        try:
            model(**row)
        except ValidationError as e:
            errors.append(e)
    return errors

def test_validate_table_with_valid_data():
    valid_data = {
        "column1": [1, 2],
        "column2": ["a", "b"],
        "column3": [1.1, 2.2],
    }
    valid_table = pd.DataFrame(valid_data)
    errors = validate_table(valid_table, MyModel)
    assert not errors, f"Validation errors were found in valid data: {errors}"
