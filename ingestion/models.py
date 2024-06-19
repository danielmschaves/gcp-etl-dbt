from pydantic import BaseModel, Field
from typing import List, Union, Annotated, Type, Dict
from pydantic import BaseModel, ValidationError
from datetime import datetime
from typing import Optional
import pandas as pd
import pyarrow as pa

DUCKDB_EXTENSION = ["aws", "httpfs"]


# Model for distribution_centers table
class DistributionCenter(BaseModel):
    id: Optional[int]
    name: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]

# Model for events table
class Event(BaseModel):
    id: Optional[int]
    user_id: Optional[int]
    sequence_number: Optional[int]
    session_id: Optional[str]
    created_at: Optional[datetime]
    ip_address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    postal_code: Optional[str]
    browser: Optional[str]
    traffic_source: Optional[str]
    uri: Optional[str]
    event_type: Optional[str]

# Model for inventory_items table
class InventoryItem(BaseModel):
    id: Optional[int]
    product_id: Optional[int]
    created_at: Optional[datetime]
    sold_at: Optional[datetime]
    cost: Optional[float]
    product_category: Optional[str]
    product_name: Optional[str]
    product_brand: Optional[str]
    product_retail_price: Optional[float]
    product_department: Optional[str]
    product_sku: Optional[str]
    product_distribution_center_id: Optional[int]

# Model for order_items table
class OrderItem(BaseModel):
    id: Optional[int]
    order_id: Optional[int]
    user_id: Optional[int]
    product_id: Optional[int]
    inventory_item_id: Optional[int]
    status: Optional[str]
    created_at: Optional[datetime]
    shipped_at: Optional[datetime]
    delivered_at: Optional[datetime]
    returned_at: Optional[datetime]
    sale_price: Optional[float]

# Model for orders table
class Order(BaseModel):
    order_id: Optional[int]
    user_id: Optional[int]
    status: Optional[str]
    gender: Optional[str]
    created_at: Optional[datetime]
    returned_at: Optional[datetime]
    shipped_at: Optional[datetime]
    delivered_at: Optional[datetime]
    num_of_item: Optional[int]

# Model for products table
class Product(BaseModel):
    id: Optional[int]
    cost: Optional[float]
    category: Optional[str]
    name: Optional[str]
    brand: Optional[str]
    retail_price: Optional[float]
    department: Optional[str]
    sku: Optional[str]
    distribution_center_id: Optional[int]

# Model for users table
class User(BaseModel):
    id: Optional[int]
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    age: Optional[int]
    gender: Optional[str]
    state: Optional[str]
    street_address: Optional[str]
    postal_code: Optional[str]
    city: Optional[str]
    country: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    traffic_source: Optional[str]
    created_at: Optional[datetime]    

    
class EcommerceJobParameters(BaseModel):
    table_names: List[str]
    gcp_project: str
    destination: Annotated[Union[List[str], str], Field(default_factory=lambda: ["local"])]
    s3_path: Optional[str]
    aws_profile: Optional[str]

# Mapping of table names to Pydantic models
table_model_mapping: Dict[str, Type[BaseModel]] = {
    "distribution_center": DistributionCenter,
    "event": Event,
    "inventory_item": InventoryItem,
    "order_item": OrderItem,
    "order": Order,
    "product": Product,
    "user": User,
}


class TableValidationError(Exception):
    """Custom exception for DataFrame validation errors."""

def validate_table(table: pa.Table, table_name: str):
    """
    Validates each row of a PyArrow Table against a Pydantic model based on table name.
    Raises TableValidationError if any row fails validation.

    :param table: PyArrow Table to validate.
    :param table_name: The name of the table to determine the model to validate against.
    :raises: TableValidationError
    """
    model = table_model_mapping.get(table_name)
    if not model:
        raise ValueError(f"No model mapping found for table: {table_name}")

    errors = []
    for i in range(table.num_rows):
        row = {column: table[column][i].as_py() for column in table.column_names}
        try:
            model(**row)
        except ValidationError as e:
            errors.append(f"Row {i} failed validation: {e}")

    if errors:
        error_message = "\n".join(errors)
        raise TableValidationError(
            f"Table validation failed with the following errors:\n{error_message}"
        )