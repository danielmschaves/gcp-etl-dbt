from pydantic import BaseModel, Field
from typing import List, Union, Annotated, Type
from pydantic import BaseModel, ValidationError
from datetime import datetime
from typing import Optional
import pandas as pd

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
    

class DataFrameValidationError(Exception):
    """Custom exception for DataFrame validation errors."""

def validate_dataframe(df: pd.DataFrame, model: BaseModel):
    """Validates each row of a DataFrame against a Pydantic model."""
    errors = []
    for i, row in enumerate(df.to_dict(orient='records')):
        try:
            model(**row)
        except ValidationError as e:
            errors.append(f"Row {i} failed validation: {e}")
    if errors:
        raise DataFrameValidationError(f"DataFrame validation failed with the following errors:\n{'\n'.join(errors)}")