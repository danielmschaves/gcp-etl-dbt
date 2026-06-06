from datetime import datetime
from typing import Annotated, Dict, List, Optional, Set, Type, Union

import pyarrow as pa
import pyarrow.compute as pc
from pydantic import BaseModel, Field, ValidationError


# ---------------------------------------------------------------------------
# Source table Pydantic models (schema documentation + type enforcement)
# Primary and foreign keys are non-optional to enforce data integrity.
# ---------------------------------------------------------------------------

class DistributionCenters(BaseModel):
    id: int
    name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class Events(BaseModel):
    id: int
    user_id: int
    sequence_number: Optional[int] = None
    session_id: Optional[str] = None
    created_at: Optional[datetime] = None
    ip_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    browser: Optional[str] = None
    traffic_source: Optional[str] = None
    uri: Optional[str] = None
    event_type: Optional[str] = None


class InventoryItems(BaseModel):
    id: int
    product_id: int
    created_at: Optional[datetime] = None
    sold_at: Optional[datetime] = None
    cost: Optional[float] = None
    product_category: Optional[str] = None
    product_name: Optional[str] = None
    product_brand: Optional[str] = None
    product_retail_price: Optional[float] = None
    product_department: Optional[str] = None
    product_sku: Optional[str] = None
    product_distribution_center_id: Optional[int] = None


class OrderItems(BaseModel):
    id: int
    order_id: int
    user_id: int
    product_id: int
    inventory_item_id: Optional[int] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    shipped_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    returned_at: Optional[datetime] = None
    sale_price: Optional[float] = None


class Orders(BaseModel):
    order_id: int
    user_id: int
    status: Optional[str] = None
    gender: Optional[str] = None
    created_at: Optional[datetime] = None
    returned_at: Optional[datetime] = None
    shipped_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    num_of_item: Optional[int] = None


class Products(BaseModel):
    id: int
    cost: Optional[float] = None
    category: Optional[str] = None
    name: Optional[str] = None
    brand: Optional[str] = None
    retail_price: Optional[float] = None
    department: Optional[str] = None
    sku: Optional[str] = None
    distribution_center_id: Optional[int] = None


class Users(BaseModel):
    id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    age: Optional[int] = None
    gender: Optional[str] = None
    state: Optional[str] = None
    street_address: Optional[str] = None
    postal_code: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    traffic_source: Optional[str] = None
    created_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Job parameters
# ---------------------------------------------------------------------------

class EcommerceJobParameters(BaseModel):
    table_names: List[str]
    gcp_project: str
    destination: Annotated[Union[List[str], str], Field(default_factory=lambda: ["local"])]
    s3_path: Optional[str] = None
    aws_profile: Optional[str] = None


# ---------------------------------------------------------------------------
# Validation — vectorised via PyArrow compute, O(columns) not O(rows)
# ---------------------------------------------------------------------------

# Non-nullable columns that must have zero nulls for the table to be valid
REQUIRED_FIELDS: Dict[str, List[str]] = {
    "distribution_centers": ["id"],
    "events":               ["id", "user_id"],
    "inventory_items":      ["id", "product_id"],
    "order_items":          ["id", "order_id", "user_id", "product_id"],
    "orders":               ["order_id", "user_id"],
    "products":             ["id"],
    "users":                ["id"],
}

# Tables that carry a created_at timestamp (used for watermark tracking)
TABLES_WITH_TIMESTAMP: Set[str] = {
    "events", "inventory_items", "order_items", "orders", "users"
}

table_model_mapping: Dict[str, Type[BaseModel]] = {
    "distribution_centers": DistributionCenters,
    "events":               Events,
    "inventory_items":      InventoryItems,
    "order_items":          OrderItems,
    "orders":               Orders,
    "products":             Products,
    "users":                Users,
}


class TableValidationError(Exception):
    """Raised when a table fails data quality checks."""


def validate_table(table: pa.Table, table_name: str) -> None:
    """
    Validate a PyArrow table using column-level null checks.
    O(columns) — does not iterate rows.
    """
    required = REQUIRED_FIELDS.get(table_name)
    if required is None:
        raise ValueError(f"No schema defined for table: {table_name}")

    errors: List[str] = []

    if table.num_rows == 0:
        errors.append("table is empty")

    for col_name in required:
        if col_name not in table.column_names:
            errors.append(f"missing required column '{col_name}'")
            continue
        null_count = table.column(col_name).null_count
        if null_count > 0:
            errors.append(f"column '{col_name}' has {null_count} null value(s)")

    if errors:
        raise TableValidationError(
            f"Validation failed for '{table_name}': " + "; ".join(errors)
        )


def get_max_timestamp(table: pa.Table, column: str = "created_at") -> Optional[str]:
    """Return the ISO string of the maximum timestamp in a column, or None."""
    if column not in table.column_names:
        return None
    max_val = pc.max(table.column(column)).as_py()
    return max_val.isoformat() if max_val is not None else None
