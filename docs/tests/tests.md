## Fixtures for Each Table

### Purpose:
Fixtures in pytest are used to set up the environment for tests, including preparing data and resources. For each table, the fixture reads data from a CSV file and validates it using Pydantic models to ensure that the data structure is correct and all fields meet the defined constraints.

### Explanation:

- Reading CSV Files: Each fixture reads a corresponding CSV file using pd.read_csv.
- Parsing Dates: For columns that represent dates, parse_dates is used to ensure these columns are interpreted as datetime objects.
- Validation Using Pydantic Models: After reading the data into a DataFrame, each row is converted into an instance of a Pydantic model (DistributionCenters, Events, etc.). This step validates the data according to the model's schema.

Example for the distribution_centers table:

```
@pytest.fixture
def distribution_centers():
    df = pd.read_csv('distribution_centers.csv', parse_dates=['created_at'])
    validated_data = [DistributionCenters(**row) for index, row in df.iterrows()]
    return validated_data
```

This ensures that the data for distribution_centers is loaded and validated before being used in tests.

## DuckDB Integration

### Purpose:
DuckDB is an in-memory SQL database that allows for efficient querying and manipulation of data. Integrating DuckDB helps in testing SQL queries and database operations without needing a persistent database setup.

### Explanation:

- duckdb_connection Fixture: This fixture sets up a DuckDB connection that will be used for the duration of the tests. The connection is closed after the tests are done.

```
@pytest.fixture
def duckdb_connection():
    conn = duckdb.connect(database=":memory:", read_only=False)
    yield conn
    conn.close()
```

- load_data_into_duckdb Fixture: This fixture uses the DuckDB connection to create tables and load data from CSV files into DuckDB. It reads each CSV file and creates a corresponding table in DuckDB.

```
@pytest.fixture
def load_data_into_duckdb(duckdb_connection):
    conn = duckdb_connection
    csv_files = {
        'distribution_centers': 'distribution_centers.csv',
        'events': 'events.csv',
        # Add other tables as necessary
    }
    for table, csv_file in csv_files.items():
        conn.execute(f"""
            CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{csv_file}', header=True)
        """)
    return conn
```

This allows tests to run SQL queries against the loaded data.

## Table Tests

### Purpose:

These tests ensure that the data loaded by the fixtures is correct and that specific attributes within the data meet the expected values.

### Explanation:

- Length Check: The test checks that the table is not empty by asserting that its length is greater than 0.
- Attribute Validation: The test validates specific attributes within the first record of the table to ensure data accuracy.
- Example for the distribution_centers table:

```
def test_distribution_centers(distribution_centers):
    assert len(distribution_centers) > 0
    assert distribution_centers[0].name == 'Memphis TN'
```

This ensures the data is loaded correctly and that the first entry has the expected values.

## Query Testing

### Purpose:

To test SQL queries executed against the DuckDB instance to ensure that the queries return the expected results.

### Explanation:

- Query Execution: The test executes a SQL query using the DuckDB connection.
- Result Validation: The test checks the result of the query to ensure it meets the expected condition.

Example:

```
def test_query_on_duckdb(load_data_into_duckdb):
    conn = load_data_into_duckdb
    result = conn.execute("SELECT COUNT(*) FROM orders").fetchone()
    assert result[0] > 0
```

This verifies that the orders table has records.

## Validation Function

### Purpose:

To validate the data in a table using corresponding Pydantic models, ensuring that the data adheres to the defined schema.

### Explanation:

- Model Mapping: The function retrieves the appropriate Pydantic model for the table.
- Row Validation: Each row in the table is validated against the model.
- Error Handling: If validation fails, errors are collected and raised as a TableValidationError.

Example:

```
def validate_table(table: pa.Table, table_name: str):
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
```

## EcommerceJobParameters Validation

### Purpose:

To test the validation of job parameters ensuring they meet the required schema.

### Explanation:

- Creating Parameters: The test creates an instance of EcommerceJobParameters with test data.
- Attribute Validation: The test checks that the attributes of the parameters are set correctly.

Example:

```
def test_ecommerce_job_parameters():
    params = EcommerceJobParameters(
        table_names=["orders", "order_items"],
        gcp_project="test_project",
        destination=["local", "s3"],
        s3_path="s3://bucket/path",
        aws_profile="test_profile"
    )
    assert params.table_names == ["orders", "order_items"]
    assert params.gcp_project == "test_project"
    assert params.destination == ["local", "s3"]
    assert params.s3_path == "s3://bucket/path"
    assert params.aws_profile == "test_profile"
```

This ensures the job parameters are validated correctly.

## Table Validation with Valid Data

### Purpose:

To test the table validation function using a simple model and ensuring it works correctly with valid data.

### Explanation:

- Creating Valid Data: The test creates a DataFrame with valid data.
- Validation: The test validates the table using the validate_table function.
- Error Check: The test ensures no validation errors are found.

Example:

```
def test_table_validation_with_valid_data():
    valid_data = {
        "column1": pa.array([1, 2]),
        "column2": pa.array(["a", "b"]),
        "column3": pa.array([1.1, 2.2]),
    }
    valid_table = pa.Table.from_pandas(pd.DataFrame(valid_data))
    errors = validate_table(valid_table, 'my_model')
    assert not errors, f"Validation errors were found in valid data: {errors}"
```
This ensures that the validation function works correctly for valid data.