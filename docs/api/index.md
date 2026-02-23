# API Reference

[← Back to Main Docs](../README.md)

Complete API documentation for `ds-platform-utils`.

## Table of Contents

- [Metaflow Utilities](#metaflow-utilities)
- [Snowflake Utilities](#snowflake-utilities)

## Metaflow Utilities

Located in `ds_platform_utils.metaflow`

### Query Functions

#### `query_pandas_from_snowflake()`

Query Snowflake and return a pandas DataFrame.

**Signature:**
```python
def query_pandas_from_snowflake(
    query: Optional[str] = None,
    query_fpath: Optional[str] = None,
    ctx: Optional[Dict[str, Any]] = None,
    warehouse: Optional[str] = None,
    use_s3_stage: bool = False,
    timeout_seconds: Optional[int] = None,
) -> pd.DataFrame
```

**Parameters:**
- `query` (str, optional): SQL query string. Mutually exclusive with `query_fpath`.
- `query_fpath` (str, optional): Path to SQL file containing query. Mutually exclusive with `query`.
- `ctx` (dict, optional): Template variables for query substitution using `{{variable}}` syntax.
- `warehouse` (str, optional): Snowflake warehouse name. If not provided, uses default from connection.
- `use_s3_stage` (bool, default=False): Use S3 staging for large results (recommended for > 1GB).
- `timeout_seconds` (int, optional): Query timeout in seconds.

**Returns:**
- `pd.DataFrame`: Query results as pandas DataFrame

**Raises:**
- `ValueError`: If neither `query` nor `query_fpath` provided, or if both provided.
- `FileNotFoundError`: If `query_fpath` does not exist.
- `SnowflakeQueryError`: If query execution fails.
- `TimeoutError`: If query exceeds timeout.

**Example:**
```python
# Direct query
df = query_pandas_from_snowflake(
    query="SELECT * FROM my_table WHERE date >= '2024-01-01'",
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
)

# From SQL file with template variables
df = query_pandas_from_snowflake(
    query_fpath="sql/extract.sql",
    ctx={"start_date": "2024-01-01", "end_date": "2024-12-31"},
)

# Large dataset via S3
df = query_pandas_from_snowflake(
    query="SELECT * FROM large_table",
    use_s3_stage=True,
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
)
```

**See Also:**
- [Pandas Integration Guide](../metaflow/pandas.md)
- [Performance Tuning](../guides/performance_tuning.md)

---

### Publish Functions

#### `publish_pandas()`

Publish a pandas DataFrame to Snowflake.

**Signature:**
```python
def publish_pandas(
    table_name: str,
    df: pd.DataFrame,
    schema: Optional[str] = None,
    mode: str = "replace",
    warehouse: Optional[str] = None,
    comment: Optional[str] = None,
) -> None
```

**Parameters:**
- `table_name` (str): Target table name (without schema).
- `df` (pd.DataFrame): DataFrame to publish.
- `schema` (str, optional): Target schema. If not provided, uses default dev schema.
- `mode` (str, default="replace"): Write mode:
  - `"replace"`: Drop and recreate table
  - `"append"`: Append to existing table
  - `"fail"`: Fail if table exists
- `warehouse` (str, optional): Snowflake warehouse name.
- `comment` (str, optional): Table comment for documentation.

**Returns:** None

**Raises:**
- `ValueError`: If DataFrame is empty or invalid mode.
- `SnowflakeError`: If publish operation fails.
- `PermissionError`: If no write access to schema.

**Example:**
```python
# Basic publish
publish_pandas(
    table_name="my_results",
    df=results_df,
    schema="my_dev_schema",
)

# Append mode
publish_pandas(
    table_name="incremental_data",
    df=new_data_df,
    mode="append",
)

# With comment
publish_pandas(
    table_name="features",
    df=features_df,
    comment="Daily feature refresh - 2024-01-15",
)
```

**See Also:**
- [Pandas Integration Guide](../metaflow/pandas.md)
- [Common Patterns](../guides/common_patterns.md)

---

#### `publish()`

Query, optionally transform, and publish in one call.

**Signature:**
```python
def publish(
    query_fpath: str,
    ctx: Dict[str, Any],
    publish_query_fpath: str,
    transform_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    comment: Optional[str] = None,
    warehouse: Optional[str] = None,
) -> None
```

**Parameters:**
- `query_fpath` (str): Path to SQL file for querying data.
- `ctx` (dict): Template variables for both query and publish SQL.
- `publish_query_fpath` (str): Path to SQL file for publishing.
- `transform_fn` (callable, optional): Function to transform DataFrame between query and publish.
- `comment` (str, optional): Table comment.
- `warehouse` (str, optional): Snowflake warehouse name.

**Returns:** None

**Example:**
```python
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add engineered features."""
    df['new_feature'] = df['value'] * 2
    return df

publish(
    query_fpath="sql/extract.sql",
    ctx={"start_date": "2024-01-01", "end_date": "2024-12-31"},
    transform_fn=add_features,
    publish_query_fpath="sql/publish.sql",
    comment="Daily feature engineering",
)
```

---

### BatchInferencePipeline

Class for large-scale batch inference with parallel processing.

**Signature:**
```python
class BatchInferencePipeline:
    def __init__(self) -> None
```

#### Methods

##### `query_and_batch()`

Query input data and split into batches for parallel processing.

**Signature:**
```python
def query_and_batch(
    self,
    input_query: str,
    batch_size_in_mb: int = 256,
    parallel_workers: int = 10,
    warehouse: str = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
) -> List[int]
```

**Parameters:**
- `input_query` (str): SQL query to fetch input data
- `batch_size_in_mb` (int, default=256): Target size per batch file in MB
- `parallel_workers` (int, default=10): Number of parallel workers
- `warehouse` (str): Snowflake warehouse name

**Returns:**
- `List[int]`: Worker IDs for use in `process_batch()`

**Example:**
```python
pipeline = BatchInferencePipeline()
worker_ids = pipeline.query_and_batch(
    input_query="SELECT * FROM large_input",
    batch_size_in_mb=256,
    parallel_workers=20,
)
```

##### `process_batch()`

Process a single batch with predictions.

**Signature:**
```python
def process_batch(
    self,
    worker_id: int,
    predict_fn: Callable[[pd.DataFrame], pd.DataFrame],
    batch_size_in_mb: int = 64,
    timeout_per_batch: int = 3600,
) -> None
```

**Parameters:**
- `worker_id` (int): Worker ID from `query_and_batch()`
- `predict_fn` (callable): Function that takes DataFrame and returns predictions
- `batch_size_in_mb` (int, default=64): Batch size for processing
- `timeout_per_batch` (int, default=3600): Timeout per batch in seconds

**Returns:** None

**Example:**
```python
def predict(df: pd.DataFrame) -> pd.DataFrame:
    df['score'] = model.predict(df[['f1', 'f2']])
    return df[['id', 'score']]

pipeline = BatchInferencePipeline()
pipeline.process_batch(
    worker_id=worker_id,
    predict_fn=predict,
)
```

##### `publish_results()`

Publish all processed results to Snowflake.

**Signature:**
```python
def publish_results(
    self,
    output_table: str,
    output_schema: str,
    warehouse: str = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
) -> None
```

**Parameters:**
- `output_table` (str): Target table name
- `output_schema` (str): Target schema
- `warehouse` (str): Snowflake warehouse name

**Returns:** None

**Example:**
```python
pipeline = BatchInferencePipeline()
pipeline.publish_results(
    output_table="predictions",
    output_schema="my_dev_schema",
)
```

**See Also:**
- [BatchInferencePipeline Guide](../metaflow/batch_inference_pipeline.md)
- [Common Patterns - Batch Inference](../guides/common_patterns.md#batch-inference)

---

### Configuration Validation

#### `make_pydantic_parser_fn()`

Create a parser function for Pydantic model validation in Metaflow Parameters.

**Signature:**
```python
def make_pydantic_parser_fn(
    model_class: Type[BaseModel]
) -> Callable[[str], BaseModel]
```

**Parameters:**
- `model_class` (Type[BaseModel]): Pydantic model class

**Returns:**
- `Callable[[str], BaseModel]`: Parser function for Metaflow Parameter

**Example:**
```python
from pydantic import BaseModel
from metaflow import FlowSpec, Parameter
from ds_platform_utils.metaflow import make_pydantic_parser_fn

class Config(BaseModel):
    start_date: str
    end_date: str
    threshold: float = 0.5

class MyFlow(FlowSpec):
    config = Parameter(
        'config',
        type=make_pydantic_parser_fn(Config),
        default='{"start_date": "2024-01-01", "end_date": "2024-12-31"}',
    )
```

**See Also:**
- [Configuration Validation Guide](../metaflow/validate_config.md)

---

### Utility Functions

#### `add_query_tags()`

Add metadata tags to SQL queries for tracking.

**Signature:**
```python
def add_query_tags(
    query: str,
    flow_name: str,
    step_name: str,
) -> str
```

**Parameters:**
- `query` (str): SQL query
- `flow_name` (str): Metaflow flow name
- `step_name` (str): Metaflow step name

**Returns:**
- `str`: Query with tags prepended

**Example:**
```python
tagged_query = add_query_tags(
    query="SELECT * FROM my_table",
    flow_name="MyFlow",
    step_name="query_data",
)
```

#### `restore_step_state()`

Restore Metaflow step state for debugging.

**Signature:**
```python
@contextmanager
def restore_step_state(
    flow_name: str,
    run_id: str,
    step: str,
) -> Generator[None, None, None]
```

**Parameters:**
- `flow_name` (str): Flow name
- `run_id` (str): Run ID
- `step` (str): Step name

**Yields:** Context with restored step state

**Example:**
```python
from ds_platform_utils.metaflow import restore_step_state

with restore_step_state("MyFlow", run_id="123", step="process"):
    # Access self.df from that step
    print(self.df.head())
```

---

## Snowflake Utilities

Located in `ds_platform_utils._snowflake`

### Connection Management

#### `get_snowflake_connection()`

Get a Snowflake connection cursor.

**Signature:**
```python
def get_snowflake_connection() -> snowflake.connector.cursor.SnowflakeCursor
```

**Returns:**
- `SnowflakeCursor`: Snowflake cursor for executing queries

**Example:**
```python
from ds_platform_utils._snowflake import get_snowflake_connection

cursor = get_snowflake_connection()
cursor.execute("SELECT * FROM my_table")
results = cursor.fetchall()
```

---

### Write Audit Publish

#### `write_audit_publish()`

Execute SQL with audit logging and publish to target table.

**Signature:**
```python
def write_audit_publish(
    sql: str,
    warehouse: Optional[str] = None,
    comment: Optional[str] = None,
) -> None
```

**Parameters:**
- `sql` (str): SQL statement to execute
- `warehouse` (str, optional): Snowflake warehouse
- `comment` (str, optional): Audit comment

**Returns:** None

**Example:**
```python
from ds_platform_utils._snowflake import write_audit_publish

write_audit_publish(
    sql="CREATE OR REPLACE TABLE my_table AS SELECT * FROM source",
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
    comment="Daily refresh",
)
```

---

## Type Definitions

### Common Types

```python
from typing import Optional, Dict, List, Callable, Any
import pandas as pd

# Query context
QueryContext = Dict[str, Any]

# Prediction function signature
PredictFn = Callable[[pd.DataFrame], pd.DataFrame]

# Transform function signature
TransformFn = Callable[[pd.DataFrame], pd.DataFrame]
```

---

## Error Classes

### `SnowflakeQueryError`

Raised when Snowflake query fails.

### `BatchProcessingError`

Raised when batch processing fails.

### `ValidationError`

Raised when configuration validation fails (from Pydantic).

---

## Related Documentation

- [Getting Started Guide](../guides/getting_started.md)
- [Best Practices](../guides/best_practices.md)
- [Common Patterns](../guides/common_patterns.md)
- [Module-Specific Docs](../metaflow/README.md)
