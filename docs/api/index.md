# API Reference

[← Back to Main Docs](../README.md)

Complete API documentation for `ds-platform-utils`.

## Public API

All public functions are exported from `ds_platform_utils.metaflow`:

```python
from ds_platform_utils.metaflow import (
    BatchInferencePipeline,      # Scalable batch inference
    make_pydantic_parser_fn,      # Config validation
    publish,                      # Query, transform, and publish
    publish_pandas,               # Publish DataFrame to Snowflake
    query_pandas_from_snowflake,  # Query from Snowflake to DataFrame
    restore_step_state,           # Restore flow state for debugging
)
```

## Table of Contents

- [Query Functions](#query-functions)
  - [query_pandas_from_snowflake()](#query_pandas_from_snowflake)
- [Publish Functions](#publish-functions)
  - [publish_pandas()](#publish_pandas)
  - [publish()](#publish)
- [Batch Processing](#batch-processing)
  - [BatchInferencePipeline](#batchinferencepipeline)
- [Configuration](#configuration)
  - [make_pydantic_parser_fn()](#make_pydantic_parser_fn)
- [State Management](#state-management)
  - [restore_step_state()](#restore_step_state)

---

## Query Functions

### `query_pandas_from_snowflake()`

Query Snowflake and return a pandas DataFrame.

**Signature:**
```python
def query_pandas_from_snowflake(
    query: Union[str, Path],
    warehouse: Optional[str] = None,
    ctx: Optional[Dict[str, Any]] = None,
    use_utc: bool = True,
    use_s3_stage: bool = False,
) -> pd.DataFrame
```

**Parameters:**
- `query` (str | Path): SQL query string or path to a .sql file.
- `warehouse` (str, optional): Snowflake warehouse name. Defaults to `OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH` in dev or `OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_XS_WH` in production.
- `ctx` (dict, optional): Template variables for query substitution using `{{variable}}` syntax.
- `use_utc` (bool, default=True): Whether to set Snowflake session to UTC timezone.
- `use_s3_stage` (bool, default=False): Use S3 staging for large results (more efficient for > 1GB).

**Returns:**
- `pd.DataFrame`: Query results as pandas DataFrame (column names lowercased)

**Notes:**
- If the query contains `{{schema}}` placeholders, they will be replaced with the appropriate schema (prod or dev).
- Query tags are automatically added for cost tracking in select.dev.

**Example:**
```python
# Direct query
df = query_pandas_from_snowflake(
    query="SELECT * FROM my_table WHERE date >= '2024-01-01'",
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
)

# From SQL file with template variables
df = query_pandas_from_snowflake(
    query="sql/extract.sql",
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

---

## Publish Functions

### `publish_pandas()`

Publish a pandas DataFrame to Snowflake.

**Signature:**
```python
def publish_pandas(
    table_name: str,
    df: pd.DataFrame,
    add_created_date: bool = False,
    chunk_size: Optional[int] = None,
    compression: Literal["snappy", "gzip"] = "snappy",
    warehouse: Optional[str] = None,
    parallel: int = 4,
    quote_identifiers: bool = True,
    auto_create_table: bool = False,
    overwrite: bool = False,
    use_logical_type: bool = True,
    use_utc: bool = True,
    use_s3_stage: bool = False,
    table_definition: Optional[List[Tuple[str, str]]] = None,
) -> None
```

**Parameters:**
- `table_name` (str): Name of the table to create (automatically uppercased).
- `df` (pd.DataFrame): DataFrame to publish.
- `add_created_date` (bool, default=False): Add a `created_date` column with current UTC timestamp.
- `chunk_size` (int, optional): Number of rows per insert batch. Default: all rows at once.
- `compression` (str, default="snappy"): Parquet compression: `"snappy"` or `"gzip"`.
- `warehouse` (str, optional): Snowflake warehouse name.
- `parallel` (int, default=4): Number of threads for uploading chunks.
- `quote_identifiers` (bool, default=True): Quote column/table names (preserve case).
- `auto_create_table` (bool, default=False): Auto-create table if it doesn't exist.
- `overwrite` (bool, default=False): Drop/truncate existing table before writing.
- `use_logical_type` (bool, default=True): Use Parquet logical types for timestamps.
- `use_utc` (bool, default=True): Set Snowflake session to UTC timezone.
- `use_s3_stage` (bool, default=False): Use S3 staging (more efficient for large DataFrames).
- `table_definition` (list, optional): Column schema as `[(col_name, col_type), ...]` for S3 staging.

**Returns:** None

**Notes:**
- Schema is automatically selected: prod schema in production, dev schema otherwise.
- Table name is automatically uppercased for Snowflake standardization.

**Example:**
```python
# Basic publish with auto-create
publish_pandas(
    table_name="my_results",
    df=results_df,
    auto_create_table=True,
    overwrite=True,
)

# Large DataFrame via S3 staging
publish_pandas(
    table_name="large_table",
    df=large_df,
    use_s3_stage=True,
    table_definition=[
        ("id", "NUMBER"),
        ("name", "STRING"),
        ("score", "FLOAT"),
    ],
)

# With timestamp tracking
publish_pandas(
    table_name="features",
    df=features_df,
    add_created_date=True,
    auto_create_table=True,
)
```

**See Also:**
- [Pandas Integration Guide](../metaflow/pandas.md)

---

### `publish()`

Publish a Snowflake table using the write-audit-publish (WAP) pattern.

**Signature:**
```python
def publish(
    table_name: str,
    query: Union[str, Path],
    audits: Optional[List[Union[str, Path]]] = None,
    ctx: Optional[Dict[str, Any]] = None,
    warehouse: Optional[str] = None,
    use_utc: bool = True,
) -> None
```

**Parameters:**
- `table_name` (str): Name of the Snowflake table to publish (e.g., `"OUT_OF_STOCK_ADS"`).
- `query` (str | Path): SQL query string or path to .sql file that generates the table data.
- `audits` (list, optional): SQL audit scripts or file paths that validate data quality. Each script should return zero rows for success.
- `ctx` (dict, optional): Template variables for SQL substitution.
- `warehouse` (str, optional): Snowflake warehouse name.
- `use_utc` (bool, default=True): Whether to use UTC timezone for the Snowflake connection.

**Returns:** None

**Notes:**
- Uses the write-audit-publish pattern: write to temp table → run audits → promote to final table.
- Query tags are automatically added for cost tracking in select.dev.
- Schema is automatically selected based on production/dev environment.

**Example:**
```python
# Simple publish
publish(
    table_name="OUT_OF_STOCK_ADS",
    query="sql/create_training_data.sql",
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
)

# With audits for data validation
publish(
    table_name="DAILY_FEATURES",
    query="sql/create_features.sql",
    audits=[
        "sql/audit_row_count.sql",
        "sql/audit_null_check.sql",
    ],
    ctx={"date": "2024-01-01"},
)
```

---

## Batch Processing

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
    input_query: Union[str, Path],
    ctx: Optional[dict] = None,
    warehouse: Optional[str] = None,
    use_utc: bool = True,
    parallel_workers: int = 1,
) -> List[int]
```

**Parameters:**
- `input_query` (str | Path): SQL query string or file path to query
- `ctx` (dict, optional): Dict of variable substitutions for SQL template (e.g., `{{schema}}`)
- `warehouse` (str, optional): Snowflake warehouse name
- `use_utc` (bool, default=True): Whether to use UTC timezone for Snowflake
- `parallel_workers` (int, default=1): Number of parallel workers to use for processing

**Returns:**
- `List[int]`: Worker IDs to use with `foreach` in next step

**Example:**
```python
pipeline = BatchInferencePipeline()
worker_ids = pipeline.query_and_batch(
    input_query="SELECT * FROM large_input",
    parallel_workers=10,
)
```

##### `process_batch()`

Process a single batch with predictions using a queue-based 3-thread pipeline.

**Signature:**
```python
def process_batch(
    self,
    worker_id: int,
    predict_fn: Callable[[pd.DataFrame], pd.DataFrame],
    batch_size_in_mb: int = 128,
    timeout_per_batch: int = 300,
) -> str
```

**Parameters:**
- `worker_id` (int): Worker ID from `query_and_batch()`
- `predict_fn` (callable): Function that takes DataFrame and returns predictions DataFrame
- `batch_size_in_mb` (int, default=128): Target size for each batch in MB
- `timeout_per_batch` (int, default=300): Timeout in seconds for each batch operation

**Returns:**
- `str`: S3 path where predictions were written

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

Publish all processed results to Snowflake (call this in join step).

**Signature:**
```python
def publish_results(
    self,
    output_table_name: str,
    output_table_definition: Optional[List[Tuple[str, str]]] = None,
    auto_create_table: bool = True,
    overwrite: bool = True,
    warehouse: Optional[str] = None,
    use_utc: bool = True,
) -> None
```

**Parameters:**
- `output_table_name` (str): Name of the Snowflake table
- `output_table_definition` (list, optional): Schema as list of `(column, type)` tuples
- `auto_create_table` (bool, default=True): Whether to auto-create table if not exists
- `overwrite` (bool, default=True): Whether to overwrite existing data
- `warehouse` (str, optional): Snowflake warehouse name
- `use_utc` (bool, default=True): Whether to use UTC timezone for Snowflake

**Returns:** None

**Example:**
```python
pipeline = BatchInferencePipeline()
pipeline.publish_results(
    output_table_name="predictions",
)

# With custom schema
pipeline.publish_results(
    output_table_name="predictions",
    output_table_definition=[
        ("id", "NUMBER"),
        ("score", "FLOAT"),
        ("prediction", "STRING"),
    ],
)
```

**See Also:**
- [BatchInferencePipeline Guide](../metaflow/batch_inference_pipeline.md)

---

## Configuration

### `make_pydantic_parser_fn()`

Create a parser function for Pydantic model validation in Metaflow Config.

**Signature:**
```python
def make_pydantic_parser_fn(
    pydantic_model: type[BaseModel]
) -> Callable[[str], dict]
```

**Parameters:**
- `pydantic_model` (type[BaseModel]): Pydantic model class for validation

**Returns:**
- `Callable[[str], dict]`: Parser function that validates and returns a dict

**Notes:**
- Supports JSON, TOML, and YAML config formats
- YAML is preferred because it supports comments
- Returns a dict with default values applied after validation

**Example:**
```python
from pydantic import BaseModel, Field
from metaflow import FlowSpec, step, Config
from ds_platform_utils.metaflow import make_pydantic_parser_fn

class PydanticFlowConfig(BaseModel):
    \"\"\"Validate and provide autocompletion for config values.\"\"\"
    n_rows: int = Field(ge=1)
    threshold: float = 0.5

class MyFlow(FlowSpec):
    config: PydanticFlowConfig = Config(
        name="config",
        default="./configs/default.yaml",
        parser=make_pydantic_parser_fn(PydanticFlowConfig)
    )  # type: ignore[assignment]

    @step
    def start(self):
        print(f"{self.config.n_rows=}")
        self.next(self.end)
```

**See Also:**
- [Configuration Validation Guide](../metaflow/validate_config.md)

---

## State Management

### `restore_step_state()`

Restore Metaflow step state for debugging and development.

**Signature:**
```python
def restore_step_state(
    flow_class: Optional[type[FlowSpec]] = None,
    flow_name: Optional[str] = None,
    step_name: str = "end",
    flow_run_id: Union[Literal["latest_successful_run", "latest"], str] = "latest_successful_run",
    secrets: Optional[list[str]] = None,
    namespace: Optional[str] = None,
) -> FlowSpec
```

**Parameters:**
- `flow_class` (type[FlowSpec], optional): Flow class for type hints and autocompletion
- `flow_name` (str, optional): Flow name (defaults to flow_class name if provided)
- `step_name` (str, default="end"): Step to restore state from (restores from step before this)
- `flow_run_id` (str, default="latest_successful_run"): Run ID to restore:
  - `"latest_successful_run"`: Latest successful run
  - `"latest"`: Latest run (even if failed)
  - Or specific run ID
- `secrets` (list[str], optional): Secrets to export as environment variables
- `namespace` (str, optional): Metaflow namespace to filter runs

**Returns:**
- `FlowSpec`: Restored flow state with access to all step artifacts

**Example:**
```python
from ds_platform_utils.metaflow import restore_step_state
from my_flows import MyPredictionFlow

# Restore state from latest successful run
self = restore_step_state(
    MyPredictionFlow,
    step_name="process",
    secrets=["outerbounds.my-secret"],
)

# Now you can access step artifacts
print(self.df.head())
print(self.config)

# Debug or test step logic
result = process_data(self.df)
```

**Use Cases:**
- 🐛 **Debugging**: Inspect data and artifacts from failed runs
- 🧪 **Testing**: Test step logic without running entire flow
- 📊 **Analysis**: Explore intermediate results
- 🔄 **Development**: Iterate on step logic quickly

**See Also:**
- [Examples](../examples/README.md)

---

## Snowflake Utilities

The `ds_platform_utils._snowflake` module contains lower-level utilities for Snowflake operations:
- `_execute_sql()` - Execute SQL statements with batch support
- `write_audit_publish()` - Implement write-audit-publish pattern

**Note:** This module is marked as private (underscore prefix) because its APIs may change. Most users should use the high-level Metaflow utilities above.

**For Advanced Users:** If you need direct access to these utilities for custom workflows, see the [Snowflake Utilities Documentation](../snowflake/README.md).

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

- [Metaflow Utilities](../metaflow/README.md)
- [Snowflake Utilities](../snowflake/README.md)
