# Pandas Integration

[← Back to Metaflow Docs](README.md)

Query and publish pandas DataFrames with Snowflake.

## Table of Contents

- [Overview](#overview)
- [Querying Data](#querying-data)
- [Publishing Data](#publishing-data)
- [Using SQL Files](#using-sql-files)
- [Advanced Usage](#advanced-usage)

## Overview

The pandas integration provides simple functions to move data between Snowflake and pandas DataFrames:

```python
from ds_platform_utils.metaflow import query_pandas_from_snowflake, publish_pandas
```

## Querying Data

### Basic Query

```python
from ds_platform_utils.metaflow import query_pandas_from_snowflake

df = query_pandas_from_snowflake(
    query="SELECT * FROM my_table WHERE date >= '2024-01-01'",
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
)
```

### Query from SQL File

```python
df = query_pandas_from_snowflake(
    query="sql/extract_data.sql",  # Pass file path as query parameter
    ctx={
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "min_value": 100,
    },
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
)
```

```sql
-- sql/extract_data.sql
SELECT *
FROM transactions
WHERE date >= '{{start_date}}'
    AND date <= '{{end_date}}'
    AND amount >= {{min_value}}
```

### Large Datasets via S3

For datasets > 1GB, use S3 staging:

```python
df = query_pandas_from_snowflake(
    query="SELECT * FROM large_table",
    use_s3_stage=True,  # ← Enable S3 staging
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
)
```

**Benefits:**
- Much faster for large datasets (3-5x speedup)
- Reduces memory pressure
- More reliable for very large results

**When to use:**
- Dataset > 1 GB
- Many columns (wide tables)
- Network bandwidth limited

### Custom Timeouts

Note: Timeouts are managed by Metaflow decorators and Outerbounds, not the query function directly.

```python
from metaflow import FlowSpec, step, timeout

class MyFlow(FlowSpec):
    @timeout(seconds=1800)  # 30 minutes
    @step
    def query_large_data(self):
        self.df = query_pandas_from_snowflake(
            query="SELECT * FROM huge_table",
            warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
        )
        self.next(self.end)
```

## Publishing Data

### Basic Publish

```python
from ds_platform_utils.metaflow import publish_pandas

publish_pandas(
    table_name="my_results",
    df=results_df,
    auto_create_table=True,
    overwrite=True,
)
```

### Replace vs. Append

```python
# Replace existing table (overwrite=True)
publish_pandas(
    table_name="my_table",
    df=df,
    auto_create_table=True,
    overwrite=True,  # Drops table first
)

# Append to existing table (overwrite=False)
publish_pandas(
    table_name="my_table",
    df=df,
    auto_create_table=False,  # Table must already exist
    overwrite=False,  # Appends data
)
```

### Add Created Date

```python
publish_pandas(
    table_name="my_table",
    df=df,
    add_created_date=True,  # Adds 'created_date' column with UTC timestamp
    auto_create_table=True,
)
```

### Specify Warehouse

```python
publish_pandas(
    table_name="large_table",
    df=large_df,
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",  # Use larger warehouse
    auto_create_table=True,
    overwrite=True,
)
```

### Large DataFrame via S3 Staging

For very large DataFrames, use S3 staging for better performance:

```python
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
```

## Using SQL Files

### Write-Audit-Publish Pattern

The `publish()` function implements the write-audit-publish pattern for data quality:

```python
from ds_platform_utils.metaflow import publish

publish(
    table_name="DAILY_FEATURES",
    query="sql/create_features.sql",
    audits=[
        "sql/audit_row_count.sql",
        "sql/audit_null_check.sql",
    ],
    ctx={"start_date": "2024-01-01", "end_date": "2024-12-31"},
)
```

```sql
-- sql/create_features.sql
CREATE OR REPLACE TABLE {{schema}}.{{table_name}} AS
SELECT
    user_id,
    COUNT(*) as event_count,
    AVG(value) as avg_value,
    MAX(timestamp) as last_seen
FROM events
WHERE date >= '{{start_date}}'
    AND date <= '{{end_date}}'
GROUP BY user_id;
```

```sql
-- sql/audit_row_count.sql (should return 0 rows if passing)
SELECT 1 WHERE (SELECT COUNT(*) FROM {{schema}}.{{table_name}}) < 100;
```

```sql
-- sql/audit_null_check.sql (should return 0 rows if passing)
SELECT 1 WHERE EXISTS (
    SELECT 1 FROM {{schema}}.{{table_name}} WHERE user_id IS NULL
);
```

### Feature Engineering in Python

For Python transformations, combine query and publish_pandas:

```python
from ds_platform_utils.metaflow import query_pandas_from_snowflake, publish_pandas
from datetime import datetime

# Query raw data
df = query_pandas_from_snowflake(
    query="sql/create_features.sql",
    ctx={"start_date": "2024-01-01", "end_date": "2024-12-31"},
)

# Transform in Python
def transform_features(df):
    df['recency_days'] = (
        datetime.now() - pd.to_datetime(df['last_seen'])
    ).dt.days
    df['frequency_per_day'] = df['event_count'] / 30
    return df

df = transform_features(df)

# Publish
publish_pandas(
    table_name="user_features",
    df=df,
    auto_create_table=True,
    overwrite=True,
)
```

## Advanced Usage

### Multiple Queries in Sequence

```python
# Query 1: Get user data
users_df = query_pandas_from_snowflake(
    query="SELECT * FROM users WHERE active = TRUE"
)

# Query 2: Get events for these users
user_ids = tuple(users_df['user_id'].tolist())
events_df = query_pandas_from_snowflake(
    query=f"SELECT * FROM events WHERE user_id IN {user_ids}"
)

# Join in pandas
result = users_df.merge(events_df, on='user_id')

# Publish
publish_pandas(table_name="user_events", df=result)
```

### Chunked Publishing

For very large DataFrames, use chunk_size parameter:

```python
publish_pandas(
    table_name="large_table",
    df=large_df,
    chunk_size=100000,  # Insert 100k rows at a time
    auto_create_table=True,
    overwrite=True,
)
```

Or use S3 staging for even better performance:

```python
publish_pandas(
    table_name="large_table",
    df=large_df,
    use_s3_stage=True,
    table_definition=[
        ("id", "NUMBER"),
        ("value", "FLOAT"),
    ],
)
```

### Query with Date Range

```python
from datetime import datetime, timedelta

# Query last 7 days
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

df = query_pandas_from_snowflake(
    query=f"""
        SELECT *
        FROM events
        WHERE date >= '{start_date.strftime('%Y-%m-%d')}'
            AND date < '{end_date.strftime('%Y-%m-%d')}'
    """
)
```

### Error Handling

```python
from metaflow import retry

@retry(times=3)
def query_with_retry():
    """Query with automatic retries."""
    try:
        df = query_pandas_from_snowflake(
            query="SELECT * FROM sometimes_flaky_table",
        )
        return df
    except Exception as e:
        print(f"⚠️ Query failed: {e}")
        raise  # Will trigger retry

df = query_with_retry()
```

## API Reference

### query_pandas_from_snowflake()

Query Snowflake and return a pandas DataFrame.

**Parameters:**
- `query` (str | Path): SQL query string or path to .sql file
- `warehouse` (str, optional): Snowflake warehouse name
- `ctx` (dict, optional): Template variables for query substitution
- `use_utc` (bool): Use UTC timezone (default: True)
- `use_s3_stage` (bool): Use S3 staging for large results (default: False)

**Returns:** `pandas.DataFrame` (column names lowercased)

**Example:**
```python
df = query_pandas_from_snowflake(
    query="SELECT * FROM my_table",
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
)
```

### publish_pandas()

Publish a pandas DataFrame to Snowflake.

**Parameters:**
- `table_name` (str): Target table name (auto-uppercased)
- `df` (pd.DataFrame): DataFrame to publish
- `add_created_date` (bool): Add created_date column (default: False)
- `chunk_size` (int, optional): Rows per insert batch
- `compression` (str): Parquet compression "snappy" or "gzip" (default: "snappy")
- `warehouse` (str, optional): Snowflake warehouse name
- `parallel` (int): Upload threads (default: 4)
- `quote_identifiers` (bool): Quote column names (default: True)
- `auto_create_table` (bool): Create table if missing (default: False)
- `overwrite` (bool): Drop/truncate before write (default: False)
- `use_logical_type` (bool): Parquet logical types for timestamps (default: True)
- `use_utc` (bool): Use UTC timezone (default: True)
- `use_s3_stage` (bool): Use S3 staging (default: False)
- `table_definition` (list, optional): Schema as [(col, type), ...] for S3 staging

**Returns:** None

**Example:**
```python
publish_pandas(
    table_name="my_results",
    df=results_df,
    auto_create_table=True,
    overwrite=True,
)
```

### publish()

Publish a Snowflake table using the write-audit-publish pattern.

**Parameters:**
- `table_name` (str): Name of the Snowflake table to publish
- `query` (str | Path): SQL query string or path to .sql file
- `audits` (list, optional): SQL audit scripts or file paths for validation
- `ctx` (dict, optional): Template variables for SQL substitution
- `warehouse` (str, optional): Snowflake warehouse name
- `use_utc` (bool): Use UTC timezone (default: True)

**Returns:** None

**Example:**
```python
publish(
    table_name="DAILY_FEATURES",
    query="sql/create_features.sql",
    audits=["sql/audit_row_count.sql"],
    ctx={"date": "2024-01-01"},
)
```

## Performance Tips

1. **Filter early**: Apply WHERE clauses in SQL, not pandas
2. **Select only needed columns**: Avoid `SELECT *` when possible
3. **Use S3 staging**: For datasets > 1GB
4. **Choose right warehouse**: Larger warehouse for larger datasets
5. **Optimize data types**: Use `int32`, `float32`, `category` to reduce memory

## Related Documentation

- [BatchInferencePipeline](batch_inference_pipeline.md) - For very large datasets
- [S3 Integration](s3.md) - Direct S3 operations
- [Configuration Validation](validate_config.md) - Pydantic integration
