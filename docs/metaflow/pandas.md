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
    query_fpath="sql/extract_data.sql",
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

```python
df = query_pandas_from_snowflake(
    query="SELECT * FROM huge_table",
    timeout_seconds=1800,  # 30 minutes
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
)
```

## Publishing Data

### Basic Publish

```python
from ds_platform_utils.metaflow import publish_pandas

publish_pandas(
    table_name="my_results",
    df=results_df,
    schema="my_dev_schema",
)
```

### Replace vs. Append

```python
# Replace existing table (default)
publish_pandas(
    table_name="my_table",
    df=df,
    mode="replace",
)

# Append to existing table
publish_pandas(
    table_name="my_table",
    df=df,
    mode="append",
)

# Fail if table exists
publish_pandas(
    table_name="my_table",
    df=df,
    mode="fail",
)
```

### Add Comments

```python
publish_pandas(
    table_name="my_table",
    df=df,
    comment="Daily feature refresh - 2024-01-15",
)
```

### Specify Warehouse

```python
publish_pandas(
    table_name="large_table",
    df=large_df,
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",  # Use larger warehouse
)
```

## Using SQL Files

### Query and Publish Pattern

The most common pattern: query data with one SQL file, transform in Python, publish with another SQL file.

```python
from ds_platform_utils.metaflow import publish

publish(
    query_fpath="sql/create_features.sql",
    ctx={"start_date": "2024-01-01", "end_date": "2024-12-31"},
    publish_query_fpath="sql/publish_features.sql",
    comment="Daily feature engineering",
)
```

```sql
-- sql/create_features.sql
CREATE OR REPLACE TEMPORARY TABLE temp_features AS
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
-- sql/publish_features.sql
CREATE OR REPLACE TABLE my_dev_schema.user_features AS
SELECT * FROM temp_features;
```

### Transform Function

Add Python transformation between query and publish:

```python
def transform_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add engineered features."""
    df['recency_days'] = (
        datetime.now() - pd.to_datetime(df['last_seen'])
    ).dt.days
    df['frequency_per_day'] = df['event_count'] / 30
    return df

publish(
    query_fpath="sql/create_features.sql",
    ctx={"start_date": "2024-01-01", "end_date": "2024-12-31"},
    transform_fn=transform_features,  # ← Add transformation
    publish_query_fpath="sql/publish_features.sql",
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

For very large DataFrames:

```python
# Split into chunks
chunk_size = 100000
for i in range(0, len(large_df), chunk_size):
    chunk = large_df.iloc[i:i+chunk_size]
    
    publish_pandas(
        table_name="large_table",
        df=chunk,
        mode="append" if i > 0 else "replace",  # Replace first, append rest
    )
    
    print(f"Published chunk {i//chunk_size + 1}")
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
- `query` (str, optional): SQL query string
- `query_fpath` (str, optional): Path to SQL file
- `ctx` (dict, optional): Template variables for query
- `warehouse` (str, optional): Snowflake warehouse name
- `use_s3_stage` (bool): Use S3 staging for large results (default: False)
- `timeout_seconds` (int, optional): Query timeout in seconds

**Returns:** `pandas.DataFrame`

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
- `table_name` (str): Target table name
- `df` (pd.DataFrame): DataFrame to publish
- `schema` (str, optional): Target schema (default: dev schema)
- `mode` (str): "replace", "append", or "fail" (default: "replace")
- `warehouse` (str, optional): Snowflake warehouse name
- `comment` (str, optional): Table comment

**Returns:** None

**Example:**
```python
publish_pandas(
    table_name="my_results",
    df=results_df,
    schema="my_dev_schema",
    mode="replace",
)
```

### publish()

Query, transform, and publish in one call.

**Parameters:**
- `query_fpath` (str): Path to query SQL file
- `ctx` (dict): Template variables
- `publish_query_fpath` (str): Path to publish SQL file
- `transform_fn` (callable, optional): Transformation function
- `comment` (str, optional): Table comment
- `warehouse` (str, optional): Snowflake warehouse name

**Returns:** None

**Example:**
```python
publish(
    query_fpath="sql/query.sql",
    ctx={"date": "2024-01-01"},
    publish_query_fpath="sql/publish.sql",
    comment="Daily update",
)
```

## Performance Tips

1. **Filter early**: Apply WHERE clauses in SQL, not pandas
2. **Select only needed columns**: Avoid `SELECT *` when possible
3. **Use S3 staging**: For datasets > 1GB
4. **Choose right warehouse**: Larger warehouse for larger datasets
5. **Optimize data types**: Use `int32`, `float32`, `category` to reduce memory

## Common Patterns

See [Common Patterns Guide](../guides/common_patterns.md) for more examples.

## Troubleshooting

See [Troubleshooting Guide](../guides/troubleshooting.md) for solutions to common issues.

## Related Documentation

- [BatchInferencePipeline](batch_inference_pipeline.md) - For very large datasets
- [S3 Integration](s3.md) - Direct S3 operations
- [Configuration Validation](validate_config.md) - Pydantic integration
