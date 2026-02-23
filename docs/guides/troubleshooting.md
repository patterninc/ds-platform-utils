# Troubleshooting Guide

[← Back to Main Docs](../README.md)

Solutions to common issues and error messages.

## Table of Contents

- [Snowflake Connection Issues](#snowflake-connection-issues)
- [Query Errors](#query-errors)
- [Memory Errors](#memory-errors)
- [S3 Staging Issues](#s3-staging-issues)
- [Batch Inference Errors](#batch-inference-errors)
- [Metaflow Issues](#metaflow-issues)
- [Publishing Errors](#publishing-errors)

## Snowflake Connection Issues

### Error: "Snowflake connector not configured"

**Cause**: Metaflow's Snowflake connector is not set up.

**Solution**: Use Metaflow's built-in Snowflake integration:

```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow.get_snowflake_connection import get_snowflake_connection

class MyFlow(FlowSpec):
    
    @step
    def start(self):
        # Use Metaflow's connection
        cursor = get_snowflake_connection()
        # Your code...
```

### Error: "Authentication failed"

**Cause**: Snowflake credentials not available or expired.

**Solution**:
```bash
# Check your Snowflake connection
snowsql -c my_connection

# If using SSO, re-authenticate
snowsql -a <account> -u <username> --authenticator externalbrowser
```

### Error: "Warehouse does not exist"

**Cause**: Wrong warehouse name or no access.

**Solution**: Verify warehouse name:
```python
# Check available warehouses
cursor = get_snowflake_connection()
cursor.execute("SHOW WAREHOUSES")
print(cursor.fetchall())

# Use correct warehouse name (case-sensitive!)
query_pandas_from_snowflake(
    query="SELECT * FROM table",
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",  # Correct name
)
```

## Query Errors

### Error: "SQL compilation error: Object does not exist"

**Cause**: Table/view not found or no access.

**Solution**:

```python
# Check table exists
cursor = get_snowflake_connection()
cursor.execute("""
    SHOW TABLES LIKE 'my_table' IN SCHEMA my_database.my_schema
""")
print(cursor.fetchall())

# Check you have access
cursor.execute("""
    SELECT * FROM my_database.my_schema.my_table
    LIMIT 1
""")
```

### Error: "Template variable not provided"

**Cause**: Missing variable in `ctx` dictionary.

**Solution**:
```python
# ❌ Bad - missing variable
query_pandas_from_snowflake(
    query_fpath="sql/query.sql",  # Uses {{start_date}}
    ctx={"end_date": "2024-12-31"},  # Missing start_date!
)

# ✅ Good - all variables provided
query_pandas_from_snowflake(
    query_fpath="sql/query.sql",
    ctx={
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
    },
)
```

### Error: "Query timeout exceeded"

**Cause**: Query takes too long.

**Solutions**:

1. **Optimize query** (filter early, select fewer columns)
2. **Use larger warehouse**
3. **Increase timeout**

```python
query_pandas_from_snowflake(
    query="SELECT * FROM huge_table",
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",  # Larger warehouse
    timeout_seconds=1800,  # 30 minutes
)
```

### Error: "Statement reached its statement or warehouse timeout"

**Cause**: Query exceeded warehouse timeout.

**Solution**: Break query into smaller chunks or use intermediate tables:

```sql
-- Instead of one massive query
CREATE TEMPORARY TABLE temp_results AS
SELECT * FROM huge_table
WHERE date >= '2024-01-01';

-- Then query the smaller result
SELECT * FROM temp_results;
```

## Memory Errors

### Error: "MemoryError" or "Killed"

**Cause**: Dataset too large for available RAM.

**Solutions**:

#### Option 1: Use S3 Staging

```python
df = query_pandas_from_snowflake(
    query="SELECT * FROM large_table",
    use_s3_stage=True,  # ← Reduces memory pressure
)
```

#### Option 2: Process in Chunks

```python
def process_in_chunks(query: str, chunk_size: int = 100000):
    """Process large query in chunks."""
    offset = 0
    results = []
    
    while True:
        chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
        chunk = query_pandas_from_snowflake(query=chunk_query)
        
        if len(chunk) == 0:
            break
        
        # Process chunk
        result = process(chunk)
        results.append(result)
        
        offset += chunk_size
    
    return pd.concat(results)
```

#### Option 3: Use BatchInferencePipeline

```python
# For very large datasets (> 10M rows)
pipeline = BatchInferencePipeline()
worker_ids = pipeline.query_and_batch(
    input_query="SELECT * FROM huge_table",
    parallel_workers=20,  # Split into 20 batches
)
```

#### Option 4: Optimize Data Types

```python
# After loading data
df = df.astype({
    'int_col': 'int32',  # Instead of int64
    'float_col': 'float32',  # Instead of float64
    'category_col': 'category',  # For repeated values
})

# Can reduce memory by 50%+
```

### Error: "DataFrame too large to serialize"

**Cause**: Metaflow cannot serialize large DataFrames between steps.

**Solution**: Don't pass large DataFrames, use temporary tables instead:

```python
@step
def query_data(self):
    """Query and store in temp table."""
    cursor = get_snowflake_connection()
    cursor.execute("""
        CREATE TEMPORARY TABLE temp_my_data AS
        SELECT * FROM large_table
        WHERE date >= '2024-01-01'
    """)
    
    # Just pass the table name, not the data
    self.temp_table = "temp_my_data"
    self.next(self.process)

@step
def process(self):
    """Query from temp table."""
    self.df = query_pandas_from_snowflake(
        query=f"SELECT * FROM {self.temp_table}"
    )
    # Process...
```

## S3 Staging Issues

### Error: "S3 upload failed"

**Cause**: No S3 access or wrong permissions.

**Solution**: Check S3 configuration:

```bash
# Check AWS credentials
aws sts get-caller-identity

# Test S3 access
aws s3 ls s3://your-bucket/
```

### Error: "Slow performance with S3 staging"

**Cause**: Too many small files or inefficient batch size.

**Solution**: Tune batch size:

```python
# For wide tables (many columns)
pipeline.query_and_batch(
    input_query="SELECT * FROM wide_table",
    batch_size_in_mb=128,  # Smaller batches
    parallel_workers=30,
)

# For narrow tables (few columns)
pipeline.query_and_batch(
    input_query="SELECT id, value FROM narrow_table",
    batch_size_in_mb=512,  # Larger batches
    parallel_workers=10,
)
```

## Batch Inference Errors

### Error: "Cannot process batch before query_and_batch"

**Cause**: Trying to call `process_batch()` before `query_and_batch()`.

**Solution**: Follow the correct order:

```python
# ✅ Correct order
pipeline = BatchInferencePipeline()

# 1. Query and split
worker_ids = pipeline.query_and_batch(...)

# 2. Process each batch
for worker_id in worker_ids:
    pipeline.process_batch(worker_id, ...)

# 3. Publish results
pipeline.publish_results(...)
```

### Error: "Cannot publish before processing"

**Cause**: Trying to `publish_results()` before processing any batches.

**Solution**: Ensure at least one batch is processed:

```python
@step
def process_batches(self):
    worker_id = self.input
    pipeline = BatchInferencePipeline()
    
    # Process this batch
    pipeline.process_batch(
        worker_id=worker_id,
        predict_fn=my_predict_fn,
    )
    
    self.next(self.join)

@step
def join(self, inputs):
    """Now safe to publish."""
    pipeline = BatchInferencePipeline()
    pipeline.publish_results(
        output_table="predictions",
        output_schema="my_dev_schema",
    )
    self.next(self.end)
```

### Error: "Worker ID not found"

**Cause**: Invalid worker ID or not from current pipeline.

**Solution**: Use worker IDs from `query_and_batch()`:

```python
# ❌ Bad - made-up worker ID
pipeline.process_batch(worker_id=999, ...)

# ✅ Good - use returned worker IDs
worker_ids = pipeline.query_and_batch(...)
for worker_id in worker_ids:  # Use these IDs
    pipeline.process_batch(worker_id=worker_id, ...)
```

### Error: "Prediction function failed"

**Cause**: Exception in your `predict_fn`.

**Solution**: Test your function separately:

```python
# Test predict_fn with sample data
sample_df = pd.DataFrame({
    'feature_1': [1, 2, 3],
    'feature_2': [4, 5, 6],
})

try:
    result = my_predict_fn(sample_df)
    print("✅ Predict function works")
    print(result.head())
except Exception as e:
    print(f"❌ Predict function failed: {e}")
    import traceback
    traceback.print_exc()
```

## Metaflow Issues

### Error: "Step failed with StepTimeout"

**Cause**: Step exceeded time limit.

**Solutions**:

1. **Increase timeout**:
```python
@timeout(seconds=7200)  # 2 hours
@step
def long_running_step(self):
    # Your code...
```

2. **Optimize processing** (see [Performance Tuning](performance_tuning.md))

3. **Split into parallel tasks**:
```python
@step
def split_work(self):
    self.chunks = range(10)
    self.next(self.process_chunk, foreach='chunks')

@step
def process_chunk(self):
    # Process one chunk (faster)
    chunk_id = self.input
    # Your code...
```

### Error: "Resume failed"

**Cause**: Metaflow cannot resume from checkpoint.

**Solution**: Re-run from start or from different step:

```bash
# Re-run entire flow
python flow.py run

# Resume from specific step
python flow.py resume --origin-run-id <run_id>

# Re-run from specific step
python flow.py run --start-at process_data
```

### Error: "Parameter validation failed"

**Cause**: Invalid parameter value.

**Solution**: Check parameter constraints:

```python
from pydantic import BaseModel, validator

class Config(BaseModel):
    date: str
    
    @validator('date')
    def validate_date(cls, v):
        """Validate date format."""
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError(f"Invalid date format: {v}. Use YYYY-MM-DD")
```

## Publishing Errors

### Error: "Table already exists"

**Cause**: Table exists and mode is not specified.

**Solution**: Specify mode:

```python
publish_pandas(
    table_name="my_table",
    df=df,
    mode="replace",  # or "append" or "fail"
)
```

### Error: "Permission denied"

**Cause**: No write access to schema.

**Solution**: Use your dev schema:

```python
# ✅ Use your dev schema
publish_pandas(
    table_name="my_table",
    df=df,
    schema="my_dev_schema",  # You have access here
)

# ❌ Don't write to production without permission
publish_pandas(
    table_name="my_table",
    df=df,
    schema="production_schema",  # No access!
)
```

### Error: "Column name mismatch"

**Cause**: DataFrame columns don't match target table.

**Solution**: 

```python
# Check current columns
print(df.columns.tolist())

# Rename to match target
df = df.rename(columns={
    'old_name': 'new_name',
})

# Or select specific columns
df = df[['col1', 'col2', 'col3']]

publish_pandas(table_name="my_table", df=df)
```

## General Debugging Tips

### Enable Verbose Logging

```python
import logging

logging.basicConfig(level=logging.DEBUG)

# Now you'll see more detailed output
df = query_pandas_from_snowflake(query="...")
```

### Check Snowflake Query History

```sql
-- View recent queries
SELECT
    query_id,
    query_text,
    execution_status,
    error_message,
    total_elapsed_time / 1000 as seconds
FROM table(information_schema.query_history())
WHERE user_name = CURRENT_USER()
ORDER BY start_time DESC
LIMIT 10;
```

### Test SQL Separately

Before running in Metaflow, test SQL in Snowflake UI:

1. Copy your SQL query
2. Run in Snowflake console
3. Check results
4. Fix issues
5. Then use in flow

### Isolate the Problem

```python
# Instead of running full flow
python flow.py run

# Run just one step
python flow.py step query_data
```

### Use Restore Step State

For debugging Metaflow flows:

```python
from ds_platform_utils.metaflow import restore_step_state

# Restore state from previous run
with restore_step_state("MyFlow", run_id="123", step="process"):
    # Access self.df and other artifacts
    print(self.df.head())
    
    # Debug your processing logic
    result = process(self.df)
    print(result.head())
```

## Getting Help

If you're still stuck:

1. **Check the logs**: Full error messages often contain the solution
2. **Review the docs**: [Getting Started](getting_started.md), [Best Practices](best_practices.md)
3. **Search Snowflake docs**: [docs.snowflake.com](https://docs.snowflake.com)
4. **Search Metaflow docs**: [docs.metaflow.org](https://docs.metaflow.org)
5. **Ask your team**: Someone may have seen the issue before

## Common Error Patterns

| Error Message                       | Likely Cause            | Solution                     |
| ----------------------------------- | ----------------------- | ---------------------------- |
| "Object does not exist"             | Table/schema name wrong | Check table path             |
| "Authentication failed"             | Credentials expired     | Re-authenticate              |
| "MemoryError"                       | DataFrame too large     | Use S3 staging or chunks     |
| "Timeout exceeded"                  | Query too slow          | Optimize query or warehouse  |
| "Permission denied"                 | No write access         | Use dev schema               |
| "Template variable not provided"    | Missing ctx variable    | Add to ctx dict              |
| "Cannot process batch before query" | Wrong order             | Call query_and_batch() first |
| "Serialization failed"              | Object too large        | Use temp tables              |

## Additional Resources

- [Best Practices](best_practices.md)
- [Performance Tuning](performance_tuning.md)
- [Common Patterns](common_patterns.md)
- [Getting Started](getting_started.md)
