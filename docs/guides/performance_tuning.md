# Performance Tuning Guide

[← Back to Main Docs](../README.md)

Optimize your workflows for speed, cost, and reliability.

## Table of Contents

- [Understanding Performance Bottlenecks](#understanding-performance-bottlenecks)
- [Snowflake Optimization](#snowflake-optimization)
- [S3 Staging Optimization](#s3-staging-optimization)
- [Metaflow Parallelization](#metaflow-parallelization)
- [Memory Management](#memory-management)
- [Cost Optimization](#cost-optimization)

## Understanding Performance Bottlenecks

Common bottlenecks in data pipelines:

1. **Snowflake Query Time** - Complex queries, large scans, inefficient joins
2. **Data Transfer** - Moving large datasets between Snowflake and Python
3. **Python Processing** - CPU-intensive operations (ML inference, transformations)
4. **Memory Constraints** - Loading datasets larger than available RAM
5. **Sequential Processing** - Not leveraging parallelization

## Snowflake Optimization

### Query Performance

#### ✅ Use Query Tags for Monitoring

```python
from ds_platform_utils.metaflow import add_query_tags

query = add_query_tags(
    query="SELECT * FROM large_table",
    flow_name="MyFlow",
    step_name="query_data",
)
```

This adds metadata for tracking query performance in Snowflake.

#### ✅ Filter Early and Aggressively

```python
# ❌ Bad - returns 100M rows, filters in Python
df = query_pandas_from_snowflake(
    query="SELECT * FROM events"
)
df = df[df['date'] >= '2024-01-01']

# ✅ Good - returns only needed rows
df = query_pandas_from_snowflake(
    query="""
        SELECT *
        FROM events
        WHERE date >= '2024-01-01'
    """
)
```

#### ✅ Select Only Needed Columns

```python
# ❌ Bad - returns all 50 columns
df = query_pandas_from_snowflake(
    query="SELECT * FROM wide_table"
)

# ✅ Good - returns only 5 needed columns
df = query_pandas_from_snowflake(
    query="""
        SELECT user_id, feature_1, feature_2, feature_3, target
        FROM wide_table
    """
)
```

#### ✅ Use Clustering Keys

```sql
-- For tables with common filter patterns
ALTER TABLE events CLUSTER BY (date, user_id);

-- Helps queries like:
SELECT * FROM events
WHERE date >= '2024-01-01'
    AND user_id IN (1, 2, 3);
```

### Warehouse Sizing

Choose the right warehouse for your workload:

| Warehouse | Use Case                     | Query Time | Cost      |
| --------- | ---------------------------- | ---------- | --------- |
| XS        | Small queries (<100K rows)   | Slower     | Low       |
| S         | Development, ad-hoc queries  | Moderate   | Low-Med   |
| M         | Regular production workloads | Fast       | Medium    |
| L         | Large batch jobs (>10M rows) | Fast       | High      |
| XL        | Massive parallel processing  | Fastest    | Very High |

```python
# Size based on your workload
def get_warehouse(row_count: int) -> str:
    """Get optimal warehouse for row count."""
    if row_count < 100_000:
        return "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH"
    elif row_count < 1_000_000:
        return "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_S_WH"
    elif row_count < 10_000_000:
        return "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"
    else:
        return "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH"
```

### Query Result Caching

Snowflake automatically caches query results for 24 hours:

```python
# First run - hits database
df1 = query_pandas_from_snowflake(
    query="SELECT COUNT(*) FROM events WHERE date = '2024-01-01'"
)

# Second run within 24h - returns from cache (instant!)
df2 = query_pandas_from_snowflake(
    query="SELECT COUNT(*) FROM events WHERE date = '2024-01-01'"
)
```

## S3 Staging Optimization

### When to Use S3 Staging

**Enable S3 staging when:**
- Dataset > 1 GB
- Network bandwidth is limited
- Query returns many columns (wide tables)

```python
# Automatically use S3 for large results
df = query_pandas_from_snowflake(
    query="SELECT * FROM large_table",
    use_s3_stage=True,  # ← Enable for datasets > 1GB
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
)
```

### Performance Comparison

| Dataset Size | Without S3 | With S3 | Speedup        |
| ------------ | ---------- | ------- | -------------- |
| 100 MB       | 30s        | 35s     | 0.86x (slower) |
| 500 MB       | 2.5min     | 1.5min  | 1.67x          |
| 2 GB         | 10min      | 3min    | 3.33x          |
| 10 GB        | 50min      | 12min   | 4.17x          |

### Optimize S3 File Size

```python
# For BatchInferencePipeline
pipeline = BatchInferencePipeline()

# Adjust batch size based on row width
# Wide tables (many columns) → smaller batches
pipeline.query_and_batch(
    input_query="SELECT * FROM wide_table",  # 100 columns
    batch_size_in_mb=128,  # Smaller batches for wide tables
    parallel_workers=20,
)

# Narrow tables (few columns) → larger batches
pipeline.query_and_batch(
    input_query="SELECT id, value FROM narrow_table",  # 2 columns
    batch_size_in_mb=512,  # Larger batches for narrow tables
    parallel_workers=10,
)
```

## Metaflow Parallelization

### Simple Parallelization with foreach

```python
from metaflow import FlowSpec, step

class ParallelFlow(FlowSpec):
    
    @step
    def start(self):
        """Split work into chunks."""
        self.chunks = list(range(10))  # 10 parallel tasks
        self.next(self.process, foreach='chunks')
    
    @step
    def process(self):
        """Process each chunk in parallel."""
        chunk_id = self.input
        # Process chunk...
        self.result = f"Processed {chunk_id}"
        self.next(self.join)
    
    @step
    def join(self, inputs):
        """Collect results."""
        self.results = [inp.result for inp in inputs]
        self.next(self.end)
    
    @step
    def end(self):
        print(f"Processed {len(self.results)} chunks")
```

### Parallel Batch Inference

```python
@step
def query_and_split(self):
    """Query and split into batches."""
    pipeline = BatchInferencePipeline()
    self.worker_ids = pipeline.query_and_batch(
        input_query="SELECT * FROM input_data",
        batch_size_in_mb=256,
        parallel_workers=20,  # 20 parallel tasks
    )
    self.next(self.process_batch, foreach='worker_ids')

@step
def process_batch(self):
    """Process each batch in parallel."""
    worker_id = self.input
    pipeline = BatchInferencePipeline()
    
    # This runs in parallel across 20 workers
    pipeline.process_batch(
        worker_id=worker_id,
        predict_fn=my_prediction_function,
        batch_size_in_mb=64,
    )
    self.next(self.join_batches)
```

### Optimizing Parallel Workers

Choose the number of workers based on:

1. **Dataset Size**: Larger datasets → more workers
2. **Processing Time**: Longer processing → more workers
3. **Cost**: More workers = more compute cost

```python
def calculate_optimal_workers(total_rows: int, processing_time_per_row: float) -> int:
    """Calculate optimal number of parallel workers."""
    # Target: each worker processes ~30 minutes of work
    target_time_minutes = 30
    rows_per_minute = 60 / processing_time_per_row
    rows_per_worker = target_time_minutes * rows_per_minute
    
    workers = max(1, int(total_rows / rows_per_worker))
    return min(workers, 50)  # Cap at 50 workers

# Example
total_rows = 10_000_000
time_per_row = 0.1  # seconds
workers = calculate_optimal_workers(total_rows, time_per_row)
print(f"Use {workers} workers")  # → 28 workers
```

## Memory Management

### Chunked Processing

Process large datasets in chunks to avoid memory issues:

```python
def process_in_chunks(df: pd.DataFrame, chunk_size: int = 10000):
    """Process DataFrame in chunks."""
    results = []
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        result = process_chunk(chunk)
        results.append(result)
        
        # Free memory
        del chunk
        gc.collect()
    
    return pd.concat(results)
```

### Monitor Memory Usage

```python
import psutil
import os

def log_memory_usage(step_name: str):
    """Log current memory usage."""
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024 / 1024
    print(f"📊 {step_name}: Memory usage = {mem_mb:.1f} MB")

@step
def process_data(self):
    log_memory_usage("start")
    
    df = query_pandas_from_snowflake(query="...")
    log_memory_usage("after_query")
    
    df = process(df)
    log_memory_usage("after_process")
    
    publish_pandas(table_name="results", df=df)
    log_memory_usage("after_publish")
```

### Use Appropriate Data Types

```python
# ❌ Bad - uses default types
df = pd.DataFrame({
    'id': [1, 2, 3],  # int64 (8 bytes per value)
    'category': ['A', 'B', 'C'],  # object (variable size)
    'value': [1.0, 2.0, 3.0],  # float64 (8 bytes per value)
})

# ✅ Good - optimize types
df = df.astype({
    'id': 'int32',  # 4 bytes per value (50% memory reduction)
    'category': 'category',  # Much smaller for repeated values
    'value': 'float32',  # 4 bytes per value (50% memory reduction)
})

# For 10M rows, this saves ~150 MB memory!
```

## Cost Optimization

### Warehouse Auto-suspend

Warehouses auto-suspend after inactivity, but you can optimize timing:

```sql
-- Set shorter auto-suspend for dev warehouses
ALTER WAREHOUSE OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH
SET AUTO_SUSPEND = 60;  -- Suspend after 1 minute

-- Longer auto-suspend for production (avoid cold starts)
ALTER WAREHOUSE OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_MED_WH
SET AUTO_SUSPEND = 600;  -- Suspend after 10 minutes
```

### Query Result Caching

Leverage Snowflake's result cache to avoid redundant queries:

```python
# Development: run queries multiple times during debugging
# → Results are cached, subsequent runs are free!

@step
def explore_data(self):
    # First run: hits database (costs money)
    df = query_pandas_from_snowflake(
        query="SELECT * FROM my_table WHERE date = '2024-01-01'"
    )
    print(df.head())  # Check data
    
    # Realize you need to adjust something...
    
    # Re-run flow: uses cached result (free!)
    df = query_pandas_from_snowflake(
        query="SELECT * FROM my_table WHERE date = '2024-01-01'"  # Same query
    )
```

### Right-size Your Work

```python
# Development: use smaller datasets
if not is_production():
    query += " LIMIT 10000"  # Only 10K rows for testing

# Production: use full dataset
df = query_pandas_from_snowflake(query=query)
```

### Monitor Costs

```sql
-- Check warehouse usage
SELECT
    warehouse_name,
    SUM(credits_used) as total_credits,
    SUM(credits_used) * 3 as estimated_cost_usd  -- ~$3 per credit
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY total_credits DESC;
```

## Performance Checklist

Before running large workloads:

- [ ] Query filters data as early as possible
- [ ] Only SELECT needed columns
- [ ] Using appropriate warehouse size
- [ ] S3 staging enabled for datasets > 1GB
- [ ] Parallel processing configured for long-running tasks
- [ ] Memory usage monitored and optimized
- [ ] Data types optimized (int32, float32, category)
- [ ] Result caching leveraged for development
- [ ] Chunk size tuned for workload
- [ ] Cost tracking enabled

## Benchmarking

Use this template to benchmark your optimizations:

```python
import time

def benchmark_query(query: str, use_s3: bool = False) -> dict:
    """Benchmark a query."""
    start = time.time()
    
    df = query_pandas_from_snowflake(
        query=query,
        use_s3_stage=use_s3,
    )
    
    duration = time.time() - start
    
    return {
        'duration_seconds': duration,
        'rows': len(df),
        'columns': len(df.columns),
        'memory_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
        'rows_per_second': len(df) / duration,
    }

# Test both approaches
results_no_s3 = benchmark_query(my_query, use_s3=False)
results_with_s3 = benchmark_query(my_query, use_s3=True)

print(f"Without S3: {results_no_s3['duration_seconds']:.1f}s")
print(f"With S3: {results_with_s3['duration_seconds']:.1f}s")
print(f"Speedup: {results_no_s3['duration_seconds'] / results_with_s3['duration_seconds']:.2f}x")
```

## Additional Resources

- [Best Practices](best_practices.md)
- [Common Patterns](common_patterns.md)
- [Troubleshooting](troubleshooting.md)
- [Snowflake Query Performance Guide](https://docs.snowflake.com/en/user-guide/ui-snowsight-query-performance)
