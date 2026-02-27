# BatchInferencePipeline

[← Back to Metaflow Utilities](README.md) | [← Back to Main Docs](../README.md)

A scalable batch inference pipeline for running ML predictions on large datasets using Metaflow, Snowflake, and S3.

## Key Features

- **Snowflake Integration**: Query data directly from Snowflake and write results back
- **S3 Staging**: Efficient data transfer via S3 for large datasets
- **Parallel Processing**: Built-in support for Metaflow's foreach parallelization
- **Pipeline Orchestration**: Three-stage pipeline (query → process → publish)
- **Queue-based Processing**: Multi-threaded download→inference→upload pipeline for optimal throughput
- **Execution State Validation**: Prevents out-of-order execution with clear error messages

#### Quick Start

##### Option 1: Manual Control with Foreach Parallelization

Use this approach when you need fine-grained control and want to parallelize across multiple Metaflow workers:

```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import BatchInferencePipeline

class MyPredictionFlow(FlowSpec):

    @step
    def start(self):
        # Initialize pipeline and export data to S3
        self.pipeline = BatchInferencePipeline()
        self.worker_ids = self.pipeline.query_and_batch(
            input_query="SELECT * FROM my_table WHERE date >= '2024-01-01'",
            parallel_workers=10,  # Split into 10 parallel workers
        )
        self.next(self.predict, foreach='worker_ids')

    @step
    def predict(self):
        # Process single batch (runs in parallel via foreach)
        worker_id = self.input
        self.pipeline.process_batch(
            worker_id=worker_id,
            predict_fn=my_model.predict,
            batch_size_in_mb=256,
        )
        self.next(self.join)

    @step
    def join(self, inputs):
        # Merge and write results to Snowflake
        self.pipeline = inputs[0].pipeline
        self.pipeline.publish_results(
            output_table_name="predictions_table",
            auto_create_table=True,
        )
        self.next(self.end)

    @step
    def end(self):
        print("✅ Pipeline complete!")
```

##### Option 2: Convenience Method

Use this for simpler workflows without foreach parallelization:

```python
from ds_platform_utils.metaflow import BatchInferencePipeline

def my_predict_function(df):
    # Your prediction logic here
    df['prediction'] = model.predict(df[feature_columns])
    return df[['id', 'prediction']]

# Run the complete pipeline
pipeline = BatchInferencePipeline()
pipeline.run(
    input_query="SELECT * FROM input_table",
    output_table_name="predictions_table",
    predict_fn=my_predict_function,
    batch_size_in_mb=128,
    auto_create_table=True,
    overwrite=True,
)
```

#### API Reference

##### `BatchInferencePipeline()`

Initialize the pipeline. Automatically configures S3 paths based on Metaflow context.

##### `query_and_batch()`

**Step 1**: Export data from Snowflake to S3 and create worker batches.

```python
worker_ids = pipeline.query_and_batch(
    input_query: Union[str, Path],      # SQL query or path to .sql file
    ctx: Optional[dict] = None,         # Template variables (e.g., {"schema": "dev"})
    warehouse: Optional[str] = None,    # Snowflake warehouse
    use_utc: bool = True,               # Use UTC timezone
    parallel_workers: int = 1,          # Number of parallel workers
)
```

**Returns**: List of worker IDs for foreach parallelization

##### `process_batch()`

**Step 2**: Process a single batch with streaming pipeline.

```python
s3_path = pipeline.process_batch(
    worker_id: int,                                        # Worker ID from foreach
    predict_fn: Callable[[pd.DataFrame], pd.DataFrame],   # Prediction function
    batch_size_in_mb: int = 128,                          # Batch size in MB
    timeout_per_batch: int = 300,                         # Timeout in seconds
)
```

**Your `predict_fn` signature**:
```python
def predict_fn(input_df: pd.DataFrame) -> pd.DataFrame:
    # Process the input DataFrame and return predictions
    return predictions_df
```

##### `publish_results()`

**Step 3**: Write all predictions from S3 to Snowflake.

```python
pipeline.publish_results(
    output_table_name: str,                              # Snowflake table name
    output_table_definition: Optional[List[Tuple]] = None,  # Schema definition
    auto_create_table: bool = True,                      # Auto-create if missing
    overwrite: bool = True,                              # Overwrite existing data
    warehouse: Optional[str] = None,                     # Snowflake warehouse
    use_utc: bool = True,                                # Use UTC timezone
)
```

##### `run()`

Convenience method that combines all three steps for simple workflows.

```python
pipeline.run(
    input_query: Union[str, Path],
    output_table_name: str,
    predict_fn: Callable[[pd.DataFrame], pd.DataFrame],
    # ... plus all parameters from query_and_batch(), process_batch(), publish_results()
)
```

#### Advanced Usage

##### Custom Table Schema

```python
table_schema = [
    ("id", "VARCHAR(100)"),
    ("prediction", "FLOAT"),
    ("confidence", "FLOAT"),
    ("predicted_at", "TIMESTAMP_NTZ"),
]

pipeline.publish_results(
    output_table_name="predictions",
    output_table_definition=table_schema,
    auto_create_table=True,
)
```

##### Using SQL Template Variables

```python
worker_ids = pipeline.query_and_batch(
    input_query="""
        SELECT * FROM {{schema}}.my_table
        WHERE date >= '{{start_date}}'
    """,
    ctx={
        "schema": "production",
        "start_date": "2024-01-01",
    },
)
```

##### External SQL Files

```python
worker_ids = pipeline.query_and_batch(
    input_query=Path("queries/input_query.sql"),
    ctx={"schema": "production"},
)
```

#### Error Handling & Validation

The pipeline validates execution order and provides clear error messages:

```python
pipeline = BatchInferencePipeline()

# ❌ This will raise RuntimeError
pipeline.process_batch(worker_id=1, predict_fn=my_fn)
# Error: "Cannot process batch: query_and_batch() must be called first."

# ❌ This will also raise RuntimeError
pipeline.publish_results(output_table_name="results")
# Error: "Cannot publish results: No batches have been processed."
```

Re-execution warnings:

```python
# First execution
worker_ids = pipeline.query_and_batch(input_query="SELECT * FROM table")
pipeline.process_batch(worker_id=1, predict_fn=my_fn)

# Second execution - warns about state reset
worker_ids = pipeline.query_and_batch(input_query="SELECT * FROM table")
# ⚠️ Warning: Re-executing query_and_batch() will reset batch processing state.

# Publishing again - warns about duplicates
pipeline.publish_results(output_table_name="results")  # First time - OK
pipeline.publish_results(output_table_name="results")  # Second time
# ⚠️ Warning: Results have already been published. Publishing again may cause duplicate data.
```

#### Performance Tips

1. **Batch Size**: Tune `batch_size_in_mb` based on your data and memory constraints
   - Larger batches = fewer S3 operations but more memory usage
   - Recommended: 128-512 MB per batch

2. **Parallel Workers**: Balance parallelization with Metaflow cluster capacity
   - More workers = faster processing but more resources
   - Consider your data size and available compute

3. **Timeouts**: Adjust `timeout_per_batch` for long-running inference
   - Default: 300 seconds (5 minutes)
   - Increase for complex models or large batches

#### Troubleshooting

##### "Worker X not found"
- The worker_id doesn't match any created worker
- Check that you're using worker_ids from `query_and_batch()`

##### Timeout Errors
- Increase `timeout_per_batch` parameter
- Reduce `batch_size_in_mb` to process smaller chunks
- Check model inference performance

##### Memory Issues
- Reduce `batch_size_in_mb`
- Ensure predict_fn doesn't accumulate data
- Monitor Metaflow task memory usage

#### Architecture

```
┌──────────────┐
│  Snowflake   │
│   (Query)    │
└──────┬───────┘
       │ COPY INTO
       ▼
┌──────────────┐      ┌─────────────────────────┐
│      S3      │      │   Metaflow Workers      │
│   (Stage)    │◄────►│   (Foreach Parallel)    │
│  Input Data  │      │                         │
└──────────────┘      │  ┌───────────────────┐  │
       │              │  │  Queue Pipeline:  │  │
       │              │  │  Download ──→     │  │
       │              │  │  Inference ──→    │  │
       │              │  │  Upload           │  │
       │              │  └───────────────────┘  │
       │              └─────────┬───────────────┘
       ▼                        │
┌──────────────┐               │
│      S3      │◄──────────────┘
│   (Stage)    │
│ Output Data  │
└──────┬───────┘
       │ COPY INTO
       ▼
┌──────────────┐
│  Snowflake   │
│  (Publish)   │
└──────────────┘
```
