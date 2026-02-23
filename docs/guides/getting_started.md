# Getting Started with ds-platform-utils

[← Back to Main Docs](../README.md)

This guide will help you get started with `ds-platform-utils` for building ML workflows on Pattern's data platform.

## Prerequisites

Before you begin, ensure you have:

1. **Metaflow installed and configured**
   ```bash
   pip install metaflow
   metaflow configure aws
   ```

2. **Access to Pattern's Snowflake account**
   - You should be able to connect through Metaflow's Snowflake integration
   
3. **AWS credentials configured**
   - Metaflow will handle S3 access through your configured AWS profile

## Installation

### Production Use

```bash
pip install git+https://github.com/patterninc/ds-platform-utils.git
```

### Development

```bash
git clone https://github.com/patterninc/ds-platform-utils.git
cd ds-platform-utils
uv sync
```

## Your First Query

Let's start with a simple example: querying data from Snowflake into a pandas DataFrame.

```python
from ds_platform_utils.metaflow import query_pandas_from_snowflake

# Query data from Snowflake
df = query_pandas_from_snowflake(
    query="""
        SELECT *
        FROM pattern_db.data_science.my_table
        LIMIT 1000
    """,
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
)

print(f"Retrieved {len(df)} rows")
print(df.head())
```

### What's Happening Here?

1. The library connects to Snowflake using Metaflow's integration
2. Executes your SQL query
3. Handles timezone conversion (UTC by default)
4. Returns a pandas DataFrame

## Your First Data Publication

Now let's publish some results back to Snowflake:

```python
from ds_platform_utils.metaflow import publish_pandas
import pandas as pd

# Create some sample results
results_df = pd.DataFrame({
    'id': [1, 2, 3],
    'prediction': [0.8, 0.6, 0.9],
    'confidence': [0.95, 0.82, 0.91]
})

# Publish to Snowflake
publish_pandas(
    table_name="my_predictions",
    df=results_df,
    auto_create_table=True,  # Creates table if it doesn't exist
    overwrite=True,          # Replaces existing data
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
)

print("✅ Results published successfully!")
```

## Your First Metaflow Flow

Let's combine everything into a simple Metaflow flow:

```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import (
    query_pandas_from_snowflake,
    publish_pandas
)

class SimpleMLFlow(FlowSpec):
    """A simple ML workflow."""
    
    @step
    def start(self):
        """Query training data."""
        print("📊 Querying training data...")
        self.df = query_pandas_from_snowflake(
            query="""
                SELECT *
                FROM pattern_db.data_science_stage.training_features
                WHERE date >= '2024-01-01'
                LIMIT 10000
            """,
            warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
        )
        print(f" Retrieved {len(self.df)} rows")
        self.next(self.train)
    
    @step
    def train(self):
        """Train a simple model."""
        print("🤖 Training model...")
        # Your model training code here
        # For demo, just create predictions
        self.predictions = self.df[['id']].copy()
        self.predictions['prediction'] = 0.5
        self.next(self.publish_results)
    
    @step
    def publish_results(self):
        """Publish predictions to Snowflake."""
        print("📤 Publishing results...")
        publish_pandas(
            table_name="simple_ml_predictions",
            df=self.predictions,
            auto_create_table=True,
            overwrite=True,
            warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
        )
        print("✅ Flow complete!")
        self.next(self.end)
    
    @step
    def end(self):
        """Flow end."""
        pass

if __name__ == '__main__':
    SimpleMLFlow()
```

Run the flow:

```bash
python simple_ml_flow.py run
```

## Working with Large Datasets

For datasets larger than a few GB, use S3 staging:

```python
from ds_platform_utils.metaflow import query_pandas_from_snowflake

# For large datasets, enable S3 staging
df = query_pandas_from_snowflake(
    query="SELECT * FROM very_large_table",
    use_s3_stage=True,  # ← This enables S3 staging
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
)
```

### When to Use S3 Staging?

| Data Size | Method                            | Reason                           |
| --------- | --------------------------------- | -------------------------------- |
| < 1 GB    | Direct                            | Simpler, faster for small data   |
| 1-10 GB   | S3 Stage                          | More reliable, prevents timeouts |
| > 10 GB   | S3 Stage + BatchInferencePipeline | Parallel processing required     |

## Batch Inference

For very large-scale predictions, use `BatchInferencePipeline`:

```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import BatchInferencePipeline

class BatchPredictionFlow(FlowSpec):
    
    @step
    def start(self):
        """Setup pipeline and create worker batches."""
        self.pipeline = BatchInferencePipeline()
        self.worker_ids = self.pipeline.query_and_batch(
            input_query="SELECT * FROM large_input_table",
            parallel_workers=5,  # Split work across 5 workers
        )
        self.next(self.predict, foreach='worker_ids')
    
    @step
    def predict(self):
        """Process one batch in parallel."""
        def my_predict_fn(df):
            # Your prediction logic
            df['prediction'] = 0.5  # Replace with actual model
            return df[['id', 'prediction']]
        
        self.pipeline.process_batch(
            worker_id=self.input,
            predict_fn=my_predict_fn,
        )
        self.next(self.join)
    
    @step
    def join(self, inputs):
        """Merge results and publish."""
        self.pipeline = inputs[0].pipeline
        self.pipeline.publish_results(
            output_table_name="batch_predictions",
        )
        self.next(self.end)
    
    @step
    def end(self):
        print("✅ Batch inference complete!")

if __name__ == '__main__':
    BatchPredictionFlow()
```

## Dev vs Prod

The library automatically handles dev/prod schema separation:

```python
# In development (default Metaflow perimeter)
publish_pandas(
    table_name="my_table",  # Goes to: pattern_db.data_science_stage.my_table
    df=df,
)

# In production (production Metaflow perimeter)
# Same code, but goes to: pattern_db.data_science.my_table
```

## Understanding Warehouses

Pattern provides several Snowflake warehouses:

| Warehouse  | Size        | Use Case                           |
| ---------- | ----------- | ---------------------------------- |
| `*_XS_WH`  | Extra Small | Quick queries, small data          |
| `*_MED_WH` | Medium      | Medium workloads, ML training      |
| `*_XL_WH`  | Extra Large | Large batch jobs, heavy processing |

Choose based on your workload:
- **Development**: Use `_DEV_` warehouses
- **Production**: Use `_PROD_` warehouses  
- **Shared**: Use `_SHARED_` for general work
- **ADS**: Use `_ADS_` for ads-specific work

## Next Steps

Now that you've got the basics:

1. 📖 Learn [Common Patterns](common_patterns.md) for typical workflows
2. 🎯 Review [Best Practices](best_practices.md) for production code
3. 🔧 Check out [Performance Tuning](performance_tuning.md) for optimization
4. 🔍 Explore the complete [API Reference](../api/index.md)

## Need Help?

- 📚 Check the [Troubleshooting Guide](troubleshooting.md)
- 💬 Ask in the #data-science-platform Slack channel
- 🐛 Report issues on GitHub
