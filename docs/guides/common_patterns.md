# Common Patterns

[← Back to Main Docs](../README.md)

Proven patterns for common data science workflows.

## Table of Contents

- [Query Patterns](#query-patterns)
- [Feature Engineering](#feature-engineering)
- [Batch Inference](#batch-inference)
- [Incremental Processing](#incremental-processing)
- [Error Recovery](#error-recovery)
- [Testing Patterns](#testing-patterns)

## Query Patterns

### Simple Query and Publish

The most basic pattern: query data, transform, publish results.

```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import query_pandas_from_snowflake, publish_pandas

class SimpleFlow(FlowSpec):
    
    @step
    def start(self):
        """Query input data."""
        self.df = query_pandas_from_snowflake(
            query="SELECT * FROM input_table",
            warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
        )
        self.next(self.transform)
    
    @step
    def transform(self):
        """Transform data."""
        self.df['new_column'] = self.df['old_column'] * 2
        self.next(self.publish)
    
    @step
    def publish(self):
        """Publish results."""
        publish_pandas(
            table_name="output_table",
            df=self.df,
            schema="my_dev_schema",
        )
        self.next(self.end)
    
    @step
    def end(self):
        print(f"✅ Published {len(self.df)} rows")
```

### Parameterized Query with SQL File

Use external SQL files with template variables:

```python
# config.py
from pydantic import BaseModel

class QueryConfig(BaseModel):
    start_date: str
    end_date: str
    min_value: float

# flow.py
from metaflow import FlowSpec, Parameter, step
from ds_platform_utils.metaflow import query_pandas_from_snowflake, make_pydantic_parser_fn

class ParameterizedFlow(FlowSpec):
    
    config = Parameter(
        'config',
        type=make_pydantic_parser_fn(QueryConfig),
        default='{"start_date": "2024-01-01", "end_date": "2024-01-31", "min_value": 100}'
    )
    
    @step
    def start(self):
        """Query with parameters."""
        self.df = query_pandas_from_snowflake(
            query_fpath="sql/extract_data.sql",
            ctx={
                "start_date": self.config.start_date,
                "end_date": self.config.end_date,
                "min_value": self.config.min_value,
            },
        )
        self.next(self.end)
    
    @step
    def end(self):
        print(f"Retrieved {len(self.df)} rows")
```

```sql
-- sql/extract_data.sql
SELECT *
FROM transactions
WHERE date >= '{{start_date}}'
    AND date <= '{{end_date}}'
    AND amount >= {{min_value}}
```

### Query with Large Results via S3

For datasets > 1GB:

```python
@step
def query_large_dataset(self):
    """Query large dataset via S3 staging."""
    self.df = query_pandas_from_snowflake(
        query="SELECT * FROM large_table",
        use_s3_stage=True,  # ← Enable S3 for large results
        warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
    )
    print(f"Retrieved {len(self.df):,} rows via S3")
    self.next(self.process)
```

## Feature Engineering

### Multiclass Classification Features

```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import publish

class FeatureEngineeringFlow(FlowSpec):
    
    @step
    def start(self):
        """Create features and publish."""
        publish(
            query_fpath="sql/create_features.sql",
            ctx={"lookback_days": 30},
            publish_query_fpath="sql/publish_features.sql",
            comment="Daily feature refresh",
        )
        self.next(self.end)
    
    @step
    def end(self):
        print("✅ Features published")
```

```sql
-- sql/create_features.sql
CREATE OR REPLACE TEMPORARY TABLE temp_features AS
SELECT
    user_id,
    COUNT(*) as event_count_30d,
    AVG(value) as avg_value_30d,
    MAX(timestamp) as last_seen,
    DATEDIFF(day, MAX(timestamp), CURRENT_DATE()) as recency,
    COUNT(DISTINCT date) as active_days
FROM events
WHERE date >= DATEADD(day, -{{lookback_days}}, CURRENT_DATE())
GROUP BY user_id;
```

```sql
-- sql/publish_features.sql
CREATE OR REPLACE TABLE my_dev_schema.user_features AS
SELECT * FROM temp_features;
```

### Time-based Features

```python
@step
def create_time_features(self):
    """Create time-based features."""
    self.df['day_of_week'] = pd.to_datetime(self.df['timestamp']).dt.dayofweek
    self.df['hour'] = pd.to_datetime(self.df['timestamp']).dt.hour
    self.df['is_weekend'] = self.df['day_of_week'].isin([5, 6]).astype(int)
    self.df['is_business_hours'] = (
        (self.df['hour'] >= 9) & (self.df['hour'] < 17)
    ).astype(int)
    
    self.next(self.publish)
```

### Aggregate Features

```python
@step
def create_aggregate_features(self):
    """Create user-level aggregates."""
    user_features = self.df.groupby('user_id').agg({
        'transaction_amount': ['sum', 'mean', 'max', 'count'],
        'timestamp': ['min', 'max'],
    }).reset_index()
    
    # Flatten column names
    user_features.columns = [
        '_'.join(col).strip('_') for col in user_features.columns
    ]
    
    self.features_df = user_features
    self.next(self.publish)
```

## Batch Inference

### Simple Batch Scoring

For datasets that fit in memory:

```python
import pandas as pd
from metaflow import FlowSpec, step

class BatchScoringFlow(FlowSpec):
    
    @step
    def start(self):
        """Load model and data."""
        import pickle
        with open('model.pkl', 'rb') as f:
            self.model = pickle.load(f)
        
        self.df = query_pandas_from_snowflake(
            query="SELECT * FROM inference_input"
        )
        self.next(self.predict)
    
    @step
    def predict(self):
        """Generate predictions."""
        features = self.df[['feature_1', 'feature_2', 'feature_3']]
        self.df['prediction'] = self.model.predict(features)
        self.df['probability'] = self.model.predict_proba(features)[:, 1]
        self.next(self.publish)
    
    @step
    def publish(self):
        """Publish predictions."""
        publish_pandas(
            table_name="predictions",
            df=self.df[['id', 'prediction', 'probability']],
        )
        self.next(self.end)
    
    @step
    def end(self):
        print(f"✅ Scored {len(self.df)} rows")
```

### Large-Scale Batch Inference

For datasets > 10M rows:

```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import BatchInferencePipeline

class LargeScaleScoringFlow(FlowSpec):
    
    @step
    def start(self):
        """Query and split into batches."""
        pipeline = BatchInferencePipeline()
        self.worker_ids = pipeline.query_and_batch(
            input_query="SELECT * FROM large_input_table",
            batch_size_in_mb=256,
            parallel_workers=20,
        )
        self.next(self.predict, foreach='worker_ids')
    
    @step
    def predict(self):
        """Predict for each batch (parallel)."""
        worker_id = self.input
        
        # Load model (cached across batches)
        import pickle
        with open('model.pkl', 'rb') as f:
            model = pickle.load(f)
        
        def predict_fn(df: pd.DataFrame) -> pd.DataFrame:
            """Prediction function."""
            df['score'] = model.predict_proba(df[['f1', 'f2', 'f3']])[:, 1]
            return df[['id', 'score']]
        
        pipeline = BatchInferencePipeline()
        pipeline.process_batch(
            worker_id=worker_id,
            predict_fn=predict_fn,
            batch_size_in_mb=64,
        )
        self.next(self.join)
    
    @step
    def join(self, inputs):
        """Collect all predictions."""
        pipeline = BatchInferencePipeline()
        pipeline.publish_results(
            output_table="predictions",
            output_schema="my_dev_schema",
        )
        self.next(self.end)
    
    @step
    def end(self):
        print("✅ Predictions published")
```

### Batch Inference with Post-processing

```python
def predict_with_postprocessing(df: pd.DataFrame) -> pd.DataFrame:
    """Predict and post-process."""
    # Generate predictions
    scores = model.predict_proba(df[feature_cols])[:, 1]
    
    # Post-processing: apply business rules
    df['raw_score'] = scores
    df['final_score'] = scores
    
    # Rule: cap scores for new users
    new_user_mask = df['account_age_days'] < 30
    df.loc[new_user_mask, 'final_score'] = df.loc[new_user_mask, 'final_score'] * 0.8
    
    # Rule: boost scores for loyal customers
    loyal_mask = df['total_purchases'] > 50
    df.loc[loyal_mask, 'final_score'] = df.loc[loyal_mask, 'final_score'] * 1.2
    
    # Clip to [0, 1]
    df['final_score'] = df['final_score'].clip(0, 1)
    
    return df[['id', 'raw_score', 'final_score']]
```

## Incremental Processing

### Daily Incremental Load

Process only new data each day:

```python
from datetime import datetime, timedelta
from metaflow import FlowSpec, Parameter, step

class IncrementalFlow(FlowSpec):
    
    date = Parameter(
        'date',
        help='Date to process (YYYY-MM-DD)',
        default=datetime.now().strftime('%Y-%m-%d')
    )
    
    @step
    def start(self):
        """Process single day of data."""
        self.df = query_pandas_from_snowflake(
            query=f"""
                SELECT *
                FROM events
                WHERE date = '{self.date}'
            """
        )
        print(f"Processing {len(self.df)} rows for {self.date}")
        self.next(self.transform)
    
    @step
    def transform(self):
        """Transform data."""
        # Your transformation logic
        self.results = self.df  # Placeholder
        self.next(self.publish)
    
    @step
    def publish(self):
        """Append to existing table."""
        publish_pandas(
            table_name="incremental_results",
            df=self.results,
            mode="append",  # ← Append instead of replace
        )
        self.next(self.end)
    
    @step
    def end(self):
        print(f"✅ Appended {len(self.results)} rows for {self.date}")
```

### Backfill Pattern

Process historical data in parallel:

```python
from datetime import datetime, timedelta
from metaflow import FlowSpec, Parameter, step

class BackfillFlow(FlowSpec):
    
    start_date = Parameter('start_date', default='2024-01-01')
    end_date = Parameter('end_date', default='2024-01-31')
    
    @step
    def start(self):
        """Generate list of dates to backfill."""
        start = datetime.strptime(self.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        self.dates = []
        current = start
        while current <= end:
            self.dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        print(f"Backfilling {len(self.dates)} days")
        self.next(self.process_date, foreach='dates')
    
    @step
    def process_date(self):
        """Process each date in parallel."""
        date = self.input
        
        df = query_pandas_from_snowflake(
            query=f"SELECT * FROM events WHERE date = '{date}'"
        )
        
        # Transform
        result = transform(df)
        
        # Publish
        publish_pandas(
            table_name="backfill_results",
            df=result,
            mode="append",
        )
        
        self.rows_processed = len(result)
        self.next(self.join)
    
    @step
    def join(self, inputs):
        """Collect statistics."""
        total_rows = sum(inp.rows_processed for inp in inputs)
        print(f"✅ Backfilled {total_rows:,} rows across {len(inputs)} days")
        self.next(self.end)
    
    @step
    def end(self):
        pass
```

## Error Recovery

### Retry Failed Steps

```python
from metaflow import FlowSpec, step, retry

class ResilientFlow(FlowSpec):
    
    @retry(times=3)  # Retry up to 3 times
    @step
    def query_data(self):
        """Query with retry."""
        try:
            self.df = query_pandas_from_snowflake(
                query="SELECT * FROM flaky_table",
                warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
            )
        except Exception as e:
            print(f"⚠️ Query failed: {e}")
            raise  # Will trigger retry
        
        self.next(self.process)
```

### Checkpoint Pattern

Save intermediate results to resume from failures:

```python
@step
def process_with_checkpoints(self):
    """Process with checkpoints."""
    results = []
    
    for i, chunk in enumerate(self.chunks):
        try:
            result = process_chunk(chunk)
            results.append(result)
            
            # Checkpoint every 10 chunks
            if i % 10 == 0:
                checkpoint_df = pd.concat(results)
                publish_pandas(
                    table_name="checkpoint_results",
                    df=checkpoint_df,
                    mode="replace",
                )
                print(f"✅ Checkpoint saved at chunk {i}")
                
        except Exception as e:
            print(f"❌ Failed at chunk {i}: {e}")
            # Can resume from last checkpoint
            raise
    
    self.results = pd.concat(results)
    self.next(self.publish)
```

## Testing Patterns

### Test with Sampled Data

```python
from metaflow import FlowSpec, Parameter, step

class TestableFlow(FlowSpec):
    
    sample_size = Parameter(
        'sample_size',
        type=int,
        default=None,
        help='Sample size for testing (None = all data)'
    )
    
    @step
    def start(self):
        """Query with optional sampling."""
        query = "SELECT * FROM large_table"
        
        if self.sample_size is not None:
            query += f" LIMIT {self.sample_size}"
            print(f"📊 Testing with {self.sample_size} rows")
        
        self.df = query_pandas_from_snowflake(query=query)
        self.next(self.process)
```

Run with: `python flow.py run --sample_size 1000`

### Dry Run Pattern

```python
class ProductionFlow(FlowSpec):
    
    dry_run = Parameter(
        'dry_run',
        type=bool,
        default=False,
        help='If True, skip publishing'
    )
    
    @step
    def publish_results(self):
        """Publish or dry-run."""
        if self.dry_run:
            print(f"🔍 DRY RUN: Would publish {len(self.df)} rows")
            print(self.df.head())
        else:
            publish_pandas(table_name="results", df=self.df)
            print(f"✅ Published {len(self.df)} rows")
        
        self.next(self.end)
```

Run with: `python flow.py run --dry_run True`

## Additional Resources

- [Best Practices](best_practices.md)
- [Performance Tuning](performance_tuning.md)
- [Troubleshooting](troubleshooting.md)
- [Getting Started](getting_started.md)
