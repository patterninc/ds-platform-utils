# Examples

[← Back to Main Docs](../README.md)

Practical examples for common use cases.

## Table of Contents

- [Simple Query and Publish](#simple-query-and-publish)
- [Feature Engineering Pipeline](#feature-engineering-pipeline)
- [Batch Inference at Scale](#batch-inference-at-scale)
- [Incremental Data Processing](#incremental-data-processing)
- [Multi-Table Join Pipeline](#multi-table-join-pipeline)

## Simple Query and Publish

Basic workflow: query → transform → publish.

### Code

```python
# simple_pipeline.py
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import query_pandas_from_snowflake, publish_pandas

class SimplePipeline(FlowSpec):
    """Query data, transform, and publish."""
    
    @step
    def start(self):
        """Query input data."""
        print("Querying data...")
        self.df = query_pandas_from_snowflake(
            query="""
                SELECT
                    user_id,
                    transaction_date,
                    amount,
                    category
                FROM transactions
                WHERE transaction_date >= '2024-01-01'
            """,
            warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
        )
        print(f"Retrieved {len(self.df):,} rows")
        self.next(self.transform)
    
    @step
    def transform(self):
        """Transform data."""
        print("Transforming data...")
        
        # Add month column
        self.df['month'] = self.df['transaction_date'].dt.to_period('M')
        
        # Calculate monthly spending per user
        self.results = self.df.groupby(['user_id', 'month']).agg({
            'amount': ['sum', 'mean', 'count']
        }).reset_index()
        
        # Flatten column names
        self.results.columns = [
            '_'.join(col).strip('_') for col in self.results.columns
        ]
        
        print(f"Created {len(self.results):,} aggregated rows")
        self.next(self.publish)
    
    @step
    def publish(self):
        """Publish results."""
        print("Publishing results...")
        publish_pandas(
            table_name="user_monthly_spending",
            df=self.results,
            schema="my_dev_schema",
            comment="Monthly user spending aggregates",
        )
        print("✅ Done!")
        self.next(self.end)
    
    @step
    def end(self):
        pass

if __name__ == '__main__':
    SimplePipeline()
```

### Run

```bash
# Local execution
python simple_pipeline.py run

# View results in Snowflake
snowsql -q "SELECT * FROM my_dev_schema.user_monthly_spending LIMIT 10;"
```

---

## Feature Engineering Pipeline

Create ML features using SQL and Python.

### Files

**config.py**
```python
from pydantic import BaseModel

class FeatureConfig(BaseModel):
    """Feature generation configuration."""
    start_date: str
    end_date: str
    lookback_days: int = 30
```

**sql/extract_raw_features.sql**
```sql
-- Extract raw features from events
CREATE OR REPLACE TEMPORARY TABLE temp_raw_features AS
SELECT
    user_id,
    COUNT(*) as event_count,
    COUNT(DISTINCT date) as active_days,
    MIN(timestamp) as first_seen,
    MAX(timestamp) as last_seen,
    AVG(value) as avg_value,
    STDDEV(value) as std_value
FROM events
WHERE date >= '{{start_date}}'
    AND date <= '{{end_date}}'
GROUP BY user_id;
```

**sql/publish_features.sql**
```sql
-- Publish to target table
CREATE OR REPLACE TABLE {{schema}}.ml_features AS
SELECT * FROM temp_engineered_features;
```

**feature_pipeline.py**
```python
from metaflow import FlowSpec, Parameter, step
from ds_platform_utils.metaflow import query_pandas_from_snowflake, publish_pandas, make_pydantic_parser_fn
import pandas as pd
from datetime import datetime

from config import FeatureConfig

class FeaturePipeline(FlowSpec):
    """ML feature engineering pipeline."""
    
    config = Parameter(
        'config',
        type=make_pydantic_parser_fn(FeatureConfig),
        default='{"start_date": "2024-01-01", "end_date": "2024-12-31"}',
    )
    
    @step
    def start(self):
        """Extract raw features from Snowflake."""
        print(f"Extracting features from {self.config.start_date} to {self.config.end_date}")
        
        self.df = query_pandas_from_snowflake(
            query_fpath="sql/extract_raw_features.sql",
            ctx={
                "start_date": self.config.start_date,
                "end_date": self.config.end_date,
            },
        )
        print(f"Extracted features for {len(self.df):,} users")
        self.next(self.engineer_features)
    
    @step
    def engineer_features(self):
        """Engineer features in Python."""
        print("Engineering features...")
        
        # Time-based features
        now = pd.Timestamp.now()
        self.df['recency_days'] = (now - pd.to_datetime(self.df['last_seen'])).dt.days
        self.df['account_age_days'] = (now - pd.to_datetime(self.df['first_seen'])).dt.days
        
        # Engagement features
        self.df['events_per_day'] = self.df['event_count'] / self.df['active_days']
        self.df['engagement_ratio'] = self.df['active_days'] / self.df['account_age_days']
        
        # Value features
        self.df['value_volatility'] = self.df['std_value'] / (self.df['avg_value'] + 1)
        
        # Segments
        self.df['user_segment'] = pd.cut(
            self.df['event_count'],
            bins=[0, 10, 50, 200, float('inf')],
            labels=['low', 'medium', 'high', 'power_user']
        )
        
        print(f"Engineered {len(self.df.columns)} features")
        self.next(self.publish)
    
    @step
    def publish(self):
        """Publish features."""
        print("Publishing features...")
        publish_pandas(
            table_name="ml_features",
            df=self.df,
            schema="my_dev_schema",
            comment=f"Features for {self.config.start_date} to {self.config.end_date}",
        )
        print(f"✅ Published {len(self.df):,} rows with {len(self.df.columns)} columns")
        self.next(self.end)
    
    @step
    def end(self):
        pass

if __name__ == '__main__':
    FeaturePipeline()
```

### Run

```bash
# With default dates
python feature_pipeline.py run

# With custom dates
python feature_pipeline.py run --config '{"start_date": "2024-06-01", "end_date": "2024-12-31"}'

# Check results
snowsql -q "SELECT * FROM my_dev_schema.ml_features LIMIT 10;"
```

---

## Batch Inference at Scale

Large-scale ML predictions with parallel processing.

### Code

**batch_inference.py**
```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import BatchInferencePipeline
import pandas as pd
import pickle

class LargeScaleInference(FlowSpec):
    """Batch inference for millions of rows."""
    
    @step
    def start(self):
        """Query and split into batches."""
        print("Querying input data and splitting into batches...")
        
        pipeline = BatchInferencePipeline()
        self.worker_ids = pipeline.query_and_batch(
            input_query="""
                SELECT
                    user_id,
                    feature_1,
                    feature_2,
                    feature_3,
                    feature_4,
                    feature_5
                FROM ml_features
                WHERE last_updated >= '2024-01-01'
            """,
            batch_size_in_mb=256,
            parallel_workers=20,  # 20 parallel workers
            warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
        )
        
        print(f"Split into {len(self.worker_ids)} batches")
        self.next(self.predict, foreach='worker_ids')
    
    @step
    def predict(self):
        """Predict for each batch (runs in parallel)."""
        worker_id = self.input
        print(f"Processing batch {worker_id}")
        
        # Load model (cached across batches on same worker)
        with open('model.pkl', 'rb') as f:
            model = pickle.load(f)
        
        def predict_fn(df: pd.DataFrame) -> pd.DataFrame:
            """Generate predictions."""
            feature_cols = [
                'feature_1', 'feature_2', 'feature_3', 
                'feature_4', 'feature_5'
            ]
            
            # Generate predictions
            predictions = model.predict_proba(df[feature_cols])[:, 1]
            
            # Create output DataFrame
            result = pd.DataFrame({
                'user_id': df['user_id'],
                'score': predictions,
                'prediction': (predictions >= 0.5).astype(int),
            })
            
            return result
        
        # Process this batch
        pipeline = BatchInferencePipeline()
        pipeline.process_batch(
            worker_id=worker_id,
            predict_fn=predict_fn,
            batch_size_in_mb=64,  # Process in 64MB chunks
        )
        
        print(f"✅ Batch {worker_id} complete")
        self.next(self.join)
    
    @step
    def join(self, inputs):
        """Collect results and publish."""
        print(f"All {len(inputs)} batches processed, publishing results...")
        
        pipeline = BatchInferencePipeline()
        pipeline.publish_results(
            output_table="user_predictions",
            output_schema="my_dev_schema",
            warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
        )
        
        print("✅ All predictions published!")
        self.next(self.end)
    
    @step
    def end(self):
        pass

if __name__ == '__main__':
    LargeScaleInference()
```

### Run

```bash
# Local execution
python batch_inference.py run

# Production execution
python batch_inference.py run --production

# Check results
snowsql -q "SELECT COUNT(*), AVG(score) FROM my_dev_schema.user_predictions;"
```

---

## Incremental Data Processing

Process new data daily and append to existing table.

### Code

**incremental_pipeline.py**
```python
from metaflow import FlowSpec, Parameter, step
from ds_platform_utils.metaflow import query_pandas_from_snowflake, publish_pandas
from datetime import datetime, timedelta

class IncrementalPipeline(FlowSpec):
    """Process daily incremental data."""
    
    date = Parameter(
        'date',
        default=datetime.now().strftime('%Y-%m-%d'),
        help='Date to process (YYYY-MM-DD)',
    )
    
    @step
    def start(self):
        """Query new data for specified date."""
        print(f"Processing data for {self.date}")
        
        self.df = query_pandas_from_snowflake(
            query=f"""
                SELECT *
                FROM raw_events
                WHERE date = '{self.date}'
            """
        )
        
        if len(self.df) == 0:
            print(f"⚠️ No data found for {self.date}")
        else:
            print(f"Found {len(self.df):,} rows for {self.date}")
        
        self.next(self.transform)
    
    @step
    def transform(self):
        """Transform new data."""
        if len(self.df) > 0:
            print("Transforming data...")
            
            # Your transformation logic
            self.df['processed_date'] = datetime.now()
            self.df['derived_field'] = self.df['value'] * 2
            
            print(f"Transformed {len(self.df):,} rows")
        
        self.next(self.publish)
    
    @step
    def publish(self):
        """Append to existing table."""
        if len(self.df) > 0:
            print(f"Appending {len(self.df):,} rows...")
            
            publish_pandas(
                table_name="processed_events",
                df=self.df,
                schema="my_dev_schema",
                mode="append",  # ← Append instead of replace
                comment=f"Incremental load for {self.date}",
            )
            
            print(f"✅ Appended {len(self.df):,} rows for {self.date}")
        else:
            print("⏭️ No data to publish")
        
        self.next(self.end)
    
    @step
    def end(self):
        pass

if __name__ == '__main__':
    IncrementalPipeline()
```

### Run

```bash
# Process today
python incremental_pipeline.py run

# Process specific date
python incremental_pipeline.py run --date 2024-01-15

# Schedule with cron (runs daily at 2 AM)
# 0 2 * * * cd /path/to/project && python incremental_pipeline.py run
```

---

## Multi-Table Join Pipeline

Join data from multiple Snowflake tables.

### Code

**multi_table_pipeline.py**
```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import query_pandas_from_snowflake, publish_pandas

class MultiTableJoin(FlowSpec):
    """Join multiple tables and create enriched dataset."""
    
    @step
    def start(self):
        """Start parallel queries."""
        self.next(self.query_users, self.query_events, self.query_demographics)
    
    @step
    def query_users(self):
        """Query user data."""
        print("Querying users...")
        self.users_df = query_pandas_from_snowflake(
            query="SELECT user_id, signup_date, status FROM users WHERE status = 'active'"
        )
        print(f"Retrieved {len(self.users_df):,} users")
        self.next(self.join_data)
    
    @step
    def query_events(self):
        """Query event data."""
        print("Querying events...")
        self.events_df = query_pandas_from_snowflake(
            query="""
                SELECT
                    user_id,
                    COUNT(*) as event_count,
                    MAX(timestamp) as last_event
                FROM events
                WHERE date >= '2024-01-01'
                GROUP BY user_id
            """
        )
        print(f"Retrieved events for {len(self.events_df):,} users")
        self.next(self.join_data)
    
    @step
    def query_demographics(self):
        """Query demographic data."""
        print("Querying demographics...")
        self.demographics_df = query_pandas_from_snowflake(
            query="SELECT user_id, age, country, segment FROM user_demographics"
        )
        print(f"Retrieved demographics for {len(self.demographics_df):,} users")
        self.next(self.join_data)
    
    @step
    def join_data(self, inputs):
        """Join all data sources."""
        print("Joining data from all sources...")
        
        # Merge users + events
        result = inputs.query_users.users_df.merge(
            inputs.query_events.events_df,
            on='user_id',
            how='left'
        )
        
        # Merge with demographics
        result = result.merge(
            inputs.query_demographics.demographics_df,
            on='user_id',
            how='left'
        )
        
        # Fill missing event counts with 0
        result['event_count'] = result['event_count'].fillna(0)
        
        self.enriched_df = result
        print(f"Created enriched dataset with {len(self.enriched_df):,} rows")
        self.next(self.publish)
    
    @step
    def publish(self):
        """Publish enriched dataset."""
        print("Publishing enriched dataset...")
        publish_pandas(
            table_name="enriched_user_data",
            df=self.enriched_df,
            schema="my_dev_schema",
            comment="Enriched user data from multiple sources",
        )
        print(f"✅ Published {len(self.enriched_df):,} rows")
        self.next(self.end)
    
    @step
    def end(self):
        pass

if __name__ == '__main__':
    MultiTableJoin()
```

### Run

```bash
# Run the pipeline
python multi_table_pipeline.py run

# View results
snowsql -q "SELECT * FROM my_dev_schema.enriched_user_data LIMIT 10;"
```

---

## Additional Resources

- [Getting Started Guide](../guides/getting_started.md)
- [Best Practices](../guides/best_practices.md)
- [Common Patterns](../guides/common_patterns.md)
- [API Reference](../api/index.md)
