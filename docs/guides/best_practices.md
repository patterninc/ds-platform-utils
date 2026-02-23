# Best Practices

[← Back to Main Docs](../README.md)

Guidelines for production-ready code using `ds-platform-utils`.

## Table of Contents

- [Code Organization](#code-organization)
- [Error Handling](#error-handling)
- [Performance](#performance)
- [Security](#security)
- [Testing](#testing)
- [Production Deployment](#production-deployment)

## Code Organization

### ✅ DO: Separate Configuration from Logic

```python
# config.py
from pydantic import BaseModel

class FlowConfig(BaseModel):
    start_date: str
    end_date: str
    warehouse: str = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"
    batch_size: int = 1000

# flow.py
from metaflow import FlowSpec, Parameter
from ds_platform_utils.metaflow import make_pydantic_parser_fn

class MyFlow(FlowSpec):
    config = Parameter(
        'config',
        type=make_pydantic_parser_fn(FlowConfig),
        default='{"start_date": "2024-01-01", "end_date": "2024-12-31"}'
    )
```

### ✅ DO: Use External SQL Files

```python
# ✅ Good - SQL in separate file
publish(
    query_fpath="sql/create_features.sql",
    ctx={"start_date": self.config.start_date},
    publish_query_fpath="sql/publish_features.sql",
)

# ❌ Bad - SQL in Python strings
query = f"""
    CREATE OR REPLACE TABLE my_features AS
    SELECT * FROM raw_data
    WHERE date >= '{self.config.start_date}'
"""
```

### ✅ DO: Use Type Hints

```python
from typing import Optional
import pandas as pd

def process_data(
    df: pd.DataFrame,
    threshold: float = 0.5,
    warehouse: Optional[str] = None,
) -> pd.DataFrame:
    """Process data with predictions."""
    # Your logic here
    return result_df
```

##Error Handling

### ✅ DO: Handle Expected Failures Gracefully

```python
from metaflow import FlowSpec, step, retry

class RobustFlow(FlowSpec):
    
    @retry(times=3)  # Retry up to 3 times
    @step
    def query_data(self):
        """Query data with retry logic."""
        try:
            self.df = query_pandas_from_snowflake(
                query="SELECT * FROM my_table",
                warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
            )
        except Exception as e:
            print(f"⚠️ Query failed: {e}")
            # Log error, send alert, etc.
            raise  # Re-raise to trigger retry
        
        if len(self.df) == 0:
            raise ValueError("No data returned from query")
        
        self.next(self.process)
```

### ✅ DO: Validate Data Early

```python
@step
def validate_input(self):
    """Validate input data before processing."""
    required_columns = ['id', 'feature_1', 'feature_2']
    
    missing = set(required_columns) - set(self.df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    if self.df.isnull().any().any():
        raise ValueError("Input data contains null values")
    
    print(f"✅ Validation passed: {len(self.df)} rows")
    self.next(self.process)
```

### ❌ DON'T: Silently Catch All Exceptions

```python
# ❌ Bad - swallows all errors
try:
    result = process_data(df)
except:
    result = None

# ✅ Good - specific exception handling
try:
    result = process_data(df)
except ValueError as e:
    print(f"Invalid input: {e}")
    raise
except Exception as e:
    print(f"Unexpected error: {e}")
    # Log for debugging
    raise
```

## Performance

### ✅ DO: Use S3 Staging for Large Datasets

```python
# For datasets > 1GB
df = query_pandas_from_snowflake(
    query="SELECT * FROM large_table",
    use_s3_stage=True,  # ← Enable S3 staging
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
)
```

### ✅ DO: Choose Appropriate Warehouse Size

```python
# Small queries (< 1M rows)
warehouse = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH"

# Medium workloads (1M-10M rows)
warehouse = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"

# Large batch jobs (> 10M rows)
warehouse = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH"
```

### ✅ DO: Use BatchInferencePipeline for Very Large Scale

```python
# For > 10M rows with parallel processing
pipeline = BatchInferencePipeline()
worker_ids = pipeline.query_and_batch(
    input_query="SELECT * FROM huge_table",
    parallel_workers=10,  # Adjust based on data size
)
```

### ✅ DO: Optimize Batch Sizes

```python
# For memory-constrained environments
pipeline.process_batch(
    worker_id=worker_id,
    predict_fn=predict_fn,
    batch_size_in_mb=64,  # Smaller batches
)

# For high-memory environments
pipeline.process_batch(
    worker_id=worker_id,
    predict_fn=predict_fn,
    batch_size_in_mb=512,  # Larger batches = fewer S3 ops
)
```

### ❌ DON'T: Query Everything When You Need Subset

```python
# ❌ Bad - queries all data
df = query_pandas_from_snowflake(
    query="SELECT * FROM huge_table"
)
df = df[df['date'] >= '2024-01-01']  # Filter in Python

# ✅ Good - filter in SQL
df = query_pandas_from_snowflake(
    query="""
        SELECT *
        FROM huge_table
        WHERE date >= '2024-01-01'
    """
)
```

## Security

### ✅ DO: Use Template Variables for SQL

```python
# ✅ Good - prevents SQL injection
ctx = {
    "table_name": "my_table",
    "start_date": user_input_date,
}
publish(
    query_fpath="sql/query.sql",  # Uses {{table_name}}, {{start_date}}
    ctx=ctx,
)

# ❌ Bad - SQL injection risk
query = f"SELECT * FROM {user_input_table} WHERE date >= '{user_input_date}'"
```

### ✅ DO: Validate User Inputs

```python
from datetime import datetime

def validate_date(date_str: str) -> str:
    """Validate date format."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}")

# Use in flow
start_date = validate_date(self.config.start_date)
```

**Note:** Outerbounds automatically handles all credentials and authentication, so you never need to manage Snowflake passwords or AWS keys. Just use the library functions directly.

## Testing

### ✅ DO: Write Unit Tests for Business Logic

```python
# tests/test_processing.py
import pandas as pd
import pytest

def test_process_predictions():
    """Test prediction processing."""
    # Arrange
    input_df = pd.DataFrame({
        'id': [1, 2, 3],
        'score': [0.1, 0.6, 0.9]
    })
    
    # Act
    result = process_predictions(input_df, threshold=0.5)
    
    # Assert
    assert len(result) == 2  # Only scores >= 0.5
    assert all(result['score'] >= 0.5)
```

### ✅ DO: Use Fixtures for Test Data

```python
# tests/conftest.py
import pytest
import pandas as pd

@pytest.fixture
def sample_features():
    """Sample feature data."""
    return pd.DataFrame({
        'id': range(100),
        'feature_1': range(100),
        'feature_2': range(100, 200),
    })

# tests/test_flow.py
def test_feature_engineering(sample_features):
    """Test feature engineering."""
    result = engineer_features(sample_features)
    assert 'engineered_feature' in result.columns
```

### ✅ DO: Test Edge Cases

```python
def test_empty_dataframe():
    """Test handling of empty input."""
    df = pd.DataFrame()
    with pytest.raises(ValueError, match="Empty DataFrame"):
        process_data(df)

def test_missing_columns():
    """Test handling of missing columns."""
    df = pd.DataFrame({' id': [1, 2]})  # Missing required columns
    with pytest.raises(ValueError, match="Missing required columns"):
        process_data(df)
```

## Production Deployment

### ✅ DO: Use Production Warehouses in Prod

```python
# Use environment-aware warehouse selection
from metaflow import current

def get_warehouse():
    """Get warehouse based on environment."""
    if hasattr(current, 'is_production') and current.is_production:
        return "OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_MED_WH"
    return "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"

# Use in flow
warehouse = get_warehouse()
```

### ✅ DO: Enable Monitoring and Alerts

```python
@step
def publish_with_monitoring(self):
    """Publish results with monitoring."""
    start_time = time.time()
    
    try:
        publish_pandas(
            table_name="production_features",
            df=self.features_df,
        )
        
        duration = time.time() - start_time
        self.metrics = {
            'rows_published': len(self.features_df),
            'duration_seconds': duration,
            'timestamp': datetime.now().isoformat(),
        }
        
        # Log metrics
        print(f"📊 Published {self.metrics['rows_published']} rows in {duration:.2f}s")
        
    except Exception as e:
        # Send alert
        send_alert(f"Pipeline failed: {e}")
        raise
    
    self.next(self.end)
```

### ✅ DO: Version Your Flows

```python
from metaflow import FlowSpec, Parameter

class ProductionFlow(FlowSpec):
    """Production ML pipeline.
    
    Version: 2.1.0
    Last Updated: 2024-01-15
    Owner: data-science-team
    """
    
    version = Parameter(
        'version',
        default='2.1.0',
        help='Pipeline version'
    )
```

### ✅ DO: Document Your SQL

```sql
-- sql/create_features.sql
-- Feature Engineering Pipeline
-- Owner: data-science-team
-- Description: Creates ML features from raw events
-- Dependencies: pattern_db.raw_data.events

CREATE OR REPLACE TABLE {{schema}}.ml_features AS
SELECT
    user_id,
    COUNT(*) as event_count,
    AVG(value) as avg_value,
    MAX(timestamp) as last_seen
FROM pattern_db.raw_data.events
WHERE date >= '{{start_date}}'
    AND date <= '{{end_date}}'
GROUP BY user_id;
```

### ❌ DON'T: Deploy Untested Code

```python
# ✅ Good - run tests before deploying
$ pytest tests/
$ python flow.py run  # Test locally
$ python flow.py run --production  # Deploy

# ❌ Bad - deploy without testing
$ python flow.py run --production  # YOLO
```

## Checklist for Production Code

Before deploying to production:

- [ ] All tests passing
- [ ] SQL queries optimized
- [ ] Appropriate warehouse selected
- [ ] Error handling implemented
- [ ] Monitoring/alerts configured
- [ ] Documentation updated
- [ ] Code reviewed
- [ ] Dev environment tested
- [ ] Staging environment tested (if available)
- [ ] Rollback plan documented

## Additional Resources

- [Getting Started Guide](getting_started.md)
- [Common Patterns](common_patterns.md)
- [Performance Tuning](performance_tuning.md)
- [Troubleshooting](troubleshooting.md)
