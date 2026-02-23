# Configuration Validation

[← Back to Metaflow Docs](README.md)

Use Pydantic for type-safe flow configuration.

## Table of Contents

- [Overview](#overview)
- [Basic Usage](#basic-usage)
- [Advanced Validation](#advanced-validation)
- [Best Practices](#best-practices)

## Overview

`make_pydantic_parser_fn` integrates Pydantic models with Metaflow Parameters for type-safe configuration:

```python
from pydantic import BaseModel
from metaflow import FlowSpec, Parameter
from ds_platform_utils.metaflow import make_pydantic_parser_fn

class FlowConfig(BaseModel):
    start_date: str
    end_date: str
    threshold: float = 0.5

class MyFlow(FlowSpec):
    config = Parameter(
        'config',
        type=make_pydantic_parser_fn(FlowConfig),
        default='{"start_date": "2024-01-01", "end_date": "2024-12-31"}',
    )
```

## Basic Usage

### Simple Configuration

```python
from pydantic import BaseModel
from metaflow import FlowSpec, Parameter, step
from ds_platform_utils.metaflow import make_pydantic_parser_fn

class Config(BaseModel):
    """Flow configuration."""
    table_name: str
    warehouse: str = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"
    limit: int = 1000

class SimpleFlow(FlowSpec):
    """Flow with validated config."""
    
    config = Parameter(
        'config',
        type=make_pydantic_parser_fn(Config),
        default='{"table_name": "my_table"}',
        help='JSON configuration'
    )
    
    @step
    def start(self):
        # Access validated config
        print(f"Table: {self.config.table_name}")
        print(f"Warehouse: {self.config.warehouse}")
        print(f"Limit: {self.config.limit}")
        self.next(self.end)
    
    @step
    def end(self):
        pass
```

**Run:**
```bash
# Use default config
python flow.py run

# Override config
python flow.py run --config '{"table_name": "other_table", "limit": 5000}'
```

### Date Range Configuration

```python
from pydantic import BaseModel, validator
from datetime import datetime

class DateRangeConfig(BaseModel):
    """Configuration with date validation."""
    start_date: str
    end_date: str
    
    @validator('start_date', 'end_date')
    def validate_date_format(cls, v):
        """Ensure dates are in YYYY-MM-DD format."""
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError(f"Date must be in YYYY-MM-DD format, got: {v}")
    
    @validator('end_date')
    def end_after_start(cls, v, values):
        """Ensure end_date is after start_date."""
        if 'start_date' in values and v < values['start_date']:
            raise ValueError("end_date must be after start_date")
        return v

class DateRangeFlow(FlowSpec):
    config = Parameter(
        'config',
        type=make_pydantic_parser_fn(DateRangeConfig),
        default='{"start_date": "2024-01-01", "end_date": "2024-12-31"}',
    )
    
    @step
    def start(self):
        print(f"Processing {self.config.start_date} to {self.config.end_date}")
        self.next(self.end)
    
    @step
    def end(self):
        pass
```

**Run:**
```bash
# Valid
python flow.py run --config '{"start_date": "2024-01-01", "end_date": "2024-12-31"}'

# Invalid - will fail validation
python flow.py run --config '{"start_date": "2024-12-31", "end_date": "2024-01-01"}'
# Error: end_date must be after start_date
```

## Advanced Validation

### Nested Configuration

```python
from pydantic import BaseModel
from typing import List

class SnowflakeConfig(BaseModel):
    """Snowflake settings."""
    warehouse: str = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"
    database: str = "PATTERN_DB"
    schema: str = "my_dev_schema"

class ModelConfig(BaseModel):
    """Model settings."""
    model_path: str
    threshold: float = 0.5
    features: List[str]

class FullConfig(BaseModel):
    """Complete flow configuration."""
    snowflake: SnowflakeConfig
    model: ModelConfig
    debug: bool = False

class AdvancedFlow(FlowSpec):
    config = Parameter(
        'config',
        type=make_pydantic_parser_fn(FullConfig),
        default='''
        {
            "snowflake": {
                "warehouse": "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
                "database": "PATTERN_DB",
                "schema": "my_dev_schema"
            },
            "model": {
                "model_path": "models/my_model.pkl",
                "threshold": 0.5,
                "features": ["feature_1", "feature_2", "feature_3"]
            },
            "debug": false
        }
        ''',
    )
    
    @step
    def start(self):
        print(f"Warehouse: {self.config.snowflake.warehouse}")
        print(f"Model: {self.config.model.model_path}")
        print(f"Features: {self.config.model.features}")
        self.next(self.end)
    
    @step
    def end(self):
        pass
```

### Custom Validators

```python
from pydantic import BaseModel, validator, root_validator

class MLConfig(BaseModel):
    """ML pipeline configuration."""
    training_start: str
    training_end: str
    inference_date: str
    min_samples: int = 1000
    max_samples: int = 1_000_000
    
    @validator('inference_date')
    def inference_after_training(cls, v, values):
        """Inference date must be after training period."""
        if 'training_end' in values and v <= values['training_end']:
            raise ValueError("inference_date must be after training_end")
        return v
    
    @validator('min_samples', 'max_samples')
    def positive_samples(cls, v):
        """Sample counts must be positive."""
        if v <= 0:
            raise ValueError("Sample count must be positive")
        return v
    
    @root_validator
    def check_sample_range(cls, values):
        """min_samples must be less than max_samples."""
        min_s = values.get('min_samples')
        max_s = values.get('max_samples')
        
        if min_s and max_s and min_s >= max_s:
            raise ValueError("min_samples must be less than max_samples")
        
        return values
```

### Enum Validation

```python
from enum import Enum
from pydantic import BaseModel

class Warehouse(str, Enum):
    """Valid warehouse names."""
    XS = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH"
    SMALL = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_S_WH"
    MEDIUM = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"
    LARGE = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_L_WH"
    XL = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH"

class QueryConfig(BaseModel):
    """Query configuration."""
    query: str
    warehouse: Warehouse  # Only accepts valid warehouses
    limit: int = 10000

# Valid
config = QueryConfig(
    query="SELECT * FROM table",
    warehouse=Warehouse.MEDIUM,  # or "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"
)

# Invalid - will fail
config = QueryConfig(
    query="SELECT * FROM table",
    warehouse="INVALID_WAREHOUSE",  # Error!
)
```

## Best Practices

### ✅ DO: Use Type Hints

```python
from typing import List, Optional

class Config(BaseModel):
    # Clear types
    features: List[str]
    threshold: float
    warehouse: Optional[str] = None  # Explicitly optional
```

### ✅ DO: Provide Defaults

```python
class Config(BaseModel):
    # Sensible defaults
    warehouse: str = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"
    batch_size: int = 1000
    timeout: int = 3600
```

### ✅ DO: Add Docstrings

```python
class Config(BaseModel):
    """Flow configuration.
    
    Attributes:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        warehouse: Snowflake warehouse name
    """
    start_date: str
    end_date: str
    warehouse: str = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"
```

### ✅ DO: Validate Early

```python
@validator('threshold')
def threshold_in_range(cls, v):
    """Threshold must be between 0 and 1."""
    if not 0 <= v <= 1:
        raise ValueError(f"threshold must be in [0, 1], got {v}")
    return v
```

### ❌ DON'T: Over-validate

```python
# ❌ Bad - too restrictive
class Config(BaseModel):
    table_name: str
    
    @validator('table_name')
    def specific_table(cls, v):
        if v != "exactly_this_table":  # Too rigid!
            raise ValueError("Only specific table allowed")
        return v

# ✅ Good - validate format, not content
class Config(BaseModel):
    table_name: str
    
    @validator('table_name')
    def valid_table_name(cls, v):
        if not v.replace('_', '').isalnum():  # Allow alphanumeric + underscore
            raise ValueError("Invalid table name format")
        return v
```

## Example: Production Configuration

```python
from pydantic import BaseModel, validator, Field
from typing import List, Optional
from datetime import datetime
from enum import Enum

class Environment(str, Enum):
    """Deployment environment."""
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"

class Schedule(BaseModel):
    """Schedule configuration."""
    enabled: bool = True
    cron: str = "0 2 * * *"  # Daily at 2 AM
    timezone: str = "UTC"

class ProductionConfig(BaseModel):
    """Production-ready flow configuration."""
    
    # Environment
    env: Environment = Environment.DEV
    
    # Data
    start_date: str = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: str = Field(..., description="End date (YYYY-MM-DD)")
    table_name: str = Field(..., description="Input table name")
    
    # Model
    model_path: str = Field(..., description="Path to model file")
    features: List[str] = Field(..., description="Feature columns")
    threshold: float = Field(0.5, ge=0, le=1, description="Prediction threshold")
    
    # Snowflake
    warehouse: str = "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"
    schema_override: Optional[str] = None
    
    # Performance
    use_s3_stage: bool = True
    parallel_workers: int = Field(10, ge=1, le=50)
    batch_size_mb: int = Field(256, ge=64, le=512)
    
    # Monitoring
    enable_alerts: bool = True
    alert_email: Optional[str] = None
    
    # Schedule
    schedule: Optional[Schedule] = None
    
    @validator('start_date', 'end_date')
    def validate_date(cls, v):
        """Validate date format."""
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError(f"Invalid date format: {v}")
    
    @validator('warehouse')
    def validate_warehouse(cls, v, values):
        """Validate warehouse based on environment."""
        env = values.get('env')
        if env == Environment.PROD and 'DEV' in v:
            raise ValueError("Cannot use DEV warehouse in PROD environment")
        return v
    
    @root_validator
    def validate_alerts(cls, values):
        """If alerts enabled, email is required."""
        if values.get('enable_alerts') and not values.get('alert_email'):
            raise ValueError("alert_email required when enable_alerts=true")
        return values

class ProductionFlow(FlowSpec):
    config = Parameter(
        'config',
        type=make_pydantic_parser_fn(ProductionConfig),
        default='{"start_date": "2024-01-01", "end_date": "2024-12-31", "table_name": "input_data", "model_path": "model.pkl", "features": ["f1", "f2"]}',
    )
    
    @step
    def start(self):
        print(f"Environment: {self.config.env.value}")
        print(f"Date range: {self.config.start_date} to {self.config.end_date}")
        self.next(self.end)
    
    @step
    def end(self):
        pass
```

## Troubleshooting

### Validation Error

```bash
# Error: ValidationError
python flow.py run --config '{"invalid": "config"}'

# Error message shows which fields are missing/invalid
ValidationError: 2 validation errors for ProductionConfig
start_date
  field required (type=value_error.missing)
end_date
  field required (type=value_error.missing)
```

**Solution**: Provide all required fields.

### JSON Parsing Error

```bash
# Error: Invalid JSON
python flow.py run --config '{"start_date": 2024-01-01}'  # Missing quotes!

# Fix: properly quote values
python flow.py run --config '{"start_date": "2024-01-01"}'
```

### Type Mismatch

```python
# Error: wrong type
config = Config(threshold="0.5")  # String, not float

# Fix: use correct type
config = Config(threshold=0.5)  # Float
```

## Related Documentation

- [Best Practices](../guides/best_practices.md)
- [Common Patterns](../guides/common_patterns.md)
- [Pydantic Documentation](https://docs.pydantic.dev/)
