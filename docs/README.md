# ds-platform-utils Documentation

Comprehensive documentation for Pattern's data science platform utilities.

## Overview

`ds-platform-utils` is a utility library designed to streamline ML workflows on Pattern's data platform. It provides high-level abstractions for common operations involving Metaflow, Snowflake, and S3.

## Table of Contents

### Core Modules

**[Snowflake Utilities](snowflake/README.md)**
- Query execution and connection management
- Write-audit-publish pattern for data quality
- Schema management (dev/prod separation)
- Integrated with Outerbounds for automatic authentication

**[Metaflow Utilities](metaflow/README.md)**
- [BatchInferencePipeline](metaflow/batch_inference_pipeline.md) - Scalable batch inference orchestration
- [Pandas Integration](metaflow/pandas.md) - Query and publish functions for Snowflake
- [Config Validation](metaflow/validate_config.md) - Pydantic-based configuration validation

### Examples

- [Practical Examples](examples/README.md) - Complete working examples for common scenarios
  - Simple Query and Publish
  - Feature Engineering Pipeline
  - Batch Inference at Scale
  - Incremental Data Processing
  - Multi-Table Join Pipeline

### API Reference

- [Complete API Reference](api/index.md)

## Quick Links

- [API Reference →](api/index.md)
- [Practical Examples →](examples/README.md)
- [Installation](#installation)
- [Quick Start](#quick-start)

## Installation

```bash
# Install from the repository
pip install git+https://github.com/patterninc/ds-platform-utils.git

# For development
git clone https://github.com/patterninc/ds-platform-utils.git
cd ds-platform-utils
uv sync
```

## Configuration

**No manual configuration required!** 

This library integrates seamlessly with Outerbounds, which automatically handles all Snowflake and AWS configuration. Simply use the functions in your Metaflow flows, and Outerbounds takes care of:

- ✅ Snowflake authentication and connection management
- ✅ AWS credentials and S3 access
- ✅ Warehouse selection and optimization
- ✅ Query tagging for cost tracking

## Quick Start

### Example 1: Query Data from Snowflake

```python
from ds_platform_utils.metaflow import query_pandas_from_snowflake

# Query data into a pandas DataFrame
df = query_pandas_from_snowflake(
    query="SELECT * FROM my_schema.my_table LIMIT 1000",
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
)
```

### Example 2: Publish Results to Snowflake

```python
from ds_platform_utils.metaflow import publish_pandas

# Publish DataFrame to Snowflake
publish_pandas(
    table_name="my_results_table",
    df=results_df,
    auto_create_table=True,
    overwrite=True,
)
```

### Example 3: Batch Inference Pipeline

```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import BatchInferencePipeline

class PredictionFlow(FlowSpec):
    @step
    def start(self):
        self.pipeline = BatchInferencePipeline()
        self.worker_ids = self.pipeline.query_and_batch(
            input_query="SELECT * FROM features_table",
            parallel_workers=10,
        )
        self.next(self.predict, foreach='worker_ids')
    
    @step
    def predict(self):
        worker_id = self.input
        self.pipeline.process_batch(
            worker_id=worker_id,
            predict_fn=my_model.predict,
        )
        self.next(self.join)
    
    @step
    def join(self, inputs):
        self.pipeline = inputs[0].pipeline
        self.pipeline.publish_results(
            output_table_name="predictions",
        )
        self.next(self.end)
    
    @step
    def end(self):
        pass
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│              ds-platform-utils Library                   │
│                                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Public API (ds_platform_utils.metaflow)         │   │
│  │  • BatchInferencePipeline                       │   │
│  │  • query_pandas_from_snowflake / publish_pandas │   │
│  │  • publish (query + transform + publish)        │   │
│  │  • make_pydantic_parser_fn                      │   │
│  │  • restore_step_state                           │   │
│  └─────────────────────────────────────────────────┘   │
│                                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Snowflake Utilities (_snowflake)                │   │
│  │  • Query execution (_execute_sql)               │   │
│  │  • Write-audit-publish pattern                  │   │
│  │  • Schema management (dev/prod)                 │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│            Outerbounds Platform                          │
│  (Handles all configuration automatically)               │
│                                                          │
│  • Snowflake Authentication & Connections               │
│  • AWS Credentials & S3 Access                          │
│  • Metaflow Orchestration                               │
│  • Query Tagging & Cost Tracking                        │
└─────────────────┬───────────────────────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
        ▼                   ▼
┌───────────────┐    ┌───────────────┐
│   Snowflake   │    │      S3       │
│   Database    │◄──►│   Storage     │
└───────────────┘    └───────────────┘
```

## Key Features

### 🚀 Scalable Batch Inference
- Automatic parallelization with Metaflow foreach
- Efficient S3 staging for large datasets
- Queue-based streaming pipeline
- Built-in error handling and validation

### 📊 Snowflake Integration
- Direct pandas integration
- S3 stage operations for large datasets
- Production-ready write patterns
- Automatic schema management

### 🔄 State Management
- Flow state restoration
- Artifact management
- Configuration validation

### 🛡️ Production Ready
- Audit trail generation
- Dev/Prod schema separation
- Query tagging and tracking
- Safe publishing patterns

## Contributing

Contributions are welcome! Please ensure you:
- Follow the existing code style
- Add tests for new features
- Update documentation as needed

## License

Internal use only - Pattern Inc.

## Support

For questions or issues:
- Create an issue in the [GitHub repository](https://github.com/patterninc/ds-platform-utils)
- Contact the Data Science Platform team
