# ds-platform-utils Documentation

Comprehensive documentation for Pattern's data science platform utilities.

## Overview

`ds-platform-utils` is a utility library designed to streamline ML workflows on Pattern's data platform. It provides high-level abstractions for common operations involving Metaflow, Snowflake, and S3.

## Table of Contents

### Core Modules

**[Metaflow Utilities](metaflow/README.md)**
- [BatchInferencePipeline](metaflow/batch_inference_pipeline.md) - Scalable batch inference orchestration
- [Pandas Integration](metaflow/pandas.md) - Query and publish functions for Snowflake
- [Config Validation](metaflow/validate_config.md) - Pydantic-based configuration validation

### Guides

- [Getting Started](guides/getting_started.md)
- [Best Practices](guides/best_practices.md)
- [Performance Tuning](guides/performance_tuning.md)
- [Common Patterns](guides/common_patterns.md)
- [Troubleshooting](guides/troubleshooting.md)

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

- [Getting Started →](guides/getting_started.md)
- [Best Practices →](guides/best_practices.md)
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

### Environment Variables

```bash
# Enable debug logging
export DEBUG=1

# Snowflake configuration (usually handled by Metaflow integration)
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_WAREHOUSE=your_warehouse
```

### Metaflow Setup

This library is designed to work seamlessly with Metaflow. Ensure your Metaflow configuration is properly set up:

```bash
# Configure Metaflow with Outerbounds
metaflow configure aws
```

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
from ds_platform_utils.metaflow import BatchInferencePipeline

pipeline = BatchInferencePipeline()
pipeline.run(
    input_query="SELECT * FROM features_table",
    output_table_name="predictions_table",
    predict_fn=my_model.predict,
)
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  ds-platform-utils                       │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │           Metaflow Integration                  │    │
│  │                                                 │    │
│  │  • BatchInferencePipeline                      │    │
│  │  • Pandas Integration (query/publish)          │    │
│  │  • Write, Audit, Publish                       │    │
│  │  • State Management                            │    │
│  │  • Config Validation                           │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │              Data Operations                    │    │
│  │                                                 │    │
│  │  • S3 File Operations                          │    │
│  │  • S3 Stage Management                         │    │
│  │  • Snowflake Connection                        │    │
│  └────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┴─────────────────┐
        │                                   │
        ▼                                   ▼
┌───────────────┐                  ┌───────────────┐
│   Snowflake   │                  │      S3       │
│   Database    │◄─────────────────►   Storage    │
│               │  S3 Stage Copy   │               │
└───────────────┘                  └───────────────┘
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
- Check the [Troubleshooting Guide](guides/troubleshooting.md)
- Contact the Data Science Platform team
- Check the [Troubleshooting Guide](guides/troubleshooting.md)
