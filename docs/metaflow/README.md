# Metaflow Utilities

High-level utilities for building ML workflows with Metaflow, Snowflake, and S3.

## Modules Overview

### 🤖 [BatchInferencePipeline](batch_inference_pipeline.md)
**Purpose**: Orchestrate large-scale batch inference workflows

**Key Features**:
- Automatic data export from Snowflake to S3
- Parallel processing with Metaflow foreach
- Queue-based streaming pipeline (download → inference → upload)
- Automatic results publishing back to Snowflake
- Execution state validation

**When to Use**:
- Running predictions on millions of rows
- Need for parallel processing
- Memory constraints require streaming
- Production batch scoring jobs

**Example**:
```python
from ds_platform_utils.metaflow import BatchInferencePipeline

pipeline = BatchInferencePipeline()
pipeline.run(
    input_query="SELECT * FROM features",
    output_table_name="predictions",
    predict_fn=model.predict,
)
```

---

### 📊 [Pandas Integration](pandas.md)
**Purpose**: Seamless Pandas ↔ Snowflake operations

**Key Functions**:
- `query_pandas_from_snowflake()` - Query Snowflake into DataFrame
- `publish_pandas()` - Write DataFrame to Snowflake

**Key Features**:
- Automatic timezone handling (UTC)
- S3 staging for large datasets
- Schema auto-creation
- Compression options (snappy/gzip)
- Parallel uploads

**When to Use**:
- Ad-hoc data analysis
- Feature engineering
- Model training data retrieval
- Publishing model outputs

**Example**:
```python
from ds_platform_utils.metaflow import (
    query_pandas_from_snowflake,
    publish_pandas
)

# Query data
df = query_pandas_from_snowflake(
    query="SELECT * FROM training_data",
    use_s3_stage=True,  # For large datasets
)

# Publish results
publish_pandas(
    table_name="model_outputs",
    df=predictions_df,
    auto_create_table=True,
)
```

---

### ✍️ [Write, Audit & Publish](write_audit_publish.md)
**Purpose**: Safe, auditable data publishing patterns

**Key Features**:
- SQL file management
- Template variable substitution
- Query tagging for tracking
- Dev/Prod schema separation
- Audit trail generation
- Table URL generation

**When to Use**:
- Publishing production models
- Executing parameterized SQL
- Audit requirements
- Schema-aware deployments

**Example**:
```python
from ds_platform_utils.metaflow import publish

publish(
    table_name="aggregates",
    query="queries/create_aggregates.sql",
    audits=["queries/audit_row_count.sql"],
    ctx={"start_date": "2024-01-01"},
    warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_MED_WH",
)
```

---

### 🔄 [State Management](restore_step_state.md)
**Purpose**: Restore Metaflow step state for debugging

**Key Features**:
- Artifact restoration
- Namespace recreation
- Interactive debugging support

**When to Use**:
- Debugging failed flows
- Reproducing specific step states
- Testing step logic interactively

**Example**:
```python
from ds_platform_utils.metaflow import restore_step_state

# Restore state from a previous run
namespace = restore_step_state(
    flow_name="MyFlow",
    run_id="123",
    step_name="process_data",
)

# Access restored artifacts
df = namespace.df
model = namespace.model
```

---

### ⚙️ [Config Validation](validate_config.md)
**Purpose**: Type-safe configuration with Pydantic

**Key Features**:
- Pydantic model validation
- Metaflow parameter parsing
- Type checking and coercion
- Clear error messages

**When to Use**:
- Complex flow configurations
- Type safety requirements
- Parameter validation
- Configuration schemas

**Example**:
```python
from pydantic import BaseModel
from ds_platform_utils.metaflow import make_pydantic_parser_fn

class FlowConfig(BaseModel):
    start_date: str
    end_date: str
    batch_size: int = 1000

class MyFlow(FlowSpec):
    config = Parameter(
        'config',
        type=make_pydantic_parser_fn(FlowConfig),
        default='{"start_date": "2024-01-01", "end_date": "2024-12-31"}'
    )
```

---

## Module Comparison

| Module                     | Data Size | Processing | Use Case       | Complexity |
| -------------------------- | --------- | ---------- | -------------- | ---------- |
| **BatchInferencePipeline** | 100GB+    | Parallel   | Batch scoring  | Medium     |
| **Pandas Integration**     | <10GB     | Sequential | Analysis, ETL  | Low        |
| **Write/Audit/Publish**    | Any       | Sequential | Production SQL | Low        |
| **State Management**       | N/A       | N/A        | Debugging      | Low        |
| **Config Validation**      | N/A       | N/A        | Configuration  | Low        |

## Common Workflows

### Workflow 1: Model Training Pipeline
```python
from ds_platform_utils.metaflow import (
    query_pandas_from_snowflake,
    publish_pandas
)

class TrainingFlow(FlowSpec):
    @step
    def start(self):
        # Query training data
        self.df = query_pandas_from_snowflake(
            query="SELECT * FROM features WHERE date >= '2024-01-01'",
            use_s3_stage=True,
        )
        self.next(self.train)
    
    @step
    def train(self):
        # Train model
        self.model = train_model(self.df)
        self.next(self.end)
    
    @step
    def end(self):
        # Publish metrics
        publish_pandas(
            table_name="model_metrics",
            df=self.metrics_df,
        )
```

### Workflow 2: Batch Inference Pipeline
```python
from ds_platform_utils.metaflow import BatchInferencePipeline

class PredictionFlow(FlowSpec):
    @step
    def start(self):
        self.pipeline = BatchInferencePipeline()
        self.worker_ids = self.pipeline.query_and_batch(
            input_query="SELECT * FROM input_features",
            parallel_workers=10,
        )
        self.next(self.predict, foreach='worker_ids')
    
    @step
    def predict(self):
        self.pipeline.process_batch(
            worker_id=self.input,
            predict_fn=self.model.predict,
        )
        self.next(self.join)
    
    @step
    def join(self, inputs):
        self.pipeline = inputs[0].pipeline
        self.pipeline.publish_results(
            output_table_name="predictions",
        )
        self.next(self.end)
```

### Workflow 3: Audited Data Publication
```python
from ds_platform_utils.metaflow import publish

class DataPipelineFlow(FlowSpec):
    @step
    def start(self):
        publish(
            table_name="processed_results",
            query="sql/transform_data.sql",
            audits=["sql/audit_row_count.sql"],
            ctx={
                "start_date": self.start_date,
                "end_date": self.end_date,
            },
        )
        self.next(self.end)
```

## Design Principles

### 1. **Simplicity First**
- High-level abstractions hide complexity
- Sensible defaults for common cases
- Progressive disclosure of advanced features

### 2. **Production Ready**
- Built-in error handling
- Audit trails
- Dev/Prod separation
- Query tagging

### 3. **Performance**
- S3 staging for large data
- Parallel processing where applicable
- Streaming pipelines to manage memory
- Efficient compression

### 4. **Type Safety**
- Type hints throughout
- Pydantic validation
- Clear error messages

## Next Steps

- 📖 Read the [Getting Started Guide](../guides/getting_started.md)
- 🎯 Check out [Common Patterns](../guides/common_patterns.md)
- 🔧 Review [Best Practices](../guides/best_practices.md)
- 🐛 See [Troubleshooting](../guides/troubleshooting.md)

## Related Modules

### [Snowflake Utilities](../snowflake/README.md)
Lower-level utilities for direct Snowflake operations:
- SQL query execution
- Write-audit-publish pattern
- Schema management

The Metaflow utilities build on top of these Snowflake utilities to provide higher-level abstractions. Most users should use the Metaflow API, but the Snowflake utilities are available for custom use cases.
