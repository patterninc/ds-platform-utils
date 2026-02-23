# Snowflake Utilities

Core utilities for interacting with Snowflake in Pattern's data platform.

## Overview

The Snowflake utilities module provides low-level functions for executing queries and implementing the write-audit-publish pattern. These utilities are integrated with Outerbounds, which automatically handles Snowflake authentication and connection management.

> **Note:** Most users should use the higher-level [Metaflow Pandas Integration](../metaflow/pandas.md) functions (`query_pandas_from_snowflake`, `publish_pandas`, `publish`) rather than calling these utilities directly.

## Key Features

### 1. Query Execution

Execute SQL statements against Snowflake with automatic connection handling via Outerbounds:

```python
from ds_platform_utils._snowflake.run_query import _execute_sql

# Execute returns the cursor for the last statement
cursor = _execute_sql(conn, """
    SELECT * FROM my_table LIMIT 10;
    SELECT COUNT(*) FROM my_table;
""")
```

**Features:**
- Supports batch execution (multiple statements separated by semicolons)
- Returns cursor from the last executed statement
- Handles empty SQL statements gracefully
- Provides clear error messages for SQL syntax errors

### 2. Write-Audit-Publish Pattern

Implement data quality checks during table writes:

```python
from ds_platform_utils._snowflake.write_audit_publish import write_audit_publish

# Write table with audit checks
operations = write_audit_publish(
    table_name="my_feature_table",
    query="SELECT * FROM source_table WHERE date > '2024-01-01'",
    audits=[
        "SELECT COUNT(*) > 0 AS has_rows FROM {{schema}}.{{table_name}}",
        "SELECT COUNT(DISTINCT user_id) > 100 FROM {{schema}}.{{table_name}}"
    ],
    cursor=cursor,
    is_production=False
)

# Execute each operation
for op in operations:
    print(f"Executing: {op.description}")
    op.execute()
```

**The Pattern:**
1. **Write**: Create/replace table in dev schema
2. **Audit**: Run validation queries to check data quality
3. **Publish**: If audits pass, promote table to production schema

**Benefits:**
- Catch data quality issues before production
- Atomic operations (all-or-nothing)
- Automatic schema management (dev vs prod)
- Query templating with Jinja2

## Authentication

All Snowflake operations automatically use credentials managed by Outerbounds. No manual configuration required!

Outerbounds handles:
- ✅ Snowflake authentication
- ✅ Warehouse selection
- ✅ Database/schema configuration
- ✅ Connection pooling
- ✅ Query tagging for audit trails

## Common Patterns

### Pattern 1: Simple Query Execution

```python
from ds_platform_utils.metaflow import query_pandas_from_snowflake

# High-level API (recommended)
df = query_pandas_from_snowflake("SELECT * FROM my_table")
```

### Pattern 2: Table Creation with Validation

```python
from ds_platform_utils.metaflow import publish

# High-level API with audits
publish(
    table_name="features",
    query="CREATE TABLE {{schema}}.{{table_name}} AS SELECT ...",
    audits=["SELECT COUNT(*) > 1000 FROM {{schema}}.{{table_name}}"]
)
```

### Pattern 3: pandas DataFrame Publishing

```python
from ds_platform_utils.metaflow import publish_pandas
import pandas as pd

df = pd.DataFrame({"col1": [1, 2, 3]})

# Publish with automatic schema inference
publish_pandas(
    table_name="my_data",
    df=df,
    auto_create_table=True,
    overwrite=True,
)
```

## Query Templating

The write-audit-publish functions use Jinja2 templating for dynamic table names:

```sql
-- Use template variables in your queries
CREATE OR REPLACE TABLE {{schema}}.{{table_name}} AS
SELECT * FROM source_table;

-- In audits
SELECT 
    COUNT(*) > 0 AS has_data,
    MAX(created_at) > CURRENT_DATE - 7 AS is_recent
FROM {{schema}}.{{table_name}};
```

**Template Variables:**
- `{{schema}}` - Automatically set to dev or prod schema
- `{{table_name}}` - The table name you specify
- Custom context via `ctx` parameter

## Schema Management

### Development vs Production

```python
# Development (default)
publish(..., is_production=False)  # Writes to DEV_SCHEMA

# Production
publish(..., is_production=True)   # Writes to PROD_SCHEMA
```

The library automatically manages schema selection:
- **Dev**: Fast iteration, no data quality gates
- **Prod**: Requires audit checks to pass

### Branch-based Development

```python
# Tables can be scoped to git branches
publish(
    ...,
    branch_name="feature-xyz"  # Creates feature-xyz_table_name
)
```

## Error Handling

The Snowflake utilities provide detailed error messages:

```python
try:
    cursor = _execute_sql(conn, bad_sql)
except Exception as e:
    # Detailed error includes:
    # - SQL syntax errors with line numbers
    # - Table/column not found errors
    # - Permission errors
    print(f"Query failed: {e}")
```

**Common Errors:**
- `Empty SQL statement` - Query contains only whitespace/comments
- `SQL compilation error` - Syntax or schema errors
- `Insufficient privileges` - Permission issues (contact DevOps)

## Best Practices

### 1. Use High-Level APIs

Prefer `query_pandas_from_snowflake` and `publish_pandas` over low-level utilities:

```python
# ✅ Recommended
from ds_platform_utils.metaflow import query_pandas_from_snowflake
df = query_pandas_from_snowflake("SELECT ...")

# ❌ Avoid (unless you need low-level control)
from ds_platform_utils._snowflake.run_query import _execute_sql
cursor = _execute_sql(conn, "SELECT ...")
```

### 2. Always Add Audit Checks

Data quality checks prevent bad data from reaching production:

```python
audits = [
    "SELECT COUNT(*) > 0 FROM {{schema}}.{{table_name}}",  # Non-empty
    "SELECT COUNT(*) = COUNT(DISTINCT id) FROM {{schema}}.{{table_name}}",  # Unique IDs
    "SELECT MAX(updated_at) > CURRENT_DATE - 1 FROM {{schema}}.{{table_name}}"  # Fresh data
]
```

### 3. Use Template Variables

Always use `{{schema}}.{{table_name}}` in queries for write-audit-publish:

```sql
-- ✅ Correct
CREATE TABLE {{schema}}.{{table_name}} AS SELECT ...

-- ❌ Wrong (hardcoded schema)
CREATE TABLE production.my_table AS SELECT ...
```

### 4. Handle Cursors Properly

Cursors should be closed after use:

```python
cursor = _execute_sql(conn, sql)
try:
    results = cursor.fetchall()
finally:
    cursor.close()
```

## Integration with Metaflow

The Snowflake utilities are designed to work seamlessly with Metaflow flows:

```python
from metaflow import FlowSpec, step
from ds_platform_utils.metaflow import query_pandas_from_snowflake, publish

class MyFlow(FlowSpec):
    @step
    def start(self):
        # Query data
        self.df = query_pandas_from_snowflake("""
            SELECT * FROM raw_data 
            WHERE date = CURRENT_DATE
        """)
        self.next(self.transform)
    
    @step
    def transform(self):
        # Transform data
        self.features = self.df.groupby('user_id').size()
        self.next(self.publish)
    
    @step
    def publish(self):
        # Publish DataFrame
        publish_pandas(
            table_name="daily_features",
            df=self.features.reset_index(),
            auto_create_table=True,
            overwrite=True,
        )
        self.next(self.end)
    
    @step
    def end(self):
        print("Pipeline complete!")
```

## Related Documentation

- [Metaflow Pandas Integration](../metaflow/pandas.md) - High-level query/publish functions
- [API Reference](../api/index.md) - Complete function signatures
- [Troubleshooting](../guides/troubleshooting.md) - Common Snowflake issues
- [Best Practices](../guides/best_practices.md) - Production patterns

## See Also

- [Getting Started Guide](../guides/getting_started.md)
- [Write-Audit-Publish Examples](../examples/README.md)
- [Snowflake Official Docs](https://docs.snowflake.com/)
