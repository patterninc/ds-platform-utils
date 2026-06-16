# `create_ownership_registry_view`

Source: `ds_platform_utils.metaflow.registry.create_ownership_registry_view`

Creates (or replaces) the central **table-ownership registry view**,
`PATTERN_DB.DATA_SCIENCE.TABLE_OWNERSHIP_REGISTRY`. The view pivots the object tags
applied by [`publish`](publish.md) / [`publish_pandas`](publish_pandas.md) into one row
per table, exposing `owner`, `team`, `domain`, `project`, `status`, `sla` and `contact`.

This is a one-time admin helper.

## Signature

```python
create_ownership_registry_view(conn: SnowflakeConnection | None = None) -> str
```

| Parameter | Type                          | Required | Description                                                              |
| --------- | ----------------------------- | -------: | ------------------------------------------------------------------------ |
| `conn`    | `SnowflakeConnection \| None` |       No | Open Snowflake connection. If omitted, one is created via `get_snowflake_connection()`. |

**Returns:** the executed `CREATE OR REPLACE VIEW` SQL string.

## Usage

```python
from ds_platform_utils.metaflow import create_ownership_registry_view

create_ownership_registry_view()
```

Then query it:

```sql
SELECT * FROM PATTERN_DB.DATA_SCIENCE.TABLE_OWNERSHIP_REGISTRY
ORDER BY team, table_name;
```

## Notes

- **No refresh needed.** A view is not materialized — it re-runs its query on every read,
  so it is always live.
- **~2h lag.** The view reads `SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES`, which itself lags
  up to ~2 hours. For the current value of a single table's tag, use
  `SYSTEM$GET_TAG('PATTERN_DB.DATA_SCIENCE.TABLE_OWNER', '<table>', 'table')` instead.
- **Adoption-based.** Only tables that have at least one ownership tag appear in the view.
