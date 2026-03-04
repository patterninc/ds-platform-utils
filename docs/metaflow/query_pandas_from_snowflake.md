# `query_pandas_from_snowflake`

Source: `ds_platform_utils.metaflow.pandas.query_pandas_from_snowflake`

Executes a Snowflake query and returns results as a pandas DataFrame.

## Signature

```python
query_pandas_from_snowflake(
    query: str | Path,
    warehouse: Literal["XS", "MED", "XL"] = None,
    ctx: dict[str, Any] | None = None,
    use_utc: bool = True,
    use_s3_stage: bool = False,
) -> pd.DataFrame
```

## What it does

- Accepts SQL text or `.sql` file path.
- Substitutes template values, including `{schema}`.
- Runs query directly or through Snowflake → S3 path.
- Normalizes resulting columns to lowercase.

## Parameters

| Parameter      | Type                                 | Required | Description                                                                                               |
| -------------- | ------------------------------------ | -------: | --------------------------------------------------------------------------------------------------------- |
| `query`        | `str \| Path`                        |      Yes | SQL query text or path to a `.sql` file.                                                                  |
| `warehouse`    | `Literal["XS", "MED", "XL"] \| None` |       No | Snowflake warehouse override for this query. Supports `XS`/`MED`/`XL` shortcuts or a full warehouse name. |
| `ctx`          | `dict[str, Any] \| None`             |       No | Optional substitutions for SQL templating (merged with internal `{schema}` resolution).                   |
| `use_utc`      | `bool`                               |       No | If `True`, uses UTC timezone for Snowflake session.                                                       |
| `use_s3_stage` | `bool`                               |       No | If `True`, executes Snowflake → S3 → pandas path for large-query handling.                                |

**Returns:** `pd.DataFrame` query results with lowercase column names.

## Limitations

- `use_s3_stage` does not support Snowflake `TIMESTAMP_TZ` and `TIMESTAMP_LTZ` data types.
- When `use_s3_stage=True`, Snowflake `DATE` data types are converted to `datetime`.
