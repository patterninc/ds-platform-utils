# `query_pandas_from_snowflake`

Source: `ds_platform_utils.metaflow.pandas.query_pandas_from_snowflake`

Executes a Snowflake query and returns results as a pandas DataFrame.

## Signature

```python
query_pandas_from_snowflake(
    query: str | Path,
    warehouse: Literal["XS", "MED", "XL"] | None = None,
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
