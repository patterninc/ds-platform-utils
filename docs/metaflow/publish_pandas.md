# `publish_pandas`

Source: `ds_platform_utils.metaflow.pandas.publish_pandas`

Writes a pandas DataFrame to Snowflake.

## Signature

```python
publish_pandas(
    table_name: str,
    df: pd.DataFrame,
    add_created_date: bool = False,
    chunk_size: int | None = None,
    compression: Literal["snappy", "gzip"] = "snappy",
    warehouse: Literal["XS", "MED", "XL"] | None = None,
    parallel: int = 4,
    quote_identifiers: bool = False,
    auto_create_table: bool = False,
    overwrite: bool = False,
    use_logical_type: bool = True,
    use_utc: bool = True,
    use_s3_stage: bool = False,
    table_definition: list[tuple[str, str]] | None = None,
) -> None
```

## What it does

- Validates DataFrame input.
- Writes directly via `write_pandas` or via S3 stage flow for large data.
- Adds a Snowflake table URL to Metaflow card output.
