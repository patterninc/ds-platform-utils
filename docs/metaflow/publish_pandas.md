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

## Parameters

| Parameter           | Type                                 | Required | Description                                                                            |
| ------------------- | ------------------------------------ | -------: | -------------------------------------------------------------------------------------- |
| `table_name`        | `str`                                |      Yes | Destination Snowflake table name.                                                      |
| `df`                | `pd.DataFrame`                       |      Yes | DataFrame to publish.                                                                  |
| `add_created_date`  | `bool`                               |       No | If `True`, adds a `created_date` UTC timestamp column before publish.                  |
| `chunk_size`        | `int \| None`                        |       No | Number of rows per uploaded chunk. If not provided, calculate based on DataFrame size. |
| `compression`       | `Literal["snappy", "gzip"]`          |       No | Compression codec used for staged parquet files.                                       |
| `warehouse`         | `Literal["XS", "MED", "XL"] \| None` |       No | Snowflake warehouse override for this operation.                                       |
| `parallel`          | `int`                                |       No | Number of upload threads used by `write_pandas` path.                                  |
| `quote_identifiers` | `bool`                               |       No | If `False`, passes identifiers unquoted so Snowflake applies uppercase coercion.       |
| `auto_create_table` | `bool`                               |       No | If `True`, creates destination table when missing.                                     |
| `overwrite`         | `bool`                               |       No | If `True`, replaces existing table contents.                                           |
| `use_logical_type`  | `bool`                               |       No | Controls parquet logical type handling when loading data.                              |
| `use_utc`           | `bool`                               |       No | If `True`, uses UTC timezone for Snowflake session.                                    |
| `use_s3_stage`      | `bool`                               |       No | If `True`, publishes via S3 stage flow; otherwise uses direct `write_pandas`.          |
| `table_definition`  | `list[tuple[str, str]] \| None`      |       No | Optional Snowflake table schema; used by S3 stage flow when table creation is needed.  |

**Returns:** `None`
