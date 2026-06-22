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
    warehouse: Literal["XS", "MED", "XL"] = None,
    parallel: int = 4,
    quote_identifiers: bool = False,
    auto_create_table: bool = False,
    overwrite: bool = False,
    use_logical_type: bool = True,
    use_utc: bool = True,
    use_s3_stage: bool = False,
    table_definition: list[tuple[str, str]] | None = None,
    tags: dict[str, str] | None = None,
) -> None
```

## What it does

- Validates DataFrame input.
- Writes directly via `write_pandas` or via S3 stage flow for large data.
- Adds a Snowflake table URL to Metaflow card output.
- **Automatically applies ownership object tags to every published table** (see
  [Ownership tags](#ownership-tags) below).

## Parameters

| Parameter           | Type                            | Required | Description                                                                                                   |
| ------------------- | ------------------------------- | -------: | ------------------------------------------------------------------------------------------------------------- |
| `table_name`        | `str`                           |      Yes | Destination Snowflake table name.                                                                             |
| `df`                | `pd.DataFrame`                  |      Yes | DataFrame to publish.                                                                                         |
| `add_created_date`  | `bool`                          |       No | If `True`, adds a `created_date` UTC timestamp column before publish.                                         |
| `chunk_size`        | `int \| None`                   |       No | Number of rows per uploaded chunk. If not provided, calculate based on DataFrame size.                        |
| `compression`       | `Literal["snappy", "gzip"]`     |       No | Compression codec used for staged parquet files.                                                              |
| `warehouse`         | `str \| None`                   |       No | Snowflake warehouse override for this operation. Supports `XS`/`MED`/`XL` shortcuts or a full warehouse name. |
| `parallel`          | `int`                           |       No | Number of upload threads used by `write_pandas` path.                                                         |
| `quote_identifiers` | `bool`                          |       No | If `False`, passes identifiers unquoted so Snowflake applies uppercase coercion.                              |
| `auto_create_table` | `bool`                          |       No | If `True`, creates destination table when missing.                                                            |
| `overwrite`         | `bool`                          |       No | If `True`, replaces existing table contents.                                                                  |
| `use_logical_type`  | `bool`                          |       No | Controls parquet logical type handling when loading data.                                                     |
| `use_utc`           | `bool`                          |       No | If `True`, uses UTC timezone for Snowflake session.                                                           |
| `use_s3_stage`      | `bool`                          |       No | If `True`, publishes via S3 stage flow; otherwise uses direct `write_pandas`.                                 |
| `table_definition`  | `list[tuple[str, str]] \| None` |       No | Optional Snowflake table schema; used by S3 stage flow when table creation is needed.                         |
| `tags`              | `dict[str, str] \| None`        |       No | Overrides for the ownership object tags applied to the published table. See [Ownership tags](#ownership-tags).|

**Returns:** `None`

## Ownership tags

On **every** publish, `publish_pandas()` automatically applies the same
seven table-ownership object tags as [`publish`](publish.md#ownership-tags):
`TABLE_OWNER`, `TABLE_TEAM`, `TABLE_DOMAIN`, `TABLE_PROJECT`, `TABLE_STATUS` and
(when provided via `tags=`) `TABLE_SLA` / `TABLE_CONTACT`.

```python
publish_pandas(
    table_name="MY_TABLE",
    df=df,
    tags={"sla": "daily", "contact": "#ds-recsys"},
)
```

- Tags are applied to **every** published table — prod in `DATA_SCIENCE`, dev/stage in
  `DATA_SCIENCE_STAGE`. Tags are co-located with the table's schema, so the definitions
  must exist in **each** schema and the publishing role needs `APPLY` on the tags there.
- `TABLE_DOMAIN` / `TABLE_PROJECT` come from the `ds.domain` / `ds.project` Metaflow tags;
  if a flow runs without them they fall back to the literal `unknown` and a warning is
  printed. Ensure the flow carries those tags (automatic in CI / standard `poe` commands)
  or pass `tags={"domain": ..., "project": ...}`. See [`publish`](publish.md#ownership-tags).
- Tag *definitions* must first be created by a Snowflake admin (RFC `CREATE TAG` setup);
  until then tagging is **skipped with a warning** and the publish still succeeds.
- Invalid `status`/`sla` values raise `ValueError` before any data is written.

## Limitations

- When `use_s3_stage=True`, some column data types may not map exactly as expected between pandas/parquet and Snowflake.
- If needed, provide an explicit `table_definition` and/or cast columns before publishing to avoid data type mismatches.
