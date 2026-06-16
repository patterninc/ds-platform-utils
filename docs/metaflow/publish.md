# `publish`

Source: `ds_platform_utils.metaflow.write_audit_publish.publish`

Publishes data to a Snowflake table using the write-audit-publish (WAP) pattern.

## Signature

```python
publish(
    table_name: str,
    query: str | Path,
    audits: list[str | Path] | None = None,
    ctx: dict[str, Any] | None = None,
    warehouse: Literal["XS", "MED", "XL"] = None,
    use_utc: bool = True,
    tags: dict[str, str] | None = None,
) -> None
```

## What it does

- Reads SQL from a string or `.sql` path.
- Runs write/audit/publish operations through Snowflake.
- Adds operation details and table links to the Metaflow card when available.
- **Automatically applies ownership object tags to production tables** (see
  [Ownership tags](#ownership-tags) below).

## Parameters

| Parameter    | Type                                 | Required | Description                                                                                                   |
| ------------ | ------------------------------------ | -------: | ------------------------------------------------------------------------------------------------------------- |
| `table_name` | `str`                                |      Yes | Destination Snowflake table name for the publish operation.                                                   |
| `query`      | `str \| Path`                        |      Yes | SQL query text or path to SQL file that produces the table data.                                              |
| `audits`     | `list[str \| Path] \| None`          |       No | Optional SQL audits (strings or file paths) executed as validation checks.                                    |
| `ctx`        | `dict[str, Any] \| None`             |       No | Optional template substitution context for SQL operations.                                                    |
| `warehouse`  | `Literal["XS", "MED", "XL"] \| None` |       No | Snowflake warehouse override for this operation. Supports `XS`/`MED`/`XL` shortcuts or a full warehouse name. |
| `use_utc`    | `bool`                               |       No | If `True`, uses UTC timezone for Snowflake session.                                                           |
| `tags`       | `dict[str, str] \| None`             |       No | Overrides for the ownership object tags applied to the published table. See [Ownership tags](#ownership-tags).|

**Returns:** `None`

## Typical usage

```python
from ds_platform_utils.metaflow import publish

publish(
    table_name="MY_TABLE",
    query="SELECT * FROM PATTERN_DB.{{schema}}.SOURCE",
    audits=["SELECT COUNT(*) > 0 FROM PATTERN_DB.{{schema}}.{{table_name}}"],
)
```

## Ownership tags

When publishing to **production**, `publish()` automatically applies the table-ownership
object tags from the table-ownership RFC. The seven tags are:

| Tag             | Source                                                  | Always set?     |
| --------------- | ------------------------------------------------------- | --------------- |
| `TABLE_OWNER`   | Metaflow `current.username`                             | yes             |
| `TABLE_TEAM`    | `data-science`                                          | yes             |
| `TABLE_DOMAIN`  | `ds.domain` Metaflow tag                                | yes             |
| `TABLE_PROJECT` | `ds.project` Metaflow tag                               | yes             |
| `TABLE_STATUS`  | `active` (override allows `active`/`deprecated`/`archived`) | yes          |
| `TABLE_SLA`     | override only (`realtime`/`hourly`/`daily`/`weekly`/`ad_hoc`) | only if given |
| `TABLE_CONTACT` | override only (Slack channel or email)                  | only if given   |

Pass `tags=` to override any value. Keys may be `owner`/`team`/`domain`/`project`/
`status`/`sla`/`contact` (optionally `TABLE_`-prefixed):

```python
publish(
    table_name="OUT_OF_STOCK_ADS",
    query="sql/create_training_data.sql",
    tags={"sla": "daily", "contact": "#ds-recsys", "status": "active"},
)
```

Notes:

- Tags are applied **only to production tables**. Non-prod (`DATA_SCIENCE_STAGE`) runs
  apply no tags.
- The tag *definitions* must first be created once by a Snowflake admin (the RFC
  `CREATE TAG` setup). Until then, tagging is **skipped with a warning** — the publish
  still succeeds.
- Invalid `status`/`sla` values raise `ValueError` before any data is written.
- Tagged tables surface in the `TABLE_OWNERSHIP_REGISTRY` view (see
  `create_ownership_registry_view`).
