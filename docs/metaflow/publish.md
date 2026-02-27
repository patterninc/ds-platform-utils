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
    warehouse: Literal["XS", "MED", "XL"] | None = None,
    use_utc: bool = True,
) -> None
```

## What it does

- Reads SQL from a string or `.sql` path.
- Runs write/audit/publish operations through Snowflake.
- Adds operation details and table links to the Metaflow card when available.

## Typical usage

```python
from ds_platform_utils.metaflow import publish

publish(
    table_name="MY_TABLE",
    query="SELECT * FROM PATTERN_DB.{{schema}}.SOURCE",
    audits=["SELECT COUNT(*) > 0 FROM PATTERN_DB.{{schema}}.{{table_name}}"],
)
```
