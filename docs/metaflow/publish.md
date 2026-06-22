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
- **Automatically applies ownership object tags to every published table** (see
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

On **every** publish, `publish()` automatically applies the table-ownership object tags
from the table-ownership RFC. The seven tags are:

| Tag             | Source                                                  | Always set?     |
| --------------- | ------------------------------------------------------- | --------------- |
| `TABLE_OWNER`   | owning-team alias derived from the domain (`ds-<domain>-team`), else `unknown` | yes |
| `TABLE_TEAM`    | `data-science`                                          | yes             |
| `TABLE_DOMAIN`  | `ds.domain` Metaflow tag, else `unknown`                | yes             |
| `TABLE_PROJECT` | `ds.project` Metaflow tag, else `unknown`               | yes             |
| `TABLE_STATUS`  | `active` (override allows `active`/`development`/`testing`/`deprecated`/`archived`/`retired`) | yes |
| `TABLE_SLA`     | override only (`streaming`/`realtime`/`hourly`/`daily`/`weekly`/`monthly`/`quarterly`/`ad_hoc`/`on_demand`) | only if given |
| `TABLE_CONTACT` | override only (Slack channel or email)                  | only if given   |

> **`TABLE_OWNER` is derived from the domain, not the run user.** Owner is resolved by
> priority: (1) an explicit `tags={"owner": ...}` override, else (2) the owning-team alias
> `ds-<domain>-team` when the domain is known (e.g. domain `advertising` →
> `ds-advertising-team`), else (3) `unknown`. We don't use `current.username`, because on
> deployed/argo runs it resolves to a service identity (`argo-workflows`) rather than a
> person. Pass `tags={"owner": ...}` to set a specific individual or alias.

> **`TABLE_DOMAIN` / `TABLE_PROJECT` depend on flow tags.** These are read from the
> `ds.domain` / `ds.project` Metaflow tags. If a flow runs without them, the value falls
> back to the literal string `unknown` and a warning is printed (the same warning used
> for select.dev cost tracking). Make sure your flow carries `--tag "ds.domain:..."` and
> `--tag "ds.project:..."` — these are applied automatically in CI and the standard `poe`
> run commands in the monorepo — or pass `tags={"domain": ..., "project": ...}` explicitly.
> Note: because owner is derived from the domain, a missing domain also means
> `TABLE_OWNER` falls back to `unknown`.

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

- Tags are applied to **every** published table — prod tables in `DATA_SCIENCE` and
  dev/stage tables in `DATA_SCIENCE_STAGE`. Tags are **co-located with the table's schema**:
  a prod table references the tag definitions in `DATA_SCIENCE`, a dev table references
  those in `DATA_SCIENCE_STAGE`. So the definitions must exist in **each** schema, and the
  publishing role needs `APPLY` on the tags **in that schema** (e.g. the dev role needs
  `APPLY` on the `DATA_SCIENCE_STAGE` tags).
- The tag *definitions* must first be created once by a Snowflake admin in each schema
  (the RFC `CREATE TAG` setup). Until then, tagging is **skipped with a warning** — the publish
  still succeeds.
- Invalid `status`/`sla` values raise `ValueError` before any data is written.
- Tagged tables surface in the `TABLE_OWNERSHIP_REGISTRY` view (see
  `create_ownership_registry_view`).
