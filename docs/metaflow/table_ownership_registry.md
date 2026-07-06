# Table-ownership registry view

The central **table-ownership registry view**,
`PATTERN_DB.DATA_SCIENCE.TABLE_OWNERSHIP_REGISTRY`, pivots the object tags on each table
into one row per table, exposing `owner`, `team`, `domain`, `project`, `status`, `sla`,
`contact` and `last_updated`.

It reads the tags straight from `SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES`, so it is
**source-agnostic**: it surfaces both tables tagged automatically by
[`publish`](publish.md) / [`publish_pandas`](publish_pandas.md) *and* tables tagged
manually (e.g. by someone using Claude or plain `ALTER TABLE ... SET TAG` who doesn't use
`ds-platform-utils`). Any table carrying at least one of these tags appears, no matter how
it was tagged.

This is a **one-time admin setup step**, not part of the Python API. A Snowflake admin
runs the SQL below once; the view is live thereafter (see [Notes](#notes)).

## Create (or replace) the view

```sql
CREATE OR REPLACE VIEW PATTERN_DB.DATA_SCIENCE.TABLE_OWNERSHIP_REGISTRY AS
SELECT
    tr.object_name AS table_name,
    MAX(CASE WHEN tr.tag_name = 'TABLE_OWNER'   THEN tr.tag_value END) AS owner,
    MAX(CASE WHEN tr.tag_name = 'TABLE_TEAM'    THEN tr.tag_value END) AS team,
    MAX(CASE WHEN tr.tag_name = 'TABLE_DOMAIN'  THEN tr.tag_value END) AS domain,
    MAX(CASE WHEN tr.tag_name = 'TABLE_PROJECT' THEN tr.tag_value END) AS project,
    MAX(CASE WHEN tr.tag_name = 'TABLE_STATUS'  THEN tr.tag_value END) AS status,
    MAX(CASE WHEN tr.tag_name = 'TABLE_SLA'     THEN tr.tag_value END) AS sla,
    MAX(CASE WHEN tr.tag_name = 'TABLE_CONTACT' THEN tr.tag_value END) AS contact,
    MAX(CASE WHEN tr.tag_name = 'LAST_UPDATED'  THEN tr.tag_value END) AS last_updated
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr
WHERE tr.object_database = 'PATTERN_DB'
    AND tr.object_schema  = 'DATA_SCIENCE'
    AND tr.domain         = 'TABLE'
    AND tr.tag_name IN (
        'TABLE_OWNER', 'TABLE_TEAM', 'TABLE_DOMAIN', 'TABLE_PROJECT',
        'TABLE_STATUS', 'TABLE_SLA', 'TABLE_CONTACT', 'LAST_UPDATED'
    )
GROUP BY tr.object_name;
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
- **Adoption-based.** Only tables that have at least one of these tags appear in the view,
  regardless of whether the tags were applied by `ds-platform-utils` or manually.
- **`last_updated`.** For tables published via `publish` / `publish_pandas` this is stamped
  automatically (UTC, `YYYY-MM-DD HH:MI:SS`) on each publish. For manually tagged tables it
  reflects whatever value the person set, and may be absent if the `LAST_UPDATED` tag was
  never applied.
