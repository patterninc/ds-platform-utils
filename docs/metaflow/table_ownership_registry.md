# Table-ownership registry

The central **table-ownership registry**,
`PATTERN_DB.DATA_SCIENCE.TABLE_OWNERSHIP_REGISTRY`, is a Snowflake **table** that
pivots the object tags on each table into one row per table, exposing `owner`,
`team`, `domain`, `project`, `status`, `sla`, `contact` and `last_updated`.

It is **refreshed daily** by
[`TableOwnershipRegistryFlow`](https://ui.pattern.obp.outerbounds.com/dashboard/p/prod/j/table_ownership_registry/b/main/workflows/tableoregistry.prod.tableowstryflow-3vt56?tab=recentRuns)
in the **prod** perimeter on Outerbounds. Query the table directly.

The refresh reads tags from `SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES`, so the
registry is **source-agnostic**: it surfaces both tables tagged automatically by
[`publish`](publish.md) / [`publish_pandas`](publish_pandas.md) *and* tables tagged
manually (e.g. by someone using Claude or plain `ALTER TABLE ... SET TAG` who
doesn't use `ds-platform-utils`). Any table carrying at least one of these tags
appears, no matter how it was tagged.

## Query

```sql
SELECT * FROM PATTERN_DB.DATA_SCIENCE.TABLE_OWNERSHIP_REGISTRY
ORDER BY team, table_name;
```

## Columns

| Column         | Source tag      |
| -------------- | --------------- |
| `table_name`   | object name     |
| `owner`        | `TABLE_OWNER`   |
| `team`         | `TABLE_TEAM`    |
| `domain`       | `TABLE_DOMAIN`  |
| `project`      | `TABLE_PROJECT` |
| `status`       | `TABLE_STATUS`  |
| `sla`          | `TABLE_SLA`     |
| `contact`      | `TABLE_CONTACT` |
| `last_updated` | `LAST_UPDATED`  |

The daily refresh rebuilds the table with this pivot:

```sql
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

## Notes

- **Daily refresh.** The registry is a table, not a live view.
  [`TableOwnershipRegistryFlow`](https://ui.pattern.obp.outerbounds.com/dashboard/p/prod/j/table_ownership_registry/b/main/workflows/tableoregistry.prod.tableowstryflow-3vt56?tab=recentRuns)
  rebuilds it once a day, so newly tagged (or retagged) tables can take up to
  ~24 hours to appear or update.
- **~2h lag on top of that.** The refresh reads `SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES`,
  which itself lags up to ~2 hours. For the current value of a single table's tag, use
  `SYSTEM$GET_TAG('PATTERN_DB.DATA_SCIENCE.TABLE_OWNER', '<table>', 'table')` instead.
- **Adoption-based.** Only tables that have at least one of these tags appear in the
  registry, regardless of whether the tags were applied by `ds-platform-utils` or
  manually.
- **`last_updated`.** For tables published via `publish` / `publish_pandas` this is stamped
  automatically (UTC, `YYYY-MM-DD HH:MI:SS`) on each publish. For manually tagged tables it
  reflects whatever value the person set, and may be absent if the `LAST_UPDATED` tag was
  never applied.
