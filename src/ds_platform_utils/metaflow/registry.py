"""Central table-ownership registry view (RFC §6).

Exposes the ownership tags applied by :func:`publish` / :func:`publish_pandas` as a
single queryable view. The view is *not* materialized -- it is always live at query
time. Its only staleness is the inherent ~2h lag of
``SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES`` (see the RFC risks section); no periodic
refresh is needed. It is adoption-based: only tables that have at least one ownership
tag appear.
"""

from typing import Optional

from ds_platform_utils._snowflake.run_query import _execute_sql

REGISTRY_VIEW_NAME = "PATTERN_DB.DATA_SCIENCE.TABLE_OWNERSHIP_REGISTRY"

OWNERSHIP_REGISTRY_VIEW_SQL = f"""
CREATE OR REPLACE VIEW {REGISTRY_VIEW_NAME} AS
SELECT
    tr.object_name AS table_name,
    MAX(CASE WHEN tr.tag_name = 'TABLE_OWNER'   THEN tr.tag_value END) AS owner,
    MAX(CASE WHEN tr.tag_name = 'TABLE_TEAM'    THEN tr.tag_value END) AS team,
    MAX(CASE WHEN tr.tag_name = 'TABLE_DOMAIN'  THEN tr.tag_value END) AS domain,
    MAX(CASE WHEN tr.tag_name = 'TABLE_PROJECT' THEN tr.tag_value END) AS project,
    MAX(CASE WHEN tr.tag_name = 'TABLE_STATUS'  THEN tr.tag_value END) AS status,
    MAX(CASE WHEN tr.tag_name = 'TABLE_SLA'     THEN tr.tag_value END) AS sla,
    MAX(CASE WHEN tr.tag_name = 'TABLE_CONTACT' THEN tr.tag_value END) AS contact
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr
WHERE tr.object_database = 'PATTERN_DB'
    AND tr.object_schema  = 'DATA_SCIENCE'
    AND tr.domain         = 'TABLE'
    AND tr.tag_name IN (
        'TABLE_OWNER', 'TABLE_TEAM', 'TABLE_DOMAIN', 'TABLE_PROJECT',
        'TABLE_STATUS', 'TABLE_SLA', 'TABLE_CONTACT'
    )
GROUP BY tr.object_name;
"""


def create_ownership_registry_view(conn: Optional["object"] = None) -> str:
    """Create (or replace) the table-ownership registry view.

    Intended as a one-time admin helper. If ``conn`` is omitted, a connection is opened
    via :func:`get_snowflake_connection`.

    :param conn: Optional open Snowflake connection. If None, one is created.
    :return: The executed ``CREATE OR REPLACE VIEW`` SQL.
    """
    if conn is None:
        from ds_platform_utils.metaflow.snowflake_connection import get_snowflake_connection

        conn = get_snowflake_connection()
    _execute_sql(conn, OWNERSHIP_REGISTRY_VIEW_SQL)
    conn.commit()
    print(f"Created/replaced view {REGISTRY_VIEW_NAME}.")
    return OWNERSHIP_REGISTRY_VIEW_SQL
