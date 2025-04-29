from pathlib import Path
from typing import Any

from metaflow import Snowflake, current

from ._write_audit_publish import write_audit_publish

# an integration with this name exists both in the default and prod perimeters
SNOWFLAKE_INTEGRATION = "snowflake-default"


def publish(
    table_name: str,
    query: str | Path,
    audits: list[str | Path] | None = None,
    ctx: dict[str, Any] | None = None,
) -> None:
    """Publish a table using write-audit-publish pattern with Metaflow's Snowflake connection.

    :param flow: The Metaflow flow instance
    :param table_name: Name of the table to create
    :param query: SQL query containing {schema} and {table_name} placeholders
        (or path to a .sql file containing the query)
    :param audits: List of SQL audit queries with boolean checks
    :param is_test: When True, adds test suffix to avoid name conflicts
    """
    with Snowflake(integration=SNOWFLAKE_INTEGRATION) as conn:
        write_audit_publish(
            table_name=table_name,
            query=query,
            audits=audits,
            conn=conn,
            is_production=current.is_production,
            ctx=ctx,
        )
