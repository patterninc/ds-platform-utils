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
    warehouse: str | None = None,
) -> None:
    """Publish a table using write-audit-publish pattern with Metaflow's Snowflake connection."""
    snowflake_kwargs = {"integration": SNOWFLAKE_INTEGRATION}
    if warehouse is not None:
        snowflake_kwargs["warehouse"] = warehouse

    with Snowflake(**snowflake_kwargs) as conn:
        write_audit_publish(
            table_name=table_name,
            query=query,
            audits=audits,
            conn=conn,
            is_production=current.is_production,
            ctx=ctx,
            branch_name=current.run_id,
        )
