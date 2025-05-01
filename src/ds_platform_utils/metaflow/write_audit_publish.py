from pathlib import Path
from typing import Any

from metaflow import Snowflake, current
from metaflow.cards import Markdown, Table, Artifact

from ._write_audit_publish import write_audit_publish, SQLOperation, AuditSQLOperation
from textwrap import dedent

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
        for op in write_audit_publish(
            table_name=table_name,
            query=query,
            audits=audits,
            conn=conn,
            is_production=current.is_production,
            ctx=ctx,
            branch_name=current.run_id,
        ):
            content = get_card_content(op)
            # Handle both string and Table objects
            for item in content:
                current.card.append(item)
            current.card.refresh()


def get_card_content(op: SQLOperation) -> list[str | Table]:
    """Generate Markdown card content for an operation.

    :param op: SQL operation to generate card content for
    :return: List of strings and Table objects containing card content

    The function handles three cases:
    1. Operations with schema and table_name: Shows table reference with Snowflake link
    2. Operations without schema/table (e.g. utility queries): Shows basic operation type
    3. Audit operations: Additionally shows audit results in tabular format
    """
    content = [
        Markdown(f"## {op.operation_type.title()}: {op.schema}.{op.table_name}"),
        Markdown(f"```sql\n{dedent(op.query)}\n```"),
    ]

    # for audits: show the results
    if isinstance(op, AuditSQLOperation) and op.results:
        content.append(Markdown("Results:"))
        # Convert the result dict into a table with columns and values
        table_rows = [[col, Artifact(val)] for col, val in op.results.items()]
        content.append(Table(table_rows))

    # for publish: link to the published table
    if op.operation_type == "publish":
        table_url = _make_snowflake_table_url(
            database="PATTERN_DB",
            schema=op.schema,
            table=op.table_name,
        )
        content.append(
            Markdown(
                f"[View table in Snowflake]({table_url})"
            )
        )

    return content


def _make_snowflake_table_url(database: str, schema: str, table: str) -> str:
    database = database.upper()
    schema = schema.upper()
    table = table.upper()
    return f"https://app.snowflake.com/wedsqvx/pattern/#/data/databases/{database}/schemas/{schema}/table/{table}"
