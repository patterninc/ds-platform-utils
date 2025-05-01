from pathlib import Path
from textwrap import dedent
from typing import Any

from metaflow import Snowflake, current
from metaflow.cards import Artifact, Markdown, Table
from snowflake.connector.connection import SnowflakeConnection

from ._write_audit_publish import AuditSQLOperation, SQLOperation, write_audit_publish

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

    last_op_was_write = False
    with Snowflake(**snowflake_kwargs) as conn:
        for operation in write_audit_publish(
            table_name=table_name,
            query=query,
            audits=audits,
            conn=conn,
            is_production=current.is_production,
            ctx=ctx,
            branch_name=current.run_id,
        ):
            update_card_with_operation_info(
                operation=operation,
                last_op_was_write=last_op_was_write,
                conn=conn,
            )
            last_op_was_write = operation.operation_type == "write"


def update_card_with_operation_info(
    operation: SQLOperation,
    last_op_was_write: bool,
    conn: SnowflakeConnection,
) -> None:
    """Update the Metaflow card with operation info and table preview if applicable.

    :param operation: SQL operation to display
    :param last_op_was_write: Whether the previous operation was a write
    :param conn: Snowflake connection for fetching previews
    """
    print(operation.operation_type)

    # Show table preview after write operations
    if last_op_was_write:
        for element in fetch_table_preview(
            n_rows=10,
            database="PATTERN_DB",
            schema=operation.schema,
            table_name=operation.table_name,
            conn=conn,
        ):
            current.card.append(element)

    # Add operation content to card
    content = get_card_content(operation=operation, last_op_was_write=last_op_was_write)
    for item in content:
        current.card.append(item)

    # Update card live
    current.card.refresh()


def get_card_content(operation: SQLOperation, last_op_was_write: bool) -> list[Markdown | Table]:
    """Generate Markdown card content for an operation.

    :param op: SQL operation to generate card content for
    :return: List of strings and Table objects containing card content

    The function handles three cases:
    1. Operations with schema and table_name: Shows table reference with Snowflake link
    2. Operations without schema/table (e.g. utility queries): Shows basic operation type
    3. Audit operations: Additionally shows audit results in tabular format
    """
    content: list[Markdown | Table] = [
        Markdown(f"## **{operation.operation_type.title()}**: {operation.schema}.{operation.table_name}"),
        Markdown(f"```sql\n{dedent(operation.query)}\n```"),
    ]

    # for writes: show table preview if available
    if operation.operation_type == "write" and isinstance(operation, AuditSQLOperation) and operation.results:
        content.append(Markdown("\nTable Preview:"))
        table_rows = [[col, Artifact(val)] for col, val in operation.results.items()]
        content.append(Table(table_rows))

    # for audits: show the results
    if isinstance(operation, AuditSQLOperation) and operation.results:
        content.append(Markdown("Results:"))
        table_rows = [[col, Artifact(val)] for col, val in operation.results.items()]
        content.append(Table(table_rows))

    # for publish: link to the published table
    if operation.operation_type == "publish":
        table_url = _make_snowflake_table_url(
            database="PATTERN_DB",
            schema=operation.schema,
            table=operation.table_name,
        )
        content.append(Markdown(f"[View table in Snowflake]({table_url})"))

    return content


def _make_snowflake_table_url(database: str, schema: str, table: str) -> str:
    database = database.upper()
    schema = schema.upper()
    table = table.upper()
    return f"https://app.snowflake.com/wedsqvx/pattern/#/data/databases/{database}/schemas/{schema}/table/{table}"


def fetch_table_preview(
    n_rows: int,
    database: str,
    schema: str,
    table_name: str,
    conn: SnowflakeConnection,
) -> list[Markdown | Table]:
    """Fetch a preview of n rows from a table.

    :param n_rows: Number of rows to preview
    :param database: Database name
    :param schema: Schema name
    :param table_name: Table name
    :param conn: Snowflake connection
    """
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT * 
            FROM {database}.{schema}.{table_name}
            LIMIT {n_rows};
        """)
        columns = [col[0] for col in cur.description]
        rows = cur.fetchall()

        # Create header row plus data rows
        table_rows = [[Artifact(col) for col in columns]]  # Header row
        for row in rows:
            table_rows.append([Artifact(val) for val in row])  # Data rows

        return [
            Markdown(f"### Table Preview: ({database}.{schema}.{table_name})"),
            Table(table_rows),
        ]
