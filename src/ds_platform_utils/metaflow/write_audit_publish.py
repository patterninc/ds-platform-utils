import json
import warnings
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import sqlparse
from metaflow import current
from metaflow.cards import Artifact, Markdown, Table
from snowflake.connector.cursor import SnowflakeCursor

from ds_platform_utils.metaflow.get_snowflake_connection import get_snowflake_connection

if TYPE_CHECKING:
    from ds_platform_utils._snowflake.write_audit_publish import (
        # AuditSQLOperation,
        SQLOperation,
        # write_audit_publish,
    )

from typing import Literal

TWarehouse = Literal[
    "OUTERBOUNDS_DATA_SCIENCE_ADS_PROD_XS_WH",
    "OUTERBOUNDS_DATA_SCIENCE_ADS_PROD_MED_WH",
    "OUTERBOUNDS_DATA_SCIENCE_ADS_PROD_XL_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_XS_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_MED_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_XL_WH",
    "OUTERBOUNDS_DATA_SCIENCE_ADS_DEV_XS_WH",
    "OUTERBOUNDS_DATA_SCIENCE_ADS_DEV_MED_WH",
    "OUTERBOUNDS_DATA_SCIENCE_ADS_DEV_XL_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
]


def get_select_dev_query_tags() -> Dict[str, str]:
    """Return tags for the current Metaflow flow run.

    These tags are used for cost tracking in select.dev.
    See the select.dev docs on custom workload tags:
    https://select.dev/docs/reference/integrations/custom-workloads#example-query-tag

    What the main tags mean and why we set them this way:

        "app": a broad category that groups queries by domain. We set app to the value of ds.domain
           that we get from current tags of the flow, so queries are attributed to the right domain (for example, "Operations").

        "workload_id": identifies the specific project or sub-unit inside that domain.
        We set workload_id to the value of ds.project that we get from current tags of
        the flow so select.dev can attribute costs to the exact project (for example, "out-of-stock").

    For more granular attribution we have other tags:

        "flow_name": the flow name

        "step_name": the step within the flow

        "run_id": the unique id of the flow run

        "user": the username of the user who triggered the flow run (or argo-workflows if it's a deployed flow)

        "namespace": the namespace of the flow run

        "team": the team name, hardcoded as "data-science" for all flows

    **Note: all other tags are arbitrary. Add any extra key/value pairs that help you trace and group queries for cost reporting.**
    """
    fetched_tags = current.tags
    required_tags_are_present = any(tag.startswith("ds.project") for tag in fetched_tags) and any(
        tag.startswith("ds.domain") for tag in fetched_tags
    )  # Checking presence of both required Metaflow user tags in current tags of the flow
    if not required_tags_are_present:
        warnings.warn(
            dedent("""
        Warning: ds-platform-utils attempted to add query tags to a Snowflake query
        for cost tracking in select.dev, but one or both required Metaflow user tags
        ('ds.domain' and 'ds.project') were not found on this flow.

        These tags are used to correctly attribute query costs by domain and project.
        Please ensure both tags are included when running the flow, for example:

          uv run <flow_name>_flow.py \\
            --environment=fast-bakery \\
            --package-suffixes='.csv,.sql,.json,.toml,.yaml,.yml,.txt' \\
            --with card \\
            argo-workflows create \\
            --tag "ds.domain:operations" \\
            --tag "ds.project:regional-forecast"

        Note: in the monorepo, these tags are applied automatically in CI and when using
        the standard poe commands for running flows.
    """),
            stacklevel=2,
        )

    def extract(prefix: str, default: str = "unknown") -> str:
        for tag in fetched_tags:
            if tag.startswith(prefix + ":"):
                return tag.split(":", 1)[1]
        return default

    # most of these will be unknown if no tags are set on the flow
    # (most likely for the flow runs which are triggered manually locally)
    return {
        "app": extract(
            "ds.domain"
        ),  # first tag after 'app:', is the domain of the flow, fetched from current tags of the flow
        "workload_id": extract(
            "ds.project"
        ),  # second tag after 'workload_id:', is the project of the flow which it belongs to
        "flow_name": current.flow_name,  # name of the metaflow flow
        "project": current.project_name,  # Project name from the @project decorator, lets us
        # identify the flow’s project without relying on user tags (added via --tag).
        "step_name": current.step_name,  # name of the current step
        "run_id": current.run_id,  # run_id: unique id of the current run
        "user": current.username,  # username of user who triggered the run (argo-workflows if its a deployed flow)
        "domain": extract("ds.domain"),  # business unit (domain) of the flow, same as app
        "namespace": current.namespace,  # namespace of the flow
        "perimeter": "PROD" if current.is_production else "Default",  # perimeter of the flow
        "team": "data-science",  # team name, hardcoded as data-science
    }


def add_comment_to_each_sql_statement(sql_text: str, comment: str) -> str:
    """Append `comment` (e.g., /* {...} */) to every SQL statement in `sql_text`.

    Purpose:
        Some SQL files contain multiple statements separated by semicolons.
        Snowflake only associates query-level metadata (like select.dev cost-tracking tags)
        with individual statements, not entire batches. This helper ensures that the
        JSON-style comment containing query tags is added to each statement separately,
        so every query executed can be properly attributed and tracked.

    The comment is inserted immediately before the terminating semicolon of each statement.
    """
    statements = [s.strip() for s in sqlparse.split(sql_text) if s and s.strip()]
    if not statements:
        return sql_text

    annotated = []
    for stmt in statements:
        trimmed = stmt.rstrip()
        if trimmed.endswith(";"):
            trimmed = trimmed[:-1].rstrip()
        annotated.append(f"{trimmed} {comment};")

    # Separate statements with a blank line for readability
    return "\n\n".join(annotated) + "\n"


def publish(  # noqa: PLR0913, D417
    table_name: str,
    query: Union[str, Path],
    audits: Optional[List[Union[str, Path]]] = None,
    ctx: Optional[Dict[str, Any]] = None,
    warehouse: Optional[TWarehouse] = None,
    use_utc: bool = True,
) -> None:
    """Publish a Snowflake table using the write-audit-publish (WAP) pattern via Metaflow's Snowflake connection.

    Parameters
    ----------
    :param table_name: Name of the Snowflake table to publish, e.g., `"OUT_OF_STOCK_ADS"`.
    :param query: The SQL query (str or path to a .sql file) that generates the table data to be written.
    :param audits: A list of SQL audit scripts or file paths that validate the integrity or
    quality of the data before publishing. Each script should return zero rows for a successful audit.
    :param ctx: A context dictionary passed into the SQL execution environment (used for
    parameter substitution within SQL templates, if applicable).
    :param warehouse: The Snowflake warehouse to use for this operation. If not specified,
        it defaults to the `OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH` warehouse,
        when running in the Outerbounds **Default** perimeter, and to the
        `OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_XS_WH` warehouse, when running in the Outerbounds **PROD** perimeter.
    :param use_utc: Whether to use UTC timezone for the Snowflake connection (affects timestamp fields).

    Returns
    -------
    None
        This function performs database operations but does not return a value.
        It writes the table, executes audits, and finalizes the publish step.

    Example
    -------
    ```python
    publish(
        table_name="OUT_OF_STOCK_ADS",
        query="sql/create_training_data.sql",
        audits=["sql/validate_training_data.sql"],
        warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
    )
    ```

    """
    from ds_platform_utils._snowflake.write_audit_publish import (
        get_query_from_string_or_fpath,
        write_audit_publish,
    )

    conn = get_snowflake_connection(use_utc=use_utc)

    # adding query tags comment in query for cost tracking in select.dev
    tags = get_select_dev_query_tags()
    query_comment_str = f"\n\n/* {json.dumps(tags)} */"
    query = get_query_from_string_or_fpath(query)
    query = add_comment_to_each_sql_statement(query, query_comment_str)

    with conn.cursor() as cur:
        if warehouse is not None:
            cur.execute(f"USE WAREHOUSE {warehouse}")

        last_op_was_write = False
        for operation in write_audit_publish(
            table_name=table_name,
            query=query,
            audits=audits,
            cursor=cur,
            is_production=current.is_production,
            ctx=ctx,
            branch_name=current.run_id,
        ):
            if current.card:
                update_card_with_operation_info(
                    operation=operation,
                    last_op_was_write=last_op_was_write,
                    cursor=cur,
                )
            last_op_was_write = operation.operation_type == "write"


def update_card_with_operation_info(
    operation: "SQLOperation",
    last_op_was_write: bool,
    cursor: "SnowflakeCursor",
) -> None:
    """Update the Metaflow card with operation info and table preview if applicable.

    :param operation: SQL operation to display
    :param last_op_was_write: Whether the previous operation was a write
    :param cursor: Snowflake cursor for fetching previews
    """
    print(operation.operation_type)

    # Show table preview after write operations
    if last_op_was_write:
        for element in fetch_table_preview(
            n_rows=10,
            database="PATTERN_DB",
            schema=operation.schema,
            table_name=operation.table_name,
            cursor=cursor,
        ):
            current.card.append(element)

    # Add operation content to card
    content = get_card_content(operation=operation, last_op_was_write=last_op_was_write)
    for item in content:
        current.card.append(item)

    # Update card live
    current.card.refresh()


def get_card_content(operation: "SQLOperation", last_op_was_write: bool) -> list[Union[Markdown, Table]]:
    from ds_platform_utils._snowflake.write_audit_publish import AuditSQLOperation

    """Generate Markdown card content for an operation.

    :param op: SQL operation to generate card content for
    :return: List of strings and Table objects containing card content

    The function handles three cases:
    1. Operations with schema and table_name: Shows table reference with Snowflake link
    2. Operations without schema/table (e.g. utility queries): Shows basic operation type
    3. Audit operations: Additionally shows audit results in tabular format
    """
    content: List[Union[Markdown, Table]] = [
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
    cursor: "SnowflakeCursor",
) -> list[Union[Markdown, Table]]:
    """Fetch a preview of n rows from a table.

    :param n_rows: Number of rows to preview
    :param database: Database name
    :param schema: Schema name
    :param table_name: Table name
    :param cursor: Snowflake cursor
    """
    cursor.execute(f"""
        SELECT *
        FROM {database}.{schema}.{table_name}
        LIMIT {n_rows};
    """)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    # Create header row plus data rows
    table_rows = [[Artifact(col) for col in columns]]  # Header row
    for row in rows:
        table_rows.append([Artifact(val) for val in row])  # Data rows

    return [
        Markdown(f"### Table Preview: ({database}.{schema}.{table_name})"),
        Table(table_rows),
    ]
