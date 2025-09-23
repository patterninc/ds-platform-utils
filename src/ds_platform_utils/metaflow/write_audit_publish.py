import json
import warnings
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

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
    "OUTERBOUNDS_DATA_SCIENCE_XS_WH",
    "OUTERBOUNDS_DATA_SCIENCE_MED_WH",
    "OUTERBOUNDS_DATA_SCIENCE_XL_WH",
]


def select_dev_query_tags() -> Dict[str, str]:
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

        "pipeline": the flow name

        "step_name": the step within the flow

        "run_id": the unique id of the flow run

        "user": the username of the user who triggered the flow run (or argo-work

        "namespace": the namespace of the flow run

        "team": the team name, hardcoded as "data-science" for all flows

    **Note: all other tags are arbitrary. Add any extra key/value pairs that help you trace and group queries for cost reporting.**
    """
    fetched_tags = current.tags or set()

    if not fetched_tags:
        warnings.warn(
            dedent("""\
        Warning: ds-platform-utils attempted to add query tags to a Snowflake query
        for cost tracking in select.dev, but no tags were found on this Metaflow flow.
        Please add them with --tag, for example:

          uv run <flow_name>_flow.py \
            --environment=fast-bakery \
            --package-suffixes='.csv,.sql,.json,.toml,.yaml,.yml,.txt' \
            --with card \
            argo-workflows create \
            --tag "ds.domain:demand-forecast" \
            --tag "ds.project:cumulative_forecast"

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
        "pipeline": current.flow_name,  # name of the flow
        "project": extract("ds.project"),  # project of the flow which it belongs to, same as workload_id
        "step_name": current.step_name,  # name of the current step
        "run_id": current.run_id,  # run_id: unique id of the current run
        "user": current.username,  # username of user who triggered the run (argo-workflows if its a deployed flow)
        "domain": extract("ds.domain"),  # business unit (domain) of the flow, same as app
        "namespace": current.namespace,  # namespace of the flow
        "team": "data-science",  # team name, hardcoded as data-science
    }


def publish(  # noqa: PLR0913
    table_name: str,
    query: Union[str, Path],
    audits: Optional[List[Union[str, Path]]] = None,
    ctx: Optional[Dict[str, Any]] = None,
    warehouse: Optional[TWarehouse] = None,
    use_utc: bool = True,
) -> None:
    """Publish a table using write-audit-publish pattern with Metaflow's Snowflake connection."""
    from ds_platform_utils._snowflake.write_audit_publish import (
        get_query_from_string_or_fpath,
        write_audit_publish,
    )

    conn = get_snowflake_connection(use_utc=use_utc)

    # adding query tags comment in query for cost tracking in select.dev
    tags = select_dev_query_tags()
    query_comment_str = f"\n\n/* {json.dumps(tags)} */"
    query = get_query_from_string_or_fpath(query)
    query = query + query_comment_str

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
