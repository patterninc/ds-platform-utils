"""Shared Snowflake utility functions."""

import json
import os
import warnings
from textwrap import dedent
from typing import Dict, Iterable, Optional

import sqlparse
from metaflow import current
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


def get_select_dev_query_tags() -> Dict[str, Optional[str]]:
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

    def _extract(prefix: str, default: str = "unknown") -> str:
        for tag in fetched_tags:
            if tag.startswith(prefix + ":"):
                return tag.split(":", 1)[1]
        return default

    # most of these will be unknown if no tags are set on the flow
    # (most likely for the flow runs which are triggered manually locally)
    return {
        "app": _extract(
            "ds.domain"
        ),  # first tag after 'app:', is the domain of the flow, fetched from current tags of the flow
        "workload_id": _extract(
            "ds.project"
        ),  # second tag after 'workload_id:', is the project of the flow which it belongs to
        "flow_name": current.flow_name,
        "project": current.project_name,  # Project name from the @project decorator, lets us
        # identify the flow’s project without relying on user tags (added via --tag).
        "step_name": current.step_name,  # name of the current step
        "run_id": current.run_id,  # run_id: unique id of the current run
        "user": current.username,  # username of user who triggered the run (argo-workflows if its a deployed flow)
        "domain": _extract("ds.domain"),  # business unit (domain) of the flow, same as app
        "namespace": current.namespace,  # namespace of the flow
        "perimeter": str(os.environ.get("OB_CURRENT_PERIMETER") or os.environ.get("OBP_PERIMETER")),
        "is_production": str(
            current.is_production
        ),  # True, if the flow is deployed with the --production flag else false
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

    The comment is inserted immediately before the terminating semicolon of each statement,
    preserving whether the original statement had one.
    """
    statements = [s.strip() for s in sqlparse.split(sql_text) if s.strip()]
    if not statements:
        return sql_text

    annotated = []
    for stmt in statements:
        has_semicolon = stmt.rstrip().endswith(";")
        trimmed = stmt.rstrip()
        if has_semicolon:
            trimmed = trimmed[:-1].rstrip()
            annotated.append(f"{trimmed} {comment};")
        else:
            annotated.append(f"{trimmed} {comment}")

    # Separate statements with a blank line for readability
    return "\n".join(annotated)


def _execute_sql(conn: SnowflakeConnection, sql: str) -> Optional[SnowflakeCursor]:
    """Execute SQL statement(s) using Snowflake's ``connection.execute_string()`` and return the *last* resulting cursor.

    Snowflake's ``execute_string`` allows a single string containing multiple SQL
    statements (separated by semicolons) to be executed at once. Unlike
    ``cursor.execute()``, which handles exactly one statement and returns a single
    cursor object, ``execute_string`` returns a **list of cursors**—one cursor for each
    individual SQL statement in the batch.

    :param conn: Snowflake connection object
    :param sql: SQL query or batch of semicolon-delimited SQL statements
    :return: The cursor corresponding to the last executed statement, or None if no
            statements were executed or if the SQL contains only whitespace/comments
    """
    if not sql.strip():
        return None

    try:
        # adding query tags comment in query for cost tracking in select.dev
        tags = get_select_dev_query_tags()
        tag_str = json.dumps(tags, indent=2)
        sql = add_comment_to_each_sql_statement(sql, tag_str)
        cursors: Iterable[SnowflakeCursor] = conn.execute_string(sql.strip())

        if cursors is None:
            return None

        *_, last = cursors
        return last
    except ProgrammingError as e:
        if "Empty SQL statement" in str(e):
            # raise a warning and return None
            warnings.warn("Empty SQL statement encountered; returning None.", category=UserWarning, stacklevel=2)
            return None
        raise
