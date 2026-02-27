"""Shared SQL utility functions used across Snowflake and Metaflow modules."""

import json
import os
from pathlib import Path
from textwrap import dedent
from typing import Any, Optional, Union

import sqlparse
from jinja2 import DebugUndefined, Template


def substitute_map_into_string(string: str, values: dict[str, Any]) -> str:
    """Format a string using a dictionary with Jinja2 templating."""
    template = Template(string, undefined=DebugUndefined)
    return template.render(values)


def get_query_from_string_or_fpath(query_str_or_fpath: Union[str, Path]) -> str:
    """Get the SQL query from a string or file path."""
    stripped_query = str(query_str_or_fpath).strip()
    query_is_file_path = isinstance(query_str_or_fpath, Path) or stripped_query.endswith(".sql")
    if query_is_file_path:
        return Path(query_str_or_fpath).read_text()
    return stripped_query


def get_select_dev_query_tags(current_obj: Optional[Any] = None) -> dict[str, Optional[str]]:
    """Return tags for the current Metaflow flow run for select.dev tracking.

    If ``current_obj`` is not provided, this function attempts to read from
    ``metaflow.current``. Missing attributes are handled gracefully with defaults.
    """
    if current_obj is None:
        from metaflow import current as metaflow_current

        current_obj = metaflow_current

    fetched_tags = getattr(current_obj, "tags", [])
    required_tags_are_present = any(tag.startswith("ds.project") for tag in fetched_tags) and any(
        tag.startswith("ds.domain") for tag in fetched_tags
    )
    if not required_tags_are_present:
        print(
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
    """)
        )

    def _extract(prefix: str, default: str = "unknown") -> str:
        for tag in fetched_tags:
            if tag.startswith(prefix + ":"):
                return tag.split(":", 1)[1]
        return default

    def _attr(name: str, default: str = "unknown") -> str:
        return str(getattr(current_obj, name, default))

    return {
        "app": _extract("ds.domain"),
        "workload_id": _extract("ds.project"),
        "flow_name": _attr("flow_name"),
        "project": _attr("project_name"),
        "step_name": _attr("step_name"),
        "run_id": _attr("run_id"),
        "user": _attr("username"),
        "domain": _extract("ds.domain"),
        "namespace": _attr("namespace"),
        "perimeter": str(os.environ.get("OB_CURRENT_PERIMETER") or os.environ.get("OBP_PERIMETER")),
        "is_production": _attr("is_production", "False"),
        "team": "data-science",
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
    statements = [s.strip() for s in sqlparse.split(sqlparse.format(sql_text, strip_comments=True)) if s.strip()]
    annotated = []
    for stmt in statements:
        stmt = stmt.rstrip(";").strip()
        if stmt:
            annotated.append(f"{stmt}\n/* {comment} */\n;")
        else:
            print(
                "Warning: encountered empty SQL statement after parsing. This may indicate an issue with the SQL formatting."
            )
    if not annotated:
        raise ValueError("No valid SQL statements found in the provided SQL text.")
    return "\n".join(annotated)


def add_select_dev_query_tags_to_sql(sql_text: str, current_obj: Optional[Any] = None) -> str:
    """Attach select.dev query-tag JSON comment to each SQL statement."""
    tags = get_select_dev_query_tags(current_obj=current_obj)
    tag_str = json.dumps(tags, indent=2)
    return add_comment_to_each_sql_statement(sql_text, tag_str)
