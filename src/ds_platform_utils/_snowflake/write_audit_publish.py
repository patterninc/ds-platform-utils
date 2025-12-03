import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, Literal, Optional, Union

from jinja2 import DebugUndefined, Template
from snowflake.connector.cursor import SnowflakeCursor

from ds_platform_utils.metaflow._consts import NON_PROD_SCHEMA, PROD_SCHEMA
from ds_platform_utils.shared.utils import run_sql


def write_audit_publish(  # noqa: PLR0913 (too-many-arguments) this fn is an exception
    table_name: str,
    query: Union[str, Path],
    audits: Optional[list[Union[str, Path]]] = None,
    cursor: Optional[SnowflakeCursor] = None,
    is_production: bool = False,
    is_test: bool = False,
    ctx: Optional[dict[str, Any]] = None,
    branch_name: Optional[str] = None,
) -> Generator["SQLOperation", None, None]:
    """Write table with audit checks and optional production promotion.

    :param cursor: Snowflake cursor for executing queries
    :param audits: SQL queries that return a single row of boolean values representing assertions
        against PATTERN_DB.{{schema}}.{{table_name}}. If len(audits) == 0, write-audit-publish is not
        performed and the query is simply run against the final table.
    """
    # gather inputs
    publish_schema = PROD_SCHEMA if is_production else NON_PROD_SCHEMA
    query = get_query_from_string_or_fpath(query)

    audits = audits or []
    audits = [get_query_from_string_or_fpath(audit) for audit in audits]

    skip_audit_publish = len(audits) == 0

    # validate inputs
    if not query_contains_parameterized_schema_and_table_name(query):
        raise ValueError(
            "You must use the literal string '{{ schema }}.{{ table_name }}' or '{{schema}}.{{table_name}}' in your query to "
            "select the table you are writing to, so that the publish() function "
            "can dynamically substitute values into these to perform the write-audit-publish"
            " pattern.\n\n"
            f"Query:\n{query[:100]}"
        )

    for i, audit_query in enumerate(audits):
        audit_query = str(audit_query)
        if not query_contains_parameterized_schema_and_table_name(audit_query):
            raise ValueError(
                f"The audit query at index {i} must use the literal string '{{ schema }}.{{ table_name }}' or '{{schema}}.{{table_name}}' to "
                "reference the table being audited, so that the audit() function can dynamically "
                "substitute values into these to perform the checks.\n\n"
                f"Audit query:\n{audit_query[:100]}..."
            )

    # substitute any values (other than {schema} and {table_name}) into the query and audit queries
    if ctx:
        if "schema" in ctx:
            raise ValueError(f"Context must not contain 'schema' key--it is derived behind the scenes. Got: {ctx=}")
        if "table_name" in ctx:
            raise ValueError(
                f"Context must not contain 'table_name' key--it is passed as the table_name argument. Got: {ctx=}"
            )
        query = substitute_map_into_string(query, ctx)
        audits = [substitute_map_into_string(audit, ctx) for audit in audits]

    if skip_audit_publish:
        # If no audits, just write the table directly
        for op in write(
            query=query,
            table_name=table_name,
            branch_table_name=table_name,
            schema=publish_schema,
            cursor=cursor,
            skip_clone_branch=True,  # Skip cloning since we're not auditing
        ):
            yield op
    else:
        # If audits are provided, run the full write-audit-publish flow
        for op in _write_audit_publish(
            table_name=table_name,
            query=query,
            audits=audits,
            cursor=cursor,
            is_production=is_production,
            is_test=is_test,
            branch_name=branch_name,
        ):
            yield op


def query_contains_parameterized_schema_and_table_name(query: str) -> bool:
    """Check if the query contains the parameterized schema and table name.

    :param query: SQL query to check
    :return: True if the query contains '{{schema}}.{{table_name}}', False otherwise
    """
    if "{{schema}}.{{table_name}}" in query:
        return True
    if "{{ schema }}.{{ table_name }}" in query:
        return True
    return False


def _write_audit_publish(  # noqa: PLR0913 (too-many-arguments) this fn is an exception
    table_name: str,
    query: str,
    audits: list[str],
    cursor: Optional[SnowflakeCursor] = None,
    is_production: bool = False,
    is_test: bool = False,
    branch_name: Optional[str] = None,
) -> Generator["SQLOperation", None, None]:
    """Write table with audit checks and optional production promotion.

    This function assumes all inputs have been validated and formatted correctly.
    """
    audit_schema = NON_PROD_SCHEMA
    publish_schema = PROD_SCHEMA if is_production else NON_PROD_SCHEMA

    # Generate unique branch name
    branch_name = branch_name or str(uuid.uuid4())[:8]
    if is_test:
        branch_name += "_test"
    branch_table_name = f"{table_name}_{branch_name}"

    try:
        for op in write(
            query=query,
            table_name=table_name,  # Add this parameter
            branch_table_name=branch_table_name,
            schema=audit_schema,
            cursor=cursor,
        ):
            yield op

        for op in audit(
            table_name=branch_table_name,
            schema=audit_schema,
            audits=audits,
            cursor=cursor,
        ):
            yield op

        for op in publish(
            table_name=table_name,
            branch_name=branch_name,
            from_schema=audit_schema,
            to_schema=publish_schema,
            cursor=cursor,
        ):
            yield op

    finally:
        cleanup(
            table_name=table_name,
            branch_name=branch_name,
            schema=audit_schema,
            cursor=cursor,
        )


@dataclass
class SQLOperation:
    """SQL operation details."""

    query: str
    schema: str
    table_name: str
    operation_type: Literal["write", "clone to branch", "audit", "publish"]


@dataclass
class AuditSQLOperation(SQLOperation):
    """SQL operation details for audits, including results."""

    results: dict[str, Any]


def _debug_print_query(query: str) -> None:
    """Print query if DEBUG_QUERY env var is set."""
    if os.getenv("DEBUG_QUERY"):
        print("\n=== DEBUG SQL QUERY ===")
        print(query)
        print("=====================\n")


def run_query(query: str, cursor: Optional[SnowflakeCursor] = None) -> None:
    """Execute one or more SQL statements.

    :param query: SQL query or queries to execute. Multiple statements must be separated by semicolons.
    :param cursor: Snowflake cursor. If None, prints query instead of executing
    """
    _debug_print_query(query)

    if cursor is None:
        print(f"Would execute query:\n{query}")
        return

    # run the query using run_sql utility which handles multiple statements via execute_string
    run_sql(cursor.connection, query)
    cursor.connection.commit()


def run_audit_query(query: str, cursor: Optional[SnowflakeCursor] = None) -> dict[str, Any]:
    """Execute a single audit query and return results.

    :param query: SQL query that returns a single row of boolean values
    :param cursor: Snowflake cursor. If None, returns mock successful result
    """
    _debug_print_query(query)

    if cursor is None:
        return {"mock_result": True}

    cursor = run_sql(cursor.connection, query)
    if cursor is None:
        return {}

    result = cursor.fetchone()
    if not result:
        return {}

    column_names = [col[0] for col in (cursor.description or [])]
    return dict(zip(column_names, result))


def fetch_table_preview(
    n_rows: int,
    database: str,
    schema: str,
    table_name: str,
    cursor: Optional[SnowflakeCursor] = None,
) -> list[dict[str, Any]]:
    """Fetch a preview of n rows from a table.

    :param n_rows: Number of rows to preview
    :param database: Database name
    :param schema: Schema name
    :param table_name: Table name
    :param cursor: Snowflake cursor
    """
    if not cursor:
        return [{"mock_col": "mock_val"}]

    cursor = run_sql(
        cursor.connection,
        f"""
        SELECT *
        FROM {database}.{schema}.{table_name}
        LIMIT {n_rows};
        """,
    )
    if cursor is None:
        return []

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def write(  # noqa: PLR0913 (too-many-arguments)
    query: str,
    table_name: str,
    branch_table_name: str,
    schema: str,
    cursor: Optional[SnowflakeCursor] = None,
    skip_clone_branch: bool = False,
) -> Generator[SQLOperation, None, None]:
    """Write table to a temporary branch table, attempting to clone existing table first.

    :param query: SQL query to create/populate table if clone fails
    :param table_name: Name of the source table to try cloning
    :param branch_table_name: Name for the temporary branch table
    :param schema: Schema to write to
    :param cursor: Optional Snowflake cursor
    :param skip_clone_branch: If True, skip the cloning step and just run the query. Used when
        there are no audits to run, so write-audit-publish is skipped, and data is simply written
        directly to the final table.
    """
    if not skip_clone_branch:
        # First, we need to make sure a branch table exists, incase this query is trying to
        # INSERT or otherwise modify an existing table, but isn't creating it.
        clone_query = f"""
        CREATE TABLE IF NOT EXISTS PATTERN_DB.{schema}.{branch_table_name}
        CLONE PATTERN_DB.{schema}.{table_name};
        """

        clone_op = SQLOperation(
            query=clone_query,
            schema=schema,
            table_name=branch_table_name,
            operation_type="clone to branch",
        )
        yield clone_op

        run_query(query=clone_query, cursor=cursor)

    # Now that we know the branch table exists, we can run the main query
    formatted_query = substitute_map_into_string(query, {"schema": schema, "table_name": branch_table_name})
    write_op = SQLOperation(
        query=formatted_query,
        schema=schema,
        table_name=branch_table_name,
        operation_type="write",
    )
    yield write_op

    run_query(query=formatted_query, cursor=cursor)


def audit(
    table_name: str,
    schema: str,
    audits: list[str],
    cursor: Optional[SnowflakeCursor] = None,
) -> Generator[SQLOperation, None, None]:
    """Run audit queries and raise error if any fail."""
    failed_audits = []

    for i, audit_query in enumerate(audits, 1):
        formatted_query = substitute_map_into_string(audit_query, {"schema": schema, "table_name": table_name})

        result_dict = run_audit_query(query=formatted_query, cursor=cursor)

        yield AuditSQLOperation(
            query=formatted_query,
            schema=schema,
            table_name=table_name,
            results=result_dict,
            operation_type="audit",
        )

        # Check which assertions failed
        failed_assertions = [assertion_name for assertion_name, passed in result_dict.items() if not bool(passed)]

        if failed_assertions:
            failed_audits.append(f"Audit #{i} failed assertions: {', '.join(failed_assertions)}")

    if failed_audits:
        raise AssertionError(
            f"Audits failed for {schema}.{table_name}:\n" + "\n".join(f"- {failure}" for failure in failed_audits)
        )


def publish(
    table_name: str,
    branch_name: str,
    from_schema: str,
    to_schema: str,
    cursor: Optional[SnowflakeCursor] = None,
) -> Generator[SQLOperation, None, None]:
    """Promote branch table to final table using SWAP.

    SWAP is a zero-copy operation that copies the metadata (a pointer)
    of the source table to the target table.

    :param table_name: Name of the final table
    :param branch_name: Name of the branch/temporary table
    :param from_schema: Source schema containing the branch table
    :param to_schema: Target schema for the final table
    :param cursor: Optional Snowflake cursor
    """
    branch_table = f"{table_name}_{branch_name}"

    create_query = f"""
    -- create the final table if it does not exist
    CREATE TABLE IF NOT EXISTS PATTERN_DB.{to_schema}.{table_name}
    CLONE PATTERN_DB.{from_schema}.{branch_table};

    -- swap the audited branch table into the final table
    ALTER TABLE PATTERN_DB.{to_schema}.{table_name}
    SWAP WITH PATTERN_DB.{from_schema}.{branch_table};
    """

    create_op = SQLOperation(
        query=create_query,
        schema=to_schema,
        table_name=table_name,
        operation_type="publish",
    )
    yield create_op

    run_query(query=create_query, cursor=cursor)


def cleanup(
    table_name: str,
    branch_name: str,
    schema: str,
    cursor: Optional[SnowflakeCursor] = None,
) -> None:
    """Drop temporary branch table."""
    branch_table = f"{table_name}_{branch_name}"
    drop_query = f"DROP TABLE IF EXISTS PATTERN_DB.{schema}.{branch_table};"
    print(f"Dropping temp table: {schema}.{branch_table}")
    run_query(query=drop_query, cursor=cursor)


def substitute_map_into_string(string: str, values: dict[str, Any]) -> str:
    """Format a string using a dictionary with Jinja2 templating.

    :param string: The template string containing placeholders
    :param values: A dictionary of values to substitute into the template
    """
    template = Template(string, undefined=DebugUndefined)
    return template.render(values)


def get_query_from_string_or_fpath(query_str_or_fpath: Union[str, Path]) -> str:
    """Get the SQL query from a string or file path.

    :param query_str_or_fpath: SQL query string or path to a .sql file
    :return: The SQL query as a string
    """
    stripped_query = str(query_str_or_fpath).strip()
    query_is_file_path = isinstance(query_str_or_fpath, Path) or stripped_query.endswith(".sql")
    if query_is_file_path:
        return Path(query_str_or_fpath).read_text()
    return stripped_query


if __name__ == "__main__":
    # Example usage
    table_name = "pokemon_stats"

    # Main query creates the table with pokemon statistics, using intermediate temp tables
    query = """
    -- First temp table: extract raw data and filter
    create or replace temp table PATTERN_DB.{{schema}}._TEMP_POKEMON_RAW as
    select
        pokemon_name,
        pokedex_id,
        age,
        power_level,
        type,
        region
    from PATTERN_DB.PROD.raw_pokemon_data
    where age > 0;

    -- Second temp table: add derived calculations
    create or replace temp table PATTERN_DB.{{schema}}._TEMP_POKEMON_ENRICHED as
    select
        pokemon_name,
        pokedex_id,
        age,
        power_level,
        power_level * 1.5 as boosted_power_level,
        type,
        region
    from PATTERN_DB.{{schema}}._TEMP_POKEMON_RAW
    where type is not null;

    -- Final table: create the published dataset
    create table PATTERN_DB.{{schema}}.{{table_name}} as
    select
        pokemon_name,
        pokedex_id,
        age,
        power_level,
        boosted_power_level,
        type
    from PATTERN_DB.{{schema}}._TEMP_POKEMON_ENRICHED
    where region != 'UNKNOWN';
    """

    # List of audit queries that check data quality
    audits = [
        # Check for valid age and power levels
        """
        select
            min(age) > 0 as all_ages_positive,
            min(power_level) >= 0 as all_power_levels_valid
        from PATTERN_DB.{{schema}}.{{table_name}};
        """,
        # Check for uniqueness and null values
        """
        select
            count(*) = count(pokemon_name) as no_null_names,
            count(distinct pokedex_id) = count(pokedex_id) as unique_pokedex_ids
        from PATTERN_DB.{{schema}}.{{table_name}};
        """,
    ]

    # For demonstration, using None as context
    # In real usage, this would be a Snowflake connection
    for step in write_audit_publish(
        table_name=table_name,
        query=query,
        audits=audits,
        is_production=False,  # Set to True for production deployment
        is_test=True,  # Using test mode to see table names
        cursor=None,  # Would be snowflake.connector.connect().cursor() in practice
    ):
        print(step)
