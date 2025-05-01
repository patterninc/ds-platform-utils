import os
import uuid
from pathlib import Path
from typing import Any, Generator, Literal
from dataclasses import dataclass

from snowflake.connector.connection import SnowflakeConnection

PROD_SCHEMA = "DATA_SCIENCE"
NON_PROD_SCHEMA = "DATA_SCIENCE_STAGE"



def write_audit_publish(  # noqa: PLR0913 (too-many-arguments) this fn is an exception
    table_name: str,
    query: str | Path,
    audits: list[str | Path] | None = None,
    conn: SnowflakeConnection | None = None,
    is_production: bool = False,
    is_test: bool = False,
    ctx: dict[str, Any] | None = None,
    branch_name: str | None = None,
) -> Generator["SQLOperation", None, None]:
    """Write table with audit checks and optional production promotion."""
    # gather inputs
    audit_schema = NON_PROD_SCHEMA
    publish_schema = PROD_SCHEMA if is_production else NON_PROD_SCHEMA
    audits = audits or []
    query = get_query_from_string_or_fpath(query)
    audits = [get_query_from_string_or_fpath(audit) for audit in audits]

    # validate inputs
    if "{schema}.{table_name}" not in query:
        raise ValueError(
            "You must use the literal string '{schema}.{table_name}' in your query to "
            "select the table you are writing to, so that the publish() function "
            "can dynamically substitute values into these to perform the write-audit-publish"
            "pattern.\n\n"
            f"Query:\n{query[:100]}"
        )

    for i, audit_query in enumerate(audits, 1):
        audit_query = str(audit_query)
        if "{schema}.{table_name}" not in str(audit_query):
            raise ValueError(
                f"The audit query at index {i} must use the literal string '{{schema}}.{{table_name}}' to "
                "reference the table being audited, so that the audit() function can dynamically "
                "substitute values into these to perform the checks.\n\n"
                f"Audit query:\n{audit_query[:100]}"
            )

    if ctx:
        if "schema" in ctx:
            raise ValueError("Context must not contain 'schema' key--it is derived behind the scenes.")
        if "table_name" in ctx:
            raise ValueError("Context must not contain 'table_name' key--it is passed as the table_name argument.")
        query = substitute_map_into_string(query, ctx)
        audits = [substitute_map_into_string(audit, ctx) for audit in audits]

    # Generate unique branch name
    branch_name = branch_name or str(uuid.uuid4())[:8]
    if is_test:
        branch_name += "_test"
    branch_table_name = f"{table_name}_{branch_name}"

    try:
        # Write
        for op in write(
            query=query,
            branch_table_name=branch_table_name,
            schema=audit_schema,
            conn=conn,
        ):
            yield op

        # Audit
        for op in audit(
            table_name=branch_table_name,
            schema=audit_schema,
            audits=audits,
            conn=conn,
        ):
            yield op

        # Publish
        for op in publish(
            table_name=table_name,
            branch_name=branch_name,
            from_schema=audit_schema,
            to_schema=publish_schema,
            conn=conn,
        ):
            yield op

    finally:
        cleanup(
            table_name=table_name,
            branch_name=branch_name,
            schema=audit_schema,
            conn=conn,
        )


@dataclass
class SQLOperation:
    """SQL operation details."""
    
    query: str
    schema: str
    table_name: str
    operation_type: Literal["write", "audit", "publish"]

@dataclass
class AuditSQLOperation(SQLOperation):
    """SQL operation details for audits, including results."""

    results: list[dict[str, Any]]

def run_query(
    query: str,
    conn: SnowflakeConnection | None = None,
    multi: bool = True,
) -> tuple[tuple[Any, ...] | None, list[str]]:
    """Execute a query and return a single result row and column names.

    :param query: SQL query to execute or print
    :param conn: Snowflake connection. If None, prints query instead of executing
    :param multi: set this to true if there are multiple sql statements in the query
    """
    if os.getenv("DEBUG_QUERY"):
        try:
            from rich.console import Console
            from rich.syntax import Syntax
            console = Console()
            syntax = Syntax(query, "sql", theme="monokai", line_numbers=True)
            console.print(syntax)
        except ImportError:
            print(query)

    if multi:
        conn.execute_string(query)
        return None, []

    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()
        column_names = [col[0] for col in (cur.description or [])]
        return result, column_names


def write(
    query: str, 
    branch_table_name: str,
    schema: str,
    conn: SnowflakeConnection | None = None
) -> Generator[SQLOperation, None, None]:
    """Write table to a temporary branch table.
    
    :param query: SQL query to execute
    :param branch_table_name: Full name of the temporary branch table
    :param schema: Schema to write to
    :param conn: Optional Snowflake connection
    """
    formatted_query = substitute_map_into_string(query, {"schema": schema, "table_name": branch_table_name})
    
    # Yield write operation
    yield SQLOperation(
        query=formatted_query,
        schema=schema,
        table_name=branch_table_name,
        operation_type="write"
    )
    
    run_query(query=formatted_query, conn=conn)


def audit(
    table_name: str,
    schema: str, 
    audits: list[str],
    conn: SnowflakeConnection | None = None
) -> Generator[SQLOperation, None, list[dict[str, Any]]]:
    """Run audit queries and raise error if any fail."""
    failed_audits = []
    results = []

    for i, audit_query in enumerate(audits, 1):
        formatted_query = substitute_map_into_string(
            audit_query, 
            {"schema": schema, "table_name": table_name}
        )
        
        row, columns = run_query(query=formatted_query, conn=conn, multi=False)
        
        # Convert row to dict with column names
        if conn and row:
            result_dict = {columns[i]: value for i, value in enumerate(row)}
        else:
            result_dict = {"mock_col": "mock_val"}  # For when conn is None
            
        results.append({"query": formatted_query, "results": result_dict})
        
        yield AuditSQLOperation(
            query=formatted_query,
            schema=schema,
            table_name=table_name,
            results=result_dict,
            operation_type="audit",
        )

        if not all(bool(v) for v in result_dict.values()):
            failed_audits.append(f"Audit #{i}")

    if failed_audits:
        raise AssertionError(
            f"Audits failed for {schema}.{table_name}: {', '.join(failed_audits)}"
        )
    
    return results


def publish(
    table_name: str,
    branch_name: str,
    from_schema: str,
    to_schema: str,
    conn: SnowflakeConnection | None = None
) -> Generator[SQLOperation, None, None]:
    """Promote branch table to final table using SWAP.
    
    :param table_name: Name of the final table
    :param branch_name: Name of the branch/temporary table
    :param from_schema: Source schema containing the branch table
    :param to_schema: Target schema for the final table
    :param conn: Optional Snowflake connection
    """
    branch_table = f"{table_name}_{branch_name}"
    
    # Create target table if doesn't exist
    create_query = f"""
    CREATE TABLE IF NOT EXISTS PATTERN_DB.{to_schema}.{table_name}
    CLONE PATTERN_DB.{from_schema}.{branch_table};
    """
    create_op = SQLOperation(
        query=create_query,
        schema=to_schema,
        table_name=table_name,
        operation_type="publish"
    )
    yield create_op
    run_query(query=create_query, conn=conn, multi=False)

    # Perform swap
    swap_query = f"""
    ALTER TABLE PATTERN_DB.{to_schema}.{table_name} 
    SWAP WITH PATTERN_DB.{from_schema}.{branch_table};
    """
    swap_op = SQLOperation(
        query=swap_query,
        schema=to_schema,
        table_name=table_name,
        operation_type="publish"
    )
    yield swap_op
    run_query(query=swap_query, conn=conn, multi=False)


def cleanup(table_name: str, branch_name: str, schema: str, conn: SnowflakeConnection | None = None) -> None:
    """Drop temporary branch table."""
    branch_table = f"{table_name}_{branch_name}"
    drop_query = f"DROP TABLE IF EXISTS PATTERN_DB.{schema}.{branch_table};"
    print(f"Dropping temp table: {schema}.{branch_table}")
    run_query(query=drop_query, conn=conn)


def substitute_map_into_string(string: str, values: dict[str, Any]) -> str:
    """Format a string using a dictionary, ignoring keys that are not provided."""

    class SafeDict(dict):
        def __missing__(self, key):
            return f"{{{key}}}"

    return string.format_map(SafeDict(values))


def get_query_from_string_or_fpath(query_str_or_fpath: str | Path) -> str:
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
    create or replace temp table PATTERN_DB.{schema}._TEMP_POKEMON_RAW as
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
    create or replace temp table PATTERN_DB.{schema}._TEMP_POKEMON_ENRICHED as
    select
        pokemon_name,
        pokedex_id,
        age,
        power_level,
        power_level * 1.5 as boosted_power_level,
        type,
        region
    from PATTERN_DB.{schema}._TEMP_POKEMON_RAW
    where type is not null;

    -- Final table: create the published dataset
    create table PATTERN_DB.{schema}.{table_name} as
    select
        pokemon_name,
        pokedex_id,
        age,
        power_level,
        boosted_power_level,
        type
    from PATTERN_DB.{schema}._TEMP_POKEMON_ENRICHED
    where region != 'UNKNOWN';
    """

    # List of audit queries that check data quality
    audits = [
        # Check for valid age and power levels
        """
        select
            min(age) > 0 as all_ages_positive,
            min(power_level) >= 0 as all_power_levels_valid
        from PATTERN_DB.{schema}.{table_name}
        ;
        """,
        # Check for uniqueness and null values
        """
        select
            count(*) = count(pokemon_name) as no_null_names,
            count(distinct pokedex_id) = count(pokedex_id) as unique_pokedex_ids
        from PATTERN_DB.{schema}.{table_name}
        ;
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
        conn=None,  # Would be snowflake.connector.connect() in practice
    ):
        print(step)
