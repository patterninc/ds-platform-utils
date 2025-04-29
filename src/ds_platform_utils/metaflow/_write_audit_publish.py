import uuid
from pathlib import Path
from typing import Any

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
) -> None:
    """Write table with audit checks and optional production promotion.

    :param table_name: Name of the singular table created by the query
    :param query: SQL query containing {schema} and {table_name} placeholders
    :param audits: List of SQL queries that perform boolean checks
    :param conn: Snowflake connection. If None, prints queries instead of executing
    :param is_production: When True, writes to DATA_SCIENCE schema
    :param is_test: When True, adds test suffix to avoid name conflicts
    :param ctx: Context dictionary for substituting variables in query and audits
    """
    audit_schema = NON_PROD_SCHEMA
    publish_schema = PROD_SCHEMA if is_production else NON_PROD_SCHEMA

    audits = audits or []

    query = get_query_from_string_or_fpath(query)
    audits = [get_query_from_string_or_fpath(audit.strip()) for audit in audits if audit.strip()]

    if ctx:
        if "schema" in ctx:
            raise ValueError("Context must not contain 'schema' key--it is derived behind the scenes.")
        if "table_name" in ctx:
            raise ValueError("Context must not contain 'table_name' key--it is passed as the table_name argument.")
        query = substitute_map_into_string(query, ctx)
        audits = [substitute_map_into_string(audit, ctx) for audit in audits]

    # Generate unique branch name
    branch_name = str(uuid.uuid4())[:8]
    if is_test:
        branch_name += "_test"

    try:
        branch_table = write(
            table_name=table_name,
            query=query,
            branch_name=branch_name,
            schema=audit_schema,
            conn=conn,
        )
        audit(
            table_name=branch_table,
            schema=audit_schema,
            audits=audits,
            conn=conn,
        )
        publish(
            table_name=table_name,
            branch_name=branch_name,
            from_schema=audit_schema,
            to_schema=publish_schema,
            conn=conn,
        )
    finally:
        cleanup(
            table_name=table_name,
            branch_name=branch_name,
            schema=audit_schema,
            conn=conn,
        )


def run_query(query: str, conn: SnowflakeConnection | None = None) -> list[tuple[Any, ...]] | list[dict[Any, Any]]:
    """Execute a query and return results.

    :param query: SQL query to execute or print
    :param conn: Snowflake connection. If None, prints query instead of executing
    """
    # for debugging: just print the queries rather than run them
    if conn is None:
        # imports are nested here so that rich can be a 'dev' dependency;
        # this way 'rich' is not installed into flows that use this library
        # thus reducing the number of dependencies this utils library adds
        # to the flows that use it.
        from rich.console import Console
        from rich.syntax import Syntax

        console = Console()
        syntax = Syntax(query, "sql", theme="monokai", line_numbers=True)
        console.print(syntax)
        return []

    with conn.cursor() as cur:
        cur.execute(query)
        try:
            return cur.fetchall()
        except Exception:  # snowflake returns exception when no results
            return []


def write(table_name: str, query: str, branch_name: str, schema: str, conn: SnowflakeConnection | None = None) -> str:
    """Write table to a temporary branch table."""
    branch_table = f"{table_name}_{branch_name}"
    formatted_query = query.replace("{schema}", schema).replace("{table_name}", branch_table)
    run_query(query=formatted_query, conn=conn)
    return branch_table


def audit(table_name: str, schema: str, audits: list[str], conn: SnowflakeConnection | None = None) -> None:
    """Run audit queries and raise error if any fail."""
    failed_audits = []

    for i, audit_query in enumerate(audits, 1):
        formatted_query = audit_query.replace("{schema}", schema).replace("{table_name}", table_name)
        results = run_query(query=formatted_query, conn=conn)

        # Check if all boolean columns in the result are True
        if not all(bool(col) for row in results for col in row):
            failed_audits.append(f"Audit #{i}")

    if failed_audits:
        raise AssertionError(f"Audits failed for {schema}.{table_name}: {', '.join(failed_audits)}")


def publish(
    table_name: str, branch_name: str, from_schema: str, to_schema: str, conn: SnowflakeConnection | None = None
) -> None:
    """Promote branch table to final table using SWAP."""
    branch_table = f"{table_name}_{branch_name}"
    print(f"Publishing {from_schema}.{branch_table} to {to_schema}.{table_name}")

    # Create target table if it doesn't exist by cloning branch table
    create_target = f"""
    CREATE TABLE IF NOT EXISTS PATTERN_DB.{to_schema}.{table_name}
    CLONE PATTERN_DB.{from_schema}.{branch_table};
    """
    run_query(query=create_target, conn=conn)

    # Perform the swap
    swap_query = f"""
    ALTER TABLE PATTERN_DB.{to_schema}.{table_name}
    SWAP WITH PATTERN_DB.{from_schema}.{branch_table};
    """
    run_query(query=swap_query, conn=conn)


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
    if isinstance(query_str_or_fpath, Path) or query_str_or_fpath.endswith(".sql"):
        return Path(query_str_or_fpath).read_text()
    return query_str_or_fpath


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
    write_audit_publish(
        table_name=table_name,
        query=query,
        audits=audits,
        is_production=False,  # Set to True for production deployment
        is_test=True,  # Using test mode to see table names
        conn=None,  # Would be snowflake.connector.connect() in practice
    )
