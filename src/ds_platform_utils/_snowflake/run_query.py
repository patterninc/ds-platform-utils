"""Shared Snowflake utility functions."""

import os
from typing import Iterable, Optional

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from ds_platform_utils.sql_utils import add_select_dev_query_tags_to_sql


def _debug_print_query(query: str) -> None:
    """Print query if DEBUG_QUERY env var is set.

    :param query: SQL query to print
    """
    if os.getenv("DEBUG_QUERY"):
        print("\n=== DEBUG SQL QUERY ===")
        print(query)
        print("=====================\n")


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
    # adding query tags comment in query for cost tracking in select.dev
    sql = add_select_dev_query_tags_to_sql(sql)
    _debug_print_query(sql)
    cursors: Iterable[SnowflakeCursor] = conn.execute_string(sql.strip())

    if cursors is None:
        return None

    *_, last = cursors
    return last
