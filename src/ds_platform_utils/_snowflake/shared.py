""""Shared Snowflake utility functions."""

from typing import Optional

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


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
             statements were executed
    """
    last_cursor = None
    for cur in conn.execute_string(sql):
        last_cursor = cur
    return last_cursor
