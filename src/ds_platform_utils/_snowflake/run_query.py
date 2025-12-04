"""Shared Snowflake utility functions."""

import warnings
from typing import Iterable, Optional

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


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
