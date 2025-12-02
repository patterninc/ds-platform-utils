"""Utility functions which are shared."""

from typing import Optional

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


def run_sql(conn: SnowflakeConnection, sql: str) -> Optional[SnowflakeCursor]:
    """Runs SQL using execute_string and returns the *last* cursor.

    mimicking Snowflake's `cursor.execute()` behavior.

    :param conn: Snowflake connection object
    :param sql: SQL query or queries to execute
    :return: The last cursor from executing the SQL statements, or None if no statements were executed

    """
    last_cursor = None
    for cur in conn.execute_string(sql):
        last_cursor = cur
    return last_cursor
