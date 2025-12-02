"""Utility functions which are shared."""


def run_sql(cn, sql: str):
    """Runs SQL using execute_string and returns the *last* cursor.

    mimicking Snowflake's `cursor.execute()` behavior.
    """
    last_cursor = None
    for cur in cn.execute_string(sql):
        last_cursor = cur
    return last_cursor
