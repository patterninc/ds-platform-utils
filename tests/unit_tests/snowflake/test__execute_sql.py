"""Functional test for _execute_sql."""

from typing import Generator

import pytest
from snowflake.connector import SnowflakeConnection

from ds_platform_utils._snowflake.run_query import _execute_sql
from ds_platform_utils.metaflow.get_snowflake_connection import get_snowflake_connection


@pytest.fixture(scope="module")
def snowflake_conn() -> Generator[SnowflakeConnection, None, None]:
    """Get a Snowflake connection for testing."""
    yield get_snowflake_connection(use_utc=True)


def test_execute_sql_empty_string(snowflake_conn):
    """Empty string returns None."""
    cursor = _execute_sql(snowflake_conn, "")
    assert cursor is None


def test_execute_sql_whitespace_only(snowflake_conn):
    """Whitespace-only string returns None."""
    cursor = _execute_sql(snowflake_conn, "   \n\t  ")
    assert cursor is None


def test_execute_sql_only_semicolons(snowflake_conn):
    """String with only semicolons returns None and raises warning."""
    with pytest.warns(UserWarning, match="Empty SQL statement encountered"):
        cursor = _execute_sql(snowflake_conn, "   ;   ;")
    assert cursor is None


def test_execute_sql_only_comments(snowflake_conn):
    """String with only comments returns None and raises warning."""
    with pytest.warns(UserWarning, match="Empty SQL statement encountered"):
        cursor = _execute_sql(snowflake_conn, "/* only comments */")
    assert cursor is None


def test_execute_sql_single_statement(snowflake_conn):
    """Single statement returns cursor with expected result."""
    cursor = _execute_sql(snowflake_conn, "SELECT 1 AS x;")
    assert cursor is not None
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 1


def test_execute_sql_multi_statement(snowflake_conn):
    """Multi-statement returns cursor for last statement only."""
    cursor = _execute_sql(snowflake_conn, "SELECT 1 AS x; SELECT 2 AS x;")
    assert cursor is not None
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 2  # Last statement result
