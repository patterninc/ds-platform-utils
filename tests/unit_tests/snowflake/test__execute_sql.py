"""Functional test for _execute_sql."""

from typing import Generator
from unittest.mock import MagicMock

import pytest
from snowflake.connector import SnowflakeConnection

from ds_platform_utils._snowflake.run_query import _execute_sql
from ds_platform_utils.metaflow.snowflake_connection import _create_snowflake_connection


@pytest.fixture(scope="module")
def patched_current() -> Generator[MagicMock, None, None]:
    """Patch Metaflow `current` object for modules used in this test file."""
    mock_current = MagicMock("metaflow.current")
    mock_current.tags = ["ds.domain:testing", "ds.project:unit-tests"]
    mock_current.flow_name = "DummyFlow"
    mock_current.project_name = "dummy-project"
    mock_current.step_name = "dummy-step"
    mock_current.run_id = "123"
    mock_current.username = "tester"
    mock_current.is_production = False
    mock_current.namespace = "user:tester"
    mock_current.is_running_flow = True
    mock_current.card = []


@pytest.fixture(scope="module")
def snowflake_conn(patched_current) -> Generator[SnowflakeConnection, None, None]:
    """Get a Snowflake connection for testing."""
    yield _create_snowflake_connection(warehouse=None, use_utc=True)


def test_execute_sql_empty_string(snowflake_conn):
    """Empty string returns None."""
    with pytest.raises(ValueError, match="No valid SQL statements found"):
        _execute_sql(snowflake_conn, "")


def test_execute_sql_whitespace_only(snowflake_conn):
    """Whitespace-only string returns None."""
    with pytest.raises(ValueError, match="No valid SQL statements found"):
        _execute_sql(snowflake_conn, "   \n\t  ")


def test_execute_sql_only_semicolons(snowflake_conn):
    """String with only semicolons returns None and raises warning."""
    with pytest.raises(ValueError, match="No valid SQL statements found"):
        _execute_sql(snowflake_conn, "   ;   ;")


def test_execute_sql_only_comments(snowflake_conn):
    """String with only comments returns None and raises warning."""
    with pytest.raises(ValueError, match="No valid SQL statements found"):
        _execute_sql(snowflake_conn, "/* only comments */")


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
    print(rows)
    assert len(rows) == 1
    assert rows[0][0] == 2  # Last statement result
