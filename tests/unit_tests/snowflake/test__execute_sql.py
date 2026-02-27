"""Functional test for _execute_sql."""

import types
from typing import Generator

import pytest
from snowflake.connector import SnowflakeConnection

import ds_platform_utils._snowflake.run_query as run_query_module
import ds_platform_utils.metaflow.snowflake_connection as snowflake_connection_module
from ds_platform_utils._snowflake.run_query import _execute_sql
from ds_platform_utils.metaflow.snowflake_connection import get_snowflake_connection


def _make_dummy_current(*, tags):
    """Use SimpleNamespace so we can quickly mock a Metaflow's `current` object attributes used by the `get_select_dev_query_tags()` function."""
    cur = types.SimpleNamespace()
    cur.tags = tags
    cur.flow_name = "DummyFlow"
    cur.project_name = "dummy-project"
    cur.step_name = "dummy-step"
    cur.run_id = "123"
    cur.username = "tester"
    cur.namespace = "user:tester"
    cur.is_production = False
    cur.is_running_flow = True
    cur.card = None
    return cur


@pytest.fixture(scope="module")
def patched_current() -> Generator[None, None, None]:
    """Patch Metaflow `current` object for modules used in this test file."""
    dummy_current = _make_dummy_current(tags=["ds.domain:operations", "ds.project:test-project"])
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(run_query_module, "current", dummy_current)
    monkeypatch.setattr(snowflake_connection_module, "current", dummy_current)
    yield
    monkeypatch.undo()


@pytest.fixture(scope="module")
def snowflake_conn(patched_current) -> Generator[SnowflakeConnection, None, None]:
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
