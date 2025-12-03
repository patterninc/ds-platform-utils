"""Functional test for _execute_sql + publish/publish_pandas/query_pandas_from_snowflake."""

import subprocess
import sys
from datetime import datetime

import pandas as pd
import pytest
import pytz
from metaflow import FlowSpec, project, step

from ds_platform_utils._snowflake.shared import _execute_sql
from ds_platform_utils.metaflow import (
    publish,
    publish_pandas,
    query_pandas_from_snowflake,
)
from ds_platform_utils.metaflow.get_snowflake_connection import get_snowflake_connection


@project(name="test_execute_sql_integration_flow")
class TestExecuteSqlIntegrationFlow(FlowSpec):
    """Metaflow flow that will test _execute_sql integration via various paths.

    - publish_pandas()
    - publish()
    - query_pandas_from_snowflake()
    - _execute_sql() with various SQL patterns (edge cases)
    """

    @step
    def start(self):
        """Start the flow."""
        self.next(self.test_publish_pandas_basic)

    @step
    def test_publish_pandas_basic(self):
        """Publish a simple DataFrame without warehouse (no _execute_sql here, just sanity)."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "created_at": [datetime.now(pytz.UTC)] * 3,
            }
        )

        publish_pandas(
            table_name="RUN_SQL_ITG_PANDAS",
            df=df,
            auto_create_table=True,
            overwrite=True,
        )
        self.next(self.test_publish_pandas_with_warehouse)

    @step
    def test_publish_pandas_with_warehouse(self):
        """Publish using a specific warehouse.

        This hits _execute_sql via:
          - USE WAREHOUSE ...
          - ALTER SESSION SET QUERY_TAG = ...
        """
        df = pd.DataFrame(
            {
                "id": [10, 20],
                "name": ["x", "y"],
                "created_at": [datetime.now(pytz.UTC)] * 2,
            }
        )

        publish_pandas(
            table_name="RUN_SQL_ITG_PANDAS_WH",
            df=df,
            auto_create_table=True,
            overwrite=True,
            warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
        )
        self.next(self.test_publish_with_wap)

    @step
    def test_publish_with_wap(self):
        """Test publish() which uses write_audit_publish under the hood.

        This ensures publish still works when _execute_sql is used for session setup elsewhere.
        """
        # Very simple WAP query: just recreate the table from itself
        query = """
        CREATE OR REPLACE TABLE PATTERN_DB.{{schema}}.{{table_name}} AS
        SELECT * FROM PATTERN_DB.{{schema}}.RUN_SQL_ITG_PANDAS_WH;
        """

        publish(
            table_name="RUN_SQL_ITG_PUBLISH",
            query=query,
            warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
        )

        self.next(self.test_query_pandas_multi_statement)

    @step
    def test_query_pandas_multi_statement(self):
        """Test query_pandas_from_snowflake with a multi-statement SQL.

        We rely on _execute_sql() internally (execute_string) and ensure we get
        the result of the *last* statement.
        """
        multi_stmt_query = """
        -- create a temp table
        CREATE OR REPLACE TEMP TABLE PATTERN_DB.{{schema}}._RUN_SQL_ITG_TMP AS
        SELECT 42 AS answer;

        -- final select (this should be the last cursor)
        SELECT * FROM PATTERN_DB.{{schema}}._RUN_SQL_ITG_TMP;
        """

        df = query_pandas_from_snowflake(
            multi_stmt_query,
            warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
        )

        # We expect the last statement's result: a single row with answer = 42
        assert len(df) == 1
        assert "answer" in df.columns
        assert df["answer"].iloc[0] == 42

        self.next(self.test_execute_sql_edge_cases)

    @step
    def test_execute_sql_edge_cases(self):
        """Directly test _execute_sql() with different SQL patterns (edge cases).

        - empty string
        - whitespace only
        - comments only
        - simple single statement
        - simple multi-statement
        """
        conn = get_snowflake_connection(use_utc=True)

        # 1) Empty string → no statements executed → None
        cursor = _execute_sql(conn, "")
        assert cursor is None

        # 2) Whitespace only → also effectively nothing
        cursor = _execute_sql(conn, "   \n\t  ")
        assert cursor is None

        # 3) Single statement (SELECT)
        cursor = _execute_sql(conn, "SELECT 1 AS x;")
        assert cursor is not None
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1  # x == 1

        # 4) Multi-statement: ensure we get cursor for *last* stmt
        cursor = _execute_sql(conn, "SELECT 1 AS x; SELECT 2 AS x;")
        assert cursor is not None
        rows = cursor.fetchall()
        # Last cursor should correspond to 'SELECT 2 AS x;'
        assert len(rows) == 1
        assert rows[0][0] == 2

        self.next(self.end)

    @step
    def end(self):
        """End of flow."""
        pass


if __name__ == "__main__":
    TestExecuteSqlIntegrationFlow()


@pytest.mark.slow
def test_execute_sql_integration_flow():
    """Run the _execute_sql integration flow via Metaflow CLI."""
    cmd = [sys.executable, __file__, "--environment=local", "--with=card", "run"]

    print("\n=== Metaflow Output ===")
    for line in execute_with_output(cmd):
        print(line, end="")


def execute_with_output(cmd):
    """Execute a command and yield output lines as they are produced."""
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1,
    )

    for line in iter(process.stdout.readline, ""):
        yield line

    process.stdout.close()
    return_code = process.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)
