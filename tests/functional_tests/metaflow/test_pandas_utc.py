"""A Metaflow flow."""

import subprocess
import sys

import pytest
from metaflow import FlowSpec, project, step


@project(name="test_pandas_read_write_utc_flow")
class TestPandasReadWriteUTCFlow(FlowSpec):
    """A sample flow."""

    @step
    def start(self):
        """Start the flow."""
        self.next(self.test_publish_pandas_with_use_utc)

    @step
    def test_publish_pandas_with_use_utc(self):
        """Test publishing a DataFrame with a UTC datetime column."""
        from datetime import datetime, timedelta

        import pandas as pd
        import pytz

        from ds_platform_utils.metaflow import publish_pandas

        now_utc = datetime.now(pytz.UTC)
        data = {
            "id": [1, 2, 3, 4, 5],
            "name": ["Mario", "Luigi", "Peach", "Bowser", "Toad"],
            "score": [90.5, 85.2, 88.7, 92.1, 78.9],
            "created_at": [now_utc + timedelta(hours=i) for i in range(5)],
        }
        df = pd.DataFrame(data)

        # Check before publish that created_at is UTC
        assert str(df["created_at"].dt.tz) == "UTC", "created_at should be UTC before publishing"

        # Publish the table to Snowflake (UTC=True is default)
        publish_pandas(
            table_name="PANDAS_TEST_TABLE_UTC",
            df=df,
            auto_create_table=True,
            overwrite=True,
        )
        self.next(self.test_publish_with_use_utc)

    @step
    def test_publish_with_use_utc(self):
        """Test the publish function with UTC settings."""
        from ds_platform_utils.metaflow import publish

        # Publish the same table which was published via publish_pandas in previous step (UTC=True is default)
        publish(
            table_name="PANDAS_TEST_TABLE_UTC",
            query="SELECT * FROM PATTERN_DB.{{schema}}.{{table_name}};",
        )
        self.next(self.test_query_pandas_with_use_utc)

    @step
    def test_query_pandas_with_use_utc(self):
        """Test the querying of the UTC data."""
        import pandas as pd

        from ds_platform_utils.metaflow import query_pandas_from_snowflake

        query = "SELECT * FROM PATTERN_DB.{{schema}}.PANDAS_TEST_TABLE;"
        df = query_pandas_from_snowflake(query, use_utc=False)

        print(df[1])

        dt_col = df["created_at"]

        assert pd.api.types.is_datetime64_any_dtype(dt_col), "created_at is not a datetime column"
        assert dt_col.dt.tz is not None, "created_at column is not timezone-aware"
        assert str(dt_col.dt.tz) == "datetime.timezone.utc", f"created_at column is not UTC (was {dt_col.dt.tz})"

        self.next(self.test_publish_pandas_with_use_utc_false)

    @step
    def test_publish_pandas_with_use_utc_false(self):
        """Publish pandas DataFrame with use_utc=False and IST or MST timezone."""
        from datetime import datetime, timedelta

        import pandas as pd
        import pytz

        from ds_platform_utils.metaflow import publish_pandas

        # Choose IST or MST timezone
        # IST: Asia/Kolkata, MST: US/Mountain
        tz = pytz.timezone("Asia/Kolkata")  # For IST
        # tz = pytz.timezone("US/Mountain")  # For MST

        now_tz = datetime.now(tz)
        data = {
            "id": [1, 2, 3, 4, 5],
            "name": ["Mario", "Luigi", "Peach", "Bowser", "Toad"],
            "score": [90.5, 85.2, 88.7, 92.1, 78.9],
            "created_at": [now_tz + timedelta(hours=i) for i in range(5)],
        }
        df = pd.DataFrame(data)
        assert str(df["created_at"].dt.tz) == str(tz), f"created_at should be {tz} before publishing"
        # Use use_utc=False this time
        publish_pandas(
            table_name="PANDAS_TEST_TABLE_NOUTC", df=df, auto_create_table=True, overwrite=True, use_utc=False
        )
        self.next(self.test_publish_with_use_utc_false)

    @step
    def test_publish_with_use_utc_false(self):
        """Test the publish function with UTC settings."""
        from ds_platform_utils.metaflow import publish

        # Publish the same table which was published via publish_pandas in previous step
        publish(
            table_name="PANDAS_TEST_TABLE_NOUTC",
            query="SELECT * FROM PATTERN_DB.{{schema}}.{{table_name}};",
            use_utc=False,
        )
        self.next(self.test_query_pandas_with_use_utc_false)

    @step
    def test_query_pandas_with_use_utc_false(self):
        """Test the querying of the non-UTC data."""
        import pandas as pd

        from ds_platform_utils.metaflow import query_pandas_from_snowflake

        query = "SELECT * FROM PATTERN_DB.{{schema}}.PANDAS_TEST_TABLE_NOUTC;"
        df = query_pandas_from_snowflake(query, use_utc=False)

        dt_col = df["created_at"]

        assert pd.api.types.is_datetime64_any_dtype(dt_col), "created_at is not a datetime column"
        assert dt_col.dt.tz is not None, "created_at column is not timezone-aware"
        assert str(dt_col.dt.tz) != "datetime.timezone.utc", f"created_at column is UTC (was {dt_col.dt.tz})"

        self.next(self.test_publish_pandas_with_use_utc_false)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    TestPandasReadWriteUTCFlow()


@pytest.mark.slow
def test_pandas_read_write_flow():
    """Test that the publish flow runs successfully."""
    cmd = [sys.executable, __file__, "--environment=local", "--with=card", "run"]

    print("\n=== Metaflow Output ===")
    for line in execute_with_output(cmd):
        print(line, end="")


def execute_with_output(cmd):
    """Execute a command and yield output lines as they are produced."""
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Merge stderr into stdout
        universal_newlines=True,
        bufsize=1,
    )

    for line in iter(process.stdout.readline, ""):
        yield line

    process.stdout.close()
    return_code = process.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)
