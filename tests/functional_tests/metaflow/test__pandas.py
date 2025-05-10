"""A Metaflow flow."""

import subprocess
import sys

import pytest
from metaflow import FlowSpec, project, step


@project(name="test_pandas_read_write_flow")
class TestPandasReadWriteFlow(FlowSpec):
    """A sample flow."""

    @step
    def start(self):
        """Start the flow."""
        self.next(self.test_publish_pandas)

    @step
    def test_publish_pandas(self):
        """Test the publish_pandas function."""
        import pandas as pd

        from ds_platform_utils.metaflow import publish_pandas

        # Create a sample DataFrame
        data = {
            "id": [1, 2, 3, 4, 5],
            "name": ["Mario", "Luigi", "Peach", "Bowser", "Toad"],
            "score": [90.5, 85.2, 88.7, 92.1, 78.9],
        }
        df = pd.DataFrame(data)

        # Publish the DataFrame to Snowflake
        publish_pandas(
            table_name="pandas_test_table",
            df=df,
            auto_create_table=True,
            overwrite=True,
        )

        self.next(self.test_query_pandas)

    @step
    def test_query_pandas(self):
        """Test the query_pandas_from_snowflake function."""
        from ds_platform_utils.metaflow import query_pandas_from_snowflake

        # Query to retrieve the data we just published
        query = "SELECT * FROM PATTERN_DB.{{schema}}.PANDAS_TEST_TABLE;"

        # Query the data back
        result_df = query_pandas_from_snowflake(query)

        # Quick validation
        assert len(result_df) == 5, "Expected 5 rows in the result"
        assert "id" in result_df.columns, "Expected 'id' column in result"
        assert "name" in result_df.columns, "Expected 'name' column in result"
        assert "score" in result_df.columns, "Expected 'score' column in result"

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    TestPandasReadWriteFlow()


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
