"""A Metaflow flow."""

import subprocess
import sys
from pathlib import Path

import pytest

from metaflow import FlowSpec, project, step

THIS_DIR = Path(__file__).parent

AUDIT = """
SELECT
    -- assert that primary keys are unique
    COUNT(DISTINCT id) = COUNT(*) AS primary_key_unique,
    -- assert that names contain only alphabetical characters
    SUM(CASE WHEN NOT REGEXP_LIKE(name, '^[a-zA-Z]+$') THEN 1 ELSE 0 END) = 0 AS name_alphabetical,
    -- assert that ages are nonnegative integers
    SUM(CASE WHEN age < 0 OR age != CAST(age AS INT) THEN 1 ELSE 0 END) = 0 AS age_nonnegative_integer,
    -- assert all columns are non-null
    SUM(CASE WHEN id IS NULL OR name IS NULL OR age IS NULL THEN 1 ELSE 0 END) = 0 AS no_nulls
FROM PATTERN_DB.{schema}.{table_name}
"""


@project(name="test_publish_flow")
class TestPublishFlow(FlowSpec):
    """A sample flow."""

    @step
    def start(self):
        """Execute sample SQL query."""
        import os

        from ds_platform_utils.metaflow import publish

        os.environ["DEBUG_QUERY"] = "1"

        query = """
        -- Create a test table
        CREATE OR REPLACE TABLE PATTERN_DB.{schema}.{table_name} (
            id INT,
            name STRING,
            age INT
        );

        -- Insert some data
        INSERT INTO PATTERN_DB.{schema}.{table_name} (id, name, age)
        VALUES
            (1, 'Mario', 27),
            (2, 'Luigi', 30),
            (3, 'Peach', 25),
            (4, 'Bowser', 35),
            (5, 'Toad', 5);
        """

        publish(
            table_name="sample_table",
            query=query,
            audits=[AUDIT],
        )

        query = """
        -- Insert additional video game characters
        INSERT INTO PATTERN_DB.{schema}.{table_name} (id, name, age)
        VALUES
            (6, 'Link', 20),
            (7, 'Kirby', 40),
            (8, 'Zelda', 28);
        """

        # insert queries without creating/replacing the table
        publish(
            table_name="sample_table",
            query=query,
            audits=[
                # assert that there are 8 rows. The initial 5, plus the recent 3.
                "SELECT COUNT(*) = 8 FROM PATTERN_DB.{schema}.{table_name}"
            ],
        )

        # no audit
        publish(
            table_name="sample_table",
            query=query,
            audits=[],
        )

        try:
            # intentionally fail an audit
            publish(
                table_name="sample_table",
                query=query,
                audits=["SELECT COUNT(*) = 0 FROM PATTERN_DB.{schema}.{table_name}"],
            )
            raise Exception("The audit should have raised a snowflake programming error, but did not")
        except AssertionError as e:
            assert "failed assertions" in str(e)

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    TestPublishFlow()


@pytest.mark.slow
def test_publish_flow():
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
