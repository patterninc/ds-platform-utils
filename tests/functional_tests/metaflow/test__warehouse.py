"""A Metaflow flow."""

import subprocess
import sys

import pytest
from metaflow import FlowSpec, project, step

from ds_platform_utils.metaflow import query_pandas_from_snowflake


@project(name="test_warehouse_flow")
class TestWarehouseFlow(FlowSpec):
    """A sample flow."""

    @step
    def start(self):
        """Start the flow."""
        self.next(self.test_publish_with_warehouse)

    @step
    def test_publish_with_warehouse(self):
        """Test the publish function with warehouse parameter."""
        # Publish a simple query to Snowflake with a specific warehouse
        warehouse_map = [
            {
                "size": None,
                "domain": "content",
                "warehouse": "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
            },
            {
                "size": "XS",
                "domain": "content",
                "warehouse": "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
            },
            {
                "size": "MED",
                "domain": "advertising",
                "warehouse": "OUTERBOUNDS_DATA_SCIENCE_ADS_DEV_MED_WH",
            },
            {
                "size": "XL",
                "domain": "reference",
                "warehouse": "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
            },
            {
                "size": "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
                "domain": "content",
                "warehouse": "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
            },
        ]

        for item in warehouse_map:
            from metaflow import current

            current.tags.add(f"ds.domain:{item['domain']}")
            df_warehouse = query_pandas_from_snowflake(
                query="SELECT CURRENT_WAREHOUSE();",
                warehouse=item["warehouse"],
            )
            current.tags.pop()  # Clean up tag after query
            df_warehouse = df_warehouse.iloc[0, 0]
            assert df_warehouse == item["warehouse"], f"Expected warehouse {item['warehouse']}, got {df_warehouse}"

            print(f"Successfully queried warehouse: {df_warehouse}")

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    TestWarehouseFlow()


@pytest.mark.slow
def test_warehouse_flow():
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
