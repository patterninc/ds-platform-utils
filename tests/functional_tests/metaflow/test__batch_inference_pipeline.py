"""A Metaflow flow."""

import os
import subprocess
import sys

import pytest
from metaflow import FlowSpec, project, step

from ds_platform_utils.metaflow import BatchInferencePipeline
from ds_platform_utils.metaflow.pandas import query_pandas_from_snowflake


@project(name="ds_platform_utils_tests")
class TestBatchInferencePipeline(FlowSpec):
    """A sample flow."""

    @step
    def start(self):
        """Start the flow."""
        self.next(self.query_and_batch)

    @step
    def query_and_batch(self):
        """Run the query and batch step."""
        os.environ["DEBUG_QUERY"] = "1"
        self.n = 10000000
        query = f"SELECT UNIFORM(0::FLOAT, 10::FLOAT, RANDOM()) F1 , UNIFORM(0::INT, 1000::INT, RANDOM()) F2 FROM TABLE(GENERATOR(ROWCOUNT => {self.n}));"
        self.pipeline = BatchInferencePipeline()
        self.worker_ids = self.pipeline.query_and_batch(
            input_query=query,
            ctx={"extra": "value"},
            warehouse="XS",
            parallel_workers=2,
        )

        self.next(self.process_batch, foreach="worker_ids")

    @step
    def process_batch(self):
        """Process the batch for each worker."""
        worker_id = self.input

        def predict_fn(df):
            return df  # Identity function for testing

        self.pipeline.process_batch(worker_id=worker_id, predict_fn=predict_fn, batch_size_in_mb=30)
        print(f"Processing batch for worker {worker_id}...")
        self.next(self.publish_results)

    @step
    def publish_results(self, inputs):
        """Join the parallel branches."""
        os.environ["DEBUG_QUERY"] = "1"

        print("Joining results from all workers...")
        inputs[0].pipeline.publish_results(
            output_table_name="DS_PLATFORM_UTILS_TEST_BATCH_INFERENCE_OUTPUT",
            overwrite=True,
            auto_create_table=True,
        )

        df = query_pandas_from_snowflake(
            query="SELECT * FROM DS_PLATFORM_UTILS_TEST_BATCH_INFERENCE_OUTPUT", warehouse="XS"
        )
        if len(df) != inputs[0].n:
            raise ValueError(f"Output row count {len(df)} does not match input row count {inputs[0].n}")
        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    TestBatchInferencePipeline()


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
