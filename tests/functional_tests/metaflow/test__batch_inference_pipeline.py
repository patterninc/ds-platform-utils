"""A Metaflow flow."""

import os
import subprocess
import sys

import pytest
from metaflow import FlowSpec, project, step

from ds_platform_utils.metaflow import BatchInferencePipeline


@project(name="test_batch_inference_pipeline")
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
        n = 10000000
        query = f"SELECT UNIFORM(0::FLOAT, 10::FLOAT, RANDOM()) , UNIFORM(0::INT, 1000::INT, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT => {n}));"
        self.pipeline = BatchInferencePipeline()
        self.worker_ids = self.pipeline.query_and_batch(
            input_query=query,
            ctx={"extra": "value"},
            warehouse="XS",
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
