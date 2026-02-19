"""A Metaflow flow for testing batch inference."""

import subprocess
import sys

import pandas as pd
import pytest
from metaflow import FlowSpec, project, step


@project(name="test_batch_inference_flow")
class TestBatchInferenceFlow(FlowSpec):
    """A test flow for batch inference functionality using foreach parallelization."""

    @step
    def start(self):
        """Start the flow."""
        self.next(self.setup_test_data)

    @step
    def setup_test_data(self):
        """Create and publish test data to Snowflake."""
        from ds_platform_utils.metaflow import publish_pandas

        # Create a sample DataFrame with features for batch inference
        data = {
            "id": list(range(1, 101)),
            "feature_1": [i * 1.5 for i in range(1, 101)],
            "feature_2": [i * 0.8 for i in range(1, 101)],
            "feature_3": [i % 10 for i in range(1, 101)],
        }
        df = pd.DataFrame(data)

        # Publish the DataFrame to Snowflake
        publish_pandas(
            table_name="batch_inference_test_input",
            df=df,
            auto_create_table=True,
            overwrite=True,
            use_s3_stage=True,
            table_definition=[
                ("id", "INTEGER"),
                ("feature_1", "FLOAT"),
                ("feature_2", "FLOAT"),
                ("feature_3", "INTEGER"),
            ],
        )

        self.next(self.query_and_batch)

    @step
    def query_and_batch(self):
        """Query data from Snowflake and create batches for parallel processing."""
        from ds_platform_utils.metaflow.batch_inference_pipeline import BatchInferencePipeline

        # Define the input query
        input_query = """
        SELECT 
            id,
            feature_1,
            feature_2,
            feature_3
        FROM PATTERN_DB.{{schema}}.BATCH_INFERENCE_TEST_INPUT
        ORDER BY id
        """

        # Initialize pipeline and export data to S3
        self.pipeline = BatchInferencePipeline()
        self.worker_ids = self.pipeline.query_and_batch(
            input_query=input_query,
            parallel_workers=2,
        )

        self.next(self.process_batch, foreach="worker_ids")

    @step
    def process_batch(self):
        """Process a single batch (runs in parallel via foreach)."""

        def simple_model_predictor(df: pd.DataFrame) -> pd.DataFrame:
            """A simple model that generates predictions."""
            df["prediction"] = df["feature_1"] * 2 + df["feature_2"] * 0.5 + df["feature_3"]
            df["confidence"] = 0.95
            return df

        worker_id: int = self.input  # type: ignore[assignment]
        self.pipeline.process_batch(
            worker_id=worker_id,
            predict_fn=simple_model_predictor,
            batch_size_in_mb=128,
        )

        self.next(self.join_results)

    @step
    def join_results(self, inputs):
        """Join results from parallel workers and publish to Snowflake."""
        # Get pipeline from any input (all have the same output path)
        self.pipeline = inputs[0].pipeline

        # Publish results to Snowflake
        self.pipeline.publish_results(
            output_table_name="batch_inference_test_output",
            output_table_definition=[
                ("id", "INTEGER"),
                ("feature_1", "FLOAT"),
                ("feature_2", "FLOAT"),
                ("feature_3", "INTEGER"),
                ("prediction", "FLOAT"),
                ("confidence", "FLOAT"),
            ],
        )

        self.next(self.validate_results)

    @step
    def validate_results(self):
        """Validate the batch inference results with strict checks."""
        from ds_platform_utils.metaflow import query_pandas_from_snowflake

        # Query the results from Snowflake
        query = "SELECT * FROM PATTERN_DB.{{schema}}.BATCH_INFERENCE_TEST_OUTPUT ORDER BY id;"
        result_df = query_pandas_from_snowflake(query, use_s3_stage=True)

        # Normalize column names to lowercase (Snowflake returns uppercase)
        result_df.columns = [col.lower() for col in result_df.columns]

        # === Schema validation ===
        expected_columns = {"id", "feature_1", "feature_2", "feature_3", "prediction", "confidence"}
        actual_columns = set(result_df.columns)
        assert actual_columns == expected_columns, (
            f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        )

        # === Row count validation ===
        assert len(result_df) == 100, f"Expected 100 rows, got {len(result_df)}"

        # === ID integrity checks ===
        expected_ids = set(range(1, 101))
        actual_ids = set(int(x) for x in result_df["id"])
        missing_ids = expected_ids - actual_ids
        extra_ids = actual_ids - expected_ids
        assert not missing_ids, f"Missing IDs: {missing_ids}"
        assert not extra_ids, f"Unexpected IDs: {extra_ids}"

        # Check for duplicate IDs
        duplicate_ids = result_df[result_df["id"].duplicated()]["id"].tolist()
        assert not duplicate_ids, f"Duplicate IDs found: {duplicate_ids}"

        # === Null checks ===
        for col in expected_columns:
            null_count = result_df[col].isnull().sum()
            assert null_count == 0, f"Column '{col}' has {null_count} null values"

        # === Convert types for numeric validation ===
        result_df["feature_1"] = result_df["feature_1"].astype(float)
        result_df["feature_2"] = result_df["feature_2"].astype(float)
        result_df["feature_3"] = result_df["feature_3"].astype(float)
        result_df["prediction"] = result_df["prediction"].astype(float)
        result_df["confidence"] = result_df["confidence"].astype(float)
        result_df["id"] = result_df["id"].astype(int)

        # === Validate ALL predictions ===
        result_df["expected_prediction"] = (
            result_df["feature_1"] * 2 + result_df["feature_2"] * 0.5 + result_df["feature_3"]
        )
        result_df["prediction_diff"] = abs(result_df["prediction"] - result_df["expected_prediction"])

        max_diff = result_df["prediction_diff"].max()
        assert max_diff < 0.01, (
            f"Prediction mismatch. Max diff: {max_diff}. "
            f"Failed rows: {result_df[result_df['prediction_diff'] >= 0.01]['id'].tolist()}"
        )

        # === Validate ALL confidence values ===
        invalid_confidence = result_df[abs(result_df["confidence"] - 0.95) >= 0.001]
        assert len(invalid_confidence) == 0, f"Invalid confidence values for IDs: {invalid_confidence['id'].tolist()}"

        # === Validate feature values match input ===
        for i in range(1, 101):
            row = result_df[result_df["id"] == i].iloc[0]
            expected_f1 = i * 1.5
            expected_f2 = i * 0.8
            expected_f3 = i % 10
            assert abs(row["feature_1"] - expected_f1) < 0.01, f"feature_1 mismatch for id={i}"
            assert abs(row["feature_2"] - expected_f2) < 0.01, f"feature_2 mismatch for id={i}"
            assert abs(row["feature_3"] - expected_f3) < 0.01, f"feature_3 mismatch for id={i}"

        print(f"✓ All {len(result_df)} rows validated successfully")
        print("✓ All IDs 1-100 present with no duplicates")
        print(f"✓ All predictions match expected values (max diff: {max_diff:.6f})")
        print("✓ All confidence values are 0.95")
        print(f"✓ Predictions range: {result_df['prediction'].min():.2f} to {result_df['prediction'].max():.2f}")

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    TestBatchInferenceFlow()


@pytest.mark.slow
def test_batch_inference_flow():
    """Test the batch inference flow."""
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

    if process.stdout:
        for line in iter(process.stdout.readline, ""):
            yield line
        process.stdout.close()

    return_code = process.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)
