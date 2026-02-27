# `BatchInferencePipeline`

Source: `ds_platform_utils.metaflow.batch_inference_pipeline.BatchInferencePipeline`

Utility class to orchestrate batch inference with Snowflake + S3 in Metaflow steps.

## Main methods

- `query_and_batch(...)`: export source data to S3 and create worker batches.
- `process_batch(...)`: run download → inference → upload for one worker.
- `publish_results(...)`: copy prediction outputs from S3 to Snowflake.
- `run(...)`: convenience method to execute full flow sequentially.

## Detailed example (Metaflow foreach)

This example shows the intended 3-step pattern in a Metaflow `FlowSpec`:

1. `query_and_batch()` in `start`
2. `process_batch()` in `foreach`
3. `publish_results()` in `join`

```python
from metaflow import FlowSpec, step
import pandas as pd

from ds_platform_utils.metaflow import BatchInferencePipeline


def predict_fn(df: pd.DataFrame) -> pd.DataFrame:
	# Example model logic
	out = pd.DataFrame()
	out["id"] = df["id"]
	out["score"] = (df["feature_1"].fillna(0) * 0.7 + df["feature_2"].fillna(0) * 0.3).round(6)
	out["label"] = (out["score"] >= 0.5).astype(int)
	return out


class BatchPredictFlow(FlowSpec):
	@step
	def start(self):
		self.pipeline = BatchInferencePipeline()

		# Query can be inline SQL or a file path.
		# {schema} is provided by ds_platform_utils (DEV/PROD selection).
		self.worker_ids = self.pipeline.query_and_batch(
			input_query="""
				SELECT
					id,
					feature_1,
					feature_2
				FROM {schema}.model_features
				WHERE ds = '2026-02-26'
			""",
			parallel_workers=8,
			warehouse="ANALYTICS_WH",
			use_utc=True,
		)

		self.next(self.predict, foreach="worker_ids")

	@step
	def predict(self):
		# In a foreach step, self.input contains one worker_id.
		self.pipeline.process_batch(
			worker_id=self.input,
			predict_fn=predict_fn,
			batch_size_in_mb=256,
			timeout_per_batch=600,
		)
		self.next(self.join)

	@step
	def join(self, inputs):
		# Reuse one pipeline object from foreach branches.
		self.pipeline = inputs[0].pipeline

		self.pipeline.publish_results(
			output_table_name="MODEL_PREDICTIONS_DAILY",
			output_table_definition=[
				("id", "NUMBER"),
				("score", "FLOAT"),
				("label", "NUMBER"),
			],
			auto_create_table=True,
			overwrite=True,
			warehouse="ANALYTICS_WH",
			use_utc=True,
		)
		self.next(self.end)

	@step
	def end(self):
		print("Batch inference complete")
```

## Detailed example (single-step convenience)

Use `run()` when you do not need Metaflow foreach parallelization:

```python
from ds_platform_utils.metaflow import BatchInferencePipeline
import pandas as pd


def predict_fn(df: pd.DataFrame) -> pd.DataFrame:
	return pd.DataFrame(
		{
			"id": df["id"],
			"score": (df["feature_1"] * 0.9).fillna(0),
		}
	)

pipeline = BatchInferencePipeline()
pipeline.run(
	input_query="""
		SELECT id, feature_1
		FROM {schema}.model_features
		WHERE ds = '2026-02-26'
	""",
	output_table_name="MODEL_PREDICTIONS_DAILY",
	predict_fn=predict_fn,
	output_table_definition=[("id", "NUMBER"), ("score", "FLOAT")],
	warehouse="ANALYTICS_WH",
)
```

## Notes

- `input_query` accepts either raw SQL text or a file path.
- SQL templates can use placeholders (for example `{schema}`) and be resolved before execution.
- Call order matters: `query_and_batch()` must run before `process_batch()`, and at least one `process_batch()` must run before `publish_results()`.
