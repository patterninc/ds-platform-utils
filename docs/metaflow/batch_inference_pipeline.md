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
        self.next(self.query_and_batch)

    @step
    def query_and_batch(self):
        self.pipeline = BatchInferencePipeline()

        # Query can be inline SQL or a file path.
        # {schema} is provided by ds_platform_utils (DEV/PROD selection).
        self.worker_ids = self.pipeline.query_and_batch(
            input_query="""
                SELECT
                    id,
                    feature_1,
                    feature_2
                FROM {{schema}}.model_features
                WHERE ds = '2026-02-26'
            """,
            parallel_workers=8,
            warehouse="MED",
            use_utc=True,
        )

        self.next(self.process_batch, foreach="worker_ids")

    @step
    def process_batch(self):
        # In a foreach step, self.input contains one worker_id.
        self.pipeline.process_batch(
            worker_id=self.input,
            predict_fn=predict_fn,
            batch_size_in_mb=256,
            timeout_per_batch=300,
        )
        self.next(self.publish_results)

    @step
    def publish_results(self, inputs):
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
            warehouse="MED",
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


@step
def batch_inference_step(self):
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
			FROM {{schema}}.model_features
			WHERE ds = '2026-02-26'
		""",
		output_table_name="MODEL_PREDICTIONS_DAILY",
		predict_fn=predict_fn,
		output_table_definition=[("id", "NUMBER"), ("score", "FLOAT")],
		warehouse="XL",
	)

	self.next(self.end)
```

## Parameters

### `query_and_batch(...)`

| Parameter          | Type          | Required | Description                                                                                                             |
| ------------------ | ------------- | -------: | ----------------------------------------------------------------------------------------------------------------------- |
| `input_query`      | `str \| Path` |      Yes | SQL query string or SQL file path used to fetch source rows. `{schema}` placeholder is resolved by `ds_platform_utils`. |
| `ctx`              | `dict`        |       No | Optional substitution map for templated SQL; merged with the internal `{"schema": ...}` mapping before query execution. |
| `warehouse`        | `str`         |       No | Snowflake warehouse used to execute the source query/export.                                                            |
| `use_utc`          | `bool`        |       No | If `True`, uses UTC timestamps/paths for partitioning and run metadata.                                                 |
| `parallel_workers` | `int`         |       No | Number of worker partitions to create for downstream processing.                                                        |

**Returns:** `list[int]` of `worker_id` values for Metaflow `foreach`.

---

### `process_batch(...)`

| Parameter           | Type                                     | Required | Description                                                                                              |
| ------------------- | ---------------------------------------- | -------: | -------------------------------------------------------------------------------------------------------- |
| `worker_id`         | `int`                                    |      Yes | Worker partition identifier generated by `query_and_batch()`.                                            |
| `predict_fn`        | `Callable[[pd.DataFrame], pd.DataFrame]` |      Yes | Inference function applied to each input chunk. Must return a DataFrame matching expected output schema. |
| `batch_size_in_mb`  | `int`                                    |       No | Target chunk size for reading/processing batch files.                                                    |
| `timeout_per_batch` | `int`                                    |       No | Processing time for each batch in seconds. (Used for Queuing operations)                                 |

**Returns:** `None`

**Recommended**: Tune `batch_size_in_mb` for Outerbounds Small tasks (3 CPU, 15 GB memory), which are about 6x more cost-effective than Medium tasks.

## Limitations

- The pipeline uses Snowflake ↔ S3 stage copy operations, so some column data types may be inferred differently than expected.
- For predictable output types, provide an explicit `output_table_definition` in `publish_results(...)` / `run(...)` and cast columns in `predict_fn` as needed.

### `publish_results(...)`

| Parameter                 | Type                            | Required | Description                                                       |
| ------------------------- | ------------------------------- | -------: | ----------------------------------------------------------------- |
| `output_table_name`       | `str`                           |      Yes | Destination Snowflake table for predictions.                      |
| `output_table_definition` | `list[tuple[str, str]] \| None` |       No | Optional output schema as `(column_name, snowflake_type)` tuples. |
| `auto_create_table`       | `bool`                          |       No | If `True`, creates destination table when missing.                |
| `overwrite`               | `bool`                          |       No | If `True`, replaces existing table data before loading results.   |
| `warehouse`               | `str`                           |       No | Snowflake warehouse used for load/publish operations.             |
| `use_utc`                 | `bool`                          |       No | If `True`, uses UTC for load metadata/time handling.              |

**Returns:** `None`

---

### `run(...)` (convenience method)

Runs `query_and_batch()` → `process_batch()` → `publish_results()` in a single sequential call.

| Parameter                 | Type                                     | Required | Description                                                                                                             |
| ------------------------- | ---------------------------------------- | -------: | ----------------------------------------------------------------------------------------------------------------------- |
| `input_query`             | `str \| Path`                            |      Yes | SQL query string or SQL file path used to fetch source rows. `{schema}` placeholder is resolved by `ds_platform_utils`. |
| `output_table_name`       | `str`                                    |      Yes | Destination Snowflake table for predictions.                                                                            |
| `predict_fn`              | `Callable[[pd.DataFrame], pd.DataFrame]` |      Yes | Inference function applied to each input chunk. Must return a DataFrame matching expected output schema.                |
| `ctx`                     | `dict`                                   |       No | Optional substitution map for templated SQL; merged with the internal `{"schema": ...}` mapping before query execution. |
| `output_table_definition` | `list[tuple[str, str]] \| None`          |       No | Optional output schema as `(column_name, snowflake_type)` tuples.                                                       |
| `batch_size_in_mb`        | `int`                                    |       No | Target chunk size for reading/processing batch files.                                                                   |
| `timeout_per_batch`       | `int`                                    |       No | Processing time for each batch in seconds. (Used for Queuing operations)                                                |
| `auto_create_table`       | `bool`                                   |       No | If `True`, creates destination table when missing.                                                                      |
| `overwrite`               | `bool`                                   |       No | If `True`, replaces existing table data before loading results.                                                         |
| `warehouse`               | `str`                                    |       No | Snowflake warehouse used for load/publish operations.                                                                   |
| `use_utc`                 | `bool`                                   |       No | If `True`, uses UTC for load metadata/time handling.                                                                    |

**Returns:** `None`

**Recommended**: Tune `batch_size_in_mb` for Outerbounds Small tasks (3 CPU, 15 GB memory), which are about 6x more cost-effective than Medium tasks.