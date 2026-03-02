"""BatchInferencePipeline: A class for orchestrating batch inference across Metaflow steps."""

import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

import pandas as pd
from metaflow import current

from ds_platform_utils.metaflow import s3
from ds_platform_utils.metaflow._consts import (
    DEV_SCHEMA,
    PROD_SCHEMA,
)
from ds_platform_utils.metaflow.s3_stage import _copy_s3_to_snowflake, _copy_snowflake_to_s3, _generate_s3_stage_paths
from ds_platform_utils.sql_utils import get_query_from_string_or_fpath, substitute_map_into_string


def _debug(*args, **kwargs):
    if os.getenv("DEBUG"):
        print("DEBUG: ", end="")
        print(*args, **kwargs)


@contextmanager
def _timer(message: str):
    """Context manager to time and debug-print operation duration."""
    t0 = time.time()
    yield
    t1 = time.time()
    _debug(f"{message}: Completed in {t1 - t0:.2f} seconds")


class BatchInferencePipeline:
    """Orchestrates batch inference across Metaflow steps with foreach parallelization.

    This class manages a 3-step pipeline:
    1. `query_and_batch()` - Export data from Snowflake to S3, returns worker_ids for foreach
    2. `process_batch()` - Run inference on a single batch (called in foreach step)
    3. `publish_results()` - Write all results back to Snowflake

    Example Usage::

        from metaflow import FlowSpec, step
        from ds_platform_utils.metaflow import BatchInferencePipeline

        class MyPredictionFlow(FlowSpec):

            @step
            def start(self):
                # Initialize pipeline and export data to S3
                self.pipeline = BatchInferencePipeline()
                self.worker_ids = self.pipeline.query_and_batch(
                    input_query="SELECT * FROM my_table",
                    parallel_workers=4,
                )
                self.next(self.predict, foreach='worker_ids')

            @step
            def predict(self):
                # Process single batch (runs in parallel via foreach)
                worker_id = self.input
                self.pipeline.process_batch(
                    worker_id=worker_id,
                    predict_fn=my_model.predict,
                )
                self.next(self.join)

            @step
            def join(self, inputs):
                # Merge and write results to Snowflake
                self.pipeline = inputs[0].pipeline  # Get pipeline from any input
                self.pipeline.publish_results(
                    output_table_name="predictions_table",
                )
                self.next(self.end)

            @step
            def end(self):
                print("Done!")

    Attributes:
        pipeline_id: Unique identifier for this pipeline (for multiple pipelines in same flow)
        warehouse: Snowflake warehouse to use
        use_utc: Whether to use UTC timezone for Snowflake
        worker_ids: List of worker IDs after query_and_batch() is called
        workers: Mapping of worker_id -> list of S3 file paths

    """

    def __init__(self):
        """Initialize S3 paths based on Metaflow context."""
        is_production = current.is_production if hasattr(current, "is_production") else False
        self._schema = PROD_SCHEMA if is_production else DEV_SCHEMA

        self._input_s3_path, _ = _generate_s3_stage_paths()
        self._output_s3_path, _ = _generate_s3_stage_paths()

        # Execution state flags
        self._query_executed = False
        self._batch_processed = False
        self._results_published = False

    def _split_files_into_workers(self, files: List[str], parallel_workers: int) -> dict[int, List[str]]:
        """Split list of files into batches for each worker."""
        if len(files) < parallel_workers:
            print("⚠️ Fewer files than workers. Assigning one file per worker until files run out.")
            parallel_workers = len(files)

        return {worker_id + 1: files[worker_id::parallel_workers] for worker_id in range(parallel_workers)}

    def query_and_batch(
        self,
        input_query: Union[str, Path],
        ctx: Optional[dict] = None,
        warehouse: Optional[str] = None,
        use_utc: bool = True,
        parallel_workers: int = 1,
    ) -> List[int]:
        """Step 1: Export data from Snowflake to S3 and create batches for parallel processing.

        Args:
            input_query: SQL query string or file path to query
            ctx: Dict of variable substitutions for SQL template
            warehouse: Snowflake warehouse to use
            use_utc: Whether to use UTC timezone for Snowflake
            parallel_workers: Number of parallel workers to use for processing

        Returns:
            List of worker_ids to use with foreach in next step

        """
        # Warn if re-executing query_and_batch after processing
        if self._query_executed or self._batch_processed:
            raise RuntimeError(
                "Cannot re-execute query_and_batch(): Batches have already been processed. "
                "This would reset the state of the pipeline. "
                "If you need to re-run the query, create a new instance of BatchInferencePipeline."
            )

        print("🚀 Starting batch inference pipeline...")
        # Process input query
        input_query = get_query_from_string_or_fpath(input_query)
        input_query = substitute_map_into_string(input_query, (ctx or {}) | {"schema": self._schema})

        _debug(f"⏳ Exporting data from Snowflake to S3 to {self._input_s3_path}...")
        # Export from Snowflake to S3
        _copy_snowflake_to_s3(
            query=input_query,
            warehouse=warehouse,
            use_utc=use_utc,
            s3_path=self._input_s3_path,
        )
        input_files = s3._list_files_in_s3_folder(self._input_s3_path)
        _debug(f"✅ Exported data to S3: {len(input_files)} files created.")

        # Create worker batches based on file sizes
        self.worker_files = self._split_files_into_workers(input_files, parallel_workers)
        self.worker_ids = list(self.worker_files.keys())

        # Mark query as executed
        self._query_executed = True

        print(f"📊 Created {len(self.worker_ids)} workers for parallel processing")
        return self.worker_ids

    def process_batch(
        self,
        worker_id: int,
        predict_fn: Callable[[pd.DataFrame], pd.DataFrame],
        batch_size_in_mb: int = 128,
        timeout_per_batch: int = 300,
    ) -> None:
        """Step 2: Process a single batch using parallel download→inference→upload pipeline.

        Uses a queue-based 3-thread pipeline for efficient processing:
        - Download worker: Reads files from S3
        - Inference worker: Runs predict_fn on each downloaded file
        - Upload worker: Writes predictions back to S3

        Args:
            worker_id: The worker ID to process (from self.input in foreach)
            predict_fn: Function that takes DataFrame and returns predictions DataFrame
            batch_size_in_mb: Target size for each batch in MB
            timeout_per_batch: Timeout in seconds for each batch operation (default: 300)

        Returns:
            None

        """
        # Validate that query_and_batch was called first
        if not self._query_executed:
            raise RuntimeError(
                "Cannot process batch: query_and_batch() must be called first. "
                "Call query_and_batch() to export data from Snowflake before processing batches."
            )
        if self._batch_processed:
            raise RuntimeError(
                "Cannot process batch: A batch has already been processed. "
                "This would reset the state of the pipeline. "
                "If you need to re-run the batch processing, create a new instance of BatchInferencePipeline."
            )

        if worker_id not in self.worker_files:
            raise ValueError(f"Worker {worker_id} not found.")

        file_paths = self.worker_files[worker_id]
        file_batches = self._make_batches(file_paths, batch_size_in_mb=batch_size_in_mb)
        print(f"🔄 Processing worker {worker_id} ({len(file_batches)} batches)")

        download_queue: queue.Queue = queue.Queue(maxsize=1)
        inference_queue: queue.Queue = queue.Queue(maxsize=1)
        output_path = self._output_s3_path

        def download_worker(file_batches: List[List[str]]):
            for file_id, file_batch in enumerate(file_batches, 1):
                with _timer(f"📥 Downloaded file {file_id} from S3"):
                    df = s3._get_df_from_s3_files(file_batch)
                    df.columns = [col.lower() for col in df.columns]
                download_queue.put((file_id, df), timeout=timeout_per_batch)
            download_queue.put(None, timeout=timeout_per_batch)

        def inference_worker():
            while True:
                item = download_queue.get(timeout=timeout_per_batch)
                if item is None:
                    inference_queue.put(None, timeout=timeout_per_batch)
                    break
                file_id, df = item
                with _timer(f"🔹 Generated predictions for file {file_id}"):
                    predictions_df = predict_fn(df)
                inference_queue.put((file_id, predictions_df), timeout=timeout_per_batch)
                print(f"🔘 Inference completed for batch {file_id} of {len(file_batches)}")

        def upload_worker():
            while True:
                item = inference_queue.get(timeout=timeout_per_batch)
                if item is None:
                    break
                file_id, predictions_df = item
                s3_output_file = f"{output_path}/predictions_{worker_id}_{file_id}.parquet"
                with _timer(f"📤 Uploaded predictions for file {file_id} to S3"):
                    s3._put_df_to_s3_file(predictions_df, s3_output_file)

        t0 = time.time()
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(download_worker, file_batches),
                executor.submit(inference_worker),
                executor.submit(upload_worker),
            ]
            # Wait for all futures and propagate any exceptions
            for future in futures:
                future.result()  # Raises exception if worker failed
        t1 = time.time()

        # Mark that at least one batch was processed
        self._batch_processed = True

        print(f"✅ Worker {worker_id} complete ({len(file_batches)} batches processed in {t1 - t0:.2f}s)")

    def publish_results(  # noqa: PLR0913
        self,
        output_table_name: str,
        output_table_definition: Optional[List[Tuple[str, str]]] = None,
        auto_create_table: bool = False,
        overwrite: bool = False,
        warehouse: Optional[str] = None,
        use_utc: bool = True,
    ) -> None:
        """Step 3: Write all predictions from S3 to Snowflake (call this in join step).

        Args:
            output_table_name: Name of the Snowflake table
            output_table_definition: Optional schema as list of (column, type) tuples
            auto_create_table: Whether to auto-create table if not exists
            overwrite: Whether to overwrite existing data
            warehouse: Snowflake warehouse to use
            use_utc: Whether to use UTC timezone for Snowflake

        """
        # Validate that batches were processed
        if not self._query_executed:
            raise RuntimeError("Cannot publish results: query_and_batch() and process_batch() must be called first. ")

        if not self._batch_processed:
            raise RuntimeError(
                "Cannot publish results: No batches have been processed. "
                "Call process_batch() to process at least one batch before publishing."
            )

        if self._results_published:
            print("⚠️ Warning: Results have already been published. Publishing again may cause duplicate data.")

        print(f"📤 Writing predictions to Snowflake table: {output_table_name}")

        _copy_s3_to_snowflake(
            s3_path=self._output_s3_path,
            table_name=output_table_name,
            table_definition=output_table_definition,
            warehouse=warehouse,
            use_utc=use_utc,
            auto_create_table=auto_create_table,
            overwrite=overwrite,
        )

        # Mark results as published
        self._results_published = True

    def run(  # noqa: PLR0913
        self,
        input_query: Union[str, Path],
        output_table_name: str,
        predict_fn: Callable[[pd.DataFrame], pd.DataFrame],
        ctx: Optional[dict] = None,
        output_table_definition: Optional[List[Tuple[str, str]]] = None,
        batch_size_in_mb: int = 128,
        timeout_per_batch: int = 300,
        auto_create_table: bool = True,
        overwrite: bool = True,
        warehouse: Optional[str] = None,
        use_utc: bool = True,
    ) -> None:
        """Run the complete pipeline: query → process → publish in a single call.

        This is a convenience method that combines query_and_batch(), process_batch(),
        and publish_results() for cases where foreach parallelization is not needed.

        Args:
            input_query: SQL query string or file path to query
            output_table_name: Name of the Snowflake table for predictions
            predict_fn: Function that takes DataFrame and returns predictions DataFrame
            ctx: Dict of variable substitutions for SQL template
            output_table_definition: Optional schema as list of (column, type) tuples
            batch_size_in_mb: Target size for each batch in MB
            timeout_per_batch: Timeout in seconds for each batch operation
            auto_create_table: Whether to auto-create table if not exists
            overwrite: Whether to overwrite existing data
            warehouse: Snowflake warehouse to use
            use_utc: Whether to use UTC timezone for Snowflake

        Example::

            pipeline = BatchInferencePipeline()
            pipeline.run(
                input_query="SELECT * FROM my_table",
                output_table_name="predictions_table",
                predict_fn=my_model.predict,
            )

        """
        # Step 1: Query and batch
        worker_ids = self.query_and_batch(
            input_query=input_query,
            ctx=ctx,
            parallel_workers=1,
            use_utc=use_utc,
            warehouse=warehouse,
        )

        self.process_batch(
            worker_id=worker_ids[0],
            predict_fn=predict_fn,
            batch_size_in_mb=batch_size_in_mb,
            timeout_per_batch=timeout_per_batch,
        )

        # Step 3: Publish results
        self.publish_results(
            output_table_name=output_table_name,
            output_table_definition=output_table_definition,
            auto_create_table=auto_create_table,
            overwrite=overwrite,
            warehouse=warehouse,
            use_utc=use_utc,
        )

    def _make_batches(self, file_paths: List[str], batch_size_in_mb: int) -> List[List[str]]:
        with s3._get_metaflow_s3_client() as s3_client:
            file_sizes = [(file.key, file.size) for file in s3_client.info_many(file_paths)]

        batches = []
        current_batch = []
        current_batch_size = 0
        warnings = False

        batch_size_in_bytes = batch_size_in_mb * 1024 * 1024
        for file_key, file_size in file_sizes:
            current_batch.append(file_key)
            current_batch_size += file_size
            if current_batch_size > batch_size_in_bytes:
                if len(current_batch) == 1:
                    warnings = True
                batches.append(current_batch)
                current_batch = []
                current_batch_size = 0

        if current_batch:
            batches.append(current_batch)
        if warnings:
            print("⚠️ Files larger than batch size detected. Increase batch size to avoid this warning.")

        return batches

    def __repr__(self) -> str:
        """Return string representation of the pipeline."""
        worker_ids = getattr(self, "worker_ids", [])
        worker_count = len(getattr(self, "worker_files", {}))
        return f"BatchInferencePipeline(worker_count={worker_count}, worker_ids={worker_ids})"
