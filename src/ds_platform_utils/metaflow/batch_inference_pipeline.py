"""BatchInferencePipeline: A class for orchestrating batch inference across Metaflow steps."""

import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

import pandas as pd
from metaflow import current

from ds_platform_utils._snowflake.write_audit_publish import get_query_from_string_or_fpath, substitute_map_into_string
from ds_platform_utils.metaflow import s3
from ds_platform_utils.metaflow._consts import (
    DEV_SCHEMA,
    PROD_SCHEMA,
    S3_DATA_FOLDER,
)
from ds_platform_utils.metaflow.get_snowflake_connection import _debug_print_query
from ds_platform_utils.metaflow.s3_stage import (
    _get_s3_config,
    copy_s3_to_snowflake,
    copy_snowflake_to_s3,
)


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


@dataclass
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
                self.pipeline = BatchInferencePipeline(pipeline_id="my_model")
                self.worker_ids = self.pipeline.query_and_batch(
                    input_query="SELECT * FROM my_table",
                    batch_size_in_mb=128,
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

    pipeline_id: str = "default"
    warehouse: Optional[str] = None
    use_utc: bool = True

    # Internal state (populated after prepare())
    _s3_bucket: str = field(default="", repr=False)
    _base_path: str = field(default="", repr=False)
    _input_path: str = field(default="", repr=False)
    _output_path: str = field(default="", repr=False)
    _schema: str = field(default="", repr=False)
    workers: dict = field(default_factory=dict, repr=False)
    worker_ids: List[int] = field(default_factory=list)

    def __post_init__(self):
        """Initialize S3 paths based on Metaflow context."""
        is_production = current.is_production if hasattr(current, "is_production") else False
        self._s3_bucket, _ = _get_s3_config(is_production)
        self._schema = PROD_SCHEMA if is_production else DEV_SCHEMA

        # Build paths: s3://bucket/data/{flow}/{run_id}/{pipeline_id}/
        flow_name = current.flow_name if hasattr(current, "flow_name") else "local"
        run_id = current.run_id if hasattr(current, "run_id") else "dev"

        self._base_path = f"{self._s3_bucket}/{S3_DATA_FOLDER}/{flow_name}/{run_id}/{self.pipeline_id}"
        self._input_path = f"{self._base_path}/input"
        self._output_path = f"{self._base_path}/output"

    @property
    def input_path(self) -> str:
        """S3 path where input data is stored."""
        return self._input_path

    @property
    def output_path(self) -> str:
        """S3 path where output predictions are stored."""
        return self._output_path

    def query_and_batch(
        self,
        input_query: Union[str, Path],
        batch_size_in_mb: int = 128,
        query_variables: Optional[dict] = None,
        parallel_workers: int = 1,
    ) -> List[int]:
        """Step 1: Export data from Snowflake to S3 and create batches for parallel processing.

        Args:
            input_query: SQL query string or file path to query
            batch_size_in_mb: Target size for each batch in MB
            query_variables: Dict of variable substitutions for SQL template
            parallel_workers: Number of parallel workers to use for processing

        Returns:
            List of worker_ids to use with foreach in next step

        """
        print(f"🚀 Preparing batch inference pipeline: {self.pipeline_id}")

        # Process input query
        input_query = get_query_from_string_or_fpath(input_query)
        input_query = substitute_map_into_string(input_query, {"schema": self._schema} | (query_variables or {}))
        _debug_print_query(input_query)

        # Export from Snowflake to S3
        t0 = time.time()
        input_files = copy_snowflake_to_s3(
            query=input_query,
            warehouse=self.warehouse,
            use_utc=self.use_utc,
        )
        t1 = time.time()
        print(f"✅ Exported {len(input_files)} files to S3 in {t1 - t0:.2f}s")

        # Create worker batches based on file sizes
        self.workers = self._make_batches(input_files, batch_size_in_mb)
        self.worker_ids = list(self.workers.keys())

        print(f"📊 Created {len(self.worker_ids)} workers for parallel processing")
        return self.worker_ids

    def process_batch(
        self,
        worker_id: int,
        predict_fn: Callable[[pd.DataFrame], pd.DataFrame],
        timeout_per_file: int = 300,
    ) -> str:
        """Step 2: Process a single batch using parallel download→inference→upload pipeline.

        Uses a queue-based 3-thread pipeline for efficient processing:
        - Download worker: Reads files from S3
        - Inference worker: Runs predict_fn on each downloaded file
        - Upload worker: Writes predictions back to S3

        Args:
            worker_id: The worker ID to process (from self.input in foreach)
            predict_fn: Function that takes DataFrame and returns predictions DataFrame
            timeout_per_file: Timeout in seconds for each file operation (default: 300)

        Returns:
            S3 path where predictions were written

        """
        if worker_id not in self.workers:
            raise ValueError(f"Worker {worker_id} not found. Available: {list(self.workers.keys())}")

        file_paths = self.workers[worker_id]
        print(f"🔄 Processing worker {worker_id} ({len(file_paths)} files)")

        download_queue: queue.Queue = queue.Queue(maxsize=1)
        inference_queue: queue.Queue = queue.Queue(maxsize=1)
        output_path = self._output_path

        def download_worker(files: List[str]):
            for file_id, file_path in enumerate(files):
                with _timer(f"Downloading file {file_id} from S3"):
                    df = s3._get_df_from_s3_files([file_path])
                    df.columns = [col.lower() for col in df.columns]
                download_queue.put((file_id, df), timeout=timeout_per_file)
            download_queue.put(None, timeout=timeout_per_file)

        def inference_worker():
            while True:
                item = download_queue.get(timeout=timeout_per_file)
                if item is None:
                    inference_queue.put(None, timeout=timeout_per_file)
                    break
                file_id, df = item
                with _timer(f"Generating predictions for file {file_id}"):
                    predictions_df = predict_fn(df)
                inference_queue.put((file_id, predictions_df), timeout=timeout_per_file)

        def upload_worker():
            while True:
                item = inference_queue.get(timeout=timeout_per_file)
                if item is None:
                    break
                file_id, predictions_df = item
                s3_output_file = f"{output_path}/predictions_{worker_id}_{file_id}.parquet"
                with _timer(f"Uploading predictions for file {file_id} to S3"):
                    s3._put_df_to_s3_file(predictions_df, s3_output_file)

        t0 = time.time()
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(download_worker, file_paths)
            executor.submit(inference_worker)
            executor.submit(upload_worker)
        t1 = time.time()

        print(f"✅ Worker {worker_id} complete ({len(file_paths)} files processed in {t1 - t0:.2f}s)")
        return self._output_path

    def publish_results(
        self,
        output_table_name: str,
        output_table_definition: Optional[List[Tuple[str, str]]] = None,
        auto_create_table: bool = True,
        overwrite: bool = True,
    ) -> None:
        """Step 3: Write all predictions from S3 to Snowflake (call this in join step).

        Args:
            output_table_name: Name of the Snowflake table
            output_table_definition: Optional schema as list of (column, type) tuples
            auto_create_table: Whether to auto-create table if not exists
            overwrite: Whether to overwrite existing data

        """
        print(f"📤 Writing predictions to Snowflake table: {output_table_name}")

        t0 = time.time()
        copy_s3_to_snowflake(
            s3_path=self._output_path,
            table_name=output_table_name,
            table_defination=output_table_definition,
            warehouse=self.warehouse,
            use_utc=self.use_utc,
            auto_create_table=auto_create_table,
            overwrite=overwrite,
        )
        t1 = time.time()

        print(f"✅ Pipeline complete! Data written to {output_table_name} in {t1 - t0:.2f}s")

    def _make_batches(self, file_paths: List[str], batch_size_in_mb: int) -> dict:
        """Group files into batches based on size."""
        with s3._get_metaflow_s3_client() as s3_client:
            file_infos = [(f.key, f.size) for f in s3_client.info_many(file_paths)]

        batches = {}
        current_batch = []
        current_size = 0
        batch_id = 0
        batch_size_bytes = batch_size_in_mb * 1024 * 1024

        for file_path, file_size in file_infos:
            # Check if adding this file exceeds limit
            if current_batch and (current_size + file_size) > batch_size_bytes:
                batches[batch_id] = current_batch
                batch_id += 1
                current_batch = []
                current_size = 0

            current_batch.append(file_path)
            current_size += file_size

        # Don't forget last batch
        if current_batch:
            batches[batch_id] = current_batch

        return batches

    def __repr__(self) -> str:
        """Return string representation of the pipeline."""
        return (
            f"BatchInferencePipeline(pipeline_id='{self.pipeline_id}', "
            f"workers={len(self.workers)}, worker_ids={self.worker_ids})"
        )
