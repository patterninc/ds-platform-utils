import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
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

default_file_size_in_mb = 10


def _debug(*args, **kwargs):
    if os.getenv("DEBUG"):
        print("DEBUG: ", end="")
        print(*args, **kwargs)


@contextmanager
def timer(message: str):
    t0 = time.time()
    yield
    t1 = time.time()
    _debug(f"{message}: Completed in {t1 - t0:.2f} seconds")


def make_batches_of_files(file_paths, batch_size_in_mb):
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


def batch_inference_from_snowflake(  # noqa: PLR0913
    input_query: Union[str, Path],
    output_table_name: str,
    predict_fn: Callable[[pd.DataFrame], pd.DataFrame],
    output_table_definition: Optional[List[Tuple[str, str]]] = None,
    use_utc: bool = True,
    batch_size_in_mb: int = 128,
    warehouse: Optional[str] = None,
    ctx: Optional[dict] = None,
    timeout_per_batch: int = 300,
    auto_create_table: bool = True,
    overwrite: bool = True,
):
    """Execute batch inference on data from Snowflake, process it through a model, and upload results back to Snowflake.

    This function orchestrates a multi-threaded pipeline that:
    1. Exports data from Snowflake to S3 using COPY INTO
    2. Downloads data from S3 in batches
    3. Runs model predictions on each batch
    4. Uploads predictions back to S3
    5. Imports results into a Snowflake table using COPY INTO

    Args:
        input_query (Union[str, Path]): SQL query string or file path to query that defines the data to process.
        output_table_name (str): Name of the Snowflake table where predictions will be written.
        model_predictor_function (Callable[[pd.DataFrame], pd.DataFrame]): Function that takes a DataFrame and returns predictions DataFrame.
        output_table_schema (Optional[List[Tuple[str, str]]], optional): Snowflake table schema as list of (column_name, column_type) tuples.
            If None, schema is inferred from the first predictions file. Defaults to None.
        use_utc (bool, optional): Whether to use UTC timezone for Snowflake connection. Defaults to True.
        batch_size_in_mb (int, optional): Target batch size in megabytes for processing. Defaults to 128.
        warehouse (Optional[str], optional): Snowflake warehouse to use for queries. If None, uses default warehouse. Defaults to None.
        ctx (Optional[dict], optional): Dictionary of variable substitutions for the input query template. Defaults to None.
        timeout_per_batch (int, optional): Timeout in seconds for processing each batch. Defaults to 300.
        auto_create_table (bool, optional): Whether to automatically create the output table if it doesn't exist. Defaults to True.
        overwrite (bool, optional): Whether to overwrite existing data in the output table. Defaults to True.

    Raises:
        Exceptions from Snowflake connection, S3 operations, or model prediction function may propagate.

    Notes:
        - Uses production or non-production schema based on execution context (Metaflow).
        - Creates temporary S3 and Snowflake stage folders with timestamps for isolation.
        - Implements a three-threaded pipeline: download -> inference -> upload.
        - Column names are normalized to lowercase for consistent processing.
        - Displays input query, sample data, and progress messages via Metaflow cards.

    """
    ## Define S3 paths and Snowflake schema based on environment
    is_production = current.is_production if hasattr(current, "is_production") else False
    s3_bucket, _ = _get_s3_config(is_production)
    schema = PROD_SCHEMA if is_production else DEV_SCHEMA

    ## Create unique S3 paths for this batch inference run using timestamp
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f")
    upload_folder = f"publish_{timestamp}"
    output_s3_path = f"{s3_bucket}/{S3_DATA_FOLDER}/{upload_folder}"

    # Step 1: Build COPY INTO query to export data from Snowflake to S3
    input_query = get_query_from_string_or_fpath(input_query)
    input_query = substitute_map_into_string(input_query, {"schema": schema} | (ctx or {}))

    _debug_print_query(input_query)

    input_s3_path = copy_snowflake_to_s3(
        query=input_query,
        warehouse=warehouse,
        use_utc=use_utc,
    )

    batch_inference_from_s3(
        input_s3_path=input_s3_path,
        output_s3_path=output_s3_path,
        predict_fn=predict_fn,
        timeout_per_batch=timeout_per_batch,
        batch_size_in_mb=batch_size_in_mb,
    )
    ## Step 2: Build COPY INTO query to import predictions from S3 back to Snowflake

    copy_s3_to_snowflake(
        s3_path=output_s3_path,
        table_name=output_table_name,
        table_defination=output_table_definition,
        warehouse=warehouse,
        use_utc=use_utc,
        auto_create_table=auto_create_table,
        overwrite=overwrite,
    )

    print("✅ Batch inference completed successfully!")


def batch_inference_from_s3(
    input_s3_path: str | List[str],
    output_s3_path: str,
    predict_fn: Callable[[pd.DataFrame], pd.DataFrame],
    timeout_per_batch: int = 300,
    batch_size_in_mb: int = 100,
):
    if isinstance(input_s3_path, str):
        if str.endswith(input_s3_path, ".parquet"):
            input_s3_files = [input_s3_path]
        else:
            input_s3_files = s3._list_files_in_s3_folder(input_s3_path)

    elif not isinstance(input_s3_path, list):
        raise ValueError("input_s3_path must be a string or list of strings.")

    else:
        input_s3_files = input_s3_path

    ## Check if all paths are valid S3 URIs
    if any(not (path.startswith("s3://") and path.endswith(".parquet")) for path in input_s3_files):
        raise ValueError("Invalid S3 URI. All paths or folder files must start with 's3://' and end with '.parquet'.")

    input_s3_batches = make_batches_of_files(input_s3_files, batch_size_in_mb)

    print(f"📊 Total Batches to process: {len(input_s3_batches)}")

    download_queue = queue.Queue(maxsize=1)  # Adjust maxsize per memory limits
    inference_queue = queue.Queue(maxsize=1)

    def download_worker(file_keys):
        for batch_id, key in enumerate(file_keys):
            with timer(f"Downloading batch {batch_id} from S3"):
                df = s3._get_df_from_s3_files(key)
                df.columns = [
                    col.lower() for col in df.columns
                ]  # Ensure columns are lowercase for consistent processing
            download_queue.put((batch_id, df), timeout=timeout_per_batch)
        download_queue.put(None, timeout=timeout_per_batch)

    def inference_worker():
        while True:
            item = download_queue.get(timeout=timeout_per_batch)
            if item is None:
                inference_queue.put(None, timeout=timeout_per_batch)
                break
            batch_id, df = item
            _debug(f"Generating predictions for batch {batch_id}...")
            with timer(f"Generating predictions for batch {batch_id}"):
                predictions_df = predict_fn(df)
            inference_queue.put((batch_id, predictions_df), timeout=timeout_per_batch)

    def upload_worker():
        while True:
            item = inference_queue.get(timeout=timeout_per_batch)
            if item is None:
                break
            batch_id, predictions_df = item
            s3_output_file = f"{output_s3_path}/predictions_{batch_id}.parquet"
            with timer(f"Uploading predictions for batch {batch_id} to S3"):
                s3._put_df_to_s3_file(predictions_df, s3_output_file)

    with ThreadPoolExecutor(max_workers=3) as executor:
        # Use .submit() for different functions with varying arguments
        executor.submit(download_worker, input_s3_batches)
        executor.submit(inference_worker)
        executor.submit(upload_worker)

    print("✅ All batches processed successfully!")
