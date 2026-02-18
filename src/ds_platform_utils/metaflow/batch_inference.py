import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

import pandas as pd
from metaflow import current

from ds_platform_utils._snowflake.run_query import _execute_sql
from ds_platform_utils._snowflake.write_audit_publish import get_query_from_string_or_fpath, substitute_map_into_string
from ds_platform_utils.metaflow import s3
from ds_platform_utils.metaflow._consts import (
    DEV_SCHEMA,
    PROD_SCHEMA,
    S3_DATA_FOLDER,
)
from ds_platform_utils.metaflow.get_snowflake_connection import _debug_print_query, get_snowflake_connection
from ds_platform_utils.metaflow.pandas import (
    _generate_s3_to_snowflake_copy_query,
    _generate_snowflake_to_s3_copy_query,
    _get_s3_config,
    _infer_table_schema,
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


def make_batches_of_files(files_list, batch_size_in_mb):
    with s3._get_metaflow_s3_client() as s3_client:
        file_sizes = [(file.key, file.size) for file in s3_client.info_many(files_list)]

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


def snowflake_batch_inference(  # noqa: PLR0913, PLR0915
    input_query: Union[str, Path],
    output_table_name: str,
    model_predictor_function: Callable[[pd.DataFrame], pd.DataFrame],
    output_table_schema: Optional[List[Tuple[str, str]]] = None,
    use_utc: bool = True,
    batch_size_in_mb: int = 128,
    warehouse: Optional[str] = None,
    ctx: Optional[dict] = None,
    timeout_per_batch: int = 300,
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
        parallelism (int, optional): Reserved for future parallel processing capability. Defaults to 1.
        warehouse (Optional[str], optional): Snowflake warehouse to use for queries. If None, uses default warehouse. Defaults to None.
        ctx (Optional[dict], optional): Dictionary of variable substitutions for the input query template. Defaults to None.

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
    s3_bucket, snowflake_stage = _get_s3_config(is_production)
    schema = PROD_SCHEMA if is_production else DEV_SCHEMA

    ## Create unique S3 paths for this batch inference run using timestamp
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f")
    upload_folder = f"publish_{timestamp}"
    download_folder = f"query_{timestamp}"
    input_s3_path = f"{s3_bucket}/{S3_DATA_FOLDER}/{download_folder}"
    input_snowflake_stage_path = f"{snowflake_stage}/{S3_DATA_FOLDER}/{download_folder}"
    output_s3_path = f"{s3_bucket}/{S3_DATA_FOLDER}/{upload_folder}"
    output_snowflake_stage_path = f"{snowflake_stage}/{S3_DATA_FOLDER}/{upload_folder}"

    # Step 1: Build COPY INTO query to export data from Snowflake to S3
    input_query = get_query_from_string_or_fpath(input_query)
    input_query = substitute_map_into_string(input_query, {"schema": schema} | (ctx or {}))

    _debug_print_query(input_query)
    conn = get_snowflake_connection(use_utc)
    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")
    _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

    copy_to_s3_query = _generate_snowflake_to_s3_copy_query(
        query=input_query,
        snowflake_stage_path=input_snowflake_stage_path,
        batch_size_in_mb=default_file_size_in_mb,
    )

    with timer("Exporting data from Snowflake to S3"):
        _execute_sql(conn, copy_to_s3_query)
    conn.close()

    batch_inference_from_s3(
        input_s3_path=input_s3_path,
        output_s3_folder_path=output_s3_path,
        model_predictor_function=model_predictor_function,
        timeout_per_batch=timeout_per_batch,
    )

    conn = get_snowflake_connection(use_utc)
    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")
    _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

    if output_table_schema is None:
        # Infer schema from the first predictions file
        output_table_schema = _infer_table_schema(conn, output_snowflake_stage_path, True)

    copy_from_s3_query = _generate_s3_to_snowflake_copy_query(
        schema=schema,
        table_name=output_table_name,
        snowflake_stage_path=output_snowflake_stage_path,
        overwrite=True,
        auto_create_table=True,
        table_schema=output_table_schema,
    )

    with timer("Uploading predictions from s3 to Snowflake"):
        _execute_sql(conn, copy_from_s3_query)

    conn.close()

    print("✅ Batch inference completed successfully!")


def batch_inference_from_s3(
    input_s3_path: str | List[str],
    output_s3_folder_path: str,
    model_predictor_function: Callable[[pd.DataFrame], pd.DataFrame],
    timeout_per_batch: int = 300,
    batch_size_in_mb: int = 128,
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
    if any(not path.startswith("s3://") and path.endswith(".parquet") for path in input_s3_files):
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
                predictions_df = model_predictor_function(df)
            inference_queue.put((batch_id, predictions_df), timeout=timeout_per_batch)

    def upload_worker():
        while True:
            item = inference_queue.get(timeout=timeout_per_batch)
            if item is None:
                break
            batch_id, predictions_df = item
            s3_output_file = f"{output_s3_folder_path}/predictions_{batch_id}.parquet"
            with timer(f"Uploading predictions for batch {batch_id} to S3"):
                s3._put_df_to_s3_file(predictions_df, s3_output_file)

    with ThreadPoolExecutor(max_workers=3) as executor:
        # Use .submit() for different functions with varying arguments
        executor.submit(download_worker, input_s3_batches)
        executor.submit(inference_worker)
        executor.submit(upload_worker)

    print("✅ All batches processed successfully!")
