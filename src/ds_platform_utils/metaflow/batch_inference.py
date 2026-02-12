import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

import pandas as pd
from metaflow import current
from metaflow.cards import Markdown, Table

from ds_platform_utils._snowflake.run_query import _execute_sql
from ds_platform_utils._snowflake.write_audit_publish import get_query_from_string_or_fpath, substitute_map_into_string
from ds_platform_utils.metaflow import s3
from ds_platform_utils.metaflow._consts import (
    NON_PROD_SCHEMA,
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


def batch_inference(  # noqa: PLR0913, PLR0915
    input_query: Union[str, Path],
    output_table_name: str,
    model_predictor_function: Callable[[pd.DataFrame], pd.DataFrame],
    output_table_schema: Optional[List[Tuple[str, str]]] = None,
    use_utc: bool = True,
    batch_size_in_mb: int = 16,
    parallelism: int = 1,
    warehouse: Optional[str] = None,
    ctx: Optional[dict] = None,
):
    is_production = current.is_production if hasattr(current, "is_production") else False
    s3_bucket, snowflake_stage = _get_s3_config(is_production)
    schema = PROD_SCHEMA if is_production else NON_PROD_SCHEMA

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

    current.card.append(Markdown("### Batch Predictions From Snowflake via S3 Stage"))
    current.card.append(Markdown(input_query))
    current.card.append(Markdown(f"#### Input S3 staging path: `{input_s3_path}`"))
    conn = get_snowflake_connection(use_utc)
    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")
    _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

    copy_to_s3_query = _generate_snowflake_to_s3_copy_query(
        query=input_query,
        snowflake_stage_path=input_snowflake_stage_path,
        batch_size_in_mb=batch_size_in_mb,
    )
    t0 = time.time()
    print("Exporting data from Snowflake to S3...")
    _execute_sql(conn, copy_to_s3_query)
    conn.close()
    t1 = time.time()
    print(f"Data export completed in {t1 - t0:.2f} seconds. Starting batch inference...")

    input_s3_files = s3._list_files_in_s3_folder(input_s3_path)
    current.card.append(Markdown("#### Input query results"))
    current.card.append(Table.from_dataframe(s3._get_df_from_s3_file(input_s3_files[0]).head(5)))

    def process_file(batch_id, input_s3_file):
        print(f"Processing batch {batch_id}")
        print(f"Reading input files for batch {batch_id} from S3...")
        t1 = time.time()
        df = pd.read_parquet(s3._get_df_from_s3_file(input_s3_file))
        t2 = time.time()
        print(f"Read {len(input_s3_file)} files with {len(df)} rows in {t2 - t1:.2f} seconds.")
        predictions_df = model_predictor_function(df)
        t3 = time.time()
        print(f"Generated predictions for batch {batch_id} in {t3 - t2:.2f} seconds.")
        s3_output_file = f"{output_s3_path}/predictions_batch_{batch_id}.parquet"
        s3._put_df_to_s3_file(predictions_df, s3_output_file)
        t4 = time.time()
        print(f"Uploaded predictions for batch {batch_id} to S3 in {t4 - t3:.2f} seconds.")

    print("Starting batch inference...")
    print(f"Total files to process: {len(input_s3_files)}")
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = []
        for i in range(0, len(input_s3_files)):
            batch_id = i
            futures.append(executor.submit(process_file, batch_id, input_s3_files[i]))

    print("Batch inference completed. Uploading results to S3...")

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
    t0 = time.time()
    print("Copying predictions from S3 to Snowflake...")
    _execute_sql(conn, copy_from_s3_query)
    t1 = time.time()
    print(f"Data import completed in {t1 - t0:.2f} seconds.")

    conn.close()
