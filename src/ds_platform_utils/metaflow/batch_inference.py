from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

import pandas as pd
from metaflow import current
from metaflow.cards import Markdown, Table

from ds_platform_utils._snowflake.run_query import _execute_sql
from ds_platform_utils._snowflake.write_audit_publish import get_query_from_string_or_fpath, substitute_map_into_string
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
)
from ds_platform_utils.metaflow.s3 import _get_df_from_s3_file, _list_files_in_s3_folder, _put_df_to_s3_file


def batch_inference(  # noqa: PLR0913 (too many arguments)
    input_query: Union[str, Path],
    output_table_name: str,
    output_table_schema: List[Tuple[str, str]],
    model_predictor_function: Callable[[pd.DataFrame], pd.DataFrame],
    use_utc: bool = True,
    batch_size_in_mb: int = 100,
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
    _execute_sql(conn, copy_to_s3_query)
    conn.close()

    # Step 2: Get input files from S3 and apply model predictor function to generate output dataframe

    input_files = _list_files_in_s3_folder(input_s3_path)

    if not input_files:
        raise ValueError(f"No input files found in S3 path: {input_s3_path}")

    current.card.append(Markdown("#### Input query results"))
    current.card.append(Table.from_dataframe(_get_df_from_s3_file(input_files[0])))

    # Step 3: Process each file through the model and write predictions to S3
    total_predictions = 0
    for file_idx, input_file in enumerate(input_files):
        # Read single file
        input_df = _get_df_from_s3_file(input_file)

        # Run predictions
        predictions_df = model_predictor_function(input_df)

        # Write predictions to S3
        _put_df_to_s3_file(
            df=predictions_df,
            path=f"{output_s3_path}/data_part_{file_idx}.parquet",
        )

        total_predictions += len(predictions_df)

    # Step 4: Build COPY INTO query to load predictions from S3 back to Snowflake

    conn = get_snowflake_connection(use_utc)
    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")
    _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

    copy_from_s3_query = _generate_s3_to_snowflake_copy_query(
        schema=schema,
        table_name=output_table_name,
        snowflake_stage_path=output_snowflake_stage_path,
        overwrite=True,
        auto_create_table=True,
        table_schema=output_table_schema,
    )
    _execute_sql(conn, copy_from_s3_query)
    conn.close()
