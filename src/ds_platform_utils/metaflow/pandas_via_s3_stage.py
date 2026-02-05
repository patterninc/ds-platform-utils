"""Pandas operations for Snowflake via S3 stage - optimized for large-scale data transfers.

This module provides efficient data transfer between Snowflake and Pandas DataFrames using S3 as
an intermediate staging area. This approach is significantly faster for large datasets compared
to direct database connections.

Use these functions when:
- Querying large result sets (>10M rows) from Snowflake
- Writing large DataFrames (>10M rows) to Snowflake
- Processing batch predictions with large datasets

The functions automatically handle:
- Dev/prod environment switching via current.is_production
- Temporary S3 folder creation with timestamps
- Parquet file chunking for optimal performance
- Metaflow card integration for visibility
"""

import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from metaflow import current
from metaflow.cards import Markdown, Table

from ds_platform_utils._snowflake.run_query import _execute_sql
from ds_platform_utils.metaflow._consts import (
    DEV_S3_BUCKET,
    DEV_SNOWFLAKE_STAGE,
    NON_PROD_SCHEMA,
    PROD_S3_BUCKET,
    PROD_SCHEMA,
    PROD_SNOWFLAKE_STAGE,
)
from ds_platform_utils.metaflow.get_snowflake_connection import _debug_print_query, get_snowflake_connection
from ds_platform_utils.metaflow.pandas import TWarehouse
from ds_platform_utils.metaflow.s3 import (
    _get_df_from_s3_files,
    _get_df_from_s3_folder,
    _list_files_in_s3_folder,
    _put_df_to_s3_folder,
)
from ds_platform_utils.metaflow.write_audit_publish import (
    add_comment_to_each_sql_statement,
    get_select_dev_query_tags,
)


def _get_s3_config(is_production: bool) -> Tuple[str, str]:
    """Return the appropriate S3 bucket and Snowflake stage based on the environment."""
    if is_production:
        s3_bucket = PROD_S3_BUCKET
        snowflake_stage = PROD_SNOWFLAKE_STAGE
    else:
        s3_bucket = DEV_S3_BUCKET
        snowflake_stage = DEV_SNOWFLAKE_STAGE

    return s3_bucket, snowflake_stage


def _generate_snowflake_to_s3_copy_query(
    query: str,
    snowflake_stage: str,
    s3_folder_path: str,
    file_name: str = "data.parquet",
) -> str:
    """Generate SQL COPY INTO command to export Snowflake query results to S3.

    :param query: SQL query to execute
    :param snowflake_stage: Snowflake stage name (e.g., 'DEV_OUTERBOUNDS_S3_STAGE')
    :param s3_folder_path: Relative S3 folder path within the stage (e.g., 'temp/query_20260205_123456')
    :param file_name: Output file name. Default 'data.parquet'
    :return: COPY INTO SQL command
    """
    copy_query = f"""
    COPY INTO @{snowflake_stage}/{s3_folder_path}/{file_name}
    FROM ({query})
    OVERWRITE = TRUE
    FILE_FORMAT = (TYPE = 'parquet')
    HEADER = TRUE;
    """
    return copy_query


def _generate_s3_to_snowflake_copy_query(
    database: str,
    schema: str,
    table_name: str,
    snowflake_stage: str,
    s3_folder_path: str,
    table_schema: List[Tuple[str, str]],
    overwrite: bool = True,
    auto_create_table: bool = True,
) -> str:
    """Generate SQL commands to load data from S3 to Snowflake table.

    This function generates a complete SQL script that includes:
    1. DROP TABLE IF EXISTS (if overwrite=True)
    2. CREATE TABLE IF NOT EXISTS (if auto_create_table=True or overwrite=True)
    3. COPY INTO command to load data from S3

    :param database: Snowflake database name (e.g., 'PATTERN_DB')
    :param schema: Snowflake schema name (e.g., 'DATA_SCIENCE' or 'DATA_SCIENCE_STAGE')
    :param table_name: Target table name
    :param snowflake_stage: Snowflake stage name (e.g., 'DEV_OUTERBOUNDS_S3_STAGE')
    :param s3_folder_path: Relative S3 folder path within the stage
    :param table_schema: List of tuples with column names and types
    :param overwrite: If True, drop and recreate the table. Default True
    :param auto_create_table: If True, create the table if it doesn't exist. Default True
    :return: Complete SQL script with table management and COPY INTO commands
    """
    sql_statements = []

    # Step 1: Drop table if overwrite is True
    if overwrite:
        sql_statements.append(f"DROP TABLE IF EXISTS {database}.{schema}.{table_name};")

    # Step 2: Create table if auto_create_table or overwrite
    if auto_create_table or overwrite:
        table_create_columns_str = ",\n    ".join([f"{col_name} {col_type}" for col_name, col_type in table_schema])
        create_table_query = (
            f"""CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} ( {table_create_columns_str} );"""
        )
        sql_statements.append(create_table_query)

    # Step 3: Generate COPY INTO command
    columns_str = ",\n    ".join([f"PARSE_JSON($1):{col_name}::{col_type}" for col_name, col_type in table_schema])

    copy_query = f"""COPY INTO {database}.{schema}.{table_name} FROM (
        SELECT {columns_str}
        FROM @{snowflake_stage}/{s3_folder_path}/ )
        FILE_FORMAT = (TYPE = 'parquet' USE_LOGICAL_TYPE = TRUE);"""
    sql_statements.append(copy_query)

    # Combine all statements
    return "\n\n".join(sql_statements)


def query_pandas_from_snowflake_via_s3_stage(
    query: Union[str, Path],
    warehouse: Optional[TWarehouse] = None,
    ctx: Optional[Dict[str, Any]] = None,
    use_utc: bool = True,
) -> pd.DataFrame:
    """Query Snowflake and return large result sets efficiently via S3 stage.

    This function is optimized for large query results (>10M rows). It uses Snowflake's
    COPY INTO command to export query results to S3, then reads the parquet files from S3.
    This is significantly faster than using cursor.fetch_pandas_all() for large datasets.

    The function automatically:
    - Creates a timestamp-based temporary folder in S3
    - Exports query results to parquet files in S3
    - Reads and combines all parquet files into a single DataFrame
    - Uses the appropriate S3 bucket/stage based on current.is_production

    :param query: SQL query string or path to a .sql file
    :param warehouse: The Snowflake warehouse to use. Defaults to shared warehouse based on environment.
    :param ctx: Context dictionary to substitute into the query string
    :param use_utc: Whether to set the Snowflake session to use UTC time zone. Default is True.
    :return: DataFrame containing the results of the query

    Example:
        >>> df = query_pandas_from_snowflake_via_s3_stage(
        ...     query="SELECT * FROM LARGE_TABLE LIMIT 100000000",
        ...     warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH"
        ... )

    """
    from ds_platform_utils._snowflake.write_audit_publish import (
        get_query_from_string_or_fpath,
        substitute_map_into_string,
    )

    # adding query tags comment in query for cost tracking in select.dev
    tags = get_select_dev_query_tags()
    query_comment_str = f"\n\n/* {json.dumps(tags)} */"
    query = get_query_from_string_or_fpath(query)
    query = add_comment_to_each_sql_statement(query, query_comment_str)

    schema = PROD_SCHEMA if current.is_production else NON_PROD_SCHEMA
    if "{{schema}}" in query or "{{ schema }}" in query:
        query = substitute_map_into_string(query, {"schema": schema})
    if ctx:
        query = substitute_map_into_string(query, ctx)

    # print query if DEBUG_QUERY env var is set
    _debug_print_query(query)

    # Determine environment
    s3_bucket, snowflake_stage = _get_s3_config(current.is_production if hasattr(current, "is_production") else False)

    # Create timestamp-based temporary folder
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f")
    temp_folder = f"temp/query_{timestamp}"
    s3_path = f"{s3_bucket}/{temp_folder}/"

    # Build COPY INTO query to export results to S3
    copy_query = _generate_snowflake_to_s3_copy_query(
        query=query,
        snowflake_stage=snowflake_stage,
        s3_folder_path=temp_folder,
        file_name="data.parquet",
    )

    # Add to Metaflow card
    if warehouse is not None:
        current.card.append(Markdown(f"### Using Warehouse: `{warehouse}`"))
    current.card.append(Markdown(f"### S3 Staging Path: `{s3_path}`"))
    current.card.append(Markdown(f"### Query:\n```sql\n{query}\n```"))

    # Execute query
    conn = get_snowflake_connection(use_utc)

    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")

    _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

    # Copy data to S3
    _execute_sql(conn, copy_query)
    conn.close()

    # Read data from S3
    df = _get_df_from_s3_folder(s3_path)

    # Lowercase column names for consistency
    df.columns = df.columns.str.lower()

    # Add preview to card
    current.card.append(Markdown("### Query Result Preview"))
    current.card.append(Table.from_dataframe(df.head(10)))
    current.card.append(Markdown(f"**Total rows:** {len(df):,}"))

    return df


def publish_pandas_via_s3_stage(  # noqa: PLR0913 (too many arguments)
    table_name: str,
    df: pd.DataFrame,
    table_schema: List[Tuple[str, str]],
    chunk_size: int = 100000,
    warehouse: Optional[TWarehouse] = None,
    overwrite: bool = True,
    auto_create_table: bool = True,
    use_utc: bool = True,
) -> None:
    """Write large DataFrame to Snowflake table efficiently via S3 stage.

    This function is optimized for large DataFrames (>10M rows). It uploads the DataFrame
    as parquet files to S3, then uses Snowflake's COPY INTO command to load the data.
    This is significantly faster than using write_pandas() for large datasets.

    The function automatically:
    - Chunks the DataFrame into batches and writes to S3 as parquet files
    - Creates or overwrites the target table based on parameters
    - Loads all parquet files from S3 into Snowflake
    - Uses the appropriate S3 bucket/stage based on current.is_production

    :param table_name: Name of the Snowflake table to create/update
    :param df: DataFrame to write to Snowflake
    :param table_schema: List of tuples defining column names and types.
                         Example: [("col1", "VARCHAR(255)"), ("col2", "INTEGER")]
    :param chunk_size: Number of rows per parquet file. Default 100,000
    :param warehouse: The Snowflake warehouse to use. Defaults to shared warehouse based on environment.
    :param overwrite: If True, drop and recreate the table. Default True
    :param auto_create_table: If True, create the table if it doesn't exist. Default True
    :param use_utc: Whether to set the Snowflake session to use UTC time zone. Default is True

    Example:
        >>> schema = [
        ...     ("asin", "VARCHAR(255)"),
        ...     ("date", "DATE"),
        ...     ("forecast", "FLOAT")
        ... ]
        >>> publish_pandas_via_s3_stage(
        ...     table_name="FORECAST_RESULTS",
        ...     df=large_df,
        ...     table_schema=schema,
        ...     warehouse="OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH"
        ... )

    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    if df.empty:
        raise ValueError("DataFrame is empty.")

    # Determine environment
    is_production = current.is_production if hasattr(current, "is_production") else False
    s3_bucket, snowflake_stage = _get_s3_config(is_production)
    schema = PROD_SCHEMA if is_production else NON_PROD_SCHEMA

    table_name = table_name.upper()

    # Create timestamp-based temporary folder
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f")
    upload_folder = f"temp/publish_{timestamp}"
    s3_path = f"{s3_bucket}/{upload_folder}"

    # Add to Metaflow card
    environment = "PROD" if is_production else "DEV"
    current.card.append(Markdown(f"## Publishing DataFrame to Snowflake via S3 Stage ({environment})"))
    if warehouse is not None:
        current.card.append(Markdown(f"### Using Warehouse: `{warehouse}`"))
    current.card.append(Markdown(f"### Target Table: `{schema}.{table_name}`"))
    current.card.append(Markdown(f"### S3 Staging Path: `{s3_path}`"))
    current.card.append(Markdown(f"### Rows: {len(df):,} | Columns: {len(df.columns)}"))
    current.card.append(Table.from_dataframe(df.head()))

    # Upload DataFrame to S3 as parquet files
    _put_df_to_s3_folder(
        df=df,
        path=s3_path,
        chunk_size=chunk_size,
    )

    current.card.append(Markdown("### Uploaded parquet files to S3"))

    # Connect to Snowflake
    conn = get_snowflake_connection(use_utc)

    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")

    _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

    # Generate and execute SQL to create table and load data from S3
    sql_commands = _generate_s3_to_snowflake_copy_query(
        database="PATTERN_DB",
        schema=schema,
        table_name=table_name,
        snowflake_stage=snowflake_stage,
        s3_folder_path=upload_folder,
        table_schema=table_schema,
        overwrite=overwrite,
        auto_create_table=auto_create_table,
    )

    current.card.append(Markdown("### Loading data from S3 to Snowflake..."))

    # Execute all SQL commands
    _execute_sql(conn, sql_commands)
    conn.close()

    # Add success message to card
    from ds_platform_utils.metaflow.write_audit_publish import _make_snowflake_table_url

    table_url = _make_snowflake_table_url(
        database="PATTERN_DB",
        schema=schema,
        table=table_name,
    )
    current.card.append(Markdown(f"### ✅ Successfully published {len(df):,} rows"))
    current.card.append(Markdown(f"[View table in Snowflake]({table_url})"))


def make_batch_predictions_from_snowflake_via_s3_stage(  # noqa: PLR0913 (too many arguments)
    input_query: Union[str, Path],
    output_table_name: str,
    output_table_schema: List[Tuple[str, str]],
    model_predictor_function: Callable[[pd.DataFrame], pd.DataFrame],
    warehouse: Optional[TWarehouse] = None,
    ctx: Optional[Dict[str, Any]] = None,
    use_utc: bool = True,
) -> None:
    """Process large datasets through a model/function using S3 for efficient batch processing.

    This function implements an end-to-end pipeline for batch predictions:
    1. Query data from Snowflake → Export to S3
    2. Read data from S3 file by file
    3. Process each file through the model_predictor_function
    4. Write predictions to S3
    5. Load all predictions from S3 to Snowflake table

    This approach is memory-efficient for very large datasets as it processes data file by file
    rather than loading everything into memory at once.

    :param input_query: SQL query to fetch input data from Snowflake
    :param output_table_name: Name of the Snowflake table to write predictions to
    :param output_table_schema: Schema for the output table.
                                Example: [("col1", "VARCHAR(255)"), ("col2", "FLOAT")]
    :param model_predictor_function: Function that takes a DataFrame and returns a DataFrame of predictions.
                                     Signature: fn(df: pd.DataFrame) -> pd.DataFrame
    :param warehouse: The Snowflake warehouse to use. Defaults to shared warehouse based on environment.
    :param ctx: Context dictionary to substitute into the input query
    :param use_utc: Whether to set the Snowflake session to use UTC time zone. Default is True

    Example:
        >>> def predict_fn(input_df: pd.DataFrame) -> pd.DataFrame:
        ...     # Your model prediction logic here
        ...     predictions = model.predict(input_df)
        ...     return pd.DataFrame({"asin": input_df["asin"], "forecast": predictions})
        ...
        >>> output_schema = [("asin", "VARCHAR(255)"), ("forecast", "FLOAT")]
        >>> make_batch_predictions_from_snowflake_via_s3_stage(
        ...     input_query="SELECT * FROM INPUT_TABLE",
        ...     output_table_name="PREDICTIONS",
        ...     output_table_schema=output_schema,
        ...     model_predictor_function=predict_fn
        ... )

    """
    from ds_platform_utils._snowflake.write_audit_publish import (
        get_query_from_string_or_fpath,
        substitute_map_into_string,
    )

    # Determine environment
    is_production = current.is_production if hasattr(current, "is_production") else False
    s3_bucket, snowflake_stage = _get_s3_config(is_production)
    schema = PROD_SCHEMA if is_production else NON_PROD_SCHEMA

    output_table_name = output_table_name.upper()

    # Process input query
    query = get_query_from_string_or_fpath(input_query)

    # Handle schema substitution
    if "{{schema}}" in query or "{{ schema }}" in query:
        query = substitute_map_into_string(query, {"schema": schema})

    # Handle additional context substitution
    if ctx:
        query = substitute_map_into_string(query, ctx)

    # Create timestamps for input and output folders
    input_timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f")
    output_timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f")
    input_temp_folder = f"temp/batch_input_{input_timestamp}"
    output_temp_folder = f"temp/batch_output_{output_timestamp}"
    input_s3_path = f"{s3_bucket}/{input_temp_folder}/"
    output_s3_path = f"{s3_bucket}/{output_temp_folder}/"

    # Add to Metaflow card
    environment = "PROD" if is_production else "DEV"
    current.card.append(Markdown(f"## Batch Predictions Pipeline via S3 Stage ({environment})"))
    if warehouse is not None:
        current.card.append(Markdown(f"### Using Warehouse: `{warehouse}`"))
    current.card.append(Markdown(f"### Output Table: `{schema}.{output_table_name}`"))
    current.card.append(Markdown(f"### Input Query:\n```sql\n{query}\n```"))

    # Step 1: Export input data from Snowflake to S3
    current.card.append(Markdown("### Step 1: Exporting input data from Snowflake to S3..."))

    # Add query tags for cost tracking
    tags = get_select_dev_query_tags()
    query_comment_str = f"\n\n/* {json.dumps(tags)} */"
    query_with_tags = add_comment_to_each_sql_statement(query, query_comment_str)

    # Build COPY INTO query to export data from Snowflake to S3
    copy_to_s3_query = _generate_snowflake_to_s3_copy_query(
        query=query_with_tags,
        snowflake_stage=snowflake_stage,
        s3_folder_path=input_temp_folder,
        file_name="data.parquet",
    )

    conn = get_snowflake_connection(use_utc)

    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")

        # Set query tag for cost tracking
        tags_json = json.dumps(tags)
        _execute_sql(conn, f"ALTER SESSION SET QUERY_TAG = '{tags_json}';")

    _execute_sql(conn, copy_to_s3_query)

    # Step 2: Get list of input files from S3
    input_files = _list_files_in_s3_folder(input_s3_path)

    if not input_files:
        raise ValueError(f"No input files found in S3 path: {input_s3_path}")

    current.card.append(Markdown(f"### Step 2: Processing {len(input_files)} file(s)..."))

    # Step 3: Process each file through the model and write predictions to S3
    total_predictions = 0
    for file_idx, input_file in enumerate(input_files):
        current.card.append(Markdown(f"#### Processing file {file_idx + 1}/{len(input_files)}..."))

        # Read single file
        input_df = _get_df_from_s3_files([input_file])

        # Run predictions
        predictions_df = model_predictor_function(input_df)

        # Write predictions to S3
        _put_df_to_s3_as_parquet_files(
            df=predictions_df,
            s3_base_path=output_s3_path.rstrip("/"),
            batch_size=None,  # Write each prediction result as single file
            file_prefix=f"predictions_part_{file_idx}",
        )

        total_predictions += len(predictions_df)
        current.card.append(
            Markdown(f"   - Processed {len(input_df):,} rows → Generated {len(predictions_df):,} predictions")
        )

    current.card.append(Markdown(f"### Step 3: Total predictions generated: {total_predictions:,}"))

    # Step 4: Create output table and load predictions from S3 to Snowflake
    current.card.append(Markdown("### Step 4: Creating table and loading predictions from S3 to Snowflake..."))

    # Generate and execute SQL to create table and load data from S3
    sql_commands = _generate_s3_to_snowflake_copy_query(
        database="PATTERN_DB",
        schema=schema,
        table_name=output_table_name,
        snowflake_stage=snowflake_stage,
        s3_folder_path=output_temp_folder,
        table_schema=output_table_schema,
        overwrite=False,  # Don't overwrite for batch predictions
        auto_create_table=True,  # Create table if it doesn't exist
    )

    _execute_sql(conn, sql_commands)
    conn.close()

    # Add success message to card
    from ds_platform_utils.metaflow.write_audit_publish import _make_snowflake_table_url

    table_url = _make_snowflake_table_url(
        database="PATTERN_DB",
        schema=schema,
        table=output_table_name,
    )
    current.card.append(Markdown("### ✅ Successfully completed batch predictions"))
    current.card.append(Markdown(f"**Total predictions:** {total_predictions:,}"))
    current.card.append(Markdown(f"[View results in Snowflake]({table_url})"))
