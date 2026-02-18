from typing import List, Optional, Tuple

import pandas as pd
from metaflow import current

from ds_platform_utils._snowflake.run_query import _execute_sql
from ds_platform_utils.metaflow import s3
from ds_platform_utils.metaflow._consts import (
    DEV_S3_BUCKET,
    DEV_SCHEMA,
    DEV_SNOWFLAKE_STAGE,
    PROD_S3_BUCKET,
    PROD_SCHEMA,
    PROD_SNOWFLAKE_STAGE,
    S3_DATA_FOLDER,
)
from ds_platform_utils.metaflow.get_snowflake_connection import get_snowflake_connection


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
    snowflake_stage_path: str,
) -> str:
    """Generate SQL COPY INTO command to export Snowflake query results to S3.

    :param query: SQL query to execute
    :param snowflake_stage_path: The path to the Snowflake stage where the data will be exported. This should include the stage name and any necessary subfolders (e.g., 'my_snowflake_stage/my_folder').
    :return: COPY INTO SQL command
    """
    if snowflake_stage_path.endswith(".parquet"):
        single = "TRUE"
        max_file_size = 100 * 1024 * 1024 * 1024  # 100 GB
    else:
        single = "FALSE"
        max_file_size = 16 * 1024 * 1024  # 16 MB

    if query.count(";") > 1:
        raise ValueError("Multiple SQL statements detected. Please provide a single query statement.")
    query = query.replace(";", "")  # Remove trailing semicolon if present
    copy_query = f"""
    COPY INTO @{snowflake_stage_path}
    FROM (
        {query}
    )
    OVERWRITE = TRUE
    FILE_FORMAT = (TYPE = 'parquet')
    MAX_FILE_SIZE = {max_file_size}
    SINGLE = {single}
    HEADER = TRUE
    DETAILED_OUTPUT = TRUE;
    """
    return copy_query


def _generate_s3_to_snowflake_copy_query(  # noqa: PLR0913
    snowflake_stage_path: str,
    table_name: str,
    table_definition: List[Tuple[str, str]],
    overwrite: bool = True,
    auto_create_table: bool = True,
    use_logical_type: bool = True,
) -> str:
    """Generate SQL commands to load data from S3 to Snowflake table.

    This function generates a complete SQL script that includes:
    1. DROP TABLE IF EXISTS (if overwrite=True)
    2. CREATE TABLE IF NOT EXISTS (if auto_create_table=True or overwrite=True)
    3. COPY INTO command to load data from S3

    :param table_name: Target table name
    :param snowflake_stage_path: The path to the Snowflake stage where the data will be exported. This should include the stage name and any necessary subfolders (e.g., 'my_snowflake_stage/my_folder').
    :param table_definition: List of tuples with column names and types
    :param overwrite: If True, drop and recreate the table. Default True
    :param auto_create_table: If True, create the table if it doesn't exist. Default True
    :param use_logical_type: Whether to use Parquet logical types when reading the parquet files. Default True.
    :return: Complete SQL script with table management and COPY INTO commands
    """
    sql_statements = []

    if auto_create_table and not overwrite:
        table_create_columns_str = ",\n ".join([f"{col_name} {col_type}" for col_name, col_type in table_definition])
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} ( {table_create_columns_str} );"""
        sql_statements.append(create_table_query)

    if auto_create_table and overwrite:
        table_create_columns_str = ",\n ".join([f"{col_name} {col_type}" for col_name, col_type in table_definition])
        create_table_query = f"""CREATE OR REPLACE TABLE {table_name} ( {table_create_columns_str} );"""
        sql_statements.append(create_table_query)

    if not auto_create_table and overwrite:
        sql_statements.append(f"TRUNCATE TABLE IF EXISTS {table_name};")

    # columns_str = ",\n  ".join([f"PARSE_JSON($1):{col_name}::{col_type}" for col_name, col_type in table_definition])

    copy_query = f"""COPY INTO {table_name} FROM '@{snowflake_stage_path}'
        FILE_FORMAT = (TYPE = 'parquet' USE_LOGICAL_TYPE = {use_logical_type})
        MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
        ;"""
    sql_statements.append(copy_query)

    # Combine all statements into a single SQL script
    return "\n\n".join(sql_statements)


def _infer_table_schema(conn, snowflake_stage_path: str, use_logical_type: bool) -> List[Tuple[str, str]]:
    """Infer Snowflake table schema from Parquet files in a Snowflake stage.

    :param snowflake_stage_path: The path to the Snowflake stage where the Parquet files are located. This should include the stage name and any necessary subfolders (e.g., 'my_snowflake_stage/my_folder').
    :return: List of tuples with column names and inferred Snowflake data types
    """
    _execute_sql(
        conn,
        f"CREATE OR REPLACE TEMP FILE FORMAT PQT_FILE_FORMAT TYPE = PARQUET USE_LOGICAL_TYPE = {use_logical_type};",
    )
    infer_schema_query = f"""
        SELECT COLUMN_NAME, TYPE
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@{snowflake_stage_path}',
                FILE_FORMAT => 'PQT_FILE_FORMAT'
        ));
    """
    cursor = _execute_sql(conn, infer_schema_query)
    if cursor is None:
        raise ValueError("Failed to infer schema: No cursor returned from Snowflake.")
    result = cursor.fetch_pandas_all()
    return list(zip(result["COLUMN_NAME"], result["TYPE"]))


def copy_snowflake_to_s3(
    query: str,
    warehouse: Optional[str] = None,
    use_utc: bool = True,
) -> List[str]:
    """Generate SQL COPY INTO command to export Snowflake query results to S3.

    :param query: SQL query to execute
    :param warehouse: Snowflake warehouse to use
    :param use_utc: Whether to use UTC time

    :return: List of S3 file paths where the data was exported
    """
    schema = PROD_SCHEMA if current.is_production else DEV_SCHEMA
    s3_bucket, snowflake_stage = _get_s3_config(current.is_production)

    data_folder = "query_" + str(pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f"))
    s3_path = f"{s3_bucket}/{S3_DATA_FOLDER}/{data_folder}"
    sf_stage_path = f"{snowflake_stage}/{S3_DATA_FOLDER}/{data_folder}/"
    query = _generate_snowflake_to_s3_copy_query(
        query=query,
        snowflake_stage_path=sf_stage_path,
    )
    conn = get_snowflake_connection(use_utc)
    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")
    _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

    _execute_sql(conn, query)

    print(f"✅ Data exported to S3 path: {s3_path}")

    file_paths = s3._list_files_in_s3_folder(s3_path)
    return file_paths


def copy_s3_to_snowflake(  # noqa: PLR0913
    s3_path: str,
    table_name: str,
    table_definition: Optional[List[Tuple[str, str]]] = None,
    warehouse: Optional[str] = None,
    use_utc: bool = True,
    auto_create_table: bool = False,
    overwrite: bool = False,
    use_logical_type: bool = True,
):
    """Generate SQL commands to load data from S3 to Snowflake table.

    This function generates a complete SQL script that includes:
    1. DROP TABLE IF EXISTS (if overwrite=True)
    2. CREATE TABLE IF NOT EXISTS (if auto_create_table=True or overwrite=True)
    3. COPY INTO command to load data from S3

    :param s3_path: The S3 path where the data is located. This should include the bucket name and any necessary subfolders (e.g., 's3://my_bucket/my_folder').
    :param table_name: Target table name
    :param table_definition: List of tuples with column names and types
    :param overwrite: If True, drop and recreate the table. Default True
    :param auto_create_table: If True, create the table if it doesn't exist. Default True
    :param use_logical_type: Whether to use Parquet logical types when reading the parquet files. Default True.
    :return: Complete SQL script with table management and COPY INTO commands
    """
    table_name = table_name.upper()
    schema = PROD_SCHEMA if current.is_production else DEV_SCHEMA
    if current.is_production:
        if not s3_path.startswith(PROD_S3_BUCKET):
            raise ValueError(f"In production environment, s3_path must start with s3://{PROD_S3_BUCKET}")
    elif not s3_path.startswith(DEV_S3_BUCKET):
        raise ValueError(f"In development environment, s3_path must start with s3://{DEV_S3_BUCKET}")

    s3_bucket, snowflake_stage = _get_s3_config(current.is_production)
    sf_stage_path = s3_path.replace(s3_bucket, snowflake_stage)
    conn = get_snowflake_connection(use_utc)
    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")
    _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

    if table_definition is None:
        # Infer table schema from the Parquet files in the Snowflake stage
        table_definition = _infer_table_schema(conn, sf_stage_path, use_logical_type)
    if table_definition is None or len(table_definition) == 0:
        raise ValueError(
            "Failed to determine table schema. Please provide a valid table_definition or ensure that the S3 path contains valid Parquet files."
        )

    copy_query = _generate_s3_to_snowflake_copy_query(
        table_name=table_name,
        snowflake_stage_path=sf_stage_path,
        table_definition=table_definition,
        overwrite=overwrite,
        auto_create_table=auto_create_table,
        use_logical_type=use_logical_type,
    )
    _execute_sql(conn, copy_query)

    print(f"✅ Data loaded into Snowflake table: {schema}.{table_name}")
