import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import pandas as pd
import pyarrow
import pytz
from metaflow import current
from metaflow.cards import Markdown, Table
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas

from ds_platform_utils._snowflake.run_query import _execute_sql
from ds_platform_utils.metaflow._consts import (
    DEV_S3_BUCKET,
    DEV_SNOWFLAKE_STAGE,
    NON_PROD_SCHEMA,
    PROD_S3_BUCKET,
    PROD_SCHEMA,
    PROD_SNOWFLAKE_STAGE,
    S3_DATA_FOLDER,
)
from ds_platform_utils.metaflow.get_snowflake_connection import _debug_print_query, get_snowflake_connection
from ds_platform_utils.metaflow.s3 import _get_df_from_s3_folder, _put_df_to_s3_folder
from ds_platform_utils.metaflow.write_audit_publish import (
    _make_snowflake_table_url,
    add_comment_to_each_sql_statement,
    get_select_dev_query_tags,
)

TWarehouse = Literal[
    "OUTERBOUNDS_DATA_SCIENCE_ADS_PROD_XS_WH",
    "OUTERBOUNDS_DATA_SCIENCE_ADS_PROD_MED_WH",
    "OUTERBOUNDS_DATA_SCIENCE_ADS_PROD_XL_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_XS_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_MED_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_XL_WH",
    "OUTERBOUNDS_DATA_SCIENCE_ADS_DEV_XS_WH",
    "OUTERBOUNDS_DATA_SCIENCE_ADS_DEV_MED_WH",
    "OUTERBOUNDS_DATA_SCIENCE_ADS_DEV_XL_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_MED_WH",
    "OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XL_WH",
]


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
    file_name: str = "data.parquet",
) -> str:
    """Generate SQL COPY INTO command to export Snowflake query results to S3.

    :param query: SQL query to execute
    :param snowflake_stage_path: The path to the Snowflake stage where the data will be exported. This should include the stage name and any necessary subfolders (e.g., 'my_snowflake_stage/my_folder').
    :param file_name: Output file name. Default 'data.parquet'
    :return: COPY INTO SQL command
    """
    if query.count(";") > 1:
        raise ValueError("Multiple SQL statements detected. Please provide a single query statement.")
    query = query.replace(";", "")  # Remove trailing semicolon if present
    copy_query = f"""
    COPY INTO @{snowflake_stage_path}/
    FROM (
        {query}
    )
    OVERWRITE = TRUE
    FILE_FORMAT = (TYPE = 'parquet')
    HEADER = TRUE;
    """
    return copy_query


def _generate_s3_to_snowflake_copy_query(  # noqa: PLR0913
    schema: str,
    table_name: str,
    snowflake_stage_path: str,
    table_schema: List[Tuple[str, str]],
    overwrite: bool = True,
    auto_create_table: bool = True,
    use_logical_type: bool = True,
) -> str:
    """Generate SQL commands to load data from S3 to Snowflake table.

    This function generates a complete SQL script that includes:
    1. DROP TABLE IF EXISTS (if overwrite=True)
    2. CREATE TABLE IF NOT EXISTS (if auto_create_table=True or overwrite=True)
    3. COPY INTO command to load data from S3

    :param schema: Snowflake schema name (e.g., 'DATA_SCIENCE' or 'DATA_SCIENCE_STAGE')
    :param table_name: Target table name
    :param snowflake_stage_path: The path to the Snowflake stage where the data will be exported. This should include the stage name and any necessary subfolders (e.g., 'my_snowflake_stage/my_folder').
    :param table_schema: List of tuples with column names and types
    :param overwrite: If True, drop and recreate the table. Default True
    :param auto_create_table: If True, create the table if it doesn't exist. Default True
    :param use_logical_type: Whether to use Parquet logical types when reading the parquet files. Default True.
    :return: Complete SQL script with table management and COPY INTO commands
    """
    sql_statements = []

    # Step 1: Drop table if overwrite is True
    if overwrite:
        sql_statements.append(f"DROP TABLE IF EXISTS PATTERN_DB.{schema}.{table_name};")

    # Step 2: Create table if auto_create_table or overwrite
    if auto_create_table or overwrite:
        table_create_columns_str = ",\n ".join([f"{col_name} {col_type}" for col_name, col_type in table_schema])
        create_table_query = (
            f"""CREATE TABLE IF NOT EXISTS PATTERN_DB.{schema}.{table_name} ( {table_create_columns_str} );"""
        )
        sql_statements.append(create_table_query)

    # Step 3: Generate COPY INTO command
    columns_str = ",\n    ".join([f"PARSE_JSON($1):{col_name}::{col_type}" for col_name, col_type in table_schema])

    copy_query = f"""COPY INTO PATTERN_DB.{schema}.{table_name} FROM (
        SELECT {columns_str}
        FROM @{snowflake_stage_path} )
        FILE_FORMAT = (TYPE = 'parquet' USE_LOGICAL_TYPE = {use_logical_type});"""
    sql_statements.append(copy_query)

    # Combine all statements
    return "\n\n".join(sql_statements)


def _infer_snowflake_schema_from_df(df: pd.DataFrame) -> List[Tuple[str, str]]:
    """Infer Snowflake table schema from a pandas DataFrame.

    This function maps pandas data types to corresponding Snowflake data types.
    It returns a list of tuples, where each tuple contains a column name and its inferred Snowflake data type.

    :param df: Input pandas DataFrame
    :return: List of tuples with column names and inferred Snowflake data types
    """
    dtype_mapping = {
        "object": "TEXT",
        "int64": "NUMBER",
        "float64": "FLOAT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP_NTZ",
        "datetime64[ns, tz]": "TIMESTAMP_TZ",
        # Add more mappings as needed
    }

    schema = []
    for col_name, dtype in df.dtypes.items():
        dtype_str = str(dtype)
        snowflake_type = dtype_mapping.get(dtype_str, "STRING")  # Default to STRING if type is not mapped
        schema.append((col_name, snowflake_type))

    return schema


def publish_pandas(  # noqa: PLR0913 (too many arguments)
    table_name: str,
    df: pd.DataFrame,
    add_created_date: bool = False,
    chunk_size: Optional[int] = None,
    compression: Literal["snappy", "gzip"] = "gzip",
    warehouse: Optional[TWarehouse] = None,
    parallel: int = 4,
    quote_identifiers: bool = True,
    auto_create_table: bool = False,
    overwrite: bool = False,
    use_logical_type: bool = True,  # prevent date times with timezone from being written incorrectly
    use_utc: bool = True,
    use_s3_stage: bool = False,
    table_schema: Optional[List[Tuple[str, str]]] = None,
) -> None:
    """Store a pandas dataframe as a Snowflake table.

    :param table_name: Name of the table to create in Snowflake. The `write_pandas()` function does not create
        the table in upper case, when `auto_create_table` is set to True, so we need to do it manually for
        the sake of standardization.

    :param df: DataFrame to store

    :param add_created_date: When true, will add a column called `created_date` to the DataFrame with the current
        timestamp in UTC.

    :param chunk_size: Number of rows to be inserted once. If not provided, all rows will be dumped once.
        Default to None normally, 100,000 if inside a stored procedure.

    :param compression: The compression used on the Parquet files: gzip or snappy.
        Gzip gives supposedly a better compression, while snappy is faster. Use whichever is more appropriate.

    :param warehouse: The Snowflake warehouse to use for this operation. If not specified,
        it defaults to the `OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH` warehouse,
        when running in the Outerbounds **Default** perimeter, and to the
        `OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_XS_WH` warehouse, when running in the Outerbounds **PROD** perimeter.

    :param parallel: Number of threads to be used when uploading chunks. See details at parallel parameter.

    :param quote_identifiers: By default, identifiers, specifically database, schema, table and column names
        (from df.columns) will be quoted. If set to False, identifiers are passed on to Snowflake without quoting.
        I.e. identifiers will be coerced to uppercase by Snowflake. (Default value = True)

    :param auto_create_table: When true, will automatically create a table with corresponding columns for each column in
        the passed in DataFrame. The table will not be created if it already exists.

    :param overwrite: When true, and if `auto_create_table` is true, then it drops the table. Otherwise, it
        truncates the table. In both cases it will replace the existing contents of the table with that of the passed in
        Pandas DataFrame.

    :param use_logical_type: Boolean that specifies whether to use Parquet logical types when reading the
        parquet files for the uploaded pandas dataframe.

    :param use_utc: Whether to set the Snowflake session to use UTC time zone. Default is True.

    :param use_s3_stage: Whether to use the S3 stage method to publish the DataFrame, which is more efficient for large DataFrames.

    :param table_schema: Optional list of tuples specifying the column names and types for the Snowflake table.
        This is only used when `use_s3_stage` is True, and is required in that case. The list should be in the format: `[(col_name1, col_type1), (col_name2, col_type2), ...]`, where `col_type` is a valid Snowflake data type (e.g., 'STRING', 'NUMBER', 'TIMESTAMP_NTZ', etc.).
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    if df.empty:
        raise ValueError("DataFrame is empty.")

    if add_created_date:
        df["created_date"] = datetime.now().astimezone(pytz.utc)

    table_name = table_name.upper()
    schema = PROD_SCHEMA if current.is_production else NON_PROD_SCHEMA

    # Preview the DataFrame in the Metaflow card
    if warehouse is not None:
        current.card.append(Markdown(f"## Using Snowflake Warehouse: `{warehouse}`"))
    current.card.append(Markdown(f"## Publishing DataFrame to Snowflake table: `{table_name}`"))
    current.card.append(Table.from_dataframe(df.head()))

    conn: SnowflakeConnection = get_snowflake_connection(use_utc)

    # set warehouse
    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")

    # set query tag for cost tracking in select.dev
    # REASON: because write_pandas() doesn't allow modifying the SQL query to add SQL comments in it directly,
    # so we set a session query tag instead.
    tags = get_select_dev_query_tags()
    query_tag_str = json.dumps(tags)
    _execute_sql(conn, f"ALTER SESSION SET QUERY_TAG = '{query_tag_str}';")
    _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

    if use_s3_stage:
        if table_schema is None:
            raise ValueError("table_schema is required when use_s3_stage is True.")
        s3_bucket, snowflake_stage = _get_s3_config(current.is_production)
        data_folder = "publish_" + str(pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f"))
        s3_path = f"{s3_bucket}/{S3_DATA_FOLDER}/{data_folder}"
        sf_stage_path = f"{snowflake_stage}/{S3_DATA_FOLDER}/{data_folder}"

        # Write DataFrame to S3 as Parquet
        # Upload DataFrame to S3 as parquet files
        _put_df_to_s3_folder(
            df=df,
            path=s3_path,
            chunk_size=chunk_size,
            compression=compression,
        )

        # Generate and execute Snowflake SQL to load data from S3 to Snowflake
        copy_query = _generate_s3_to_snowflake_copy_query(
            schema=schema,
            table_name=table_name,
            snowflake_stage_path=sf_stage_path,
            table_schema=table_schema,
            overwrite=overwrite,
            auto_create_table=auto_create_table,
            use_logical_type=use_logical_type,
        )
        _execute_sql(conn, copy_query)

    # https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.Session.write_pandas
    write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        schema=schema,
        chunk_size=chunk_size,
        compression=compression,
        parallel=parallel,
        quote_identifiers=quote_identifiers,
        auto_create_table=auto_create_table,
        overwrite=overwrite,
        use_logical_type=use_logical_type,
    )

    # Add a link to the table in Snowflake to the card
    table_url = _make_snowflake_table_url(
        database="PATTERN_DB",
        schema=schema,
        table=table_name,
    )
    current.card.append(Markdown(f"[View table in Snowflake]({table_url})"))


def query_pandas_from_snowflake(
    query: Union[str, Path],
    warehouse: Optional[TWarehouse] = None,
    ctx: Optional[Dict[str, Any]] = None,
    use_utc: bool = True,
    use_s3_stage: bool = False,
) -> pd.DataFrame:
    """Returns a pandas dataframe from a Snowflake query.

    :param query: SQL query string or path to a .sql file.
    :param warehouse: The Snowflake warehouse to use for this operation. If not specified,
        it defaults to the `OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH` warehouse,
        when running in the Outerbounds **Default** perimeter, and to the
        `OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_XS_WH` warehouse, when running in the Outerbounds **PROD** perimeter.
    :param ctx: Context dictionary to substitute into the query string.
    :param use_utc: Whether to set the Snowflake session to use UTC time zone. Default is True.
    :param use_s3_stage: Whether to use the S3 stage method to query Snowflake, which is more efficient for large queries.
    :return: DataFrame containing the results of the query.

    **NOTE:** If the query contains `{schema}` placeholders, they will be replaced with the appropriate schema name.
    The schema name will be determined based on the current environment:
    - If in production, it will be set to `PROD_SCHEMA`.
    - If not in production, it will be set to `NON_PROD_SCHEMA`.
    - If the query does not contain `{schema}` placeholders, the schema name will not be modified.

    If the `ctx` dictionary is provided, it will be used to substitute values into the query string.
    The keys in the `ctx` dictionary should match the placeholders in the query string.
    """
    from ds_platform_utils._snowflake.write_audit_publish import (
        get_query_from_string_or_fpath,
        substitute_map_into_string,
    )

    schema = PROD_SCHEMA if current.is_production else NON_PROD_SCHEMA

    # adding query tags comment in query for cost tracking in select.dev
    tags = get_select_dev_query_tags()
    query_comment_str = f"\n\n/* {json.dumps(tags)} */"
    query = get_query_from_string_or_fpath(query)
    query = add_comment_to_each_sql_statement(query, query_comment_str)

    if "{{schema}}" in query or "{{ schema }}" in query:
        query = substitute_map_into_string(query, {"schema": schema})

    if ctx:
        query = substitute_map_into_string(query, ctx)

    # print query if DEBUG_QUERY env var is set
    _debug_print_query(query)

    if warehouse is not None:
        current.card.append(Markdown(f"## Using Snowflake Warehouse: `{warehouse}`"))
    current.card.append(Markdown("## Querying Snowflake Table"))
    current.card.append(Markdown(f"```sql\n{query}\n```"))

    conn: SnowflakeConnection = get_snowflake_connection(use_utc)
    if warehouse is not None:
        _execute_sql(conn, f"USE WAREHOUSE {warehouse};")
    _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

    if use_s3_stage:
        s3_bucket, snowflake_stage = _get_s3_config(current.is_production)
        data_folder = "query_" + str(pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f"))
        s3_path = f"{s3_bucket}/{S3_DATA_FOLDER}/{data_folder}"
        sf_stage_path = f"{snowflake_stage}/{S3_DATA_FOLDER}/{data_folder}"

        copy_query = _generate_snowflake_to_s3_copy_query(
            query=query,
            snowflake_stage_path=sf_stage_path,
        )
        # Copy data to S3
        _execute_sql(conn, copy_query)

        df = _get_df_from_s3_folder(s3_path)
    else:
        cursor_result = _execute_sql(conn, query)
        if cursor_result is None:
            # No statements to execute, return empty DataFrame
            df = pd.DataFrame()
        else:
            # force_return_table=True -- returns a Pyarrow Table always even if the result is empty
            result: pyarrow.Table = cursor_result.fetch_arrow_all(force_return_table=True)
            df = result.to_pandas()

    df.columns = df.columns.str.lower()
    current.card.append(Markdown("### Query Result"))
    current.card.append(Table.from_dataframe(df.head()))

    return df
