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
    DEV_SCHEMA,
    PROD_SCHEMA,
)
from ds_platform_utils.metaflow.s3 import _get_df_from_s3_folder, _put_df_to_s3_folder
from ds_platform_utils.metaflow.s3_stage import (
    _copy_s3_to_snowflake,
    _copy_snowflake_to_s3,
    _generate_s3_stage_paths,
)
from ds_platform_utils.metaflow.snowflake_connection import get_snowflake_connection
from ds_platform_utils.metaflow.write_audit_publish import (
    _make_snowflake_table_url,
)
from ds_platform_utils.pandas_utils import estimate_chunk_size
from ds_platform_utils.sql_utils import get_query_from_string_or_fpath, substitute_map_into_string


def publish_pandas(  # noqa: PLR0913 (too many arguments)
    table_name: str,
    df: pd.DataFrame,
    add_created_date: bool = False,
    chunk_size: Optional[int] = None,
    compression: Literal["snappy", "gzip"] = "snappy",
    warehouse: Optional[Literal["XS", "MED", "XL"]] = None,
    parallel: int = 4,
    quote_identifiers: bool = False,
    auto_create_table: bool = False,
    overwrite: bool = False,
    use_logical_type: bool = True,  # prevent date times with timezone from being written incorrectly
    use_utc: bool = True,
    use_s3_stage: bool = False,
    table_definition: Optional[List[Tuple[str, str]]] = None,
) -> None:
    """Store a pandas dataframe as a Snowflake table.

    :param table_name: Name of the table to create in Snowflake. The `write_pandas()` function does not create
        the table in upper case, when `auto_create_table` is set to True, so we need to do it manually for
        the sake of standardization.

    :param df: DataFrame to store

    :param add_created_date: When true, will add a column called `created_date` to the DataFrame with the current
        timestamp in UTC.

    :param chunk_size: Number of rows to be inserted once. If not provided, the chunk size will be
        automatically estimated based on the DataFrame's memory usage.

    :param compression: The compression used on the Parquet files: gzip or snappy.
        Gzip gives supposedly a better compression, while snappy is faster. Use whichever is more appropriate.

    :param warehouse: The Snowflake warehouse to use for this operation. If not specified,
        it defaults to the `OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH` warehouse,
        when running in the Outerbounds **Default** perimeter, and to the
        `OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_XS_WH` warehouse, when running in the Outerbounds **PROD** perimeter.

    :param parallel: Number of threads to be used when uploading chunks. See details at parallel parameter.

    :param quote_identifiers: If set to True, identifiers, specifically database, schema, table and column names
        (from df.columns) will be quoted. If set to False (default), identifiers are passed on to Snowflake without
        quoting, i.e. identifiers will be coerced to uppercase by Snowflake.

    :param auto_create_table: When true, will automatically create a table with corresponding columns for each column in
        the passed in DataFrame. The table will not be created if it already exists.

    :param overwrite: When true, and if `auto_create_table` is true, then it drops the table. Otherwise, it
        truncates the table. In both cases it will replace the existing contents of the table with that of the passed in
        Pandas DataFrame.

    :param use_logical_type: Boolean that specifies whether to use Parquet logical types when reading the
        parquet files for the uploaded pandas dataframe.

    :param use_utc: Whether to set the Snowflake session to use UTC time zone. Default is True.

    :param use_s3_stage: Whether to use the S3 stage method to publish the DataFrame, which is more efficient for large DataFrames.

    :param table_definition: Optional list of tuples specifying the column names and types for the Snowflake table.
        This is only used when `use_s3_stage` is True, and is required in that case. The list should be in the format: `[(col_name1, col_type1), (col_name2, col_type2), ...]`, where `col_type` is a valid Snowflake data type (e.g., 'STRING', 'NUMBER', 'TIMESTAMP_NTZ', etc.).
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    if df.empty:
        raise ValueError("DataFrame is empty.")

    if add_created_date:
        df["created_date"] = datetime.now().astimezone(pytz.utc)

    if chunk_size is not None and chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer.")

    if chunk_size is None:
        chunk_size = estimate_chunk_size(df)

    table_name = table_name.upper()
    schema = PROD_SCHEMA if current.is_production else DEV_SCHEMA

    # Preview the DataFrame in the Metaflow card
    if warehouse is not None:
        current.card.append(Markdown(f"## Using Snowflake Warehouse: `{warehouse}`"))
    current.card.append(Markdown(f"## Publishing DataFrame to Snowflake table: `{table_name}`"))
    current.card.append(Table.from_dataframe(df.head()))

    if use_s3_stage:
        s3_path, _ = _generate_s3_stage_paths()

        # Upload DataFrame to S3 as parquet files
        _put_df_to_s3_folder(
            df=df,
            path=s3_path,
            chunk_size=chunk_size,
            compression=compression,
        )

        _copy_s3_to_snowflake(
            s3_path=s3_path,
            table_name=table_name,
            table_definition=table_definition,
            warehouse=warehouse,
            use_utc=use_utc,
            auto_create_table=auto_create_table,
            overwrite=overwrite,
            use_logical_type=use_logical_type,
        )

    else:
        conn: SnowflakeConnection = get_snowflake_connection(warehouse=warehouse, use_utc=use_utc)
        _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")

        # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#module-snowflake-connector-pandas-tools
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
        conn.close()

    # Add a link to the table in Snowflake to the card
    table_url = _make_snowflake_table_url(
        database="PATTERN_DB",
        schema=schema,
        table=table_name,
    )
    current.card.append(Markdown(f"[View table in Snowflake]({table_url})"))


def query_pandas_from_snowflake(
    query: Union[str, Path],
    warehouse: Optional[Literal["XS", "MED", "XL"]] = None,
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
    schema = PROD_SCHEMA if current.is_production else DEV_SCHEMA

    # adding query tags comment in query for cost tracking in select.dev

    query = get_query_from_string_or_fpath(query)

    query = substitute_map_into_string(query, (ctx or {}) | {"schema": schema})

    if warehouse is not None:
        current.card.append(Markdown(f"## Using Snowflake Warehouse: `{warehouse}`"))
    current.card.append(Markdown("## Querying Snowflake Table"))
    current.card.append(Markdown(f"```sql\n{query}\n```"))

    if use_s3_stage:
        s3_path = _copy_snowflake_to_s3(
            query=query,
            warehouse=warehouse,
            use_utc=use_utc,
        )
        df = _get_df_from_s3_folder(s3_path)
    else:
        conn: SnowflakeConnection = get_snowflake_connection(warehouse=warehouse, use_utc=use_utc)
        _execute_sql(conn, f"USE SCHEMA PATTERN_DB.{schema};")
        cursor_result = _execute_sql(conn, query)
        if cursor_result is None:
            # No statements to execute, return empty DataFrame
            df = pd.DataFrame()
        else:
            # force_return_table=True -- returns a Pyarrow Table always even if the result is empty
            result: pyarrow.Table = cursor_result.fetch_arrow_all(force_return_table=True)
            df = result.to_pandas()
        conn.close()
    df.columns = df.columns.str.lower()
    current.card.append(Markdown("### Query Result"))
    current.card.append(Table.from_dataframe(df.head()))

    return df
