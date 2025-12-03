import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Union

import pandas as pd
import pyarrow
import pytz
from metaflow import current
from metaflow.cards import Markdown, Table
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas

from ds_platform_utils.metaflow._consts import NON_PROD_SCHEMA, PROD_SCHEMA
from ds_platform_utils.metaflow.get_snowflake_connection import _debug_print_query, get_snowflake_connection
from ds_platform_utils.metaflow.write_audit_publish import (
    _make_snowflake_table_url,
    add_comment_to_each_sql_statement,
    get_select_dev_query_tags,
)
from ds_platform_utils.shared.utils import run_sql

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
        # with conn.cursor() as cur:
        # cur.execute(f"USE WAREHOUSE {warehouse};")
        run_sql(conn, f"USE WAREHOUSE {warehouse};")

        # set query tag for cost tracking in select.dev
        # REASON: because write_pandas() doesn't allow modifying the SQL query to add SQL comments in it directly,
        # so we set a session query tag instead.
        tags = get_select_dev_query_tags()
        query_tag_str = json.dumps(tags)
        run_sql(conn, f"ALTER SESSION SET QUERY_TAG = '{query_tag_str}';")

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
) -> pd.DataFrame:
    """Returns a pandas dataframe from a Snowflake query.

    :param query: SQL query string or path to a .sql file.
    :param warehouse: The Snowflake warehouse to use for this operation. If not specified,
        it defaults to the `OUTERBOUNDS_DATA_SCIENCE_SHARED_DEV_XS_WH` warehouse,
        when running in the Outerbounds **Default** perimeter, and to the
        `OUTERBOUNDS_DATA_SCIENCE_SHARED_PROD_XS_WH` warehouse, when running in the Outerbounds **PROD** perimeter.
    :param ctx: Context dictionary to substitute into the query string.
    :param use_utc: Whether to set the Snowflake session to use UTC time zone. Default is True.
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

    # adding query tags comment in query for cost tracking in select.dev
    tags = get_select_dev_query_tags()
    query_comment_str = f"\n\n/* {json.dumps(tags)} */"
    query = get_query_from_string_or_fpath(query)
    query = add_comment_to_each_sql_statement(query, query_comment_str)

    if "{{schema}}" in query or "{{ schema }}" in query:
        schema = PROD_SCHEMA if current.is_production else NON_PROD_SCHEMA
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
        # cur.execute(f"USE WAREHOUSE {warehouse};")
        run_sql(conn, f"USE WAREHOUSE {warehouse};")

    # force_return_table=True -- returns a Pyarrow Table always even if the result is empty
    # result: pyarrow.Table = cur.execute(query).fetch_arrow_all(force_return_table=True)
    cursor_result = run_sql(conn, query)
    if cursor_result is None:
        # No statements to execute, return empty DataFrame
        df = pd.DataFrame()
    else:
        result: pyarrow.Table = cursor_result.fetch_arrow_all(force_return_table=True)
        df = result.to_pandas()
        df.columns = df.columns.str.lower()

    current.card.append(Markdown("### Query Result"))
    current.card.append(Table.from_dataframe(df.head()))

    return df
