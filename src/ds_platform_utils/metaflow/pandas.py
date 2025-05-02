from pathlib import Path
from typing import Optional, Union

import pandas as pd
import pyarrow
from metaflow import Snowflake
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas

from ds_platform_utils.consts import NON_PROD_SCHEMA, PROD_SCHEMA, SNOWFLAKE_INTEGRATION
from ds_platform_utils.snowflake.write_audit_publish import get_query_from_string_or_fpath


def publish_pandas(
    table_name: str,
    df: pd.DataFrame,
    is_production: bool = False,  # Should be use if current.is_production instead of passing this param?
    use_logical_type: bool = True,  # prevent date times with timezone from being written incorrectly
    conn: Optional[SnowflakeConnection] = None,
) -> None:
    """Store a pandas dataframe as a Snowflake table.

    :param table_name: str: Name of the table to create in Snowflake
    :param df: pd.DataFrame:  DataFrame to store
    :param is_production: bool: If True, PROD_SCHEMA will be used, else NON_PROD_SCHEMA.
    :param use_logical_type: bool:  Boolean that specifies whether to use Parquet logical types when reading the
        parquet files for the uploaded pandas dataframe.
    :param conn: SnowflakeConnection: Snowflake connection to use.
        If None, a new connection will be created.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    if df.empty:
        raise ValueError("DataFrame is empty.")

    try:
        if not conn:
            conn = Snowflake(integration=SNOWFLAKE_INTEGRATION).cn

        # https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.Session.write_pandas
        write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=PROD_SCHEMA if is_production else NON_PROD_SCHEMA,
            use_logical_type=use_logical_type,
        )
    finally:
        conn.close()


def read_pandas(
    query: Union[str, Path],
    conn: Optional[SnowflakeConnection] = None,
) -> pd.DataFrame:
    """Returns a pandas dataframe from a Snowflake query.

    :param query: str | Path: SQL query string or path to a .sql file.
    :param conn: SnowflakeConnection: Snowflake connection to use.
        If None, a new connection will be created.
    :return: pd.DataFrame: DataFrame containing the results of the query.
    """
    query = get_query_from_string_or_fpath(query)
    try:
        if not conn:
            conn = Snowflake(integration=SNOWFLAKE_INTEGRATION).cn

        # force_return_table=True -- returns a Pyarrow Table always even if the result is empty
        result: pyarrow.Table = conn.cursor().execute(query).fetch_arrow_all(force_return_table=True)
        if not result:
            raise ValueError("Query returned no results")

        df = result.to_pandas()
        df.columns = df.columns.str.lower()
        return df
    finally:
        conn.close()
