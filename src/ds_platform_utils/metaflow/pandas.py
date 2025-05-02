from pathlib import Path
from typing import Literal, Optional, Union

import pandas as pd
import pyarrow
from metaflow import Snowflake, current
from snowflake.connector.pandas_tools import write_pandas

from ds_platform_utils.consts import NON_PROD_SCHEMA, PROD_SCHEMA, SNOWFLAKE_INTEGRATION
from ds_platform_utils.snowflake.write_audit_publish import get_query_from_string_or_fpath


def publish_pandas(  # noqa: PLR0913 (too many arguments)
    table_name: str,
    df: pd.DataFrame,
    chunk_size: Optional[int] = None,
    compression: Literal["snappy", "gzip"] = "gzip",
    parallel: int = 4,
    use_logical_type: bool = True,  # prevent date times with timezone from being written incorrectly
) -> None:
    """Store a pandas dataframe as a Snowflake table.

    :param table_name: Name of the table to create in Snowflake
    :param df: DataFrame to store
    :param chunk_size: Number of rows to be inserted once. If not provided, all rows will be dumped once.
        Default to None normally, 100,000 if inside a stored procedure.
    :param compression: The compression used on the Parquet files: gzip or snappy.
        Gzip gives supposedly a better compression, while snappy is faster. Use whichever is more appropriate.
    :param: parallel: Number of threads to be used when uploading chunks. See details at parallel parameter.
    :param use_logical_type: Boolean that specifies whether to use Parquet logical types when reading the
        parquet files for the uploaded pandas dataframe.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    if df.empty:
        raise ValueError("DataFrame is empty.")

    with Snowflake(integration=SNOWFLAKE_INTEGRATION) as conn:
        # https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.Session.write_pandas
        write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            chunk_size=chunk_size,
            compression=compression,
            parallel=parallel,
            schema=PROD_SCHEMA if current.is_production else NON_PROD_SCHEMA,
            use_logical_type=use_logical_type,
        )


def fetch_pandas_from_snowflake(query: Union[str, Path]) -> pd.DataFrame:
    """Returns a pandas dataframe from a Snowflake query.

    :param query: SQL query string or path to a .sql file.
    :return: DataFrame containing the results of the query.
    """
    query = get_query_from_string_or_fpath(query)
    with Snowflake(integration=SNOWFLAKE_INTEGRATION) as conn:
        # force_return_table=True -- returns a Pyarrow Table always even if the result is empty
        result: pyarrow.Table = conn.cursor().execute(query).fetch_arrow_all(force_return_table=True)
        if not result:
            raise ValueError("Query returned no results")

        df = result.to_pandas()
        df.columns = df.columns.str.lower()
        return df
