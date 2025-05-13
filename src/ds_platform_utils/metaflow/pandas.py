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

# if TYPE_CHECKING:
# from ds_platform_utils._snowflake.write_audit_publish import (
#     get_query_from_string_or_fpath,
#     substitute_map_into_string,
# )
from ds_platform_utils.metaflow._consts import NON_PROD_SCHEMA, PROD_SCHEMA
from ds_platform_utils.metaflow.get_snowflake_connection import get_snowflake_connection


def publish_pandas(  # noqa: PLR0913 (too many arguments)
    table_name: str,
    df: pd.DataFrame,
    add_created_date: bool = False,
    chunk_size: Optional[int] = None,
    compression: Literal["snappy", "gzip"] = "gzip",
    parallel: int = 4,
    auto_create_table: bool = False,
    overwrite: bool = False,
    use_logical_type: bool = True,  # prevent date times with timezone from being written incorrectly
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

    :param parallel: Number of threads to be used when uploading chunks. See details at parallel parameter.

    :param auto_create_table: When true, will automatically create a table with corresponding columns for each column in
        the passed in DataFrame. The table will not be created if it already exists.

    :param overwrite: When true, and if `auto_create_table` is true, then it drops the table. Otherwise, it
        truncates the table. In both cases it will replace the existing contents of the table with that of the passed in
        Pandas DataFrame.

    :param use_logical_type: Boolean that specifies whether to use Parquet logical types when reading the
        parquet files for the uploaded pandas dataframe.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    if df.empty:
        raise ValueError("DataFrame is empty.")

    if add_created_date:
        df["created_date"] = datetime.now().astimezone(pytz.utc)

    current.card.append(Markdown(f"### Publishing DataFrame to Snowflake table: `{table_name}`"))
    current.card.append(Table.from_dataframe(df.head()))

    conn: SnowflakeConnection = get_snowflake_connection()
    # https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.Session.write_pandas
    write_pandas(
        conn=conn,
        df=df,
        table_name=table_name.upper(),
        schema=PROD_SCHEMA if current.is_production else NON_PROD_SCHEMA,
        chunk_size=chunk_size,
        compression=compression,
        parallel=parallel,
        auto_create_table=auto_create_table,
        overwrite=overwrite,
        use_logical_type=use_logical_type,
    )


def query_pandas_from_snowflake(
    query: Union[str, Path],
    ctx: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Returns a pandas dataframe from a Snowflake query.

    :param query: SQL query string or path to a .sql file.
    :param ctx: Context dictionary to substitute into the query string.
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

    query = get_query_from_string_or_fpath(query)
    if "{{schema}}" in query:
        schema = PROD_SCHEMA if current.is_production else NON_PROD_SCHEMA
        query = substitute_map_into_string(query, {"schema": schema})

    if ctx:
        query = substitute_map_into_string(query, ctx)

    current.card.append(Markdown("### Querying Snowflake table"))
    current.card.append(Markdown(f"```sql\n{query}\n```"))

    try:
        conn: SnowflakeConnection = get_snowflake_connection()
        # force_return_table=True -- returns a Pyarrow Table always even if the result is empty
        result: pyarrow.Table = conn.cursor().execute(query).fetch_arrow_all(force_return_table=True)
        # if not result:
        #     raise ValueError("Query returned no results")

        df = result.to_pandas()
        df.columns = df.columns.str.lower()

        current.card.append(Markdown("### Query Result"))
        current.card.append(Table.from_dataframe(df.head()))

        return df
    finally:
        conn.close()
