import os
from typing import Literal, Optional, Union

from metaflow import Snowflake, current
from snowflake.connector import SnowflakeConnection

####################
# --- Metaflow --- #
####################

# an integration with this name exists both in the default and prod perimeters
SNOWFLAKE_INTEGRATION = "snowflake-default"


def get_snowflake_warehouse(
    warehouse: str,
) -> Optional[str]:
    if not warehouse:
        warehouse = "XS"

    if warehouse.upper() in ["XS", "MED", "XL"]:
        domain = "ADS" if current.is_running_flow and "ds.domain:advertising" in current.tags else "SHARED"
        env = "PROD" if current.is_production else "DEV"
        warehouse = f"OUTERBOUNDS_DATA_SCIENCE_{domain}_{env}_{warehouse}_WH"
    return warehouse.upper()


# @lru_cache
def get_snowflake_connection(
    warehouse: Optional[Union[Literal["XS", "MED", "XL"], str]] = None,
    use_utc: bool = True,
) -> SnowflakeConnection:
    """Return a singleton Snowflake cursor.

    Why do we have this?

    1. We want to abstract away Snowflake creation logic from the DS
       because we want to ensure that

       - it always uses the "snowflake-default" integration.
         AKA the role will always be correct based on whether the metaflow Flow calling this
         function is running in prod (the Outerbounds platform) or non-prod (local dev, CI, etc.)

        - other standard metadata are set, e.g. universal, automatically set tags for all queries

    2. Outerbounds often fails when creating a snowflake connection due to a mysterious DNS
       resolution error that they have not fixed. Using @lru_cache makes it so this function
       always returns the same connection object for a given set of parameters. This allows
       us to easily re-use the same connection object without having to explicitly pass it into
       every function, e.g. publish(conn=), publish_pandas(conn=), etc.

    Note: the connection object returned by this function is not manually closed.
    That is okay. The Snowflake SDK automatically closes any unclosed connection objects
    when the Python process exists (which the exception of ^C SIGTERM aka manual interrupt signals).
    In metaflow, each step is a separate Python process, so the connection will automatically be
    closed at the end of any steps that use this singleton.
    """
    if current and hasattr(current, "project_name"):
        query_tag = current.project_name
    else:
        query_tag = None

    return _create_snowflake_connection(
        warehouse=get_snowflake_warehouse(warehouse),
        use_utc=use_utc,
        query_tag=query_tag,
    )


#####################
# --- Snowflake --- #
#####################


def _create_snowflake_connection(
    warehouse: str,
    use_utc: bool,
    query_tag: Optional[str] = None,
) -> SnowflakeConnection:
    conn: SnowflakeConnection = Snowflake(
        integration=SNOWFLAKE_INTEGRATION,
        client_session_keep_alive=True,
        warehouse=warehouse,
        timezone="UTC" if use_utc else None,
        session_parameters={"QUERY_TAG": query_tag},
    ).cn  # type: ignore[attr-defined]

    return conn


def _debug_print_query(query: str) -> None:
    """Print query if DEBUG_QUERY env var is set.

    :param query: SQL query to print
    """
    if os.getenv("DEBUG_QUERY"):
        print("\n=== DEBUG SQL QUERY ===")
        print(query)
        print("=====================\n")
