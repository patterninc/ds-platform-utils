import requests
import tenacity
from snowflake.connector import SnowflakeConnection

from metaflow import Snowflake


def get_snowflake_connection(
    schema: str = "DATA_SCIENCE_STAGE",
    is_utc: bool = True,
    query_tags: list[str] | None = None,
) -> SnowflakeConnection:
    """Get a Snowflake connection with optional query tags.

    :param schema: Schema to connect to
    :param is_utc: Whether to use UTC timezone
    :param query_tags: List of tags to apply to queries in this session
    """
    return _create_snowflake_connection(schema, is_utc, query_tags)


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
    wait=tenacity.wait_exponential(),
    stop=tenacity.stop_after_attempt(5),
)
def _create_snowflake_connection(
    schema: str,
    is_utc: bool,
    query_tags: list[str] | None = None,
) -> SnowflakeConnection:
    conn: SnowflakeConnection = Snowflake(
        integration="snowflake-default",
        schema=schema,
    ).cn  # type: ignore[attr-defined]

    # Set query tags
    all_tags = [configs.Repo.name]  # Start with repo name as default tag
    if query_tags:
        all_tags.extend(query_tags)

    tags_str = ";".join(all_tags)
    conn.execute_string(f"ALTER SESSION SET QUERY_TAG = '{tags_str}'")

    # Set timezone
    timezone_setting = "UTC" if is_utc else "DEFAULT"
    conn.execute_string(f"ALTER SESSION SET TIMEZONE = '{timezone_setting}';")

    return conn
