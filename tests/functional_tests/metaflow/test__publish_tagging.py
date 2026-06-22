"""Functional test: ownership object-tagging applied by publish() / publish_pandas().

Publishes a pandas DataFrame and a WAP table, each with a ``tags=`` override, then verifies
the ownership tags landed using the real-time ``INFORMATION_SCHEMA.TAG_REFERENCES`` function.

This runs as a normal (non-production) flow, so the tables land in ``DATA_SCIENCE_STAGE`` and
tagging happens there. The tag *definitions* live in ``DATA_SCIENCE`` -- Snowflake applies a
``DATA_SCIENCE``-defined tag to a ``DATA_SCIENCE_STAGE`` object fine.

Preconditions (slow test against a live Snowflake account):
  * A role that can write ``DATA_SCIENCE_STAGE`` and has ``APPLY`` on the seven ``TABLE_*`` tags.
  * The tag definitions already exist (RFC §3 admin setup). Without them (or without APPLY),
    tagging is skipped-with-a-warning and the assertions below fail -- the intended signal that
    setup is incomplete.
"""

import subprocess
import sys

import pytest
from metaflow import FlowSpec, project, step

# These flows run non-prod (--environment=local run), so tables land in the stage schema.
TABLE_SCHEMA = "DATA_SCIENCE_STAGE"

PANDAS_TABLE = "DS_PLATFORM_UTILS_TAGGING_TEST_PUBLISH_PANDAS"
WAP_TABLE = "DS_PLATFORM_UTILS_TAGGING_TEST_PUBLISH"

# publish_pandas: no owner override -> owner is derived from the domain (ds-<domain>-team).
PANDAS_TAGS = {"sla": "daily", "contact": "mlplatformteam@pattern.com"}
PANDAS_EXPECTED = {
    "TABLE_OWNER": "ds-ml-platform-team",  # derived from ds.domain:ml-platform
    "TABLE_TEAM": "data-science",
    "TABLE_DOMAIN": "ml-platform",
    "TABLE_PROJECT": "ds-platform-utils-tests",
    "TABLE_STATUS": "active",
    "TABLE_SLA": "daily",
    "TABLE_CONTACT": "mlplatformteam@pattern.com",
}

# publish: explicit owner override -> the override wins over the domain derivation.
WAP_TAGS = {"owner": "mlplatform_team", "sla": "daily", "contact": "mlplatformteam@pattern.com"}
WAP_EXPECTED = {**PANDAS_EXPECTED, "TABLE_OWNER": "mlplatform_team"}


@project(name="ds_platform_utils_tests")
class TestPublishTaggingFlow(FlowSpec):
    """Publish two tables with ownership tags, then verify the tags were applied."""

    @step
    def start(self):
        """Publish a pandas DataFrame with a tags override (owner derived from domain)."""
        import pandas as pd

        from ds_platform_utils.metaflow import publish_pandas

        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Mario", "Luigi", "Peach", "Bowser", "Toad"],
                "score": [90.5, 85.2, 88.7, 92.1, 78.9],
            }
        )

        publish_pandas(
            table_name=PANDAS_TABLE,
            df=df,
            auto_create_table=True,
            overwrite=True,
            tags=PANDAS_TAGS,
        )

        self.next(self.publish_step)

    @step
    def publish_step(self):
        """Publish a WAP table with an explicit owner override."""
        from ds_platform_utils.metaflow import publish

        query = """
        CREATE OR REPLACE TABLE PATTERN_DB.{{schema}}.{{table_name}} (
            id INT,
            name STRING
        );

        INSERT INTO PATTERN_DB.{{schema}}.{{table_name}} (id, name)
        VALUES (1, 'Mario'), (2, 'Luigi'), (3, 'Peach');
        """

        publish(
            table_name=WAP_TABLE,
            query=query,
            tags=WAP_TAGS,
        )

        self.next(self.verify_tags)

    @step
    def verify_tags(self):
        """Read tags back via the real-time INFORMATION_SCHEMA.TAG_REFERENCES function."""
        for table_name, expected in ((PANDAS_TABLE, PANDAS_EXPECTED), (WAP_TABLE, WAP_EXPECTED)):
            actual = _fetch_tags(table_name)
            for tag, expected_value in expected.items():
                assert actual.get(tag) == expected_value, (
                    f"{table_name}: expected {tag}={expected_value!r}, got {actual.get(tag)!r}. All tags: {actual}"
                )

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


def _fetch_tags(table_name: str) -> dict:
    """Return ``{TAG_NAME: TAG_VALUE}`` for a stage table using the real-time tag-references function."""
    from ds_platform_utils.metaflow import query_pandas_from_snowflake

    query = f"""
    SELECT tag_name, tag_value
    FROM TABLE(
        INFORMATION_SCHEMA.TAG_REFERENCES('PATTERN_DB.{TABLE_SCHEMA}.{table_name}', 'TABLE')
    );
    """
    df = query_pandas_from_snowflake(query)
    # query_pandas_from_snowflake lower-cases column names.
    return {row.tag_name: row.tag_value for row in df.itertuples()}


if __name__ == "__main__":
    TestPublishTaggingFlow()


@pytest.mark.slow
def test_publish_tagging_flow():
    """Run the flow and assert the ownership tags are applied and readable."""
    cmd = [
        sys.executable,
        __file__,
        "--environment=local",
        "--with=card",
        "run",
        "--tag=ds.domain:ml-platform",
        "--tag=ds.project:ds-platform-utils-tests",
    ]

    print("\n=== Metaflow Output ===")
    for line in execute_with_output(cmd):
        print(line, end="")


def execute_with_output(cmd):
    """Execute a command and yield output lines as they are produced."""
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Merge stderr into stdout
        universal_newlines=True,
        bufsize=1,
    )

    for line in iter(process.stdout.readline, ""):
        yield line

    process.stdout.close()
    return_code = process.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)
