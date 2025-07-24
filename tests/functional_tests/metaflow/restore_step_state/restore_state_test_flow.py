"""A Metaflow flow for testing restore_step_state.

Todo:
- automated tests
    - assert that state can be restored from a FAILED flow run, e.g. restore up to step 5 if
    step 5 failed--so we can iterate on that step to make it succeed.
    - pass a UUID to `random_param`, load the state, and asset that the state is restored
    and the UUID is the same.
- sample notebook in projne
    - Create a notebook in projen in the `sr/notebooks/` that is a "one time simple file"
    (similar to the src/sql/sample.sql dir--see that for reference). Have that notebook
    discover the directory of the `helpers` package and set its parent as the working directory
    with `os.chdir()` and then set the value of `THIS_DIR` based on that. Reason: so that copy/pasted
    code including calls to the `publish(query=THIS_DIR / "smthg.sql")` will work without modification.

"""

from pathlib import Path
from textwrap import dedent
from typing import Optional

from metaflow import Config, FlowSpec, Parameter, project, pypi_base, step
from pydantic import BaseModel, Field

from ds_platform_utils.metaflow import make_pydantic_parser_fn

THIS_DIR = Path(__file__).parent


class FlowConfig(BaseModel):
    """Config for the flow--provides validation and autocompletion."""

    n_rows: Optional[int] = Field(None, ge=1)
    table_name: str


@pypi_base(
    python="3.10",
    packages={
        "git+https://github.com/patterninc/ds-platform-utils.git": "@main",
        "git+https://github.com/patterninc/brain-platform-client.git": "",
    },
)
@project(name="example")
class TestRestoreFlowState(FlowSpec):
    """A sample flow."""

    config: FlowConfig = Config(
        name="config",
        default=str(THIS_DIR / "default.yaml"),
        parser=make_pydantic_parser_fn(FlowConfig),
    )  # type: ignore[assignment]

    random_param = Parameter("random_param", required=True, type=str)

    @step
    def start(self):
        """Start the flow."""
        print(f"{self.config.n_rows=}")
        self.next(self.execute_sql)

    @step
    def execute_sql(self):
        """Execute sample SQL query."""
        import os

        from ds_platform_utils.metaflow import publish

        os.environ["DEBUG_QUERY"] = "1"

        publish(
            table_name=self.config.table_name,
            query=dedent("""\
            -- Create a test table
            CREATE OR REPLACE TABLE PATTERN_DB.{{schema}}.{{table_name}} (
                id INT,
                message STRING
            );

            -- Insert a test row
            INSERT INTO PATTERN_DB.{{schema}}.{{table_name}} (id, message)
            VALUES (1, 'Hello from Snowflake!');

            -- Read the row
            SELECT * FROM PATTERN_DB.{{schema}}.{{table_name}};
            """),
        )

        self.next(self.query_and_publish_pandas)

    @step
    def query_and_publish_pandas(self):
        """Pandas operations with Snowflake using `ds_platform_utils` utility functions."""
        from ds_platform_utils.metaflow.pandas import publish_pandas, query_pandas_from_snowflake

        # Query data from Snowflake -- query must have jinja style placeholders
        # e.g. {{schema}} and {{table_name}}.
        self.df = query_pandas_from_snowflake(
            query="SELECT * FROM {{schema}}.{{table_name}}",
            ctx={"table_name": self.config.table_name},
        )

        print(f"Retrieved {len(self.df)} rows from Snowflake")

        # Process the data and publish DataFrame to Snowflake
        self.df["message"] = self.df["message"] + " - Processed from Metaflow"
        publish_pandas(
            table_name=f"{self.config.table_name}_PROCESSED",
            df=self.df,
            auto_create_table=True,
            overwrite=True,
        )

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    TestRestoreFlowState()
