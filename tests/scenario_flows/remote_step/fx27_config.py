"""The shape production flows actually have.

Taken from out-of-stock's predict flow: a pydantic-validated `Config`, a
flow-level `@project` and `@schedule`, `@pypi_base` carrying private git
dependencies, and a custom flow mutator -- all around a `@remote_step`.

`Config` is the single most-used Metaflow feature in these repos (152
occurrences) and nothing else in this suite touched it. Like `Parameter` it is
a class-level descriptor, so the driver has to collect it deliberately; unlike
`Parameter` its value is a nested object, which is exactly the kind of thing a
naive "is it picklable" filter drops.
"""

from pathlib import Path

from ds_platform_utils.metaflow import make_pydantic_parser_fn
from metaflow import Config, FlowSpec, current, project, pypi_base, remote_step, resources, schedule, step
from pydantic import BaseModel, Field

from _check import check

THIS_DIR = Path(__file__).parent


class FlowConfig(BaseModel):
    """Validated config, as the real flows declare it."""

    n_rows: int | None = Field(None, ge=1)
    table_name: str = "DEFAULT_TABLE"
    horizons: list[int] = Field(default_factory=list)
    nested: dict = Field(default_factory=dict)


# Exactly how out-of-stock declares it: the private git dependency listed in
# @pypi_base, so every step -- plain ones included -- has the package that
# provides make_pydantic_parser_fn. Without it a plain step under fast-bakery
# gets only ob-metaflow and dies on the module-level import, which is what a
# user meets first.
@pypi_base(
    python="3.11",
    packages={
        "pydantic": "",
        "git+https://github.com/patterninc/ds-platform-utils.git": "@remote-step-eks",
    },
)
@project(name="remote_step_config_probe")
@schedule(cron="15 8 * * *", timezone="UTC")
class Fx27Config(FlowSpec):
    config: FlowConfig = Config(
        name="config",
        default=str(THIS_DIR / "configs/default.yaml"),
        parser=make_pydantic_parser_fn(FlowConfig),
    )  # type: ignore[assignment]

    HORIZONS = [7, 14, 28]

    @step
    def start(self):
        # What the driver resolved, to compare against the pod.
        self.driver_config = {
            "n_rows": self.config.n_rows,
            "table_name": self.config.table_name,
            "horizons": list(self.config.horizons),
        }
        print(f"[fx27] driver config: {self.driver_config}", flush=True)
        self.next(self.work)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        # The config object itself must be usable in the pod, with attribute
        # access and types intact -- not a dict, not a string.
        check("config n_rows", int(self.config.n_rows), 250)
        check("config table_name", str(self.config.table_name), "FX27_PROBE")
        check("config list field", list(self.config.horizons), [7, 14, 28])
        check("config nested mapping", dict(self.config.nested).get("label"), "from-config")
        check("nested float survived", float(dict(self.config.nested)["alpha"]), 0.5)

        # A class attribute that is not a Config or Parameter at all.
        check("plain class attribute", list(self.HORIZONS), [7, 14, 28])

        # @project context, which travels with the schedule in production.
        check("project name in the pod", current.project_name, "remote_step_config_probe")
        check("branch name present", current.branch_name, predicate=lambda b: bool(b))

        self.pod_config = {
            "n_rows": int(self.config.n_rows),
            "table_name": str(self.config.table_name),
            "horizons": list(self.config.horizons),
        }
        self.next(self.end)

    @step
    def end(self):
        check("pod and driver agree on config", dict(self.pod_config), dict(self.driver_config))
        print("[fx27] OK")


if __name__ == "__main__":
    Fx27Config()
