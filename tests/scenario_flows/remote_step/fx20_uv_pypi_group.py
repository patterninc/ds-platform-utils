"""@uv_pypi(dependency_groups=[...]) on one step only.

uv keeps dependency groups in a separate table, so they are excluded unless a
step names them. This proves both halves: the step that asks for `extra` gets
orjson, and the step that does not, does not -- otherwise every pod pays for
every group in the project.

`start` and `end` carry a bare `@uv_pypi` even though they need nothing from
the project. Under fast-bakery every step is baked its own environment, and a
step with no pypi decorator gets one holding metaflow alone -- but the flow
module's own `from ds_platform_utils.metaflow import uv_pypi` runs in EVERY
task, including that one. Leaving them plain fails the run at `start` with
`ModuleNotFoundError: No module named 'ds_platform_utils'`, before any of the
group logic is reached. `@uv_pypi_base` (fx19) covers every step at once and
sidesteps it; per-step `@uv_pypi` does not, so every step that executes the
module has to name it.
"""

from ds_platform_utils.metaflow import uv_pypi
from metaflow import FlowSpec, remote_step, resources, step

from _check import check


class Fx20UvPypiGroup(FlowSpec):
    @uv_pypi
    @step
    def start(self):
        self.next(self.with_group)

    @uv_pypi(dependency_groups=["extra"])
    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def with_group(self):
        import orjson
        import pandas as pd

        print(f"[fx20] with group: orjson {orjson.__version__}, pandas {pd.__version__}", flush=True)
        check("the named group's package is present", orjson.__version__, predicate=lambda v: bool(v))
        check("runtime deps are present too", pd.__version__, predicate=lambda v: bool(v))
        self.had_orjson = True
        self.next(self.without_group)

    @uv_pypi
    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def without_group(self):
        import pandas as pd

        check("runtime deps still present", pd.__version__, predicate=lambda v: bool(v))
        try:
            import orjson  # noqa: F401

            present = True
        except ImportError:
            present = False
        print(f"[fx20] without group: orjson present = {present}", flush=True)
        check("an unnamed group is NOT installed", present, False)
        self.had_orjson_without = present
        self.next(self.end)

    @uv_pypi
    @step
    def end(self):
        check("group step had orjson", bool(self.had_orjson), True)
        check("non-group step did not", bool(self.had_orjson_without), False)
        print("[fx20] OK")


if __name__ == "__main__":
    Fx20UvPypiGroup()
