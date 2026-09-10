"""@uv_pypi_base: the pod's environment resolved from uv.lock.

Two things at once, and the second is the one that has bitten before.

The flow's own dependencies -- pandas here -- must arrive at the exact locked
version, so the pod cannot drift from `uv sync` locally. That matters because
an artifact pickled by one pandas and unpickled by another is the failure
gaps.md 10b is about.

And ds-platform-utils is a **private git dependency**, so uv resolves it to a
direct git reference and cloning it inside the pod needs GITHUB_TOKEN. The
driver forwards that token; without it the install dies at
STAGE=uv_pip_install. Nothing else in this suite proves the private-package
path.
"""

from ds_platform_utils.metaflow import uv_pypi_base
from metaflow import FlowSpec, remote_step, resources, step

from _check import check


@uv_pypi_base
class Fx19UvPypiBase(FlowSpec):
    @step
    def start(self):
        import pandas as pd

        # What the driver resolved, to compare against the pod.
        self.driver_pandas = pd.__version__
        print(f"[fx19] driver pandas {self.driver_pandas}", flush=True)
        self.next(self.work)

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def work(self):
        import pandas as pd

        print(f"[fx19] pod pandas {pd.__version__}", flush=True)
        check("pandas is present in the pod", pd.__version__, predicate=lambda v: bool(v))
        check("pod pandas matches the driver's", pd.__version__, str(self.driver_pandas))

        # The private package: importable only if the pod cloned it over
        # https with the forwarded token.
        import ds_platform_utils
        from ds_platform_utils.metaflow import uv_pypi_base as _probe

        print(f"[fx19] private package present: {ds_platform_utils.__name__}", flush=True)
        check("private git dependency installed", callable(_probe), True)

        # And it can actually do work, not just import.
        frame = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        check("pandas is functional", int(frame["a"].sum()), 6)

        self.pod_pandas = pd.__version__
        self.rows = len(frame)
        self.next(self.end)

    @step
    def end(self):
        check("versions agree end to end", str(self.pod_pandas), str(self.driver_pandas))
        check("work happened", int(self.rows), 3)
        print("[fx19] OK")


if __name__ == "__main__":
    Fx19UvPypiBase()
