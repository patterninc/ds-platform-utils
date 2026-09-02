"""Example flow for @uv_base / @uv.

Both decorators come from Metaflow itself -- ds-platform-utils ships them as a Metaflow
extension, so a flow imports nothing from this package:

    @uv_base    builds an image from uv.lock and gives it to every remote step
    @uv         gives a *local* step its own uv venv, scoped to a dependency group

A local step with no @uv runs in the interpreter that launched the flow -- your own venv, dev
group and all. A local step with @uv runs in a venv holding only its declared group, which is how
you catch an import that would be missing from the remote image. Remote steps (@kubernetes or
@batch) run in the baked image.

Run it from the repo root, so the uv.lock beside this file is the one that gets used:

    uv run python uv_demo_flow.py run

Deploy it, where every step becomes a pod and so every step gets an image:

    uv run python uv_demo_flow.py argo-workflows create

The first run builds and pushes an image, which takes a few minutes. After that the tag is in the
registry and every load is one API call, until uv.lock changes.
"""

import os
import sys

from metaflow import FlowSpec, kubernetes, resources, step, uv, uv_base


def _where_am_i() -> dict:
    """Report enough to tell an ambient venv, an isolated venv and a container apart."""
    try:
        import pytest

        has_pytest = pytest.__version__
    except ImportError:
        has_pytest = None
    return {
        "python": sys.version.split()[0],
        "uid": os.getuid(),
        "home": os.environ.get("HOME"),
        "executable": sys.executable,
        "pytest": has_pytest,
    }


@uv_base(aws_profile="sandbox")
class UvDemoFlow(FlowSpec):
    @step
    def start(self):
        """Local, no @uv: the venv that launched the flow -- dev group included."""
        self.ambient = _where_am_i()
        print(f"[ambient ] {self.ambient['executable']}")
        print(f"[ambient ] pytest={self.ambient['pytest']}")
        self.next(self.isolated, self.train)

    @uv
    @step
    def isolated(self):
        """Local, with @uv: its own venv, default group only -- so no pytest."""
        self.isolated_env = _where_am_i()
        print(f"[isolated] {self.isolated_env['executable']}")
        print(f"[isolated] pytest={self.isolated_env['pytest']}")
        self.next(self.join)

    @kubernetes(cpu=2, memory=4000)
    @step
    def train(self):
        """Remote: runs in the image @uv_base built from uv.lock."""
        import polars

        self.remote = _where_am_i()
        self.polars = polars.__version__
        print(f"[remote  ] uid={self.remote['uid']} HOME={self.remote['home']} polars={self.polars}")
        self.next(self.join)

    @step
    def join(self, inputs):
        """Local: joins run wherever the flow is driven from.

        `inputs` holds only the branches that fan in here -- `isolated` and `train` -- not
        `start`. What `start` set propagates down both branches instead, so it is read off one of
        them rather than off `start` directly.
        """
        self.ambient = inputs.isolated.ambient
        self.isolated_env = inputs.isolated.isolated_env
        self.remote = inputs.train.remote
        self.polars = inputs.train.polars
        self.next(self.end)

    @resources(cpu=4)
    @step
    def end(self):
        """Local: @resources sizes a step, it does not place one."""
        # the three environments must be genuinely different, or the split is not working
        assert self.ambient["pytest"] is not None, "expected the dev group in the ambient venv"
        assert self.isolated_env["pytest"] is None, "@uv did not isolate: dev group leaked in"
        assert self.isolated_env["executable"] != self.ambient["executable"], "@uv reused the ambient venv"
        assert self.remote["uid"] == 1000, self.remote["uid"]
        assert self.remote["home"] == "/metaflow", self.remote["home"]
        assert self.polars, "polars missing from the image"
        print("UV_DEMO_OK")
        print(f"  ambient  {self.ambient['executable']}  pytest={self.ambient['pytest']}")
        print(f"  isolated {self.isolated_env['executable']}  pytest={self.isolated_env['pytest']}")
        print(f"  remote   uid={self.remote['uid']} polars={self.polars}")


if __name__ == "__main__":
    UvDemoFlow()
