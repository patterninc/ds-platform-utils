"""Example flow for @uv_base / @uv.

Local steps run in the uv environment that launched the flow. Remote steps run in an image built
from this project's uv.lock and pushed to ECR, chosen per dependency group.

Run it from the repo root, so the uv.lock beside this file is the one that gets used:

    uv run python uv_demo_flow.py run

Deploy it, where every step becomes a pod and so every step gets the image:

    uv run python uv_demo_flow.py argo-workflows create

The first run builds and pushes two images -- one for the default dependency set, one for the
`dev` group -- which takes a few minutes. After that both tags are in the registry and every
subsequent load is a pair of API calls, until uv.lock changes.
"""

import os
import sys

from metaflow import FlowSpec, kubernetes, resources, step

from ds_platform_utils.metaflow import uv, uv_base


#: Everything the steps report about themselves, so local and remote can be compared directly.
def _where_am_i() -> dict:
    return {
        "python": sys.version.split()[0],
        "uid": os.getuid(),
        "home": os.environ.get("HOME"),
        "executable": sys.executable,
    }


@uv_base(aws_profile="sandbox")
class UvDemoFlow(FlowSpec):
    @step
    def start(self):
        """Local: no @kubernetes, so this runs right here in your uv environment."""
        self.local = _where_am_i()
        print(f"[local ] python {self.local['python']}  uid {self.local['uid']}")
        print(f"[local ] executable {self.local['executable']}")
        self.next(self.train, self.report)

    @kubernetes(cpu=2, memory=4000)
    @step
    def train(self):
        """Remote, default dependency group: runs in the baked image."""
        import polars

        self.remote = _where_am_i()
        self.polars = polars.__version__
        # polars is a runtime dependency, so it is in the default image
        print(f"[remote] python {self.remote['python']}  uid {self.remote['uid']}  polars {self.polars}")
        self.next(self.join)

    @kubernetes(cpu=2, memory=4000)
    @uv(group="dev")
    @step
    def report(self):
        """Remote, `dev` group: a different image, because the group changes the resolved set."""
        import pytest

        self.pytest = pytest.__version__
        # pytest is only in the dev group -- importing it proves this is not the default image
        print(f"[remote] dev-group image carries pytest {self.pytest}")
        self.next(self.join)

    @step
    def join(self, inputs):
        """Local again: joins run wherever the flow is being driven from."""
        self.local = inputs.train.local
        self.remote = inputs.train.remote
        self.polars = inputs.train.polars
        self.pytest = inputs.report.pytest
        self.next(self.end)

    @resources(cpu=4)
    @step
    def end(self):
        """Local: @resources sizes a step, it does not place one -- so this stays here."""
        # the remote steps ran as the unprivileged task user out of /metaflow; the local ones
        # ran as you. If these ever match, the local/remote split has broken.
        assert self.remote["uid"] == 1000, self.remote["uid"]
        assert self.remote["home"] == "/metaflow", self.remote["home"]
        assert self.local["uid"] != 1000, "local step ran as the container user"
        assert self.polars, "polars missing from the default image"
        assert self.pytest, "pytest missing from the dev-group image"
        print(f"UV_DEMO_OK  local uid={self.local['uid']}  remote uid={self.remote['uid']}")
        print(f"            polars={self.polars} (default group)  pytest={self.pytest} (dev group)")


if __name__ == "__main__":
    UvDemoFlow()
