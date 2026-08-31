"""Demo flow for @pattern_flow / @pattern_step.

@pattern_flow bakes this project's uv.lock into a container image, pushes it to
public.ecr.aws/l3p3c6o4/outerbounds-images, and puts @kubernetes(image=...) on every step -- so
each step starts with the dependencies already installed rather than resolving an environment.

Run it from the repo root, so the uv.lock next to this file is the one that gets baked:

    uv run python demo_flow.py run

The first run builds and pushes (~90s). After that the tag is in the registry and every load is
a single API call, until uv.lock changes.
"""

from metaflow import FlowSpec

from ds_platform_utils.metaflow import pattern_flow, pattern_step


@pattern_flow(aws_profile="sandbox")
class DemoFlow(FlowSpec):
    @pattern_step
    def start(self):
        """Show that the step is running inside the baked image."""
        import os
        import sys

        import pandas as pd

        self.python = sys.version.split()[0]
        self.pandas = pd.__version__
        # uid 1000 and /metaflow are this image's contract, so they say the pod is running the
        # image @pattern_flow built rather than some default
        self.uid = os.getuid()
        self.home = os.environ.get("HOME")

        print(f"python {self.python}  pandas {self.pandas}  uid {self.uid}  HOME {self.home}")
        self.next(self.train)

    @pattern_step(compute={"cpu": 8, "memory": 32000})
    def train(self):
        """Stand-in for real work.

        The `compute=` argument is accepted and recorded but not acted on yet -- @pattern_step is
        still the template, and wiring compute sizing into it is the next piece.
        """
        import polars

        self.rows = polars.DataFrame({"n": range(1000)}).height
        print(f"trained on {self.rows} rows")
        self.next(self.end)

    @pattern_step
    def end(self):
        """Assert the environment came from the image, not from anywhere else."""
        assert self.python.startswith("3.10"), self.python
        assert self.uid == 1000, self.uid
        assert self.home == "/metaflow", self.home
        assert self.rows == 1000, self.rows
        print(f"DEMO_OK python={self.python} pandas={self.pandas} rows={self.rows}")


if __name__ == "__main__":
    DemoFlow()
