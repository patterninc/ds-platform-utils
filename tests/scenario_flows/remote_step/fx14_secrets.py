"""@secrets has to reach the runner pod.

Metaflow fetches a secret into the *driver's* os.environ during
task_pre_step. The pod is a separate container in a separate cluster and
inherits nothing, and the forwarding list only carried GITHUB_TOKEN plus
METAFLOW_/OBP_/OUTERBOUNDS_ prefixes -- so a value exported under its own
name never crossed.

demand-forecast hit this in production: publish_artifacts reads
GH_DISPATCH_TOKEN and its own guard turns a missing token into
"skipping API redeploy trigger", so prod runs quietly stopped redeploying.

Only presence and length are ever printed here, never a value.
"""

import os

from metaflow import FlowSpec, remote_step, resources, secrets, step

from _check import check

SECRET_SOURCE = "outerbounds.demand-forecast-api-gh-dispatch"
EXPECTED_VAR = "GH_DISPATCH_TOKEN"


class Fx14Secrets(FlowSpec):
    @secrets(sources=[SECRET_SOURCE])
    @step
    def start(self):
        # The driver's own view, for comparison.
        self.driver_had_it = bool(os.environ.get(EXPECTED_VAR))
        print(f"[fx14] driver sees {EXPECTED_VAR}: {self.driver_had_it}", flush=True)
        self.next(self.work)

    @secrets(sources=[SECRET_SOURCE])
    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        value = os.environ.get(EXPECTED_VAR)
        # Never print the value itself.
        print(f"[fx14] pod sees {EXPECTED_VAR}: {bool(value)} (len={len(value or '')})", flush=True)
        check("the secret reached the pod", bool(value), True)
        check("it is not empty", len(value or ""), predicate=lambda n: n > 0)
        self.pod_had_it = True
        self.next(self.end)

    @step
    def end(self):
        check("driver had the secret too", self.driver_had_it, True)
        check("pod had the secret", self.pod_had_it, True)
        print("[fx14] OK")


if __name__ == "__main__":
    Fx14Secrets()
