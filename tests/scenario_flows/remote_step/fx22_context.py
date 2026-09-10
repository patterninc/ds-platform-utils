"""What else the body can reach from inside the pod.

@environment variables, run tag mutation, and the Metaflow client -- the
last one counted 30+ times across the flows, and it needs the service URL and
auth headers to have been forwarded, not just the datastore.
"""

import os

from metaflow import FlowSpec, current, environment, remote_step, resources, step

from _check import check


class Fx22Context(FlowSpec):
    @step
    def start(self):
        self.run_id = current.run_id
        self.next(self.work)

    @environment(vars={"FX22_MARKER": "set-by-environment", "FX22_EMPTY": ""})
    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        # @environment(vars=...) must reach the pod.
        check("@environment var present", os.environ.get("FX22_MARKER"), "set-by-environment")
        check("an empty @environment var is still set", "FX22_EMPTY" in os.environ, True)

        # current.* identity inside the pod.
        check("run_id matches the driver's", current.run_id, str(self.run_id))
        check("step_name is this step", current.step_name, "work")
        check("flow_name is set", current.flow_name, "Fx22Context")
        check("retry_count is an int", current.retry_count, predicate=lambda r: isinstance(r, int))
        check("tags are visible", current.tags, predicate=lambda t: t is not None)

        # Tag mutation from inside the pod, replayed by the driver.
        current.run.add_tags(["fx22-tagged-from-pod"])
        print("[fx22] added a run tag from the pod", flush=True)

        # The Metaflow client, from inside the pod.
        from metaflow import Run

        run = Run(f"{current.flow_name}/{current.run_id}")
        check("the client can read this run", run.id, str(current.run_id))
        steps = [s.id for s in run]
        print(f"[fx22] client sees steps: {steps}", flush=True)
        check("the client sees start", "start", predicate=lambda s: s in steps)

        self.env_marker = os.environ["FX22_MARKER"]
        self.client_worked = True
        self.next(self.end)

    @step
    def end(self):
        check("env var came back", str(self.env_marker), "set-by-environment")
        check("client worked in the pod", bool(self.client_worked), True)
        # The tag the pod added must be on the run by now.
        tags = list(current.run.tags)
        print(f"[fx22] run tags: {sorted(t for t in tags if 'fx22' in t)}", flush=True)
        check("the pod's tag reached the run", "fx22-tagged-from-pod", predicate=lambda t: t in tags)
        print("[fx22] OK")


if __name__ == "__main__":
    Fx22Context()
