"""A split-switch whose condition the remote step produced.

The driver links the condition as a RemoteArtifact, then replays the
transition. Metaflow evaluates it with `condition_value not in switch_cases`,
which hashes the value -- and RemoteArtifact sets __hash__ to None. The run
died with `TypeError: unhashable type` after the step had already succeeded.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check


class Fx05Switch(FlowSpec):
    @step
    def start(self):
        self.rows = 500
        self.next(self.decide)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def decide(self):
        # Produced in the pod, so it reaches the driver as a ref.
        self.run_validation = self.rows > 100
        print(f"[fx05] run_validation={self.run_validation}", flush=True)
        self.next({True: self.validate, False: self.skip}, condition="run_validation")

    @step
    def validate(self):
        self.route = "validate"
        print("[fx05] took the validate branch")
        self.next(self.end)

    @step
    def skip(self):
        self.route = "skip"
        print("[fx05] took the skip branch")
        self.next(self.end)

    @step
    def end(self):
        check("the true branch was taken", self.route, "validate")
        print("[fx05] OK")


if __name__ == "__main__":
    Fx05Switch()
