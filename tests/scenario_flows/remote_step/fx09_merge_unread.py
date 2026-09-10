"""merge_artifacts without reading the merged artifact.

The case the pointer optimisation is for: a join that merges a large artifact
purely to carry it forward should move no bytes at all. Downstream still has
to be able to read it.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check

PAYLOAD_MB = 8


class Fx09MergeUnread(FlowSpec):
    @step
    def start(self):
        self.next(self.left, self.right)

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def left(self):
        self.big_left = b"L" * (PAYLOAD_MB * 1024 * 1024)
        self.next(self.combine)

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def right(self):
        self.big_right = b"R" * (PAYLOAD_MB * 1024 * 1024)
        self.next(self.combine)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def combine(self, inputs):
        # Deliberately never reads either big attribute. With a 4 GB pod and
        # 16 MB of artifacts this cannot fail on memory, but the log should
        # show no download for them.
        self.merge_artifacts(inputs)
        self.note = "merged without reading"
        print("[fx09] merged, nothing read", flush=True)
        self.next(self.end)

    @step
    def end(self):
        check("note came back", self.note, "merged without reading")
        # Downstream must still resolve the merged artifacts.
        check("left artifact resolves downstream", self.big_left[:1], b"L")
        check("right artifact resolves downstream", self.big_right[:1], b"R")
        check("sizes intact", len(self.big_left), PAYLOAD_MB * 1024 * 1024)
        print("[fx09] OK")


if __name__ == "__main__":
    Fx09MergeUnread()
