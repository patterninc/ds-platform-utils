"""Static split + merge_artifacts over a large artifact.

merge_artifacts assigned `getattr(branch, name)`, which downloads and
unpickles, and the merged value was pickled straight back out to a new key --
so a join moved every merged artifact through the pod twice for content that
had not changed. It now records a pointer to the object the branch already
wrote, and only materialises if the body actually reads it.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check

PAYLOAD_MB = 6


class Fx08Merge(FlowSpec):
    @step
    def start(self):
        self.shared = "set-upstream"
        self.next(self.left, self.right)

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def left(self):
        self.model = b"L" * (PAYLOAD_MB * 1024 * 1024)
        self.left_only = "from-left"
        self.next(self.combine)

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def right(self):
        self.frame = b"R" * (PAYLOAD_MB * 1024 * 1024)
        self.right_only = "from-right"
        self.next(self.combine)

    @remote_step
    @resources(cpu=2, memory=12000)
    @step
    def combine(self, inputs):
        # Merges everything the branches produced. The large ones should
        # become pointers rather than being pulled in and pushed back out.
        self.merge_artifacts(inputs)
        check("left's small attr merged", self.left_only, "from-left")
        check("right's small attr merged", self.right_only, "from-right")
        check("shared upstream attr survived", self.shared, "set-upstream")
        # Reading a merged artifact must give the real bytes, not a proxy.
        check("merged model is real bytes", type(self.model), bytes)
        check("merged model content", self.model[:1], b"L")
        check("merged frame content", self.frame[:1], b"R")
        self.sizes = (len(self.model), len(self.frame))
        self.next(self.end)

    @step
    def end(self):
        check("both merged artifacts are intact downstream", self.sizes,
              (PAYLOAD_MB * 1024 * 1024, PAYLOAD_MB * 1024 * 1024))
        check("merged attr readable downstream", self.left_only, "from-left")
        print("[fx08] OK")


if __name__ == "__main__":
    Fx08Merge()
