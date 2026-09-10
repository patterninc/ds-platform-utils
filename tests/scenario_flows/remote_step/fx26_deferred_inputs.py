"""A step that reads one of several large inputs.

Inputs used to be materialised before the body ran, all of them, so a step
reading only a row count still downloaded and unpickled a sibling's 6 GB
artifact -- and a pod sized for the work it actually does was OOM-killed by
an artifact it never looked at. They are now fetched on first read.

The pod here asks for 4 GB while three 6 MB artifacts exist upstream; the
proof is in the log, which should name the deferred inputs and then only load
the one that is touched.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check

PAYLOAD_MB = 6


class Fx26DeferredInputs(FlowSpec):
    @step
    def start(self):
        self.next(self.produce)

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def produce(self):
        self.frame_a = b"A" * (PAYLOAD_MB * 1024 * 1024)
        self.frame_b = b"B" * (PAYLOAD_MB * 1024 * 1024)
        self.frame_c = b"C" * (PAYLOAD_MB * 1024 * 1024)
        self.row_count = 12345
        self.next(self.summarise)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def summarise(self):
        # Reads the scalar only. The three frames must not be downloaded.
        check("the scalar is readable", int(self.row_count), 12345)
        self.summary = f"rows={int(self.row_count)}"
        print("[fx26] read only row_count; frames should not have loaded", flush=True)
        self.next(self.touch_one)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def touch_one(self):
        # Now read exactly one of them, which must load on first access.
        check("frame_b loads on demand", self.frame_b[:1], b"B")
        check("and is the real object", type(self.frame_b), bytes)
        self.touched = "frame_b"
        self.next(self.end)

    @step
    def end(self):
        check("summary came back", str(self.summary), "rows=12345")
        check("touched frame", str(self.touched), "frame_b")
        print("[fx26] OK")


if __name__ == "__main__":
    Fx26DeferredInputs()
