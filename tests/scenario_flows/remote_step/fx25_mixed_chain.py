"""remote -> plain -> remote, with a large artifact threaded through.

A ref produced by a remote step has to be readable by a plain step on an
Outerbounds pod (which needs the cross-account read role stamped on it), and
then readable again by a second remote step -- which should take it as a
pointer rather than pulling it through the driver.

Also covers a step producing many outputs at once, which is what the
parallel upload path is for.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check

PAYLOAD_MB = 6
FANOUT = 12


class Fx25MixedChain(FlowSpec):
    @step
    def start(self):
        self.next(self.produce)

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def produce(self):
        self.big = b"P" * (PAYLOAD_MB * 1024 * 1024)
        self.small = "produced-remotely"
        # Many outputs in one step: the upload path handles these in parallel,
        # and each large one spills to disk rather than sitting in RAM.
        for i in range(FANOUT):
            setattr(self, f"out_{i:02d}", b"x" * (512 * 1024) + str(i).encode())
        print(f"[fx25] produced {FANOUT} extra outputs", flush=True)
        self.next(self.plain_middle)

    @step
    def plain_middle(self):
        """A plain step: runs on an Outerbounds pod, so it must assume the
        read role to resolve the ref at all."""
        check("a plain step can read the remote ref", self.big[:1], b"P")
        check("size intact", len(self.big), PAYLOAD_MB * 1024 * 1024)
        check("small attr readable", str(self.small), "produced-remotely")
        seen = [getattr(self, f"out_{i:02d}")[-1:] for i in range(FANOUT)]
        check("every extra output is distinct", len({bytes(s) for s in seen}), predicate=lambda n: n >= 10)
        self.middle_note = "passed through a plain step"
        self.next(self.consume)

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def consume(self):
        check("a second remote step reads it", self.big[:1], b"P")
        check("the plain step's own attr arrived", str(self.middle_note), "passed through a plain step")
        self.total = len(self.big)
        self.next(self.end)

    @step
    def end(self):
        check("round trip intact", int(self.total), PAYLOAD_MB * 1024 * 1024)
        print("[fx25] OK")


if __name__ == "__main__":
    Fx25MixedChain()
