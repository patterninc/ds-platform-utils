"""Baseline: one remote step, resources honoured, arm64 by default.

Also the resource-usage log line, which reported
"507549.9 of 2 vCPU (25377494%)" before the CPU counter was differenced
across the body rather than read as a lifetime total.
"""

import os
import platform
import threading
import time

from metaflow import FlowSpec, current, remote_step, resources, step

from _check import check

BURN_SECONDS = 20.0


def burn(seconds, threads=1):
    """Hold `threads` cores busy for `seconds`, so the usage line is checkable."""

    def spin(deadline):
        x = 0
        while time.time() < deadline:
            for _ in range(50_000):
                x += 1

    deadline = time.time() + seconds
    ws = [threading.Thread(target=spin, args=(deadline,)) for _ in range(threads)]
    [w.start() for w in ws]
    [w.join() for w in ws]


class Fx01Basic(FlowSpec):
    @step
    def start(self):
        self.payload = {"rows": list(range(1000))}
        self.next(self.work)

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def work(self):
        check("arch is arm64 by default", platform.machine(), "aarch64")
        check("cpu count visible", os.cpu_count(), predicate=lambda n: n and n >= 2)
        check("input arrived", len(self.payload["rows"]), 1000)
        check("current.run_id set", bool(current.run_id), True)
        check("current.step_name", current.step_name, "work")
        # One busy thread of the two requested, so the usage line must read
        # about 1.0 of 2 vCPU. Anything wildly off means the counter is being
        # read as a lifetime total again.
        burn(BURN_SECONDS, threads=1)
        self.result = sum(self.payload["rows"])
        self.next(self.end)

    @step
    def end(self):
        check("output came back", self.result, 499500)
        print("[fx01] OK")


if __name__ == "__main__":
    Fx01Basic()
