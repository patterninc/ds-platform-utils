"""@timeout expressed in seconds.

`hours * 60 + minutes + (1 if seconds else 0)` collapsed any seconds-only
timeout to one minute, so @timeout(seconds=1800) produced
activeDeadlineSeconds=60 and killed the step 60s in. This body runs for 90s,
which the old translation could not survive.
"""

import time

from metaflow import FlowSpec, remote_step, resources, step, timeout

from _check import check

BODY_SECONDS = 90


class Fx04Timeout(FlowSpec):
    @step
    def start(self):
        self.next(self.work)

    @remote_step
    @resources(cpu=1, memory=4000)
    @timeout(seconds=600)
    @step
    def work(self):
        started = time.time()
        # Longer than the 60s deadline the old arithmetic produced.
        for i in range(BODY_SECONDS // 10):
            time.sleep(10)
            print(f"[fx04] alive at {int(time.time() - started)}s", flush=True)
        elapsed = time.time() - started
        check("body outlived a 60s deadline", elapsed, predicate=lambda e: e >= BODY_SECONDS - 5)
        self.ran_for = elapsed
        self.next(self.end)

    @step
    def end(self):
        check("step completed", self.ran_for, predicate=lambda e: e >= BODY_SECONDS - 5)
        print("[fx04] OK")


if __name__ == "__main__":
    Fx04Timeout()
