"""@retry across a remote step.

The first attempt fails deliberately; the second must succeed and the pod for
the failed attempt must not be left running. current.retry_count tells the
body which attempt it is.
"""

from metaflow import FlowSpec, current, remote_step, resources, retry, step

from _check import check


class Fx12Retry(FlowSpec):
    @step
    def start(self):
        self.next(self.flaky)

    @remote_step
    @resources(cpu=1, memory=4000)
    @retry(times=2)
    @step
    def flaky(self):
        attempt = current.retry_count
        print(f"[fx12] attempt {attempt}", flush=True)
        if attempt == 0:
            raise RuntimeError("failing the first attempt on purpose")
        check("retry_count is visible in the pod", attempt, predicate=lambda a: a >= 1)
        self.succeeded_on = attempt
        self.next(self.end)

    @step
    def end(self):
        check("a later attempt succeeded", self.succeeded_on, predicate=lambda a: a >= 1)
        print("[fx12] OK")


if __name__ == "__main__":
    Fx12Retry()
