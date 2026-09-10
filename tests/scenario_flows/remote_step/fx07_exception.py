"""The step's own exception has to reach the driver.

@catch runs on the driver, where the only visible failure is the RunnerError
the poller raises. The runner persists the exception so the driver re-raises
the original.

The unpicklable case had two defects: RunnerError was constructed without its
required exit_code, so the failure handler raised TypeError and lost the real
error; and the traceback carried back described our pickling failure rather
than the user's stack.
"""

import threading

from metaflow import FlowSpec, catch, remote_step, resources, step

from _check import check


class Unpicklable(RuntimeError):
    """What a live DB cursor or socket does to an exception."""

    def __init__(self, msg):
        super().__init__(msg)
        self.lock = threading.Lock()


class Fx07Exception(FlowSpec):
    @step
    def start(self):
        # Read in the failing steps so the raise is not statically dead code;
        # Metaflow's validity checker wants self.next() as the last statement.
        self.always = True
        self.next(self.plain_failure)

    @catch(var="plain_error")
    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def plain_failure(self):
        if self.always:
            raise ValueError("an ordinary picklable failure")
        self.next(self.odd_failure)

    @catch(var="odd_error")
    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def odd_failure(self):
        if self.always:
            raise Unpicklable("holds a lock, cannot be pickled")
        self.next(self.end)

    @step
    def end(self):
        print(f"[fx07] plain_error={self.plain_error!r}", flush=True)
        print(f"[fx07] odd_error={self.odd_error!r}", flush=True)
        # @catch records the exception; the type and message must survive.
        check("plain failure was caught", self.plain_error, predicate=lambda e: e is not None)
        check(
            "plain failure kept its message",
            str(self.plain_error),
            predicate=lambda s: "ordinary picklable failure" in s,
        )
        check("unpicklable failure was caught", self.odd_error, predicate=lambda e: e is not None)
        check(
            "unpicklable failure kept type and message",
            str(self.odd_error),
            predicate=lambda s: "Unpicklable" in s and "cannot be pickled" in s,
        )
        check(
            "no TypeError from the failure handler",
            str(self.odd_error),
            predicate=lambda s: "missing 1 required positional argument" not in s,
        )
        print("[fx07] OK")


if __name__ == "__main__":
    Fx07Exception()
