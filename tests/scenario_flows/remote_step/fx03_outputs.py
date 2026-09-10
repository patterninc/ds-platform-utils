"""Which attribute writes travel back from the pod.

Output detection used to compare object identity against the inputs, which
was wrong twice: `self.x = None` was excluded by a `v is not None` guard, and
an address freed then reused made a genuine reassignment read as untouched.
It now records assignments.

The in-place case is asserted as a *documented limitation*, not a bug: a
mutation never assigns, so it does not persist. Rebinding is required.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check


class Fx03Outputs(FlowSpec):
    @step
    def start(self):
        self.keep = "untouched-upstream"
        self.to_none = "should-become-none"
        self.to_reassign = [1, 2, 3]
        self.to_mutate = [1, 2, 3]
        self.freed_then_rebuilt = {"data": list(range(50))}
        self.next(self.work)

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def work(self):
        # New attribute.
        self.fresh = "produced-here"

        # Explicitly cleared. This used to be dropped, leaving the stale value.
        self.to_none = None

        # Ordinary rebind.
        self.to_reassign = [9, 9]

        # Mutated in place, never assigned -- documented not to persist.
        self.to_mutate.append(4)

        # Free the input, then build the replacement: the replacement can land
        # on the freed object's address, which identity comparison mistook for
        # "unchanged".
        big = self.freed_then_rebuilt
        self.freed_then_rebuilt = None
        del big
        self.freed_then_rebuilt = {"data": list(range(50)), "rebuilt": True}

        print("[fx03] body done", flush=True)
        self.next(self.end)

    @step
    def end(self):
        check("a new attribute persists", self.fresh, "produced-here")
        check("None assignment persists", self.to_none, None)
        check("reassignment persists", list(self.to_reassign), [9, 9])
        check("free-then-rebuild persists", self.freed_then_rebuilt.get("rebuilt"), True)
        check("an untouched input is unchanged", self.keep, "untouched-upstream")
        # The documented limitation, pinned so a change in behaviour is loud.
        check(
            "in-place mutation does NOT persist (documented)",
            list(self.to_mutate),
            [1, 2, 3],
        )
        print("[fx03] OK")


if __name__ == "__main__":
    Fx03Outputs()
