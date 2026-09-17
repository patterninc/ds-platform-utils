"""A @remote_step that EMITS the foreach, not just one that runs a branch.

The gap 36bdf03 closed. `DAGNode.num_parallel` is 0 on every node rather than
None, so the driver's `is not None` test sent every `foreach=` transition down
the parallel branch and never passed `foreach=` at all. Metaflow's next() gates
the parallel path on `num_parallel >= 1`, so it skipped its foreach block too,
left `_foreach_num_splits` at None, and Argo's task_finished raised on
`range(None)` -- one step after the cause, naming neither the step nor
@remote_step.

It only bites when the step doing the splitting is itself remote. Every other
scenario flow emits its foreach from a plain step and puts only the branch
bodies on @remote_step, which is why 30 flows passed over it. `plan` here is
the emitter and is remote, so this is the shape that failed in production
(excess-inventory's prepare_scenarios).

`work` is remote as well, so the split values have to survive being produced in
one pod, iterated by the driver, and delivered to another pod.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check

SHARDS = [1, 2, 3, 4]
EXPECTED_TOTAL = sum(s * s for s in SHARDS)  # 30


class Fx31RemoteForeachEmitter(FlowSpec):
    @step
    def start(self):
        self.next(self.plan)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def plan(self):
        """The emitter, and the point of this flow: it is remote."""
        self.shards = list(SHARDS)
        print(f"[fx31] emitting a {len(self.shards)}-way foreach from a remote step", flush=True)
        self.next(self.work, foreach="shards")

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        # self.input must have survived: produced in the emitter's pod,
        # iterated by the driver to make the split, handed to this pod.
        self.shard = self.input
        self.squared = self.input * self.input
        print(f"[fx31] shard {self.shard} -> {self.squared}", flush=True)
        self.next(self.collect)

    @step
    def collect(self, inputs):
        got = sorted(int(i.shard) for i in inputs)
        self.branches = len(got)
        self.total = sum(int(i.squared) for i in inputs)
        check("every branch ran", self.branches, len(SHARDS))
        check("branch inputs were distinct and complete", got, sorted(SHARDS))
        check("results joined correctly", self.total, EXPECTED_TOTAL)
        self.next(self.end)

    @step
    def end(self):
        check("the join survived to the end", int(self.total), EXPECTED_TOTAL)
        print("[fx31] OK — a remote step emitted a foreach and the join closed")


if __name__ == "__main__":
    Fx31RemoteForeachEmitter()
