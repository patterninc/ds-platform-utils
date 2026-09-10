"""A foreach inside a foreach, joined at both levels.

foreach_stack() carries one entry per nesting level, and self.index is the
innermost. Both used to resolve to a no-op callable in the pod, so nested
fanout was where that did the most damage -- an inner branch's output path
was built from a placeholder function's repr.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check

OUTER = ["us", "eu"]
INNER = [0, 1, 2]


class Fx16NestedForeach(FlowSpec):
    @step
    def start(self):
        self.regions = OUTER
        self.next(self.per_region, foreach="regions")

    @step
    def per_region(self):
        self.region = self.input
        self.parts = INNER
        self.next(self.per_part, foreach="parts")

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def per_part(self):
        part = self.input
        stack = self.foreach_stack()
        print(f"[fx16] region={self.region} part={part} stack={stack}", flush=True)
        check("stack has both levels", stack, predicate=lambda s: s is not None and len(s) == 2)
        check("innermost index is the part", self.index, part)
        check("outer region carried down", self.region, predicate=lambda r: r in OUTER)
        # A path built from index -- the case that used to produce
        # "part-<function _placeholder at 0x...>".
        self.key = f"{self.region}/part-{self.index}.parquet"
        check("key is clean", self.key, predicate=lambda k: "function" not in k)
        self.next(self.join_parts)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def join_parts(self, inputs):
        keys = sorted(str(i.key) for i in inputs)
        check("all inner branches joined", len(keys), len(INNER))
        check("keys are distinct", len(set(keys)), len(INNER))
        self.region = str(inputs[0].region)
        self.keys = keys
        print(f"[fx16] region {self.region} keys {keys}", flush=True)
        self.next(self.join_regions)

    @step
    def join_regions(self, inputs):
        by_region = {str(i.region): list(i.keys) for i in inputs}
        check("both regions joined", sorted(by_region), sorted(OUTER))
        for region, keys in by_region.items():
            check(f"{region} has all parts", len(keys), len(INNER))
            check(f"{region} keys are its own", keys, predicate=lambda ks, r=region: all(k.startswith(r) for k in ks))
        self.summary = by_region
        self.next(self.end)

    @step
    def end(self):
        check("summary survived", sorted(self.summary), sorted(OUTER))
        print("[fx16] OK")


if __name__ == "__main__":
    Fx16NestedForeach()
