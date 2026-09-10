"""Foreach + join with branch attrs over the inline limit.

The blocker this exists for: every branch of a foreach join wrote its >4 MB
attrs to one S3 key, so each upload overwrote the last and every branch read
the final branch's data. A join summing across branches silently returned
N x the last one. Attrs under 4 MB are inlined per-branch and never showed it,
which is why earlier foreach tests passed.

Also self.index and self.foreach_stack(), which used to resolve to a no-op
callable in the pod -- so f"part-{self.index}" produced an address-dependent
string and `self.index == 0` was always False.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check

# Comfortably over INLINE_ATTR_LIMIT_BYTES (4 MB) so each branch's attr is
# uploaded as its own object rather than inlined in the spec.
PAYLOAD_MB = 6


class Fx02Foreach(FlowSpec):
    @step
    def start(self):
        self.shards = ["us", "eu", "apac"]
        self.next(self.work, foreach="shards")

    @remote_step
    @resources(cpu=2, memory=8000)
    @step
    def work(self):
        shard = self.input
        check("self.input is the shard", shard, predicate=lambda s: s in ("us", "eu", "apac"))
        # self.index must be a real integer, not a placeholder function.
        idx = self.index
        check("self.index is an int", idx, predicate=lambda i: isinstance(i, int))
        check("index interpolates cleanly", f"part-{idx}", predicate=lambda s: "function" not in s)
        stack = self.foreach_stack()
        check("foreach_stack is iterable", stack, predicate=lambda s: s is not None and len(s) >= 1)

        # Distinct, large, per-branch content. If the branches share an S3 key
        # the join will see three copies of whichever wrote last.
        self.shard = shard
        self.blob = (shard.encode() * (PAYLOAD_MB * 1024 * 1024 // len(shard)))
        self.marker = shard[0].encode()
        print(f"[fx02] shard={shard} index={idx} blob={len(self.blob) / 1024 / 1024:.1f} MB", flush=True)
        self.next(self.gather)

    @remote_step
    @resources(cpu=2, memory=12000)
    @step
    def gather(self, inputs):
        seen = sorted(i.shard for i in inputs)
        check("all three branches present", seen, ["apac", "eu", "us"])

        # The heart of it: each branch's large attr must still be its own.
        firsts = sorted({bytes(i.blob[:2]) for i in inputs})
        check("each branch kept its own blob", firsts, [b"ap", b"eu", b"us"])
        markers = sorted({bytes(i.marker) for i in inputs})
        check("markers are distinct", markers, [b"a", b"e", b"u"])

        sizes = {len(i.blob) for i in inputs}
        check("blobs are all the expected size", sizes, predicate=lambda s: len(s) == 1)

        self.shards_done = seen
        self.next(self.end)

    @step
    def end(self):
        check("join result survived", self.shards_done, ["apac", "eu", "us"])
        print("[fx02] OK")


if __name__ == "__main__":
    Fx02Foreach()
