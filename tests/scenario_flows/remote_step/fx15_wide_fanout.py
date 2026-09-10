"""A wide foreach: many branches at once, each with a large distinct artifact.

Three branches prove the join-key fix; forty prove it at the scale that
matters, and exercise everything narrow tests cannot:

  - Kueue admitting many Workloads against one team's quota
  - Karpenter scaling several nodes up together and packing pods onto them
  - one driver submitting and polling N pods concurrently
  - a join whose branches are all @remote_step, so their outputs reach it as
    refs and the bytes never transit the 2 vCPU driver (see gaps.md L1)

Each branch writes a distinct payload over the inline limit, so every branch
needs its own S3 object. If the keys collided, the join would see forty copies
of whichever branch wrote last.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check

WIDTH = 40
PAYLOAD_MB = 5


class Fx15WideFanout(FlowSpec):
    @step
    def start(self):
        self.shards = list(range(WIDTH))
        print(f"[fx15] fanning out {WIDTH} ways", flush=True)
        self.next(self.work, foreach="shards")

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        shard = self.input
        idx = self.index
        check("index matches the split value", idx, shard)
        # A payload unique to this branch and over the 4 MB inline limit, so
        # it is uploaded as its own object.
        token = f"s{shard:03d}".encode()
        self.shard_id = shard
        self.blob = token * (PAYLOAD_MB * 1024 * 1024 // len(token))
        self.head = self.blob[: len(token)]
        self.next(self.gather)

    @remote_step
    @resources(cpu=2, memory=12000)
    @step
    def gather(self, inputs):
        branches = list(inputs)
        check("every branch arrived", len(branches), WIDTH)

        ids = sorted(int(b.shard_id) for b in branches)
        check("all shard ids present exactly once", ids, list(range(WIDTH)))

        # The collision test at scale: each branch's large attr must still be
        # its own object.
        heads = sorted(bytes(b.head) for b in branches)
        expected = sorted(f"s{i:03d}".encode() for i in range(WIDTH))
        check("every branch kept its own payload", heads, expected)
        check("no payload appears twice", len(set(heads)), WIDTH)

        self.width = len(branches)
        self.distinct = len(set(heads))
        self.next(self.end)

    @step
    def end(self):
        check("join width", self.width, WIDTH)
        check("distinct payloads", self.distinct, WIDTH)
        print(f"[fx15] OK  {WIDTH} branches, {WIDTH} distinct payloads")


if __name__ == "__main__":
    Fx15WideFanout()
