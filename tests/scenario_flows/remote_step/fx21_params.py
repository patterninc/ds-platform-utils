"""Parameter, Config and IncludeFile as seen from inside the pod.

gaps.md counts 105 Parameter and 152 Config uses across the flows, and lists
IncludeFile as untested. All three are class-level descriptors that Metaflow
rewrites into properties, so they are not ordinary instance attributes -- the
driver has to collect them deliberately or the body sees nothing.
"""

from metaflow import FlowSpec, IncludeFile, Parameter, remote_step, resources, step

from _check import check


class Fx21Params(FlowSpec):
    threshold = Parameter("threshold", default=0.85, type=float, help="a float parameter")
    label = Parameter("label", default="baseline", help="a string parameter")
    enabled = Parameter("enabled", default=True, type=bool, help="a bool parameter")
    shards = Parameter("shards", default=4, type=int, help="an int parameter")
    fixture = IncludeFile("fixture", default="fixture.txt", help="a file shipped with the flow")

    @step
    def start(self):
        self.driver_seen = {
            "threshold": self.threshold,
            "label": self.label,
            "enabled": self.enabled,
            "shards": self.shards,
        }
        print(f"[fx21] driver: {self.driver_seen}", flush=True)
        self.next(self.work)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        # Parameters must arrive with their types intact, not as strings.
        check("float parameter", float(self.threshold), 0.85)
        check("string parameter", str(self.label), "baseline")
        check("bool parameter", bool(self.enabled), True)
        check("int parameter", int(self.shards), 4)
        check("int parameter is usable in arithmetic", int(self.shards) * 2, 8)

        # IncludeFile arrives as the file's contents.
        body = self.fixture
        text = body.decode() if isinstance(body, bytes) else str(body)
        print(f"[fx21] fixture = {text!r}", flush=True)
        check("IncludeFile content reached the pod", "line two" in text, True)
        check("IncludeFile has all three lines", len(text.strip().splitlines()), 3)

        self.pod_seen = {
            "threshold": float(self.threshold),
            "label": str(self.label),
            "enabled": bool(self.enabled),
            "shards": int(self.shards),
        }
        self.fixture_lines = len(text.strip().splitlines())
        self.next(self.end)

    @step
    def end(self):
        check("pod and driver agree on parameters", dict(self.pod_seen), dict(self.driver_seen))
        check("fixture lines", int(self.fixture_lines), 3)
        print("[fx21] OK")


if __name__ == "__main__":
    Fx21Params()
