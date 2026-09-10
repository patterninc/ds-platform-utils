"""No team tag: the run must fall back to the sandbox namespace.

Everyone tags production runs with ds.domain, so an untagged run is a
developer trying something -- it belongs in sandbox rather than failing.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check


class Fx10Sandbox(FlowSpec):
    @step
    def start(self):
        self.next(self.work)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        # The pod's own namespace proves where it landed.
        ns = ""
        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
                ns = f.read().strip()
        except OSError:
            pass
        print(f"[fx10] namespace = {ns!r}", flush=True)
        check("landed in sandbox", ns, "sandbox")
        self.ns = ns
        self.next(self.end)

    @step
    def end(self):
        check("namespace reported", self.ns, "sandbox")
        print("[fx10] OK")


if __name__ == "__main__":
    Fx10Sandbox()
