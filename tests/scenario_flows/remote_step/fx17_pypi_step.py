"""@pypi on the remote step: the named package must exist in the pod.

With no environment decorator the pod installs only ob-metaflow, so the
flow's own dependencies are absent -- this is how a step declares what it
needs. `_find_pypi_env` merges @pypi_base and @pypi into the spec and the
entrypoint installs it before the body runs.
"""

from metaflow import FlowSpec, pypi, remote_step, resources, step

from _check import check

WANT_ORJSON = "3.10.7"


class Fx17PypiStep(FlowSpec):
    @step
    def start(self):
        self.next(self.work)

    @pypi(python="3.12", packages={"orjson": WANT_ORJSON})
    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        import orjson

        print(f"[fx17] orjson {orjson.__version__} in the pod", flush=True)
        check("the @pypi package is importable", orjson.__version__, WANT_ORJSON)
        check("it actually works", orjson.loads(orjson.dumps({"a": 1})), {"a": 1})
        self.version = orjson.__version__
        self.next(self.end)

    @step
    def end(self):
        check("version came back", str(self.version), WANT_ORJSON)
        print("[fx17] OK")


if __name__ == "__main__":
    Fx17PypiStep()
