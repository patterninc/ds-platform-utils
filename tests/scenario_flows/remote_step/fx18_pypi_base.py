"""@pypi_base at flow level reaches a remote step too.

`_find_pypi_env` has to read flow-level decorators out of `_flow_decorators`,
whose shape differs between Metaflow versions -- list in some, dict in
others. If that probing fails the pod silently gets no packages and the body
dies on ImportError.
"""

from metaflow import FlowSpec, pypi_base, remote_step, resources, step

from _check import check

WANT = "3.10.7"


@pypi_base(python="3.11", packages={"orjson": WANT})
class Fx18PypiBase(FlowSpec):
    @step
    def start(self):
        self.next(self.work)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        import orjson

        print(f"[fx18] orjson {orjson.__version__} from @pypi_base", flush=True)
        check("flow-level package reached the pod", orjson.__version__, WANT)
        self.version = orjson.__version__
        self.next(self.end)

    @step
    def end(self):
        check("version came back", str(self.version), WANT)
        print("[fx18] OK")


if __name__ == "__main__":
    Fx18PypiBase()
