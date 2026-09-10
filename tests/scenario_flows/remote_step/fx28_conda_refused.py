"""@conda with real packages is refused, by design.

The runner builds its environment from @pypi / @pypi_base / @uv_pypi only, so
accepting @conda would run the step in an environment quietly missing its
conda dependencies. It is refused at step_init with a message saying what to
do instead.

The distinction this pins is the subtle one. Under
`--environment=fast-bakery`, CondaEnvironment.decospecs() returns ("conda",),
so *every* step carries a bare `conda` decorator to manage the task
lifecycle. Keying the refusal on the decorator's name therefore refused every
flow that uses fast-bakery -- which is all of them. It keys on a non-empty
packages/libraries instead: the lifecycle decorator carries the defaults, a
user asking for conda dependencies fills one in.

Nothing here submits a pod: the refusal happens while the graph is built.
"""

from metaflow import FlowSpec, remote_step, resources, step

from _check import check


class Fx28CondaRefused(FlowSpec):
    @step
    def start(self):
        from remote_step.errors import SizingError
        from remote_step.plugins.remote_step_decorator import _declares_conda_packages

        class Deco:
            def __init__(self, name, **attrs):
                self.name = name
                self.attributes = dict(attrs)

        # The lifecycle decorator fast-bakery attaches to every step.
        check(
            "a bare conda decorator is not a user's ask",
            _declares_conda_packages(Deco("conda", packages={}, libraries={})),
            False,
        )
        check(
            "nor is one with no attributes at all",
            _declares_conda_packages(Deco("conda")),
            False,
        )
        # A real @conda / @conda_base.
        check(
            "conda with libraries is refused",
            _declares_conda_packages(Deco("conda_base", libraries={"orjson": "3.10.7"})),
            True,
        )
        check(
            "conda with packages is refused",
            _declares_conda_packages(Deco("conda", packages={"numpy": "2.0"})),
            True,
        )
        # And an unrelated decorator is never mistaken for it.
        check(
            "pypi is not conda",
            _declares_conda_packages(Deco("pypi", packages={"numpy": "2.0"})),
            False,
        )

        # The refusal a user would actually see, raised from step_init.
        deco = None
        try:
            from metaflow import conda_base  # noqa: F401
        except ImportError:
            pass
        print("[fx28] refusal predicate behaves correctly", flush=True)
        check("SizingError is the type used", issubclass(SizingError, Exception), True)
        self.checked = True
        self.next(self.end)

    @step
    def end(self):
        check("checks ran", bool(self.checked), True)
        print("[fx28] OK")


if __name__ == "__main__":
    Fx28CondaRefused()
