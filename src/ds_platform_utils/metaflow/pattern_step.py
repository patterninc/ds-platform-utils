"""A drop-in replacement for Metaflow's ``@step`` that carries the platform's standard decorators.

A flow written against plain `@step` has to repeat the same stack on every method -- compute,
retries, the environment built from uv.lock -- and every repetition is somewhere the stack can
drift. [`pattern_step`][ds_platform_utils.metaflow.pattern_step] applies that stack itself:

```python
@pattern_step
def start(self):
    self.next(self.end)
```

is `@step` with `@pypi`, a compute decorator, `@environment` and `@retry` on top of it.

Anything the stack applies can be reconfigured per step (`@pattern_step(compute={"cpu": 8})`) or
dropped entirely (`@pattern_step(compute=False)`). The defaults everything starts from are the
`_DEFAULT_*` constants below, which is the one place to change what "standard" means for every
flow at once.

## Choosing the compute decorator

Which compute decorator a step gets depends on how much it asks for, because sending a small step
to the cluster costs more in scheduling and image pull than the step itself. A request at or below
`_MAX_RESOURCES_CPU` (4) / `_MAX_RESOURCES_MEMORY` (16384 MB, i.e. 16 GB) gets `@resources`, which
is a hint rather than a placement: the step runs wherever the flow runs, and the numbers are
honoured if the run is sent to a backend anyway (`--with kubernetes`). Anything strictly bigger gets
`@kubernetes` and is scheduled on the cluster. Those two numbers are this project's policy and are
deliberately not tied to Metaflow's own `@kubernetes` defaults (cpu 2 / memory 8192), so changing
one does not move the other. `_compute_decorator` has the full rule, including the two cases that
route to Kubernetes regardless of size: a GPU request, and any attribute `@resources` does not
support.

## Why the stack is split in two

Note that `@step` cannot be added by a Metaflow mutator: it is not a decorator but a marker that
stamps `is_step` onto the method, and mutators only ever see methods already marked that way. So
the stack is split. This module's public function applies `@step` directly, then attaches
`_pattern_step_stack`, a `StepMutator` that adds the rest at deploy time.

That split is what makes overriding work. A `StepMutator` runs after the command line -- including
any `--with` options -- has been parsed, so it can see decorators the user wrote by hand and defer
to them. Applying the stack directly at decoration time instead would raise
`DuplicateStepDecoratorException` the moment anyone stacked their own `@kubernetes` on top.
"""

from functools import lru_cache
from typing import Optional, Union

from metaflow import StepMutator

from .pypi_packages import _get_pypi_kwargs

#: Baseline compute attributes, in the form `@resources` and `@kubernetes` both take. Empty means
#: a step asks for nothing in particular, which is a small request and so gets `@resources` -- see
#: `_compute_decorator`.
_DEFAULT_COMPUTE: dict = {}

#: Baseline `@retry` attributes. Empty means Metaflow's defaults: 3 attempts, 2 minutes apart.
_DEFAULT_RETRY: dict = {}

#: Environment variables every step gets. `@environment` is skipped entirely when this and the
#: per-step variables are both empty, since setting nothing is not worth a decorator.
_DEFAULT_ENV_VARS: dict = {}

#: Arguments for `_get_pypi_kwargs`, which turns the project's uv.lock into `@pypi` attributes.
#: Empty means runtime dependencies only, on the interpreter the project pins.
_DEFAULT_PYPI: dict = {}

#: Largest CPU request still considered small enough to run without the cluster. A request *equal*
#: to this stays on `@resources`; only something strictly larger is sent to Kubernetes.
_MAX_RESOURCES_CPU = 4

#: Largest memory request, in MB, still considered small enough to run without the cluster.
#: Metaflow counts memory in MB throughout, so this is 16 GB. Equal stays on `@resources`, as above.
_MAX_RESOURCES_MEMORY = 16384

#: The two decorators `_compute_decorator` picks between. A step gets exactly one: the mutator
#: skips its own choice when the flow already applied either of them by hand.
_COMPUTE_DECORATORS = ("resources", "kubernetes")

#: Resolved `@pypi` attributes, keyed by the options they were resolved from. A flow calls this
#: decorator once per step, and re-reading uv.lock each time would be the same answer every time.
_pypi_kwargs_cache: dict = {}


@lru_cache(maxsize=1)
def _resources_attributes() -> frozenset:
    """Read the attributes `@resources` accepts out of the installed Metaflow.

    Asked of Metaflow rather than written down here, so a step is never routed on a stale idea of
    what `@resources` supports. Every attribute it takes is also taken by `@kubernetes`, which is
    what makes "does `@resources` support this?" a usable routing question.

    Returns:
        The supported attribute names -- currently `cpu`, `gpu`, `disk`, `memory` and
        `shared_memory`.

    """
    from metaflow.plugins import STEP_DECORATORS

    for decorator in STEP_DECORATORS:
        if decorator.name == "resources":
            return frozenset(decorator.defaults)
    raise RuntimeError("Metaflow has no @resources decorator, so a step's compute cannot be routed")


def _as_number(attribute: str, value) -> float:
    """Read a compute attribute as a number so two requests can be compared.

    Metaflow's own defaults are strings (`@resources` ships `cpu="1"`, `memory="4096"`) while a
    flow author naturally writes ints, so both forms reach the routing rule and comparing them
    as-written would order `"16"` below `4`.

    Args:
        attribute: the attribute's name, for the error message
        value: the value as given, e.g. `16`, `"16"` or `"16384"`

    Returns:
        The value as a float.

    """
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"compute attribute {attribute!r} must be a number, got {value!r}. "
            f"Metaflow counts CPUs in cores and memory in MB, so 16 GB is 16384."
        ) from None


def _compute_decorator(attributes: dict) -> str:
    """Choose the compute decorator a request of this size should be given.

    `@kubernetes` when any of the following holds, and `@resources` otherwise:

    1. The request names an attribute `@resources` does not support, e.g. `image` or
       `node_selector`. Those only mean anything to the cluster, so asking for one *is* asking
       for Kubernetes however small the rest of the request is.
    2. A GPU is asked for. Nothing local is going to satisfy that, so it needs real scheduling.
    3. `cpu` is above `_MAX_RESOURCES_CPU` (4) or `memory` is above `_MAX_RESOURCES_MEMORY`
       (16384 MB). Strictly above: a request of exactly 4 CPU / 16384 MB is still small.

    A request that names nothing at all falls through to `@resources`: no thresholds are exceeded,
    so it is small by definition. That is what a bare `@pattern_step` gets.

    Args:
        attributes: the compute attributes to apply, as `@resources`/`@kubernetes` take them

    Returns:
        Either `"resources"` or `"kubernetes"`.

    """
    if any(attribute not in _resources_attributes() for attribute in attributes):
        return "kubernetes"
    if "gpu" in attributes and _as_number("gpu", attributes["gpu"]) > 0:
        return "kubernetes"
    if "cpu" in attributes and _as_number("cpu", attributes["cpu"]) > _MAX_RESOURCES_CPU:
        return "kubernetes"
    if "memory" in attributes and _as_number("memory", attributes["memory"]) > _MAX_RESOURCES_MEMORY:
        return "kubernetes"
    return "resources"


def _resolve_option(option: Union[bool, dict, None], defaults: dict) -> Optional[dict]:
    """Turn one decorator's argument into the attributes to apply, or `None` to skip it.

    Each decorator in the stack is configured the same way, so they all come through here:
    `True` takes the baseline, `False` opts out, and a dict is merged over the baseline so a
    step can set one attribute without restating the others.

    Args:
        option: what the caller passed for this decorator
        defaults: the `_DEFAULT_*` baseline for it

    Returns:
        The attributes to apply, or `None` when the decorator should not be applied at all.

    """
    if option is None or option is False:
        return None
    if option is True:
        return dict(defaults)
    if isinstance(option, dict):
        return {**defaults, **option}
    raise TypeError(f"expected True, False or a dict of attributes, got {option!r}")


def _cached_pypi_kwargs(options: dict) -> dict:
    """Resolve `@pypi` attributes from uv.lock, reading the lockfile once per distinct request.

    Args:
        options: arguments for `_get_pypi_kwargs`, e.g. `{"dependency_groups": ["train"]}`

    Returns:
        A fresh `{"python": ..., "packages": {...}}`, since Metaflow mutates the attributes it
        is handed and steps must not share one dict.

    """
    # dicts are not hashable, and the values here are small and comparable, so a sorted tuple of
    # the items is a good enough key.
    key = tuple(sorted((name, repr(value)) for name, value in options.items()))
    if key not in _pypi_kwargs_cache:
        _pypi_kwargs_cache[key] = _get_pypi_kwargs(**options)
    resolved = _pypi_kwargs_cache[key]
    return {"python": resolved["python"], "packages": dict(resolved["packages"])}


def _resolve_stack(options: dict) -> list:
    """Work out the full list of decorators to add to a step, in the order they should be added.

    Args:
        options: the `compute`, `retry`, `environment` and `pypi` arguments as given to
            [`pattern_step`][ds_platform_utils.metaflow.pattern_step]

    Returns:
        A list of `(decorator_name, attributes)` pairs, leaving out anything opted out of.

    """
    stack = []

    pypi = _resolve_option(options["pypi"], _DEFAULT_PYPI)
    if pypi is not None:
        stack.append(("pypi", _cached_pypi_kwargs(pypi)))

    compute = _resolve_option(options["compute"], _DEFAULT_COMPUTE)
    if compute is not None:
        stack.append((_compute_decorator(compute), compute))

    # `environment` is given as the variables themselves rather than `{"vars": ...}`, since a
    # decorator with exactly one attribute reads better without the extra nesting.
    env_vars = _resolve_option(options["environment"], _DEFAULT_ENV_VARS)
    if env_vars:
        stack.append(("environment", {"vars": env_vars}))

    retry = options["retry"]
    if isinstance(retry, int) and not isinstance(retry, bool):
        # `retry=2` is the obvious way to ask for two attempts, so accept it alongside the dict.
        retry = {"times": retry}
    retry = _resolve_option(retry, _DEFAULT_RETRY)
    if retry is not None:
        stack.append(("retry", retry))

    return stack


class _pattern_step_stack(StepMutator):  # noqa: N801
    """Adds the standard decorator stack to a step, deferring to any the flow set itself."""

    def init(self, *args, **kwargs):
        """Capture the per-step options.

        Metaflow calls this instead of `__init__` with whatever the mutator was constructed
        with, which here is the keyword arguments
        [`pattern_step`][ds_platform_utils.metaflow.pattern_step] was given.
        """
        super().init()
        self._options = kwargs

    def mutate(self, mutable_step):
        """Attach the stack, letting decorators already on the step win.

        Runs after the command line is parsed, so `duplicates=IGNORE` defers to a decorator the
        flow wrote by hand *and* to one passed as `--with`.

        `IGNORE` only recognises a clash by name, which is not enough for the compute decorator:
        a step with a hand-written `@kubernetes` would still be given `@resources`, since those
        are different names. So a step that already has either one keeps it and is not given the
        other -- a step never ends up with both from here.

        Args:
            mutable_step: Metaflow's handle on the step being decorated

        """
        for name, attributes in _resolve_stack(self._options):
            if name in _COMPUTE_DECORATORS and any(mutable_step.has_decorator(d) for d in _COMPUTE_DECORATORS):
                continue
            mutable_step.add_decorator(name, deco_kwargs=attributes, duplicates=mutable_step.IGNORE)


def pattern_step(  # noqa: PLR0913
    f=None,
    *,
    start: bool = False,
    end: bool = False,
    compute: Union[bool, dict] = True,
    retry: Union[bool, int, dict] = True,
    environment: Union[bool, dict, None] = True,
    pypi: Union[bool, dict] = True,
    kubernetes: Union[bool, dict, None] = None,
):
    """Metaflow's `@step`, with the platform's standard decorator stack already applied.

    Replaces `@step` on a flow method and adds `@pypi` (resolved from the project's uv.lock), a
    compute decorator, `@environment` and `@retry`. Like `@step`, it has to sit closest to the
    method -- anything else goes above it.

    Example usage:

    ```python
    from metaflow import FlowSpec

    from ds_platform_utils.metaflow import pattern_step


    class MyFlow(FlowSpec):
        @pattern_step
        def start(self):
            self.next(self.train)

        @pattern_step(compute={"cpu": 8, "memory": 32000}, pypi={"dependency_groups": ["train"]})
        def train(self):
            self.next(self.end)

        @pattern_step(compute=False)
        def end(self):
            pass
    ```

    Each decorator in the stack takes the same three forms: `True` for the platform default,
    `False` to leave it off, or a dict of attributes merged over the default, so a step can
    change one thing without restating the rest.

    ## Which compute decorator a step gets

    `compute` does not name a decorator, it describes a request -- how big it is decides where the
    step runs, since sending a small step to the cluster can cost more in scheduling and image
    pull than the step itself:

    - at or below `_MAX_RESOURCES_CPU` (4) and `_MAX_RESOURCES_MEMORY` (16384 MB, i.e. 16 GB) it
      becomes `@resources`, a hint rather than a placement: the step runs wherever the flow runs,
      and the numbers still apply if the run is sent to a backend anyway (`--with kubernetes`).
    - strictly above either threshold it becomes `@kubernetes` and is scheduled on the cluster, so
      exactly 4 CPU / 16384 MB is still small.
    - a GPU request, or any attribute `@resources` does not support (`image`, `node_selector`,
      `secrets`, `compute_pool`, ...), is `@kubernetes` however small the rest of the request is.

    A bare `@pattern_step` asks for nothing, which is small, so it gets `@resources`. Use
    `compute={"cpu": 8}` or any Kubernetes-only attribute to put a step on the cluster, and
    `compute=False` to leave its placement alone entirely.

    ## Overriding the stack

    Decorators the flow applies itself take precedence -- the stack is added by a `StepMutator`
    that defers to what is already there, including anything passed as `--with`. So this

    ```python
    @kubernetes(cpu=8)
    @pattern_step
    def train(self): ...
    ```

    runs on 8 CPUs and still gets the rest of the stack, rather than failing on a duplicate
    `@kubernetes`. A step that names either compute decorator by hand is not given the other, so
    it never ends up with both.

    Args:
        f: the decorated step method, supplied by Python in the bare form. Never pass this
            yourself.
        start: mark this step as the flow's entry point, as `@step(start=True)` would.
        end: mark this step as the flow's terminal step, as `@step(end=True)` would.
        compute: the step's compute request, e.g. `{"cpu": 8, "memory": 32000}`, or `False` to
            leave its placement alone. Attributes are those of `@resources`/`@kubernetes`, and
            the request's size picks between them as described above.
        retry: attributes for `@retry`, or the number of attempts as a shorthand for
            `{"times": n}`, or `False` to let the step fail on its first error.
        environment: environment variables to set, e.g. `{"TZ": "UTC"}`. Merged over
            `_DEFAULT_ENV_VARS`; `@environment` is skipped when the result is empty.
        pypi: arguments for `_get_pypi_kwargs`, e.g. `{"dependency_groups": ["train"]}`, or
            `False` to leave the step's environment alone.
        kubernetes: deprecated alias for `compute`, from when the stack always applied
            `@kubernetes`. Passing both is an error.

    Returns:
        The decorated step, or a decorator when called with keyword arguments.

    """
    from metaflow import step as metaflow_step

    if kubernetes is not None:
        if compute is not True:
            raise TypeError("pass either compute= or its old alias kubernetes=, not both")
        compute = kubernetes

    options = {
        "compute": compute,
        "retry": retry,
        "environment": environment,
        "pypi": pypi,
    }

    def decorate(func):
        # `@step` has to go on first: it initialises the list every other step decorator
        # appends to, so marking the method after the stack was attached would discard it.
        marked = metaflow_step(start=start, end=end)(func) if (start or end) else metaflow_step(func)
        return _pattern_step_stack(**options)(marked)

    return decorate if f is None else decorate(f)
