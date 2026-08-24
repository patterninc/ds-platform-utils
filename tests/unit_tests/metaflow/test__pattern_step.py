import textwrap
from pathlib import Path

import pytest
from metaflow import FlowSpec, kubernetes, resources
from metaflow.user_decorators.mutable_step import MutableStep

from ds_platform_utils.metaflow import pattern_step

# `_resolve_stack` is where every option turns into decorators, so it is worth testing on its own
from ds_platform_utils.metaflow.pattern_step import (
    _MAX_RESOURCES_CPU,
    _MAX_RESOURCES_MEMORY,
    _as_number,
    _compute_decorator,
    _pypi_kwargs_cache,
    _resolve_option,
    _resolve_stack,
    _resources_attributes,
)

UV_LOCK = textwrap.dedent("""
    version = 1
    requires-python = ">=3.11"

    [[package]]
    name = "my-flows"
    version = "0.1.0"
    source = { virtual = "." }
    dependencies = [{ name = "pandas" }]

    [package.dev-dependencies]
    train = [{ name = "scikit-learn" }]

    [[package]]
    name = "pandas"
    version = "2.3.2"
    source = { registry = "https://pypi.org/simple" }

    [[package]]
    name = "scikit-learn"
    version = "1.7.0"
    source = { registry = "https://pypi.org/simple" }
""")


@pytest.fixture
def project_root(tmp_path: Path) -> Path:
    (tmp_path / "uv.lock").write_text(UV_LOCK)
    return tmp_path


@pytest.fixture(autouse=True)
def _clear_pypi_cache():
    # the cache is keyed on the options, and a tmp_path project root differs per test, but
    # clearing keeps one test's resolution from ever explaining another's result
    _pypi_kwargs_cache.clear()


def _options(**overrides) -> dict:
    return {"compute": True, "retry": True, "environment": True, "pypi": False, **overrides}


def _mutate(flow_cls, step_func) -> dict:
    """Run the stack mutator against a step, the way Metaflow does at deploy time.

    Returns:
        The decorators now on the step, as a name -> attributes map.

    """
    mutator = step_func.config_decorators[0]
    mutator.external_init()
    mutator.mutate(MutableStep(flow_cls, step_func, statically_defined=True, inserted_by=["test"]))
    return {deco.name: deco.attributes for deco in step_func.decorators}


def test_resolve_option_takes_the_default_when_true():
    assert _resolve_option(True, {"cpu": 2}) == {"cpu": 2}


@pytest.mark.parametrize("option", [False, None])
def test_resolve_option_skips_when_opted_out(option):
    assert _resolve_option(option, {"cpu": 2}) is None


def test_resolve_option_merges_a_dict_over_the_default():
    # a step setting one attribute should not have to restate the others
    assert _resolve_option({"memory": 16000}, {"cpu": 2}) == {"cpu": 2, "memory": 16000}


def test_resolve_option_does_not_mutate_the_default():
    defaults = {"cpu": 2}
    _resolve_option({"cpu": 8}, defaults)["cpu"] = 99
    assert defaults == {"cpu": 2}


def test_resolve_option_rejects_a_bare_value():
    with pytest.raises(TypeError, match="expected True, False or a dict"):
        _resolve_option("8", {"cpu": 2})


def test_resolve_stack_applies_the_whole_stack(project_root: Path):
    options = _options(pypi={"project_root": project_root}, environment={"TZ": "UTC"}, compute={"cpu": 16})
    assert set(dict(_resolve_stack(options))) == {"pypi", "kubernetes", "environment", "retry"}


def test_resolve_stack_skips_environment_when_there_is_nothing_to_set():
    # `@environment(vars={})` would be a decorator that does nothing
    assert "environment" not in dict(_resolve_stack(_options()))


def test_resolve_stack_nests_environment_vars():
    stack = dict(_resolve_stack(_options(environment={"TZ": "UTC"})))
    assert stack["environment"] == {"vars": {"TZ": "UTC"}}


def test_resolve_stack_reads_retry_shorthand():
    assert dict(_resolve_stack(_options(retry=2)))["retry"] == {"times": 2}


def test_resolve_stack_treats_retry_true_as_the_default():
    # `True` is an int in Python, so the shorthand must not swallow it as `times=1`
    assert dict(_resolve_stack(_options(retry=True)))["retry"] == {}


def test_resolve_stack_leaves_out_what_is_opted_out_of():
    assert dict(_resolve_stack(_options(compute=False, retry=False))) == {}


def test_resources_supports_exactly_what_metaflow_says():
    # the routing rule leans on this set, so pin what the installed Metaflow actually accepts
    assert _resources_attributes() == frozenset({"cpu", "gpu", "disk", "memory", "shared_memory"})


def test_the_thresholds_are_the_agreed_policy():
    # the numbers are a deliberate choice, so moving them should mean changing this test too
    assert (_MAX_RESOURCES_CPU, _MAX_RESOURCES_MEMORY) == (4, 16384)


@pytest.mark.parametrize(
    "attributes",
    [
        pytest.param({}, id="asks-for-nothing"),
        pytest.param({"cpu": 2, "memory": 8192}, id="well-under-both-thresholds"),
        pytest.param({"cpu": 4, "memory": 16384}, id="exactly-at-both-thresholds"),
        pytest.param({"cpu": _MAX_RESOURCES_CPU}, id="cpu-at-threshold"),
        pytest.param({"memory": _MAX_RESOURCES_MEMORY}, id="memory-at-threshold"),
        pytest.param({"cpu": 1, "memory": 4096, "disk": 10240}, id="small-with-disk"),
        pytest.param({"gpu": 0}, id="explicitly-no-gpu"),
        pytest.param({"cpu": "4", "memory": "16384"}, id="thresholds-as-strings"),
    ],
)
def test_small_requests_route_to_resources(attributes: dict):
    assert _compute_decorator(attributes) == "resources"


@pytest.mark.parametrize(
    "attributes",
    [
        pytest.param({"cpu": 5}, id="cpu-just-over-threshold"),
        pytest.param({"memory": 16385}, id="memory-just-over-threshold"),
        pytest.param({"cpu": 8, "memory": 32000}, id="well-over-both-thresholds"),
        pytest.param({"cpu": _MAX_RESOURCES_CPU + 1}, id="cpu-over-threshold"),
        pytest.param({"memory": _MAX_RESOURCES_MEMORY + 1}, id="memory-over-threshold"),
        pytest.param({"gpu": 1}, id="gpu"),
        pytest.param({"cpu": 1, "gpu": "2"}, id="small-but-gpu"),
        pytest.param({"cpu": 1, "image": "python:3.11"}, id="small-but-kubernetes-only-attribute"),
        pytest.param({"node_selector": "pool=big"}, id="node-selector"),
        pytest.param({"secrets": ["my-secret"]}, id="secrets"),
        pytest.param({"compute_pool": "gpu-pool"}, id="compute-pool"),
    ],
)
def test_large_or_cluster_only_requests_route_to_kubernetes(attributes: dict):
    assert _compute_decorator(attributes) == "kubernetes"


@pytest.mark.parametrize("cpu", ["16", 16, 16.0])
def test_routing_compares_numerically_however_the_value_is_written(cpu):
    # Metaflow's own defaults are strings, so "16" must not be ordered below 4 as text would be
    assert _compute_decorator({"cpu": cpu}) == "kubernetes"


@pytest.mark.parametrize("memory", ["16385", 16385])
def test_routing_compares_memory_numerically(memory):
    # "16385" sorts below "16384" as text, so only a numeric comparison gets this right
    assert _compute_decorator({"memory": memory}) == "kubernetes"


@pytest.mark.parametrize("cpu", ["4", 4, 4.0])
def test_the_boundary_holds_however_the_value_is_written(cpu):
    assert _compute_decorator({"cpu": cpu}) == "resources"


def test_routing_rejects_a_value_that_is_not_a_number():
    with pytest.raises(ValueError, match="'cpu' must be a number"):
        _compute_decorator({"cpu": "quite a lot"})


def test_as_number_reads_both_forms():
    assert _as_number("memory", "8192") == _as_number("memory", 8192)


def test_resolve_stack_routes_a_small_request_to_resources():
    stack = dict(_resolve_stack(_options(compute={"cpu": 1})))
    assert "resources" in stack
    assert "kubernetes" not in stack


def test_resolve_stack_routes_a_large_request_to_kubernetes():
    stack = dict(_resolve_stack(_options(compute={"cpu": 16})))
    assert "kubernetes" in stack
    assert "resources" not in stack


def test_resolve_stack_routes_a_bare_request_to_resources():
    # `_DEFAULT_COMPUTE` is empty, so a bare @pattern_step asks for nothing and is small
    assert "resources" in dict(_resolve_stack(_options()))


def test_resolve_stack_resolves_pypi_from_the_lockfile(project_root: Path):
    pypi = dict(_resolve_stack(_options(pypi={"project_root": project_root})))["pypi"]
    assert pypi == {"python": "3.11", "packages": {"pandas": "2.3.2"}}


def test_resolve_stack_passes_pypi_options_through(project_root: Path):
    options = _options(pypi={"project_root": project_root, "dependency_groups": ["train"]})
    assert dict(_resolve_stack(options))["pypi"]["packages"]["scikit-learn"] == "1.7.0"


def test_pypi_kwargs_are_not_shared_between_steps(project_root: Path):
    # Metaflow mutates the attributes it is handed, so two steps must not get the same dict
    options = _options(pypi={"project_root": project_root})
    first = dict(_resolve_stack(options))["pypi"]
    second = dict(_resolve_stack(options))["pypi"]
    assert first == second
    assert first["packages"] is not second["packages"]


def test_the_lockfile_is_read_once_per_distinct_request(project_root: Path):
    options = _options(pypi={"project_root": project_root})
    _resolve_stack(options)
    # a flow calls the decorator once per step, and the answer cannot change between them
    (project_root / "uv.lock").unlink()
    assert dict(_resolve_stack(options))["pypi"]["packages"] == {"pandas": "2.3.2"}


def _build_flow():
    """Return a flow whose steps use the decorator in each of its forms.

    The flow is never instantiated: building the graph would run Metaflow's own decorator
    initialisation, and these tests drive the mutator directly instead.
    """

    class MyFlow(FlowSpec):
        @pattern_step
        def start(self):
            self.next(self.configured)

        # cpu 8 is over the threshold, so this step is the one that lands on @kubernetes
        @pattern_step(compute={"cpu": 8}, retry=1, environment={"TZ": "UTC"}, pypi=False)
        def configured(self):
            self.next(self.small)

        @pattern_step(compute={"cpu": 1, "memory": 2048}, pypi=False)
        def small(self):
            self.next(self.overridden)

        @kubernetes(cpu=8)
        @pattern_step(pypi=False)
        def overridden(self):
            self.next(self.resourced)

        # a hand-written @resources must suppress the @kubernetes the stack would otherwise add
        @resources(cpu=1)
        @pattern_step(compute={"cpu": 16}, pypi=False)
        def resourced(self):
            self.next(self.end)

        @pattern_step(compute=False, retry=False, pypi=False)
        def local_only(self):
            self.next(self.end)

        @pattern_step(pypi=False)
        def end(self):
            pass

    return MyFlow


def test_marks_the_method_as_a_step():
    assert _build_flow().start.is_step


def test_attaches_the_stack_mutator():
    attached = [deco.decorator_name for deco in _build_flow().end.config_decorators]
    assert attached == ["ds_platform_utils.metaflow.pattern_step._pattern_step_stack"]


@pytest.mark.parametrize("step_name", ["start", "configured", "small", "overridden", "resourced", "local_only", "end"])
def test_every_form_produces_a_usable_step(step_name: str):
    assert getattr(_build_flow(), step_name).is_step


def test_passes_start_and_end_through_to_metaflow():
    class MyFlow(FlowSpec):
        @pattern_step(start=True, pypi=False)
        def begin(self):
            self.next(self.finish)

        @pattern_step(end=True, pypi=False)
        def finish(self):
            pass

    assert MyFlow.begin.is_start_step
    assert MyFlow.finish.is_end_step


def test_applies_the_stack_to_a_bare_step(project_root: Path, monkeypatch: pytest.MonkeyPatch):
    # `start` is the only step left on the defaults, so it is the one that resolves @pypi -- and
    # the bare form has nowhere to pass project_root, so it walks up from the launch directory
    monkeypatch.chdir(project_root)
    flow = _build_flow()
    applied = _mutate(flow, flow.start)
    # asking for nothing is a small request, so a bare step stays off the cluster
    assert set(applied) == {"pypi", "resources", "retry"}
    assert applied["pypi"]["packages"] == {"pandas": "2.3.2"}


def test_applies_configured_attributes():
    flow = _build_flow()
    applied = _mutate(flow, flow.configured)
    assert applied["kubernetes"]["cpu"] == 8
    assert applied["retry"]["times"] == 1
    assert applied["environment"]["vars"] == {"TZ": "UTC"}


def test_routes_a_small_step_to_resources():
    flow = _build_flow()
    applied = _mutate(flow, flow.small)
    assert applied["resources"]["cpu"] == 1
    assert "kubernetes" not in applied


def test_defers_to_a_decorator_the_flow_applied_itself():
    flow = _build_flow()
    applied = _mutate(flow, flow.overridden)
    # the hand-written @kubernetes(cpu=8) wins, and the rest of the stack still lands
    assert applied["kubernetes"]["cpu"] == 8
    assert "retry" in applied
    assert [deco.name for deco in flow.overridden.decorators].count("kubernetes") == 1


def test_a_hand_written_kubernetes_suppresses_the_resources_the_stack_would_add():
    flow = _build_flow()
    # `overridden` is on the defaults, so the stack would route it to @resources -- but the step
    # already names a compute decorator, and it must not end up with both
    assert "resources" not in _mutate(flow, flow.overridden)


def test_a_hand_written_resources_suppresses_the_kubernetes_the_stack_would_add():
    flow = _build_flow()
    applied = _mutate(flow, flow.resourced)
    assert applied["resources"]["cpu"] == 1
    assert "kubernetes" not in applied


def test_applies_nothing_when_everything_is_opted_out_of():
    flow = _build_flow()
    assert _mutate(flow, flow.local_only) == {}


def test_accepts_the_old_kubernetes_alias():
    class MyFlow(FlowSpec):
        @pattern_step(kubernetes={"cpu": 16}, pypi=False)
        def train(self):
            pass

    assert "kubernetes" in _mutate(MyFlow, MyFlow.train)


def test_rejects_both_compute_and_the_old_alias():
    with pytest.raises(TypeError, match="not both"):
        pattern_step(compute={"cpu": 1}, kubernetes={"cpu": 1})
