"""Resizing the driver must not disturb the list Metaflow is iterating.

Metaflow hands `step_init` the very list it is looping over:

    for deco in step.decorators:
        deco.step_init(flow, graph, step.__name__, step.decorators, ...)

so removing an element at a lower index shifts the list left under that
iterator and it skips whatever slides into the vacated slot -- the decorator
written directly above @remote_step. For @card that meant the card was never
registered, silently.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
import pytest

from remote_step.plugins.remote_step_decorator import (
    DEFAULT_DRIVER_CPU,
    DEFAULT_DRIVER_MEMORY_MB,
    _retarget_kubernetes,
)


class Deco:
    """Stand-in with the attributes step_init and our helpers read."""

    def __init__(self, name, **attrs):
        self.name = name
        self.attributes = dict(attrs)
        self.inited = False


def metaflow_step_init_sweep(decorators, mutate):
    """Metaflow's loop, verbatim in shape: iterate the list we also pass in.

    Returns (visit order, decorators that never got step_init).
    """
    visited = []
    for deco in decorators:
        visited.append(deco.name)
        deco.inited = True
        if deco.name == "remote_step":
            mutate(decorators)
    return visited, [d.name for d in decorators if not d.inited]


# The decorator list order Metaflow builds for
#     @card / @remote_step / @kubernetes / @step
# is bottom-up: kubernetes, remote_step, card.
LAYOUTS = [
    (["kubernetes", "remote_step", "card"], "card above"),
    (["kubernetes", "remote_step", "card", "retry"], "two above"),
    (["remote_step", "kubernetes", "card"], "kubernetes above"),
    (["kubernetes", "remote_step"], "nothing above"),
]


@pytest.mark.parametrize(("names", "label"), LAYOUTS, ids=[x[1] for x in LAYOUTS])
def test_every_sibling_still_gets_step_init(names, label):
    decos = [Deco(n, cpu=3, memory=29000) if n == "kubernetes" else Deco(n) for n in names]
    _, skipped = metaflow_step_init_sweep(decos, _retarget_kubernetes)
    assert skipped == [], f"{label}: step_init skipped {skipped}"


@pytest.mark.parametrize(("names", "label"), LAYOUTS, ids=[x[1] for x in LAYOUTS])
def test_the_list_keeps_its_length_and_order(names, label):
    decos = [Deco(n, cpu=3, memory=29000) if n == "kubernetes" else Deco(n) for n in names]
    _retarget_kubernetes(decos)
    assert [d.name for d in decos] == names, label


def test_removing_instead_of_retargeting_would_skip_the_decorator_above():
    """The old behaviour, kept as an executable record of the defect."""

    def drop_and_append(decorators):
        for i, d in enumerate(list(decorators)):
            if d.name == "kubernetes":
                decorators.pop(i)
                break
        decorators.append(Deco("kubernetes", cpu=DEFAULT_DRIVER_CPU))

    decos = [Deco(n, cpu=3, memory=29000) if n == "kubernetes" else Deco(n) for n in ["kubernetes", "remote_step", "card"]]
    _, skipped = metaflow_step_init_sweep(decos, drop_and_append)
    assert skipped == ["card"], "the defect this fix exists for no longer reproduces"


def test_the_sibling_is_resized_to_driver_scale():
    k8s = Deco("kubernetes", cpu=20, memory=65000, gpu=2, disk=200000)
    snapshots = _retarget_kubernetes([k8s])
    assert k8s.attributes["cpu"] == DEFAULT_DRIVER_CPU
    assert k8s.attributes["memory"] == DEFAULT_DRIVER_MEMORY_MB
    assert k8s.attributes["gpu"] == 0
    assert k8s.attributes["disk"] < 200000
    # The original ask is handed back so the caller can report what changed.
    assert snapshots == [{"cpu": 20, "memory": 65000, "gpu": 2, "disk": 200000}]


def test_placement_attributes_are_left_alone():
    """Where the driver runs carries over; only how big it is changes."""
    k8s = Deco(
        "kubernetes",
        cpu=20,
        memory=65000,
        compute_pool="obp-29gb",
        node_selector={"pool": "x"},
        namespace="ns",
        tolerations=[{"key": "k"}],
        image="repo/img:tag",
    )
    _retarget_kubernetes([k8s])
    assert k8s.attributes["compute_pool"] == "obp-29gb"
    assert k8s.attributes["node_selector"] == {"pool": "x"}
    assert k8s.attributes["namespace"] == "ns"
    assert k8s.attributes["tolerations"] == [{"key": "k"}]
    assert k8s.attributes["image"] == "repo/img:tag"


def test_no_sibling_kubernetes_reports_nothing_retargeted():
    """The caller uses an empty return to decide whether to inject one."""
    assert _retarget_kubernetes([Deco("card"), Deco("retry")]) == []


def test_two_kubernetes_decorators_are_both_resized():
    a = Deco("kubernetes", cpu=20, memory=65000)
    b = Deco("kubernetes", cpu=8, memory=32000)
    snapshots = _retarget_kubernetes([a, b])
    assert len(snapshots) == 2
    assert a.attributes["cpu"] == b.attributes["cpu"] == DEFAULT_DRIVER_CPU


def test_a_decorator_without_attributes_is_skipped_safely():
    class Bare:
        name = "kubernetes"
        attributes = None

    assert _retarget_kubernetes([Bare()]) == []
