"""The @kubernetes the driver gets when the step body goes to EKS.

When a step is offloaded, a sibling `@kubernetes` would size the *driver* pod
to the step's full ask, so it is dropped and replaced with a driver-sized one.
The replacement has to keep everything that says *where* the driver runs --
including the image -- while forcing everything that says *how big* it is.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
import pytest

from metaflow_extensions.remote_step.plugins.remote_step_decorator import (
    DEFAULT_DRIVER_CPU,
    DEFAULT_DRIVER_MEMORY_MB,
    _default_kubernetes_image,
    _inject_driver_kubernetes,
)


def inject(dropped=None, decorators=None):
    """Run the injection and hand back the appended decorator's attributes."""
    decorators = [] if decorators is None else decorators
    _inject_driver_kubernetes(decorators, dropped)
    k8s = [d for d in decorators if getattr(d, "name", "") == "kubernetes"]
    assert len(k8s) == 1, f"expected exactly one @kubernetes, got {len(k8s)}"
    return k8s[0].attributes


def test_driver_inherits_the_dropped_image():
    """Losing the image left fast-bakery a bake with no base, which it rejects."""
    attrs = inject([{"image": "ecr.io/obptask-python:v1"}])
    assert attrs["image"] == "ecr.io/obptask-python:v1"


def test_driver_image_is_imputed_when_the_dropped_one_had_none():
    """`--with kubernetes` builds the decorator from defaults, so image is None.

    KubernetesDecorator imputes it in its own step_init, which never runs for a
    decorator appended from inside another step_init.
    """
    assert inject([{"image": None}])["image"] == _default_kubernetes_image()


def test_imputed_image_is_never_none():
    assert _default_kubernetes_image()


@pytest.mark.parametrize("attr", ["compute_pool", "node_selector", "namespace", "tolerations"])
def test_driver_inherits_placement(attr):
    assert inject([{attr: "somewhere"}])[attr] == "somewhere"


def test_driver_is_sized_down_regardless_of_what_the_step_asked_for():
    """A 20 vCPU / 65 GB step must not get a 20 vCPU / 65 GB poll loop."""
    attrs = inject([{"cpu": 20, "memory": 65000, "gpu": 4}])
    assert attrs["cpu"] == DEFAULT_DRIVER_CPU
    assert attrs["memory"] == DEFAULT_DRIVER_MEMORY_MB
    assert attrs["gpu"] == 0


def test_size_wins_over_placement():
    """Both are read from the same dropped dict; size must be applied last."""
    attrs = inject([{"compute_pool": "big-pool", "cpu": 20, "memory": 65000}])
    assert attrs["compute_pool"] == "big-pool"
    assert attrs["cpu"] == DEFAULT_DRIVER_CPU


def test_empty_placement_values_do_not_overwrite_defaults():
    attrs = inject([{"compute_pool": None, "node_selector": {}, "tolerations": []}])
    assert attrs["compute_pool"] is None


def test_nothing_is_injected_over_an_existing_kubernetes():
    """A step that kept its own @kubernetes is left alone."""

    class Existing:
        name = "kubernetes"

        def __init__(self):
            self.attributes = {"image": "mine", "cpu": 20}

    decorators = [Existing()]
    _inject_driver_kubernetes(decorators, [{"image": "other"}])
    assert len(decorators) == 1
    assert decorators[0].attributes["cpu"] == 20


def test_nothing_is_injected_without_a_trigger():
    """No Argo context, no k8s runtime, nothing dropped -- a plain local run."""
    decorators = []
    _inject_driver_kubernetes(decorators, None)
    assert decorators == []


def test_attributes_metaflow_dereferences_are_never_none():
    """Metaflow's own step_init reads these without guarding for None."""
    attrs = inject([{}])
    assert attrs["gpu_vendor"] is not None
    assert attrs["disk"] is not None
