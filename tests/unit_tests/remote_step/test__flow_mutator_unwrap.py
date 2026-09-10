"""Finding the flow class when a FlowMutator has wrapped it.

A FlowMutator is a *class* decorator:

    @output_table_cleanup_mutator
    class OosPredictFlow(FlowSpec): ...

so the module name binds to the mutator instance, not the flow. The runner
did `getattr(module, flow_class)` then `getattr(that, step_name)` and died:

    AttributeError: 'output_table_cleanup_mutator' object has no attribute
    'predict'

reported as exit 6, import_step -- which reads like a packaging problem
rather than a decorator one. Mutators are a real pattern here: out-of-stock's
predict flow is wrapped exactly like this, and @gpu_profile is built on the
same machinery.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.runner_entry import _unwrap_flow_class


class FakeFlow:
    """Stands in for a FlowSpec subclass."""

    @staticmethod
    def work():
        return "body"


class FakeMutator:
    """Shaped like Metaflow's FlowMutator: holds the class at _flow_cls."""

    def __init__(self, flow_cls):
        self._flow_cls = flow_cls


def test_a_wrapped_class_is_unwrapped():
    assert _unwrap_flow_class(FakeMutator(FakeFlow)) is FakeFlow


def test_the_step_is_reachable_after_unwrapping():
    """The actual thing the runner needs."""
    cls = _unwrap_flow_class(FakeMutator(FakeFlow))
    assert getattr(cls, "work")() == "body"


def test_stacked_mutators_are_unwrapped():
    """Several can be applied to one flow."""
    assert _unwrap_flow_class(FakeMutator(FakeMutator(FakeMutator(FakeFlow)))) is FakeFlow


def test_a_plain_class_passes_through():
    assert _unwrap_flow_class(FakeFlow) is FakeFlow


def test_an_object_with_no_flow_cls_is_returned_as_is():
    """Better to hand it back and let the caller's own error surface."""
    sentinel = object()
    assert _unwrap_flow_class(sentinel) is sentinel


def test_a_cycle_cannot_hang_the_runner():
    class SelfReferential:
        pass

    obj = SelfReferential()
    obj._flow_cls = obj
    # Bounded, so this returns rather than spinning.
    assert _unwrap_flow_class(obj) is obj


def test_none_is_survivable():
    assert _unwrap_flow_class(None) is None
