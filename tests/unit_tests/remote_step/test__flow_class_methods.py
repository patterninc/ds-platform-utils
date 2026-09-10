"""A step body calling a helper defined on its own flow class.

Nine of the ninety-eight flow classes in this org do it --
`self._build_backtest_frames()`, `self._get_snowflake_connection()`,
`self._region_configs()` -- and those helpers do the actual work.

In the pod such a name hit `_FakeSelf.__getattr__` and got the no-op
placeholder that exists so `self.next(self.other_step)` does not crash. The
call returned None and the step carried on, so a step whose real work lived
in a helper "succeeded" having written nothing. Silent, and
indistinguishable from an upstream data problem.

Found by fx13, whose GPU burn is a helper method: the body "ran" in 10
microseconds.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.runner_entry import _FakeSelf, _detect_outputs


class RealFlow:
    """Shaped like the flow classes in the wild."""

    HORIZONS = [7, 14, 28]
    TABLE = "RPT_LOW_INVENTORY"

    def _build_frames(self, n):
        return [f"row{i}" for i in range(n)]

    def _uses_self_state(self):
        return f"table={self.TABLE}"

    @staticmethod
    def _static_helper(x):
        return x * 2

    @classmethod
    def _class_helper(cls):
        return cls.__name__

    @property
    def _derived(self):
        return "computed"

    def a_step(self):
        """A sibling step, referenced by self.next(self.a_step)."""
        return "step body"


def bound():
    f = _FakeSelf()
    f.bind_flow_class(RealFlow)
    f._begin_recording()
    return f


def test_a_helper_method_actually_runs():
    """The defect: this returned None."""
    assert bound()._build_frames(3) == ["row0", "row1", "row2"]


def test_a_helper_sees_the_same_self():
    """`self` inside the helper has to be the object the body is using."""
    f = bound()
    f.TABLE = "OVERRIDDEN"
    assert f._uses_self_state() == "table=OVERRIDDEN"


def test_a_staticmethod_is_not_passed_self():
    """getattr already applies the descriptor, so binding it would break it."""
    assert bound()._static_helper(21) == 42


def test_a_classmethod_resolves():
    assert bound()._class_helper() == "RealFlow"


def test_a_property_is_evaluated():
    assert bound()._derived == "computed"


def test_a_plain_class_attribute_is_returned():
    assert bound().HORIZONS == [7, 14, 28]
    assert bound().TABLE == "RPT_LOW_INVENTORY"


def test_a_sibling_step_reference_is_still_usable():
    """`self.next(self.a_step)` must not crash."""
    assert callable(bound().a_step)


def test_an_unknown_name_still_gets_the_placeholder():
    """Metaflow internals probe attributes that genuinely do not exist."""
    f = bound()
    assert f.no_such_attribute() is None


def test_reading_a_helper_does_not_create_an_output():
    f = bound()
    _ = f._build_frames(1)
    _ = f.HORIZONS
    assert _detect_outputs(vars(f), f._assigned) == {}


def test_an_instance_attribute_still_wins_over_the_class():
    """__getattr__ only fires when normal lookup fails."""
    f = bound()
    f.TABLE = "instance"
    assert f.TABLE == "instance"


def test_without_a_bound_class_the_placeholder_is_used():
    """The runner binds it, but nothing may explode if it has not yet."""
    f = _FakeSelf()
    f._begin_recording()
    assert f._build_frames(3) is None


def test_dunders_still_raise_attribute_error():
    """pickle and copy probe these; answering them breaks both."""
    f = bound()
    with pytest.raises(AttributeError):
        f.__deepcopy__
