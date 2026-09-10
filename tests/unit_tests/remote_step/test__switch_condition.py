"""A split-switch whose condition the step produced remotely.

    self.run_dqv = validate(self.df)          # in a @remote_step body
    self.next({True: self.dqv, False: self.skip}, condition="run_dqv")

The driver links `run_dqv` as a RemoteArtifact, then replays the transition.
Metaflow evaluates it with `condition_value not in switch_cases`
(flowspec.py), which hashes the value -- and RemoteArtifact sets __hash__ to
None. So the run died on the transition *after* the step had already
succeeded, with a TypeError naming neither the attribute nor @remote_step.

gaps.md counts 16 of these transitions across the flows.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.artifact import RemoteArtifact
from remote_step.plugins.remote_step_decorator import (
    MAX_CONDITION_ATTR_BYTES,
    _hydrate_condition,
)


class Flow:
    """Stands in for the driver's flow object."""


def ref(value, size=8, kind="builtins.bool"):
    a = RemoteArtifact(s3_uri="s3://b/k.pkl", size_bytes=size, kind=kind, sha256="x")
    a.load = lambda **kw: value  # noqa: ARG005
    return a


def test_hashing_a_ref_now_resolves_it_rather_than_raising():
    """The root cause is fixed too, so this is belt and braces.

    RemoteArtifact was a dataclass with a generated __eq__, which sets
    __hash__ to None -- so `condition_value not in switch_cases` raised
    `TypeError: unhashable type` from inside Metaflow's own next(). The proxy
    now forwards __hash__ and __eq__ to the loaded value.

    _hydrate_condition still runs first, so the driver resolves the condition
    itself with a clear log line and a size guard, rather than depending on a
    lazy load firing inside Metaflow's internals.
    """
    assert RemoteArtifact.__hash__ is not None
    assert ref(True) in {True: 1, False: 2}


def test_a_bool_condition_is_loaded():
    f = Flow()
    f.run_dqv = ref(True)
    _hydrate_condition(f, "run_dqv")
    assert f.run_dqv is True
    assert f.run_dqv in {True: 1, False: 2}


def test_false_is_loaded_and_not_confused_with_absent():
    f = Flow()
    f.run_dqv = ref(False)
    _hydrate_condition(f, "run_dqv")
    assert f.run_dqv is False


def test_a_string_condition_is_loaded():
    f = Flow()
    f.mode = ref("full", kind="builtins.str")
    _hydrate_condition(f, "mode")
    assert f.mode == "full"
    assert f.mode in {"full": 1, "smoke": 2}


def test_a_plain_value_is_left_alone():
    """A non-remote step's condition needs no help."""
    f = Flow()
    f.run_dqv = True
    _hydrate_condition(f, "run_dqv")
    assert f.run_dqv is True


def test_a_missing_attribute_does_not_raise():
    """Metaflow raises its own InvalidNextException for this case."""
    _hydrate_condition(Flow(), "never_set")


def test_no_condition_is_a_no_op():
    _hydrate_condition(Flow(), "")


def test_an_implausibly_large_condition_is_left_as_a_reference(capsys):
    """Not an expected path -- a condition has to equal a case key."""
    f = Flow()
    original = ref(True, size=MAX_CONDITION_ATTR_BYTES + 1)
    f.run_dqv = original
    _hydrate_condition(f, "run_dqv")
    assert f.run_dqv is original
    assert "not a scalar" in capsys.readouterr().out


def test_a_load_failure_leaves_the_reference_and_says_so(capsys):
    f = Flow()
    broken = RemoteArtifact(s3_uri="s3://b/k.pkl", size_bytes=8, kind="builtins.bool", sha256="x")

    def boom(**kw):
        raise RuntimeError("s3 is having a day")

    broken.load = boom
    f.run_dqv = broken
    _hydrate_condition(f, "run_dqv")
    assert f.run_dqv is broken
    assert "could not load switch condition" in capsys.readouterr().out


def test_metaflows_own_check_passes_after_hydration():
    """The exact expression from flowspec.py that used to raise."""
    f = Flow()
    f.run_dqv = ref(False)
    _hydrate_condition(f, "run_dqv")
    switch_cases = {True: "dqv", False: "skip"}
    assert f.run_dqv in switch_cases
    assert switch_cases[f.run_dqv] == "skip"
