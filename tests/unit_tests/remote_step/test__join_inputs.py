"""A join step's `inputs`, and `merge_artifacts` on top of it.

A join body is `def join(self, inputs)`, and the runner has to supply that
second argument itself — Metaflow is not running the task. The branches are
lazy: a join over a wide foreach must not download every branch's artifacts
just to answer `inputs[0].x`.
"""

import base64
import pickle

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

# Imported through the `remote_step` alias, not `metaflow_extensions.remote_step`.
# Both names reach the same files, but importing a submodule under each one
# executes it twice and yields two distinct classes — so an exception raised
# internally would not match a RemoteStepError imported the other way.
from remote_step.errors import RemoteStepError
from remote_step.runner_entry import _build_join_inputs, _FakeInputs, _FakeSelf


def inline(value):
    """A spec entry for a small value, as build_spec would write it."""
    blob = pickle.dumps(value)
    import hashlib

    return {
        "kind": "inline",
        "blob_b64": base64.b64encode(blob).decode(),
        "sha256": hashlib.sha256(blob).hexdigest(),
    }


def spec(*branches, is_join=True):
    return {
        "is_join": is_join,
        "join_branches": [
            {"step": step, "attrs": {k: inline(v) for k, v in attrs.items()}} for step, attrs in branches
        ],
    }


# ------------------------------------------------------------------ the shape


def test_a_non_join_step_gets_no_inputs():
    assert _build_join_inputs({"is_join": False}) is None
    assert _build_join_inputs({}) is None


def test_branches_are_reachable_by_step_name():
    """`inputs.step_a.x` — the documented split-join access pattern."""
    inputs = _build_join_inputs(spec(("middle_a", {"x": 1}), ("middle_b", {"x": 2})))
    assert inputs.middle_a.x == 1
    assert inputs.middle_b.x == 2


def test_branches_are_reachable_by_index():
    """`inputs[0].x` — the documented foreach access pattern."""
    inputs = _build_join_inputs(spec(("worker", {"y": 10}), ("worker", {"y": 20})))
    assert inputs[0].y == 10
    assert inputs[1].y == 20


def test_branches_are_iterable():
    """`(inp.x for inp in inputs)` — the documented both-cases pattern."""
    inputs = _build_join_inputs(spec(("a", {"n": 1}), ("b", {"n": 2}), ("c", {"n": 3})))
    assert sorted(inp.n for inp in inputs) == [1, 2, 3]
    assert len(inputs) == 3


def test_an_unknown_attribute_says_which_step_lacked_it():
    inputs = _build_join_inputs(spec(("middle_a", {"x": 1})))
    with pytest.raises(AttributeError) as excinfo:
        _ = inputs[0].nope
    assert "middle_a" in str(excinfo.value)
    assert "nope" in str(excinfo.value)


def test_a_branch_attribute_is_hydrated_once():
    """Cached, so a body reading `inp.df` twice does not fetch twice."""
    inputs = _build_join_inputs(spec(("a", {"v": [1, 2, 3]})))
    first = inputs[0].v
    assert inputs[0].v is first


def test_a_branch_is_lazy_until_read(monkeypatch):
    """Nothing is fetched at construction — that is the point for wide joins."""
    import metaflow_extensions.remote_step.runner_entry as re_mod

    def explode(*a, **k):
        raise AssertionError("hydrated a branch attribute that was never read")

    monkeypatch.setattr(re_mod, "_hydrate_input", explode)
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 2})))
    assert len(inputs) == 2  # constructing and counting touches nothing


# ------------------------------------------------------------ merge_artifacts


def test_merge_copies_branch_artifacts_onto_self():
    """Previously __getattr__ answered with a no-op and these were dropped."""
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1, "y": 2}), ("b", {"x": 1, "y": 2})))
    fake.merge_artifacts(inputs)
    assert fake.x == 1
    assert fake.y == 2


def test_merge_leaves_an_attribute_the_step_already_set():
    """Metaflow's rule: a value assigned on self wins and is not merged."""
    fake = _FakeSelf()
    fake.x = "mine"
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 2})))
    fake.merge_artifacts(inputs)
    assert fake.x == "mine"


def test_merge_include_takes_only_what_was_named():
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1, "y": 2}), ("b", {"x": 1, "y": 2})))
    fake.merge_artifacts(inputs, include=["x"])
    assert fake.x == 1
    assert "y" not in vars(fake)


def test_merge_exclude_skips_what_was_named():
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1, "y": 2}), ("b", {"x": 1, "y": 2})))
    fake.merge_artifacts(inputs, exclude=["y"])
    assert fake.x == 1
    assert "y" not in vars(fake)


def test_include_and_exclude_together_is_refused():
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1})))
    with pytest.raises(RemoteStepError, match="mutually exclusive"):
        fake.merge_artifacts(inputs, include=["x"], exclude=["y"])


def test_branches_disagreeing_is_an_unresolved_conflict():
    """Silently picking one branch's value would be the worst outcome."""
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 999})))
    with pytest.raises(RemoteStepError, match="unresolved conflicts"):
        fake.merge_artifacts(inputs)


def test_include_does_not_resolve_a_conflict():
    """Include narrows what is considered; it does not pick a winner.

    Matching Metaflow: a named attribute that still disagrees is an error.
    """
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 999})))
    with pytest.raises(RemoteStepError, match="unresolved conflicts"):
        fake.merge_artifacts(inputs, include=["x"])


def test_a_conflict_can_be_dropped_with_exclude():
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1, "y": 5}), ("b", {"x": 999, "y": 5})))
    fake.merge_artifacts(inputs, exclude=["x"])
    assert fake.y == 5
    assert "x" not in vars(fake)


def test_a_conflict_resolved_by_assignment_is_not_raised():
    fake = _FakeSelf()
    fake.x = "decided"
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 999})))
    fake.merge_artifacts(inputs)
    assert fake.x == "decided"


def test_include_naming_something_no_branch_produced():
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1})))
    with pytest.raises(RemoteStepError, match="incoming branch produced"):
        fake.merge_artifacts(inputs, include=["absent"])


def test_merge_without_inputs_is_refused():
    with pytest.raises(RemoteStepError, match="only be called in a join"):
        _FakeSelf().merge_artifacts(None)


def test_merged_attributes_become_outputs():
    """A join's whole purpose is to carry these forward, so they must persist."""
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 1})))
    fake.merge_artifacts(inputs)
    assert [k for k in vars(fake) if not k.startswith("_")] == ["x"]


def test_underscored_branch_attributes_are_never_merged():
    fake = _FakeSelf()
    inputs = _FakeInputs(_build_join_inputs(spec(("a", {"x": 1}))).flows)
    inputs.flows[0]._entries["_private"] = inline("no")
    fake.merge_artifacts(inputs)
    # hasattr is useless here: _FakeSelf.__getattr__ answers any non-dunder
    # name with a placeholder, so the instance dict is the only real evidence.
    assert "_private" not in vars(fake)
