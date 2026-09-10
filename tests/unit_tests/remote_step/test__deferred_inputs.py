"""Inputs are downloaded when the body reads them, not before.

Materialising everything up front is simpler, and it is what this did: a step
reading only `self.row_count` still downloaded and unpickled the 6 GB artifact
a sibling step happened to leave on the flow, so a pod sized for the work it
actually does was OOM-killed by an artifact it never looked at.

Deferred rather than handed to the body as a RemoteArtifact ref, because the
ref only proxies attribute access, indexing, iteration and truthiness -- not
arithmetic, and `isinstance`/`type()` see the wrapper. The body gets the
genuine object on first touch.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.runner_entry import _FakeSelf, _detect_outputs


class Recorder:
    """Counts loads so "was it downloaded" is observable."""

    def __init__(self, values):
        self.values = values
        self.loaded = []

    def __call__(self, name, ref):
        self.loaded.append(name)
        return self.values[name]


def fake_with(deferred, eager=None, values=None):
    f = _FakeSelf()
    for k, v in (eager or {}).items():
        setattr(f, k, v)
    loader = Recorder(values or {})
    f.defer_inputs({n: {"kind": "RemoteArtifact", "size_bytes": 1} for n in deferred}, loader)
    f._begin_recording()
    return f, loader


def test_an_untouched_input_is_never_downloaded():
    """The whole point."""
    f, loader = fake_with(["big_df"], values={"big_df": object()})
    assert loader.loaded == []


def test_reading_an_unrelated_attribute_downloads_nothing():
    f, loader = fake_with(["big_df"], eager={"row_count": 7}, values={"big_df": object()})
    assert f.row_count == 7
    assert loader.loaded == []


def test_the_first_read_loads_the_real_object():
    """A genuine object, not a proxy -- isinstance and arithmetic must work."""
    f, loader = fake_with(["total"], values={"total": 41})
    assert f.total + 1 == 42
    assert isinstance(f.total, int)
    assert loader.loaded == ["total"]


def test_a_second_read_does_not_download_again():
    f, loader = fake_with(["big_df"], values={"big_df": {"a": 1}})
    first = f.big_df
    second = f.big_df
    assert first is second
    assert loader.loaded == ["big_df"]


def test_a_deferred_input_that_was_read_is_not_an_output():
    """Reading is not producing; it must not be uploaded straight back."""
    f, loader = fake_with(["big_df"], values={"big_df": {"a": 1}})
    _ = f.big_df
    assert _detect_outputs(vars(f), f._assigned) == {}


def test_an_untouched_deferred_input_is_not_an_output():
    f, _ = fake_with(["big_df"], values={"big_df": object()})
    assert _detect_outputs(vars(f), f._assigned) == {}


def test_overwriting_a_deferred_input_without_reading_it_is_an_output():
    """`self.df = compute()` on an input the body never read."""
    f, loader = fake_with(["df"], values={"df": "upstream"})
    f.df = "mine"
    out = _detect_outputs(vars(f), f._assigned)
    assert out == {"df": "mine"}
    assert loader.loaded == [], "assigning over an input must not download it first"


def test_reading_then_reassigning_is_an_output():
    f, loader = fake_with(["df"], values={"df": [1, 2, 3]})
    f.df = f.df + [4]
    assert _detect_outputs(vars(f), f._assigned) == {"df": [1, 2, 3, 4]}


def test_a_missing_name_still_gets_the_placeholder():
    """Sibling step references like `self.next(self.scale)` must keep working."""
    f, _ = fake_with(["df"], values={"df": 1})
    assert callable(f.some_other_step)
    assert f.some_other_step() is None


def test_hasattr_on_a_deferred_input_is_true():
    f, loader = fake_with(["df"], values={"df": 1})
    assert hasattr(f, "df")
    assert loader.loaded == ["df"], "hasattr has to resolve it to answer"


def test_merge_artifacts_does_not_overwrite_a_deferred_input():
    """A deferred input must not read as absent.

    `already_set` is what stops merge_artifacts clobbering something the step
    already has; before this it used vars(self), which a deferred input is not
    in yet.
    """
    from remote_step.runner_entry import _FakeBranch, _FakeInputs

    f, loader = fake_with(["shared"], values={"shared": "from-input"})
    branch = _FakeBranch(step="a", entries={"shared": {"kind": "inline", "blob_b64": "", "sha256": "s"}})
    f.merge_artifacts(_FakeInputs([branch]))
    # Not merged over, and not downloaded just to find that out.
    assert "shared" not in f._assigned
    assert loader.loaded == []


def test_the_foreach_input_is_unaffected_by_deferral():
    f = _FakeSelf(foreach_input="us")
    f.defer_inputs({"df": {"kind": "RemoteArtifact", "size_bytes": 1}}, lambda n, r: None)
    f._begin_recording()
    assert f.input == "us"
