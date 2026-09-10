"""`@huggingface_hub(load=...)` for a remote step (gap 8).

Its registry hangs off `@checkpoint`'s task-scoped `CurrentCheckpointer`,
which does not exist in the runner, so `current.huggingface_hub` was absent
and the decorator's download happened on the driver — the wrong machine.

The read path is served in the pod from the Hugging Face Hub directly. That is
a *different source* from the driver's, which serves from the datastore cache,
so the switch is announced rather than left to be discovered.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.errors import RemoteStepError
from remote_step.plugins.remote_step_decorator import _drop_hf_hub, _find_hf_loads
from remote_step.runner_entry import (
    _HuggingfaceLoaded,
    _HuggingfaceStandIn,
    _load_hf_repos,
)


class Deco:
    def __init__(self, name, **attributes):
        self.name = name
        self.attributes = attributes


# ------------------------------------------------------- driver-side detection


def test_a_step_without_huggingface_hub():
    assert _find_hf_loads([Deco("resources", cpu=2)]) is None
    assert _find_hf_loads([]) is None


def test_a_decorator_that_loads_nothing():
    """Used only to persist a snapshot — nothing to fetch."""
    assert _find_hf_loads([Deco("huggingface_hub")]) is None
    assert _find_hf_loads([Deco("huggingface_hub", load=[])]) is None


def test_a_bare_repo_id_is_wrapped():
    got = _find_hf_loads([Deco("huggingface_hub", load="bert-base-uncased")])
    assert got["load"] == ["bert-base-uncased"]


def test_a_dict_entry_keeps_its_snapshot_arguments():
    """`load=[{"repo_id": ..., "revision": ...}]` is a documented form."""
    entry = {"repo_id": "bert-base-uncased", "revision": "abc123"}
    got = _find_hf_loads([Deco("huggingface_hub", load=[entry])])
    assert got["load"] == [entry]


def test_several_repos_are_carried():
    got = _find_hf_loads([Deco("huggingface_hub", load=["a", "b"])])
    assert got["load"] == ["a", "b"]


def test_the_decorator_is_dropped_from_the_driver():
    """Otherwise the driver downloads the snapshot it will never read."""
    decorators = [Deco("huggingface_hub", load=["a"]), Deco("resources", cpu=2)]
    removed = _drop_hf_hub(decorators)

    assert [d.name for d in decorators] == ["resources"]
    assert removed[0]["load"] == ["a"]


# ----------------------------------------------------------- pod-side loading


def test_no_request_loads_nothing():
    assert _load_hf_repos({}) is None
    assert _load_hf_repos({"hf_loads": {"load": []}}) is None


def test_repos_are_fetched_and_exposed(monkeypatch, capsys):
    import remote_step.runner_entry as re_mod

    calls = []

    def fake_snapshot(repo_id, path=None, **kwargs):
        calls.append((repo_id, kwargs))
        return f"/models/{repo_id}"

    monkeypatch.setattr(re_mod, "_hf_snapshot", fake_snapshot)
    standin = _load_hf_repos({"hf_loads": {"load": ["bert-base-uncased", {"repo_id": "gpt2", "revision": "v1"}]}})

    assert standin.loaded["bert-base-uncased"] == "/models/bert-base-uncased"
    assert standin.loaded["gpt2"] == "/models/gpt2"
    # snapshot arguments survive the trip
    assert calls[1][1]["revision"] == "v1"
    # the change of source is stated, not silent
    assert "Hugging Face Hub" in capsys.readouterr().out


def test_a_failed_fetch_is_loud(monkeypatch):
    """Silence means the body reads a path that is not there."""
    import remote_step.runner_entry as re_mod

    def explode(repo_id, path=None, **kwargs):
        raise RuntimeError("404")

    monkeypatch.setattr(re_mod, "_hf_snapshot", explode)
    with pytest.raises(RemoteStepError, match="could not fetch 'gpt2'"):
        _load_hf_repos({"hf_loads": {"load": ["gpt2"]}})


# ------------------------------------------------------------- the stand-in


def test_loaded_accepts_both_a_string_and_a_dict():
    loaded = _HuggingfaceLoaded({"gpt2": "/models/gpt2"})
    assert loaded["gpt2"] == "/models/gpt2"
    assert loaded[{"repo_id": "gpt2"}] == "/models/gpt2"
    assert "gpt2" in loaded
    assert len(loaded) == 1


def test_an_unloaded_repo_says_it_was_not_declared():
    loaded = _HuggingfaceLoaded({})
    with pytest.raises(KeyError, match="was not in @huggingface_hub"):
        _ = loaded["never-declared"]


def test_load_returns_an_already_loaded_path_without_fetching(monkeypatch):
    import remote_step.runner_entry as re_mod

    monkeypatch.setattr(re_mod, "_hf_snapshot", lambda *a, **k: pytest.fail("should not fetch again"))
    standin = _HuggingfaceStandIn(_HuggingfaceLoaded({"gpt2": "/models/gpt2"}))
    with standin.load(repo_id="gpt2") as path:
        assert path == "/models/gpt2"


def test_load_fetches_a_repo_that_was_not_declared(monkeypatch):
    """`current.huggingface_hub.load(...)` is on-demand by design."""
    import remote_step.runner_entry as re_mod

    monkeypatch.setattr(re_mod, "_hf_snapshot", lambda repo_id, **k: f"/tmp/{repo_id}")
    standin = _HuggingfaceStandIn(_HuggingfaceLoaded({}))
    with standin.load(repo_id="gpt2") as path:
        assert path == "/tmp/gpt2"


def test_snapshot_download_is_refused_with_a_way_forward():
    """It persists into the datastore, which the pod cannot write to."""
    standin = _HuggingfaceStandIn(_HuggingfaceLoaded({}))
    with pytest.raises(RemoteStepError, match="not supported inside @remote_step"):
        standin.snapshot_download(repo_id="gpt2")
