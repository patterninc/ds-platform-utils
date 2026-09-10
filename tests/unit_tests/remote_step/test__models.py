"""`@model(load=...)` for a remote step (gaps 7 and 8).

The decorator downloads in `task_pre_step`, which for a remote step runs on
the *driver* — a Small-tier pod with 10 GB of disk that never reads the file —
while `current.model.loaded[...]` did not exist in the pod at all.

Only the names travel. The model *reference* is an ordinary flow artifact
(`@model` resolves it with `getattr(flow, name)`), which the spec already
ships, so the pod fetches the model itself and nothing large crosses the
driver.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest
from remote_step.errors import RemoteStepError
from remote_step.plugins.remote_step_decorator import _drop_model, _find_model_loads
from remote_step.runner_entry import _load_models, _ModelStandIn


class Deco:
    def __init__(self, name, **attributes):
        self.name = name
        self.attributes = attributes


# ------------------------------------------------------- driver-side detection


def test_a_step_without_model():
    assert _find_model_loads([Deco("resources", cpu=2)]) is None
    assert _find_model_loads([]) is None


def test_a_model_decorator_that_loads_nothing():
    """`@model` used only to save does not need anything fetched."""
    assert _find_model_loads([Deco("model")]) is None
    assert _find_model_loads([Deco("model", load=[])]) is None


def test_a_single_name_is_normalised_to_a_list():
    assert _find_model_loads([Deco("model", load="my_model")])["load"] == ["my_model"]


def test_several_names_are_carried():
    got = _find_model_loads([Deco("model", load=["a", "b"])])
    assert got["load"] == ["a", "b"]


def test_a_name_and_path_tuple_becomes_a_json_safe_pair():
    """Tuples do not survive the JSON spec, so they travel as pairs."""
    got = _find_model_loads([Deco("model", load=[("a", "/opt/a")])])
    assert got["load"] == [["a", "/opt/a"]]


def test_temp_dir_root_is_carried():
    got = _find_model_loads([Deco("model", load=["a"], temp_dir_root="/scratch")])
    assert got["temp_dir_root"] == "/scratch"


def test_the_model_decorator_is_dropped_from_the_driver():
    """Otherwise task_pre_step downloads the model onto the driver."""
    decorators = [Deco("model", load=["a"]), Deco("resources", cpu=2)]
    removed = _drop_model(decorators)

    assert [d.name for d in decorators] == ["resources"]
    assert removed[0]["load"] == ["a"]


def test_dropping_when_there_is_no_model_changes_nothing():
    decorators = [Deco("resources", cpu=2)]
    assert _drop_model(decorators) == []
    assert len(decorators) == 1


# ----------------------------------------------------------- pod-side loading


def test_a_step_with_no_model_request_loads_nothing():
    assert _load_models({}, object()) is None
    assert _load_models({"model_loads": {}}, object()) is None
    assert _load_models({"model_loads": {"load": []}}, object()) is None


def test_an_unreachable_store_is_reported_not_raised(monkeypatch, capsys):
    """A store failure must not look like a bug in the step body."""
    import remote_step.runner_entry as re_mod

    monkeypatch.setattr(re_mod, "_model_storage_backend", lambda: None)
    assert _load_models({"model_loads": {"load": ["m"]}}, object()) is None


def test_a_failed_load_is_loud(monkeypatch):
    """Silence here means the body reads a path that is not there."""
    import remote_step.runner_entry as re_mod
    from metaflow_extensions.obcheckpoint.plugins.machine_learning_utilities.modeling_utils import (
        core as core_mod,
    )

    monkeypatch.setattr(re_mod, "_model_storage_backend", lambda: object())

    def explode(**kwargs):
        raise RuntimeError("no such model")

    monkeypatch.setattr(core_mod, "LoadedModels", explode)
    with pytest.raises(RemoteStepError, match="@model\\(load=...\\) failed"):
        _load_models({"model_loads": {"load": ["m"]}}, object())


def test_loaded_models_are_exposed_on_the_stand_in(monkeypatch):
    import remote_step.runner_entry as re_mod
    from metaflow_extensions.obcheckpoint.plugins.machine_learning_utilities.modeling_utils import (
        core as core_mod,
    )

    captured = {}

    class FakeLoadedModels:
        def __init__(self, storage_backend, flow, artifact_references, temp_dir_root=None):
            captured["refs"] = artifact_references
            captured["flow"] = flow
            self.info = {"m": {"key": "k"}}

        def __getitem__(self, name):
            return f"/tmp/{name}"

    monkeypatch.setattr(re_mod, "_model_storage_backend", lambda: object())
    monkeypatch.setattr(core_mod, "LoadedModels", FakeLoadedModels)

    fake_self = object()
    standin = _load_models({"model_loads": {"load": ["m", ["n", "/opt/n"]]}}, fake_self)

    assert standin.loaded["m"] == "/tmp/m"
    # the pair came back as a tuple, which is what @model expects
    assert captured["refs"] == ["m", ("n", "/opt/n")]
    # resolved against the stand-in self, which carries the reference artifact
    assert captured["flow"] is fake_self


def test_save_is_refused_with_a_way_forward():
    """Better an explicit refusal than a silent no-op that loses the model."""
    with pytest.raises(RemoteStepError, match="not supported inside @remote_step"):
        _ModelStandIn().save({"some": "model"})
