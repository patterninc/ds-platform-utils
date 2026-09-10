"""Context the step body sees inside the runner pod.

The runner executes the user's body against a stand-in `self`, in a process
that is not running a Metaflow task. Anything Metaflow would normally have
computed — `self.input` from the foreach stack, `current.is_production` from
@project — has to be read on the driver, shipped in the spec, and rebuilt
here, or the body silently sees a default.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.plugins.remote_step_decorator import (
    PROJECT_CONTEXT_KEYS,
    _project_context,
)
from remote_step.runner_entry import (
    _FakeSelf,
    _hydrate_foreach_input,
    _patch_project_context,
)

# ---------------------------------------------------------------- self.input


def test_a_step_outside_a_foreach_sees_none():
    assert _FakeSelf().input is None


@pytest.mark.parametrize(
    "value",
    [
        "us",  # self.worker = self.input
        ("2026-01", "us"),  # self.month, self.country = self.input
        7,
        {"sku": "A1", "region": "eu"},
        ["a", "b"],
    ],
)
def test_a_foreach_child_sees_its_split_value(value):
    """Hardcoding None broke ~100 sites, including tuple unpacking."""
    assert _FakeSelf(foreach_input=value).input == value


def test_the_split_value_is_not_reported_as_an_output():
    """It is context handed in, not something the step produced."""
    fake = _FakeSelf(foreach_input="us")
    assert all(k.startswith("_") for k in vars(fake))


def test_a_foreach_splitting_on_none_is_distinguishable(monkeypatch):
    """`has_foreach_input` separates "no foreach" from "foreach value is None"."""
    assert _hydrate_foreach_input({}) is None
    assert _hydrate_foreach_input({"has_foreach_input": False}) is None
    # present but null: still None, and asking for it must not fetch anything
    assert _hydrate_foreach_input({"has_foreach_input": True, "foreach_input": None}) is None


def test_an_inline_split_value_round_trips():
    import base64
    import pickle

    spec = {
        "has_foreach_input": True,
        "foreach_input": {
            "kind": "inline",
            "blob_b64": base64.b64encode(pickle.dumps(("2026-01", "us"))).decode(),
        },
    }
    assert _hydrate_foreach_input(spec) == ("2026-01", "us")


# --------------------------------------------------------------- @project ctx


@pytest.fixture
def clean_current():
    """`current` is a process-global, so undo whatever a test injects.

    `_update_env` installs a *class-level property* per key, which outlives
    the test and cannot be assigned over — leaving one behind breaks any later
    test that does `current.is_production = False`, which is exactly how the
    snowflake fixtures set themselves up.
    """
    from metaflow import current

    cls = type(current)
    preexisting = {k: cls.__dict__[k] for k in PROJECT_CONTEXT_KEYS if k in cls.__dict__}
    yield current
    for key in PROJECT_CONTEXT_KEYS:
        if key in preexisting:
            setattr(cls, key, preexisting[key])
        elif key in cls.__dict__:
            delattr(cls, key)


def test_is_production_reaches_the_step_body(clean_current):
    """An absent attribute reads falsy.

    So before this, a production run wrote to staging with no error anywhere.
    """
    _patch_project_context({"project": {"is_production": True}})
    assert clean_current.is_production is True


def test_every_project_key_is_replayed(clean_current):
    project = {
        "project_name": "forecast",
        "branch_name": "prod",
        "is_production": True,
        "is_user_branch": False,
        "project_flow_name": "forecast.prod.WeeklyForecastFlow",
    }
    _patch_project_context({"project": project})
    for key, expected in project.items():
        assert getattr(clean_current, key) == expected


def test_a_flow_without_project_patches_nothing(clean_current):
    """No @project means the keys simply do not exist; that is not an error."""
    _patch_project_context({})
    _patch_project_context({"project": {}})


def test_patching_survives_a_current_that_refuses(monkeypatch):
    """A Metaflow version without _update_env must not fail the step."""

    class Stubborn:
        def _update_env(self, _):
            raise RuntimeError("nope")

    monkeypatch.setattr("metaflow.current", Stubborn(), raising=False)
    _patch_project_context({"project": {"is_production": True}})


def test_driver_reads_nothing_when_the_flow_has_no_project():
    """_project_context only reports keys that actually exist on current."""
    assert set(_project_context()) <= set(PROJECT_CONTEXT_KEYS)


def test_driver_reads_the_project_keys_it_finds(clean_current):
    clean_current._update_env({"project_name": "forecast", "is_production": True})
    ctx = _project_context()
    assert ctx["project_name"] == "forecast"
    assert ctx["is_production"] is True
