"""Two ways a pod ended up with the wrong environment.

Both surfaced on the same GPU run and neither error named its cause.

1. The cached env file was per *directory*. A directory routinely holds
   several flows -- marketshare ships f0 through f4 in one src/ -- so
   whichever flow deployed last decided what every other flow's pods
   installed. A GPU flow with no @pypi of its own therefore received another
   flow's `pydantic + ds-platform-utils`.

2. The extension decorators -- @gpu_profile, @checkpoint, @model -- live in
   ob-metaflow-extensions, a distribution separate from metaflow. Whether it
   came along at all depended on the resolver having walked
   ds-platform-utils -> outerbounds -> ob-metaflow-extensions. When it had
   not, the pod died at STAGE=import_step on

       ImportError: cannot import name 'gpu_profile' from 'metaflow'
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.plugins.remote_step_decorator import (
    CACHED_ENV_FILENAME,
    _cached_env_filename,
    _ensure_metaflow_in_env,
)


# ------------------------------------------------------- per-flow env cache


def test_two_flows_in_one_directory_get_different_files():
    """The defect: one file per directory meant one flow clobbered the rest."""
    a = _cached_env_filename("F1AdSpendDataPrepFlow")
    b = _cached_env_filename("F2AdSpendTrainUsFlow")
    assert a != b


def test_the_filename_carries_the_flow_name():
    assert "Fx13Gpu" in _cached_env_filename("Fx13Gpu")


def test_an_unnamed_flow_falls_back_to_the_shared_name():
    """Nothing should crash if the flow name is unavailable."""
    assert _cached_env_filename(None) == CACHED_ENV_FILENAME
    assert _cached_env_filename("") == CACHED_ENV_FILENAME


@pytest.mark.parametrize("hostile", ["a/b", "..", "with space", "semi;colon", "a\\b"])
def test_a_hostile_flow_name_cannot_escape_the_directory(hostile):
    """The name reaches a filesystem path, so it has to be inert."""
    name = _cached_env_filename(hostile)
    assert "/" not in name
    assert "\\" not in name
    assert " " not in name
    assert not name.startswith("..")
    assert name.startswith(".remote_step_env.")
    assert name.endswith(".json")


def test_the_same_flow_name_is_stable():
    """Writer and reader are separate processes; they must agree."""
    assert _cached_env_filename("WeeklyForecastFlow") == _cached_env_filename("WeeklyForecastFlow")


# ------------------------------------------------- the extensions distribution


EXT = "ob-metaflow-extensions"


def test_extensions_are_added_when_nothing_is_declared():
    env = _ensure_metaflow_in_env({"python": "3.11", "packages": {}})
    assert EXT in env["packages"]


def test_extensions_are_added_when_metaflow_is_already_pinned():
    """The path that used to skip it: an early return once metaflow was seen."""
    env = _ensure_metaflow_in_env({"python": "3.11", "packages": {"ob-metaflow": "2.19.37.3"}})
    assert EXT in env["packages"]
    assert env["packages"]["ob-metaflow"] == "2.19.37.3", "the declared pin must be respected"


def test_extensions_are_added_alongside_user_packages():
    env = _ensure_metaflow_in_env({"python": "3.11", "packages": {"pandas": "2.2.0"}})
    assert EXT in env["packages"]
    assert env["packages"]["pandas"] == "2.2.0"


def test_an_explicit_extensions_pin_is_not_overridden():
    """A user asking for a specific version keeps it."""
    env = _ensure_metaflow_in_env({"python": "3.11", "packages": {EXT: "1.2.3"}})
    assert env["packages"][EXT] == "1.2.3"


def test_the_python_version_is_untouched():
    env = _ensure_metaflow_in_env({"python": "3.13", "packages": {}})
    assert env["python"] == "3.13"
