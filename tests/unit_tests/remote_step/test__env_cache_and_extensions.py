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

from remote_step.plugins import remote_step_decorator as rsd
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


# --------------------------------- extensions that are not a pip distribution
#
# The first fix keyed on `importlib.metadata.version("ob-metaflow-extensions")`
# and skipped the extension when that raised. On a fast-bakery task the
# Outerbounds extensions arrive inside the *code package*
# (`.mf_code/metaflow_extensions/outerbounds/...`) rather than as an installed
# distribution, so the metadata lookup raises even though
# `from metaflow import gpu_profile` works on the driver. The fix therefore
# skipped exactly the case it was written for, and the pod kept dying at
# STAGE=import_step with the spec showing one package:
#
#   [remote_step] spec env packages (1): [('ob-metaflow', '2.19.37.3')]


def test_an_extension_shipped_in_the_code_package_is_still_pinned(monkeypatch):
    """Importable module, no distribution metadata -- the fast-bakery case."""
    absent_dist = "not-an-installed-distribution-xyz"
    monkeypatch.setattr(
        rsd, "_METAFLOW_EXTENSION_DISTS", ((absent_dist, "json"),)
    )  # `json` stands in for an importable extension module

    env = _ensure_metaflow_in_env({"python": "3.11", "packages": {"ob-metaflow": "2.19.37.3"}})

    assert absent_dist in env["packages"], (
        "an extension whose module imports must be pinned even when "
        "importlib.metadata knows nothing about it"
    )
    assert env["packages"][absent_dist] == "", "with no version to pin, ship it unpinned"


def test_an_extension_that_is_not_in_use_is_not_pinned(monkeypatch):
    """No importable module means nothing needs it -- do not pin it."""
    monkeypatch.setattr(
        rsd,
        "_METAFLOW_EXTENSION_DISTS",
        (("some-extension", "a_module_that_does_not_exist_anywhere"),),
    )

    env = _ensure_metaflow_in_env({"python": "3.11", "packages": {"ob-metaflow": "2.19.37.3"}})

    assert "some-extension" not in env["packages"]


def test_the_declared_metaflow_pin_survives_an_unpinned_extension(monkeypatch):
    monkeypatch.setattr(rsd, "_METAFLOW_EXTENSION_DISTS", (("absent-dist-abc", "json"),))

    env = _ensure_metaflow_in_env({"python": "3.11", "packages": {"ob-metaflow": "2.19.37.3"}})

    assert env["packages"]["ob-metaflow"] == "2.19.37.3"
    assert env["python"] == "3.11"


def test_the_real_extension_is_resolved_however_it_arrived():
    """No patching: whichever way it is installed here, it must land."""
    env = _ensure_metaflow_in_env({"python": "3.11", "packages": {}})
    assert EXT in env["packages"]
    assert env["packages"][EXT] is not None


# ------------------------------------------- the seam to the requirements file
#
# _ensure_metaflow_in_env may emit an extension with an empty version, and it
# is requirements.py in the *image* that turns that into an install line. The
# two modules ship separately -- one via the flow's git pin, one baked into the
# runner image -- so nothing else catches a disagreement between them, and
# `name==` would fail the whole step's install rather than one package.


def test_an_unpinned_extension_renders_as_a_bare_requirement(monkeypatch):
    from remote_step.requirements import build_requirements

    monkeypatch.setattr(rsd, "_METAFLOW_EXTENSION_DISTS", (("absent-dist-def", "json"),))
    env = _ensure_metaflow_in_env({"python": "3.11", "packages": {"ob-metaflow": "2.19.37.3"}})

    lines = build_requirements(env["packages"])

    assert "absent-dist-def" in lines, "an empty version must render as the bare name"
    assert "absent-dist-def==" not in lines
    assert "ob-metaflow==2.19.37.3" in lines


def test_the_real_extension_round_trips_to_a_valid_requirement():
    from remote_step.requirements import build_requirements

    env = _ensure_metaflow_in_env({"python": "3.11", "packages": {}})
    lines = build_requirements(env["packages"])

    assert any(line == EXT or line.startswith(f"{EXT}==") for line in lines), lines
    assert not any(line.endswith("==") for line in lines), "a dangling '==' fails the install"
