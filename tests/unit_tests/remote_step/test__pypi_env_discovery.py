"""How @remote_step reads the environment the pypi decorators declare.

`_find_pypi_env` is the bridge between the four environment decorators and the
runner's spec: whatever it returns is what the runner pod installs. The flow
level (`@pypi_base`, `@uv_pypi_base`) and the step level (`@pypi`, `@uv_pypi`)
are read separately and merged. `@uv_pypi_base` and `@uv_pypi` are not handled
as their own cases -- both delegate to Metaflow's `@pypi_base` / `@pypi` after
deriving `python` and `packages` from uv.lock, so they arrive here already
wearing those names.
"""

import metaflow  # noqa: F401  -- resolves plugins first; see below
import pytest

# `import metaflow` has to land before the module below. Importing the
# decorator module directly makes it import metaflow, whose plugin resolution
# imports this same module again -- and re-entering it half-initialised fails
# to find RemoteStepDecorator. Letting metaflow finish first avoids that.
from metaflow_extensions.remote_step.plugins.remote_step_decorator import _find_pypi_env


class FakeDecorator:
    """Stands in for a Metaflow decorator: a name and an attributes dict."""

    def __init__(self, name, **attributes):
        self.name = name
        self.attributes = attributes


def FakeFlow(flow_decorators):  # noqa: N802 -- reads as a constructor
    """A flow whose `_flow_decorators` mimics Metaflow's shape.

    The attribute goes on the *class*, which is where Metaflow keeps it and
    where `_find_pypi_env` looks first -- an instance attribute is invisible
    to it.

    Metaflow has stored the value as both a list and a dict-of-lists across
    versions, so `_find_pypi_env` probes defensively and both are exercised.
    """
    return type("FakeFlow", (), {"_flow_decorators": flow_decorators})()


def pypi_base(**attributes):
    return FakeDecorator("pypi_base", **attributes)


def pypi(**attributes):
    return FakeDecorator("pypi", **attributes)


def test_reads_flow_level_pypi_base():
    env = _find_pypi_env(FakeFlow([pypi_base(python="3.11", packages={"pandas": "2.0.0"})]), [])
    assert env == {"python": "3.11", "packages": {"pandas": "2.0.0"}}


def test_reads_step_level_pypi():
    env = _find_pypi_env(FakeFlow([]), [pypi(python="3.11", packages={"pandas": "2.0.0"})])
    assert env == {"python": "3.11", "packages": {"pandas": "2.0.0"}}


def test_merges_step_packages_on_top_of_flow_packages():
    env = _find_pypi_env(
        FakeFlow([pypi_base(python="3.12", packages={"pandas": "2.0.0", "numpy": "1.0"})]),
        [pypi(packages={"numpy": "2.2.6", "polars": "1.0"})],
    )
    # step wins on the collision, flow-only entries survive
    assert env["packages"] == {"pandas": "2.0.0", "numpy": "2.2.6", "polars": "1.0"}


def test_step_python_overrides_the_flow_python():
    env = _find_pypi_env(FakeFlow([pypi_base(python="3.12", packages={})]), [pypi(python="3.10", packages={})])
    assert env["python"] == "3.10"


def test_flow_python_survives_a_step_that_does_not_name_one():
    env = _find_pypi_env(FakeFlow([pypi_base(python="3.11", packages={})]), [pypi(packages={"pandas": ""})])
    assert env["python"] == "3.11"


def test_defaults_to_3_12_when_nothing_names_a_python():
    assert _find_pypi_env(FakeFlow([]), [])["python"] == "3.12"


def test_no_pypi_decorators_yields_no_packages():
    assert _find_pypi_env(FakeFlow([]), [])["packages"] == {}


def test_ignores_unrelated_decorators():
    """@resources / @retry / @kubernetes must not contribute an environment."""
    env = _find_pypi_env(
        FakeFlow([FakeDecorator("conda_base", packages={"scipy": "1.0"})]),
        [
            FakeDecorator("resources", cpu=2, memory=4000),
            FakeDecorator("kubernetes", image="python:3.12"),
            FakeDecorator("retry", times=1),
        ],
    )
    assert env["packages"] == {}


def test_carries_a_uv_lock_direct_reference_through_untouched():
    """The direct-reference spelling must survive to the runner verbatim."""
    ref = "@ git+https://github.com/patterninc/ds-platform-utils.git@1578f9d"
    env = _find_pypi_env(FakeFlow([pypi_base(packages={"ds-platform-utils": ref})]), [])
    assert env["packages"]["ds-platform-utils"] == ref


def test_reads_flow_decorators_stored_as_a_dict():
    """Metaflow keys `_flow_decorators` by name in newer versions."""
    env = _find_pypi_env(FakeFlow({"pypi_base": [pypi_base(python="3.11", packages={"pandas": ""})]}), [])
    assert env == {"python": "3.11", "packages": {"pandas": ""}}


def test_reads_a_dict_holding_bare_decorators():
    env = _find_pypi_env(FakeFlow({"pypi_base": pypi_base(python="3.11", packages={"pandas": ""})}), [])
    assert env == {"python": "3.11", "packages": {"pandas": ""}}


@pytest.mark.parametrize("flow_decorators", [None, [], {}])
def test_tolerates_a_flow_with_no_decorators_at_all(flow_decorators):
    assert _find_pypi_env(FakeFlow(flow_decorators), [])["packages"] == {}


def test_tolerates_packages_given_as_none():
    """`@pypi_base(packages=None)` must not raise."""
    env = _find_pypi_env(FakeFlow([pypi_base(python="3.12", packages=None)]), [pypi(packages=None)])
    assert env["packages"] == {}
