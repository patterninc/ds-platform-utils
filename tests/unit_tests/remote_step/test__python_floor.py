"""The interpreter the runner's venv is built with.

A spec asking for 3.10 produced, after the node was provisioned and the image
pulled:

    Because the current Python version (3.10.21) does not satisfy Python>=3.11
    and pandas==3.0.5 depends on Python>=3.11 ... unsatisfiable

at STAGE=uv_pip_install. The version can arrive from a stale cached env
shipped inside an Argo code package, so a default alone does not prevent it.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.plugins.remote_step_decorator import (
    DEFAULT_PYTHON,
    MIN_PYTHON,
    _find_pypi_env,
    _python_at_least,
)


class Deco:
    def __init__(self, name, **attrs):
        self.name = name
        self.attributes = dict(attrs)


class Flow:
    _flow_decorators = {}


def test_the_default_is_311():
    assert DEFAULT_PYTHON == "3.11"
    assert MIN_PYTHON == (3, 11)


@pytest.mark.parametrize("older", ["3.10", "3.10.21", "3.9", "3.8.1", "2.7"])
def test_an_older_version_is_raised(older):
    assert _python_at_least(older) == "3.11"


@pytest.mark.parametrize("newer", ["3.11", "3.11.9", "3.12", "3.13", "3.14"])
def test_a_newer_version_is_left_alone(newer):
    """The floor raises; it never lowers."""
    assert _python_at_least(newer) == newer


@pytest.mark.parametrize("junk", ["", None, "weird", "3.x", []])
def test_an_unparseable_version_falls_back_to_the_floor(junk):
    assert _python_at_least(junk) == "3.11"


def test_raising_a_version_says_so(capsys):
    """Silently changing the interpreter would be worse than the failure."""
    _python_at_least("3.10")
    err = capsys.readouterr().err
    assert "3.10" in err and "3.11" in err
    assert "uv_pip_install" in err


def test_no_notice_when_nothing_changes(capsys):
    _python_at_least("3.12")
    assert capsys.readouterr().err == ""


def test_the_env_spec_defaults_to_the_floor():
    env = _find_pypi_env(Flow(), [])
    assert env["python"] == "3.11"


def test_a_pypi_python_is_honoured_when_new_enough():
    env = _find_pypi_env(Flow(), [Deco("pypi", python="3.13", packages={"orjson": "1"})])
    assert env["python"] == "3.13"


def test_a_pypi_python_below_the_floor_is_raised():
    env = _find_pypi_env(Flow(), [Deco("pypi", python="3.10", packages={"orjson": "1"})])
    assert env["python"] == "3.11"


def test_a_step_python_still_wins_over_the_flow_one():
    class WithBase:
        _flow_decorators = {"pypi_base": Deco("pypi_base", python="3.12", packages={"a": "1"})}

    env = _find_pypi_env(WithBase(), [Deco("pypi", python="3.13", packages={"b": "2"})])
    assert env["python"] == "3.13"
    assert env["packages"] == {"a": "1", "b": "2"}
