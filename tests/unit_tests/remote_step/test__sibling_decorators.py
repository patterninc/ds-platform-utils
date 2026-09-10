"""Sibling decorators on a `@remote_step` step.

Each of these is set up by Metaflow for the *driver* task. The step body runs
in another pod in another cluster, so anything the sibling configures has to
be read here and carried across, or the body silently runs without it.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest
from remote_step.plugins.remote_step_decorator import (
    _declares_conda_packages,
    _find_env_vars,
    _find_timeout_minutes,
    _job_timeout_minutes,
)


class Deco:
    def __init__(self, name, **attributes):
        self.name = name
        self.attributes = attributes


# ------------------------------------------------------------- @environment


def test_environment_vars_are_collected():
    """Metaflow sets these on the driver pod; the runner inherits nothing."""
    assert _find_env_vars([Deco("environment", vars={"AWS_PROFILE": "x", "TZ": "UTC"})]) == {
        "AWS_PROFILE": "x",
        "TZ": "UTC",
    }


def test_no_environment_decorator_yields_nothing():
    assert _find_env_vars([Deco("resources", cpu=2)]) == {}
    assert _find_env_vars([]) == {}


def test_values_are_stringified():
    """Pod env values must be strings; an int would fail the API call."""
    assert _find_env_vars([Deco("environment", vars={"WORKERS": 8})]) == {"WORKERS": "8"}


def test_none_values_are_dropped():
    """A None would serialise as the string 'None', which is worse than absent."""
    assert _find_env_vars([Deco("environment", vars={"A": None, "B": "b"})]) == {"B": "b"}


def test_several_environment_decorators_merge():
    vars_ = _find_env_vars([Deco("environment", vars={"A": "1"}), Deco("environment", vars={"B": "2"})])
    assert vars_ == {"A": "1", "B": "2"}


def test_an_empty_vars_mapping_is_fine():
    assert _find_env_vars([Deco("environment", vars={})]) == {}
    assert _find_env_vars([Deco("environment")]) == {}


# ----------------------------------------------------------------- @timeout


@pytest.mark.parametrize(
    ("attrs", "expected"),
    [
        ({"minutes": 30}, 30),
        ({"hours": 2}, 120),
        ({"hours": 1, "minutes": 30}, 90),
        ({"seconds": 45}, 1),  # rounds up: a sub-minute deadline is still one
        # Seconds as the ONLY unit. A flat "+1 if seconds" collapsed all of
        # these to 1, so @timeout(seconds=1800) got a pod killed 60s in. The
        # 45s case above passes either way, which is how it went unnoticed.
        ({"seconds": 1800}, 30),
        ({"seconds": 600}, 10),
        ({"seconds": 61}, 2),
        ({"seconds": 60}, 1),
        # Seconds alongside a coarser unit still rounds the total up.
        ({"minutes": 5, "seconds": 30}, 6),
        ({"hours": 1, "minutes": 30, "seconds": 1}, 91),
        ({"hours": 0, "minutes": 0, "seconds": 0}, None),  # @timeout() with nothing set
        ({}, None),
    ],
)
def test_timeout_is_read_as_minutes(attrs, expected):
    assert _find_timeout_minutes([Deco("timeout", **attrs)]) == expected


def test_no_timeout_decorator():
    assert _find_timeout_minutes([Deco("retry", times=2)]) is None


def test_a_user_timeout_becomes_the_job_deadline_exactly():
    """Never extended.

    Metaflow kills the driver at the same moment, so a longer Job deadline
    would leave the pod running and billing with nobody watching — the very
    thing @timeout is meant to stop. An earlier version added 5 minutes and
    had it backwards.
    """
    assert _job_timeout_minutes(30, 240) == 30
    assert _job_timeout_minutes(1, 240) == 1


def test_without_a_user_timeout_the_decorator_attribute_stands():
    assert _job_timeout_minutes(None, 240) == 240
    assert _job_timeout_minutes(0, 240) == 240


def test_the_pod_is_never_given_a_longer_deadline_than_the_step_asked_for():
    """A pod outliving its driver is the runaway-billing case."""
    for minutes in (1, 5, 60, 600):
        assert _job_timeout_minutes(minutes, 240) <= minutes


# ------------------------------------------------------------------- @conda


def test_metaflows_own_lifecycle_conda_is_not_a_user_conda():
    """`--environment=fast-bakery` attaches a bare `conda` to every step.

    CondaEnvironment.decospecs() returns ("conda",), so refusing on the name
    alone refuses every flow that uses fast-bakery — which is all of them.
    """
    assert not _declares_conda_packages(Deco("conda", packages={}, libraries={}))
    assert not _declares_conda_packages(Deco("conda"))


@pytest.mark.parametrize(
    "attrs",
    [
        {"libraries": {"numpy": "1.26"}},
        {"packages": {"scipy": "1.11"}},
        {"libraries": {"a": "1"}, "packages": {"b": "2"}},
    ],
)
def test_a_user_conda_asking_for_packages_is_recognised(attrs):
    assert _declares_conda_packages(Deco("conda", **attrs))
    assert _declares_conda_packages(Deco("conda_base", **attrs))


def test_unrelated_decorators_are_not_conda():
    assert not _declares_conda_packages(Deco("pypi", packages={"pandas": ""}))
    assert not _declares_conda_packages(Deco("resources", cpu=2))
