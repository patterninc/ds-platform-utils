"""Sibling decorators on a `@remote_step` step.

Each of these is set up by Metaflow for the *driver* task. The step body runs
in another pod in another cluster, so anything the sibling configures has to
be read here and carried across, or the body silently runs without it.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest
from remote_step.plugins.remote_step_decorator import (
    DRIVER_TIMEOUT_SLACK_MINUTES,
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
        ({"hours": 0, "minutes": 0, "seconds": 0}, None),  # @timeout() with nothing set
        ({}, None),
    ],
)
def test_timeout_is_read_as_minutes(attrs, expected):
    assert _find_timeout_minutes([Deco("timeout", **attrs)]) == expected


def test_no_timeout_decorator():
    assert _find_timeout_minutes([Deco("retry", times=2)]) is None


def test_a_user_timeout_sets_the_job_deadline_with_slack():
    """The driver must outlive the pod.

    Otherwise a timeout kills both in a race and nothing reports why.
    """
    assert _job_timeout_minutes(30, 240) == 30 + DRIVER_TIMEOUT_SLACK_MINUTES


def test_without_a_user_timeout_the_decorator_attribute_stands():
    assert _job_timeout_minutes(None, 240) == 240
    assert _job_timeout_minutes(0, 240) == 240


def test_the_pod_is_never_given_a_shorter_deadline_than_the_step_asked_for():
    for minutes in (1, 5, 60, 600):
        assert _job_timeout_minutes(minutes, 240) > minutes


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
