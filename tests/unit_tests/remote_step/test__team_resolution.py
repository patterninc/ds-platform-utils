"""Where a step runs when nothing, or several things, name a team.

The team is the Kubernetes namespace, so it decides which Kueue ClusterQueue
the step spends. Three sources, in order: `team=` on the decorator, then
`--tag ds.domain:<team>` on the run, then the sandbox fallback.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
import pytest

from metaflow_extensions.remote_step.plugins.remote_step_decorator import (
    FALLBACK_TEAM,
    TEAM_TAG_PREFIX,
    _team_from_tags,
)


@pytest.fixture
def argv(monkeypatch):
    def _set(*args):
        monkeypatch.setattr("sys.argv", ["flow.py", *args])

    return _set


def test_the_fallback_is_sandbox():
    """Named so a pod landing here by accident reads as misplaced."""
    assert FALLBACK_TEAM == "sandbox"


def test_a_domain_tag_supplies_the_team(argv):
    argv("run", "--tag", f"{TEAM_TAG_PREFIX}forecasting")
    assert _team_from_tags() == "forecasting"


def test_the_equals_form_works_too(argv):
    argv("run", f"--tag={TEAM_TAG_PREFIX}forecasting")
    assert _team_from_tags() == "forecasting"


def test_a_domain_tag_is_read_on_an_argo_deploy(argv):
    argv("--with", "remote_step", "argo-workflows", "create", "--tag", f"{TEAM_TAG_PREFIX}revops")
    assert _team_from_tags() == "revops"


def test_unrelated_tags_are_ignored(argv):
    argv("run", "--tag", "user:chinmay", "--tag", "purpose:backfill")
    assert _team_from_tags() is None


def test_no_tags_at_all(argv):
    """This is what makes the step fall back to sandbox."""
    argv("run")
    assert _team_from_tags() is None


def test_a_domain_tag_among_others(argv):
    argv("run", "--tag", "user:chinmay", "--tag", f"{TEAM_TAG_PREFIX}content", "--tag", "x:y")
    assert _team_from_tags() == "content"


def test_two_tags_naming_the_same_team_is_fine(argv):
    argv("run", "--tag", f"{TEAM_TAG_PREFIX}nlp", "--tag", f"{TEAM_TAG_PREFIX}nlp")
    assert _team_from_tags() == "nlp"


def test_two_tags_naming_different_teams_is_refused(argv):
    """Silently picking one would spend the wrong team's quota."""
    argv("run", "--tag", f"{TEAM_TAG_PREFIX}forecasting", "--tag", f"{TEAM_TAG_PREFIX}nlp")
    with pytest.raises(Exception) as excinfo:
        _team_from_tags()
    assert "forecasting" in str(excinfo.value)
    assert "nlp" in str(excinfo.value)


@pytest.mark.parametrize("team", ["demand-generation", "market-intelligence"])
def test_a_hyphenated_team_survives(argv, team):
    argv("run", "--tag", f"{TEAM_TAG_PREFIX}{team}")
    assert _team_from_tags() == team
