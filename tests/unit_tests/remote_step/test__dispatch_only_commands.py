"""Which invocations skip the step's own configuration.

`step_init` runs for every CLI invocation, including ones that only act on a
deployment that already exists. Those must not demand a resolvable team:
`argo-workflows trigger` takes no `--tag`, so a team supplied by one cannot
reach it, and requiring one made a tag-derived team unusable with Argo.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
import pytest

from remote_step.plugins.remote_step_decorator import (
    _is_dispatch_only_command,
)


@pytest.fixture
def argv(monkeypatch):
    """Set sys.argv the way Metaflow sees it: flow file first."""

    def _set(*args):
        monkeypatch.setattr("sys.argv", ["flow.py", *args])

    return _set


@pytest.mark.parametrize(
    "command",
    [
        ["argo-workflows", "trigger"],
        ["argo-workflows", "delete"],
        ["argo-workflows", "terminate"],
        ["argo-workflows", "suspend"],
        ["argo-workflows", "unsuspend"],
        ["argo-workflows", "status"],
        ["argo-workflows", "list-runs"],
        ["logs"],
        ["card"],
        ["dump"],
        ["tag"],
    ],
)
def test_dispatch_only_commands_are_recognised(argv, command):
    argv(*command)
    assert _is_dispatch_only_command() is True


@pytest.mark.parametrize(
    "command",
    [
        ["run"],
        ["resume"],
        ["step", "train"],
        # create renders the template, so it needs the full configuration
        ["argo-workflows", "create"],
    ],
)
def test_submitting_commands_are_not_exempt(argv, command):
    argv(*command)
    assert _is_dispatch_only_command() is False


def test_top_level_options_do_not_hide_the_subcommand(argv):
    """The real invocation carries --environment and --with before the command."""
    argv(
        "--environment=fast-bakery",
        "--with",
        "remote_step:team=forecasting",
        "argo-workflows",
        "trigger",
    )
    assert _is_dispatch_only_command() is True


def test_an_option_value_is_not_mistaken_for_the_subcommand(argv):
    """`--with remote_step` puts a bare word before the subcommand."""
    argv("--with", "remote_step", "run")
    assert _is_dispatch_only_command() is False


def test_option_value_that_happens_to_match_a_command_name(argv):
    """A value like `--branch logs` must not read as the `logs` command."""
    argv("--branch", "logs", "run")
    assert _is_dispatch_only_command() is False


def test_equals_form_options_are_skipped(argv):
    argv("--environment=fast-bakery", "run")
    assert _is_dispatch_only_command() is False


def test_no_subcommand_at_all(argv):
    argv("--environment=fast-bakery")
    assert _is_dispatch_only_command() is False


def test_bare_invocation(argv):
    argv()
    assert _is_dispatch_only_command() is False
