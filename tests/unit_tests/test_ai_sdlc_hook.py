"""Tests for the Cursor AI-SDLC blocked-action hook."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

HOOK_PATH = Path(__file__).resolve().parents[2] / ".cursor" / "hooks" / "ai-sdlc" / "enforce-blocked-actions.py"


@pytest.fixture(scope="module")
def hook():
    spec = importlib.util.spec_from_file_location("ai_sdlc_enforce_hook", HOOK_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_blocked_paths_does_not_include_actions(hook) -> None:
    yaml_text = (Path(__file__).resolve().parents[2] / ".ai-sdlc" / "agent-role.yaml").read_text()
    paths = hook._parse_list_field(yaml_text, "blockedPaths")
    actions = hook._parse_list_field(yaml_text, "blockedActions")
    assert paths == [".github/workflows/**", ".ai-sdlc/**"]
    assert "gh pr merge*" in actions
    assert "gh pr merge*" not in paths


def test_ai_sdlc_config_writes_are_blocked(hook) -> None:
    project = Path(__file__).resolve().parents[2]
    matched = hook._path_is_blocked(
        str(project / ".ai-sdlc" / "agent-role.yaml"),
        project,
        [".github/workflows/**", ".ai-sdlc/**"],
    )
    assert matched == ".ai-sdlc/**"


def test_src_writes_are_allowed(hook) -> None:
    project = Path(__file__).resolve().parents[2]
    matched = hook._path_is_blocked(
        str(project / "src" / "ds_platform_utils" / "pandas_utils.py"),
        project,
        [".github/workflows/**", ".ai-sdlc/**"],
    )
    assert matched is None


@pytest.mark.parametrize(
    ("command", "should_match"),
    [
        ("gh pr merge 12 --squash", True),
        ("gh pr merge 12 --auto --squash", True),  # pattern matches; --auto is exempted later
        ("git push --force origin HEAD", True),
        ("git push --force-with-lease origin HEAD", True),
        ("git merge origin/main", True),
        ("git merge-base HEAD origin/main", False),
        ("uv run pytest", False),
    ],
)
def test_command_glob_matching(hook, command: str, should_match: bool) -> None:
    actions = hook._parse_list_field(
        (Path(__file__).resolve().parents[2] / ".ai-sdlc" / "agent-role.yaml").read_text(),
        "blockedActions",
    )
    matches = [p for p in actions if hook._command_matches(command, p)]
    assert bool(matches) is should_match
