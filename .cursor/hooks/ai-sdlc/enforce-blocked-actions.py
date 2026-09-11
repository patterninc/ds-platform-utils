#!/usr/bin/env python3
"""AI-SDLC governance hook for Cursor (beforeShellExecution + preToolUse).

Enforces blockedActions / blockedPaths from .ai-sdlc/agent-role.yaml.
Fail-open: any parse or I/O error allows the action.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import subprocess
import sys
from pathlib import Path

DEFAULT_BLOCKED_ACTIONS = [
    "gh pr merge*",
    "git merge*",
    "git push --force*",
    "git push -f*",
    "gh pr close*",
    "gh issue close*",
    "git branch -D*",
    "git branch -d*",
    "git reset --hard*",
    "git checkout -- .",
    "git restore .",
]
DEFAULT_BLOCKED_PATHS = [".ai-sdlc/**"]
ALWAYS_BLOCKED_PATHS = [".ai-sdlc/**"]


def _fail_open() -> None:
    sys.stdout.write(json.dumps({"permission": "allow"}) + "\n")
    raise SystemExit(0)


def _deny(message: str) -> None:
    sys.stdout.write(
        json.dumps(
            {
                "continue": True,
                "permission": "deny",
                "user_message": message,
                "agent_message": message,
            }
        )
        + "\n"
    )
    raise SystemExit(0)


def _allow() -> None:
    sys.stdout.write(json.dumps({"permission": "allow"}) + "\n")
    raise SystemExit(0)


def _read_stdin() -> dict:
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    return json.loads(raw)


def _project_root() -> Path:
    env = os.environ.get("CURSOR_PROJECT_DIR") or os.environ.get("CLAUDE_PROJECT_DIR")
    if env:
        return Path(env)
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        return Path(out.strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        return Path.cwd()


def _parse_list_field(yaml_text: str, field: str) -> list[str]:
    items: list[str] = []
    in_section = False
    for line in yaml_text.splitlines():
        if re.match(rf"^\s*{re.escape(field)}:\s*$", line):
            in_section = True
            continue
        if in_section:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            # Next mapping key (any indent) ends the list.
            if re.match(r"^[A-Za-z0-9_-]+:\s*", stripped) and not stripped.startswith("-"):
                break
            match = re.match(r"^\s+-\s+['\"]?(.+?)['\"]?\s*$", line)
            if match:
                items.append(match.group(1))
    return items


def _load_policy(project: Path) -> tuple[list[str], list[str]]:
    path = project / ".ai-sdlc" / "agent-role.yaml"
    if not path.is_file():
        return DEFAULT_BLOCKED_ACTIONS, DEFAULT_BLOCKED_PATHS
    yaml_text = path.read_text(encoding="utf-8")
    actions = _parse_list_field(yaml_text, "blockedActions") or DEFAULT_BLOCKED_ACTIONS
    paths = _parse_list_field(yaml_text, "blockedPaths") or DEFAULT_BLOCKED_PATHS
    return actions, paths


def _normalize_command(command: str) -> str:
    return re.sub(r"\s+", " ", command.strip())


def _command_matches(command: str, pattern: str) -> bool:
    cmd = _normalize_command(command)
    pat = pattern.strip()
    # `git merge*` must not match `git merge-base` / `git merge-tree`.
    if pat == "git merge*":
        return bool(re.search(r"(^|[\s;|&])git merge(\s|$)", cmd)) and not re.search(r"(^|[\s;|&])git merge-", cmd)
    if fnmatch.fnmatch(cmd, pat):
        return True
    for segment in re.split(r"\s*(?:&&|\|\||;|\|)\s*", cmd):
        if fnmatch.fnmatch(segment.strip(), pat):
            return True
    return False


def _is_force_with_lease(command: str) -> bool:
    return "--force-with-lease" in command


def _enforce_command(command: str, blocked_actions: list[str]) -> None:
    if not command:
        return
    for pattern in blocked_actions:
        if "push --force" in pattern and _is_force_with_lease(command):
            continue
        if _command_matches(command, pattern):
            if re.search(r"\bgh pr merge\b", command) and "--auto" in command:
                continue
            _deny(
                f"AI-SDLC blocked action: command matches '{pattern}'. "
                "Do not merge, force-push, close issues/PRs, delete branches, "
                "or run destructive git resets. Use --force-with-lease after a "
                "rebase if the branch needs updating."
            )


def _glob_to_parts(pattern: str) -> str:
    normalized = pattern.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _path_is_blocked(file_path: str, project: Path, blocked_paths: list[str]) -> str | None:
    if not file_path:
        return None
    abs_path = Path(file_path)
    if not abs_path.is_absolute():
        abs_path = (project / file_path).resolve()
    try:
        rel = abs_path.resolve().relative_to(project.resolve()).as_posix()
    except ValueError:
        rel = abs_path.as_posix()

    for pattern in [*ALWAYS_BLOCKED_PATHS, *blocked_paths]:
        normalized = _glob_to_parts(pattern)
        if fnmatch.fnmatch(rel, normalized) or fnmatch.fnmatch(rel, normalized.rstrip("/")):
            return pattern
        if normalized.endswith("/**") and (rel == normalized[:-3].rstrip("/") or rel.startswith(normalized[:-3])):
            return pattern
    return None


def _tool_file_path(tool_input: object) -> str:
    if not isinstance(tool_input, dict):
        return ""
    for key in ("file_path", "path", "target_file", "filePath"):
        value = tool_input.get(key)
        if isinstance(value, str):
            return value
    return ""


def _tool_command(tool_input: object) -> str:
    if not isinstance(tool_input, dict):
        return ""
    value = tool_input.get("command")
    return value if isinstance(value, str) else ""


def main() -> None:
    try:
        payload = _read_stdin()
    except json.JSONDecodeError:
        _fail_open()

    try:
        project = _project_root()
        blocked_actions, blocked_paths = _load_policy(project)

        command = payload.get("command") if isinstance(payload.get("command"), str) else ""
        tool_name = str(payload.get("tool_name") or payload.get("toolName") or "")
        tool_input = payload.get("tool_input") or payload.get("toolInput") or {}

        if command:
            _enforce_command(command, blocked_actions)

        if tool_name.lower() in {"shell", "bash"}:
            _enforce_command(_tool_command(tool_input) or command, blocked_actions)

        if tool_name.lower() in {"write", "edit", "delete", "strreplace", "applypatch"}:
            matched = _path_is_blocked(_tool_file_path(tool_input), project, blocked_paths)
            if matched:
                _deny(
                    f"AI-SDLC blocked path: writes under '{matched}' are refused. "
                    "Governance config is out of scope for task work."
                )

        file_path = payload.get("file_path") if isinstance(payload.get("file_path"), str) else ""
        if file_path and payload.get("hook_event_name") in {
            "afterFileEdit",
            "preToolUse",
            "beforeReadFile",
        }:
            matched = _path_is_blocked(file_path, project, blocked_paths)
            if matched and payload.get("hook_event_name") != "beforeReadFile":
                _deny(
                    f"AI-SDLC blocked path: writes under '{matched}' are refused. "
                    "Governance config is out of scope for task work."
                )

        _allow()
    except SystemExit:
        raise
    except Exception:  # noqa: BLE001 — fail-open is the hook contract
        _fail_open()


if __name__ == "__main__":
    main()
