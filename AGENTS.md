# Agent instructions

This repository is `patterninc/ds-platform-utils`, a published Python
library of Metaflow / Snowflake / pandas helpers for Pattern Data Science.

AI-SDLC (Cursor plugin, project scope) is installed. Follow
`.cursor/rules/ai-sdlc-governance.mdc` and `.ai-sdlc/agent-role.yaml`.

## Commands

- `/ai-sdlc-review-pr` — code + test + security review
- `/ai-sdlc-triage` — score a GitHub issue
- `/ai-sdlc-pipeline-status` — PR / issue CI and review status
- `/ai-sdlc-fix-pr` — fix CI failures and review findings
- `/ai-sdlc-doctor` — audit AI-SDLC config health

## Skills

- `ai-sdlc-governance` — hard rules and Python pre-commit checklist
- `decision-rubric` — how to ask non-trivial design questions

## Quality

- Lint: `uv run poe lint`
- Tests: `uv run pytest -m "not slow"`
- Never merge PRs; a human merges
