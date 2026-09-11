---
name: ai-sdlc-governance
description: AI-SDLC project governance rules, workflow expectations, and pre-commit checklist for ds-platform-utils. Use when implementing changes, committing, opening PRs, or deciding whether an action is allowed.
---

# AI-SDLC Governance Rules

Policy source of truth: `.ai-sdlc/agent-role.yaml`. Hooks enforce it via `.cursor/hooks/ai-sdlc/enforce-blocked-actions.py`.

## Critical rules — NEVER violate these

1. **NEVER merge any pull request.** Do not run `gh pr merge`, `git merge` into main, or any merge operation. Only create or update PRs. The human merges.
2. **Dismiss PR reviews only with a documented reason.** Prefer updating `.ai-sdlc/review-policy.md` for recurring false positives.
3. **NEVER close issues or PRs.** Do not run `gh pr close` or `gh issue close`.
4. **NEVER force push** with `git push --force` or `git push -f`. After a rebase, `git push --force-with-lease` is allowed.
5. **NEVER delete branches.** Do not run `git branch -D` or `git branch -d`.
6. **NEVER run destructive git operations.** No `git reset --hard`, `git checkout -- .`, `git restore .`.
7. **NEVER edit `.ai-sdlc/**`.** Configuration is out of scope for task work.
8. **NEVER write GitHub Actions CI-skip tokens** (`[skip ci]`, `[ci skip]`, `[no ci]`) into commit messages.

## Pre-commit checklist

This is a published Python library (`src/ds_platform_utils/`) built with `uv`, `ruff`, `pytest`, and `poethepoet`.

Before EVERY commit:

```bash
uv run poe lint
uv run pytest -m "not slow"
```

### Test file check

Before committing new Python modules under `src/`:

- Every new public function should have tests under `tests/`
- Run the relevant tests and confirm they pass before staging
- Do not rely on CI to catch missing tests

Do NOT commit if lint or tests fail. Fix first, then commit.

## Git flow

- Always rebase feature branches onto `main`. Never merge `main` into a feature branch.
- When updating a feature branch: `git fetch origin && git rebase origin/main`
- After rebase with conflicts resolved: `git push --force-with-lease origin <branch>`
- Use conventional commits: `feat:`, `fix:`, `test:`, `docs:`, `chore:`

## Workflow expectations

When given a multi-step task, complete ALL steps before stopping:

1. Research the task by reading relevant files
2. Plan the approach for non-trivial work
3. Implement the changes
4. Run lint and tests — fix any failures
5. Commit with a conventional commit message
6. Push to the branch
7. Create a PR if needed (but do NOT merge)
8. Report what was done and what remains

If blocked, say which step you are stuck on and why.

## PR workflow

- Create PRs with descriptive titles and bodies
- After pushing, tell the user the PR is ready for their review
- If CI fails or reviews request changes, fix and push again
- Use `/ai-sdlc-fix-pr` to gather and fix PR issues
- NEVER merge — always wait for the human

## Review policy

When review agents post findings:

- **APPROVE with suggestions/minors** → PR is ready for human merge
- **CHANGES_REQUESTED with critical/major** → fix the real issues, push again
- **False positives** → update `.ai-sdlc/review-policy.md`, don't dismiss reviews

## Project structure

- `src/ds_platform_utils/` — library code (Metaflow helpers, Snowflake, pandas)
- `tests/unit_tests/` — unit tests
- `tests/functional_tests/` — slower integration tests (often Snowflake)
- `docs/` — Metaflow API docs and engineering notes
- `.ai-sdlc/` — pipeline / agent-role / quality-gate config (agents must not edit)
- `.cursor/` — Cursor plugin components (skills, commands, agents, hooks)
