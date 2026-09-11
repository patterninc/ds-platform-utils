---
name: ai-sdlc-doctor
description: Audit AI-SDLC configuration health for this repo (read-only)
---

Audit this project's AI-SDLC install. Read-only unless the user passed `--fix`.

## What to check

1. Required files exist:
   - `.ai-sdlc/pipeline.yaml`
   - `.ai-sdlc/agent-role.yaml`
   - `.ai-sdlc/quality-gate.yaml`
   - `.ai-sdlc/autonomy-policy.yaml`
   - `.ai-sdlc/review-policy.md`
   - `.cursor/hooks.json`
   - `.cursor/mcp.json`
   - `.cursor/rules/ai-sdlc-governance.mdc`
2. `agent-role.yaml` lists `blockedActions` and `blockedPaths` (must include `.ai-sdlc/**`)
3. Cursor hook script is present and parseable:
   `python3 -m py_compile .cursor/hooks/ai-sdlc/enforce-blocked-actions.py`
4. MCP config points at `@ai-sdlc/mcp-advisor`
5. If `npx` is available, try:

```bash
npx --yes @ai-sdlc/orchestrator doctor --help
```

If the CLI is installed (`ai-sdlc` on PATH or via `npx @ai-sdlc/orchestrator`), run `doctor` and surface its output.

## Report

Pass / warn / fail per check, with one-line remediation. Do not modify `.ai-sdlc/**` unless the user explicitly asked for `--fix` **and** the change is mechanical (missing file restore from git). Never apply GitHub branch protection from this command.
