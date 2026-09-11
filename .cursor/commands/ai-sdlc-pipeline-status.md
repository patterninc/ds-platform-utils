---
name: ai-sdlc-pipeline-status
description: Show AI-SDLC / GitHub pipeline status for the current branch or an issue
argument-hint: "[issue-or-pr-number]"
---

Show pipeline status. Do not hardcode `--repo`.

## Mode

- No argument → current branch: open PR, checks, reviews
- Numeric / `#N` → GitHub issue, plus linked PRs

```bash
BRANCH=$(git branch --show-current)
gh pr view --json number,title,state,url,statusCheckRollup,reviews,isDraft
gh pr checks
```

For an issue:

```bash
gh issue view "$ARGUMENTS" --json number,title,state,labels,assignees,url
gh pr list --search "$ARGUMENTS" --json number,title,state,headRefName,url
```

## Report

- Issue / PR title, state, URL
- CI checks (pass / fail / pending)
- Review state
- Next action:
  - CI failing → run `/ai-sdlc-fix-pr`
  - Reviews requesting changes → fix findings or run `/ai-sdlc-fix-pr`
  - All green → ready for **human** merge

Do not merge. Do not close.
