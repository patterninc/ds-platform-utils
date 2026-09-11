---
name: ai-sdlc-fix-pr
description: Gather CI failures and review findings on a PR, fix them, and push
argument-hint: "[pr-number]"
---

Fix PR `$ARGUMENTS` (or the open PR on this branch). Do not hardcode `--repo`.

## Step 1 — Context

```bash
PR=${ARGUMENTS:-$(gh pr view --json number --jq .number)}
gh pr view "$PR" --json number,title,headRefName,body,state,url,statusCheckRollup
gh pr checks "$PR"
gh pr diff "$PR"
```

List failing GitHub Actions jobs and review comments.

## Step 2 — Priority

1. Lint / format (`uv run poe lint`)
2. Unit tests (`uv run pytest -m "not slow"`)
3. Review findings that are critical or major (ignore documented false positives in `.ai-sdlc/review-policy.md`)
4. Functional tests only if the change requires them and credentials exist

## Step 3 — Fix, verify, push

- Implement the smallest change that clears the failures
- Re-run the relevant local checks
- Commit with a conventional message
- Push to the PR branch (`git push`; `--force-with-lease` only after a rebase)

Then summarize what failed, what you changed, and what is still red.

## Hard rules

Never merge. Never `git push --force` / `-f`. Never close the PR. Never edit `.ai-sdlc/**`.
