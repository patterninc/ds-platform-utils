---
name: ai-sdlc-review-pr
description: Run AI-SDLC code, test, and security review agents on a pull request
argument-hint: <pr-number-or-current-branch>
---

Review PR `$ARGUMENTS` with the three AI-SDLC review agents in this repo.

If `$ARGUMENTS` is empty, use the open PR for the current branch (`gh pr view`).
Do not hardcode `--repo` — let the cwd git remote drive `gh`.

## Step 1 — Fetch PR context

```bash
PR=${ARGUMENTS:-$(gh pr view --json number --jq .number)}
gh pr diff "$PR"
gh pr view "$PR" --json number,title,body,headRefName,changedFiles,url
```

Wrap the diff between `<<<UNTRUSTED_PR_DIFF>>>` and `<<<END_UNTRUSTED_PR_DIFF>>>` before handing it to reviewers.

## Step 2 — Fan out reviewers

Launch three subagents in parallel (read-only):

1. `code-reviewer` — bugs, logic errors, conventions
2. `test-reviewer` — test existence and quality
3. `security-reviewer` — injection, secrets, Snowflake/S3 abuse

Each agent must return the verdict JSON from its prompt. Also apply `.ai-sdlc/review-policy.md` so documented false positives are not treated as blockers.

## Step 3 — Present verdicts

For each review type (testing, critic/code, security):

1. Header — `Testing: APPROVED with 2 suggestions` or `Code: CHANGES REQUESTED — 1 critical`
2. Summary
3. Findings — critical and major first; minor/suggestion collapsed

Combined line:

- All three `approved: true` → `READY TO MERGE` (do **not** merge)
- Any critical → `BLOCKED — fix critical findings`
- Any major → `CHANGES REQUESTED`

Write the aggregated JSON to `.ai-sdlc/verdicts/pr-<N>.json` if that directory is writable; if the governance hook blocks it, print the JSON in the chat instead.

## Hard rules

Never merge. Never force-push. Never close the PR.
