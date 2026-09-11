---
name: ai-sdlc-triage
description: Score and triage a GitHub issue for AI-SDLC admission (effort, risk, routing)
argument-hint: <issue-number>
---

Triage GitHub issue `$ARGUMENTS` for this repo. Do not hardcode `--repo`.

## Step 1 — Fetch the issue

```bash
gh issue view "$ARGUMENTS" --json number,title,body,labels,assignees,comments,state,url
```

If `$ARGUMENTS` is empty, list open issues labeled `ai-eligible` (or unlabeled if that label does not exist) and ask which one to triage.

## Step 2 — Score

Produce a structured admission score:

| Signal | What to look for |
| --- | --- |
| Conviction | How clearly is the problem stated? |
| Demand | Is this blocking users of the library? |
| Effort | Files / subsystems likely touched (`src/`, tests, docs) |
| Risk | Snowflake/S3/public API / CI |
| Testability | Can it be covered with unit tests, or only functional Snowflake tests? |
| Routing | small fix / feature / docs / spike |

Recommend a complexity 1–10 and a routing:

- 1–3: current agent, keep the PR small
- 4–7: implement with tests + `/ai-sdlc-review-pr`
- 8–10: needs a design note in the issue before coding

## Step 3 — Report

Print:

- Issue title, URL, labels
- Score table
- Recommended next action (implement, ask a clarifying question, or split the work)
- Whether it should get the `ai-eligible` label

Do not close the issue. Do not start implementation unless the user asked.
