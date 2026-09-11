---
name: decision-rubric
description: Apply a rigorous decision rubric when asking the user for a non-trivial design, architectural, or policy choice. Replaces shallow "do you agree with the author's recommendation?" prompts with a full problem statement → industry research → 3-4 options with tradeoffs → recommendation + counter-argument → question. Invoke whenever the user is being asked to resolve an open question on ANY work item — an RFC, a backlog task, a GitHub/Jira/Linear issue, a design doc — or to pick a library or pattern, set a default, choose a deprecation policy, or make any choice they would later regret if the framing were shallow. Do NOT use for trivial preferences (naming, formatting, which file to edit first).
---

# Decision rubric — how to ask the operator a non-trivial design question

Shallow question-asking is worse than not asking at all. The failure mode this rubric exists to prevent:

> Read the open question → restate the author's lean → ask "agree?". The operator picks "yes" because there's nothing else to compare against, and the decision they thought they were making was actually the author's decision rubber-stamped.

This skill exists so that does not happen.

## When to invoke this rubric

**YES — apply the rubric** when the user is choosing:

- An open-question or design-decision resolution on any work item
- A default value that ships to adopters (timeouts, retry counts, batch sizes, severity thresholds)
- A library, framework, or major dependency
- An architectural pattern (sync vs async, monolith vs split, push vs pull)
- A deprecation / migration / lifecycle policy
- A schema shape that's hard to change later (DB columns, public API contracts, file formats)
- A trade-off between two real engineering concerns
- Anything where the user's likely answer depends on context they have and you don't

**NO — skip the rubric** when:

- The choice is a personal preference (file naming, commit message style)
- The choice is fully reversible in under five minutes
- One option is dominant on every axis
- The user has already stated a preference in this session
- You're asking for missing facts ("what's the issue number?")

## The five-part rubric

Every part is non-optional when the rubric applies.

### 1. Problem statement

One short paragraph. Restate the decision in your own words. Name the axes of trade-off explicitly. If you can't write the problem statement in two sentences, you don't understand the question well enough to recommend anything.

### 2. Industry research

A short, evidence-loaded paragraph or table. What do comparable systems do? Cite specific products, conventions, or published patterns. Don't fabricate. If you don't know, say so.

### 3. Three to four options with tradeoffs

A table is usually the right shape. Columns: option label, pros, cons, verdict.

Each option must be genuinely different. At least one option should be the author's lean. At least one should be a meaningful alternative.

### 4. Recommendation + counter-argument

State the recommendation. Then immediately write the strongest counter-argument you can construct against it, and respond to that counter-argument. The counter-argument is load-bearing.

### 5. Ask the user

Exactly 3-4 mutually exclusive options. Recommended option first, with `(Recommended)` in the label. Each option's description should name the concrete cost or benefit — not "this is safer".

## Anti-patterns

- Asking "do you agree with the recommendation?"
- Putting the recommendation only in prose, not as a selectable option
- Hedge words instead of concrete costs
- Non-mutually-exclusive options
- Skipping the counter-argument because the recommendation feels obvious
- Batch-resolving multiple questions in one prompt

## Output shape

```
### <Question identifier>: <one-line restatement>

**Problem statement.** <paragraph>

**Industry research.** <bullets or table>

**Options.** <table with 3-4 rows>

**Recommendation: <Option label>.** <reasoning>

**Counter-argument.** "<steel-manned objection>" <rebuttal>

**Selected over <other options> because <load-bearing reason>.**
```

Then ask the user with 3-4 options matching the rubric.

After the user answers, write the resolution to the relevant file with full rationale — not just "answer: A".
