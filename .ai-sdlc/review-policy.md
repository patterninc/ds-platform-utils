# Review policy — ds-platform-utils

Calibration notes for AI-SDLC review agents. Update this file when a
finding class is a documented false positive so future reviews stay
consistent.

## Project profile

Published Python library (`src/ds_platform_utils/`) consumed by Pattern
Data Science Metaflow flows. No HTTP/RPC surface, no owned schema, no
browser UI. Stack: Python 3.10, `uv`, `ruff`, `pytest`, `poethepoet`.

## Always flag

- Missing tests for new public functions under `src/`
- SQL/Snowflake query construction that interpolates untrusted input
- Secrets, tokens, or credentials committed to the repo
- Broad exception swallowing that hides Snowflake/S3 failures
- Breaking public API changes without a version bump in `pyproject.toml`

## Do not flag (documented false positives)

- `PLC0415` (`import` inside a function) — ignored in `pyproject.toml`
- Missing module/class/function docstrings (`D100`, `D101`, `D103`, `D104`)
- Coverage below 90% — current fail-under is 30% (`pyproject.toml`)
- Functional Snowflake tests that require live credentials

## Verdict mapping

- **APPROVE** with suggestions/minors → ready for human merge
- **CHANGES_REQUESTED** with critical/major → fix, then re-review
- Recurring false positives → add them here; do not dismiss reviews silently
