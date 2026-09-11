# Engineering Best Practices Audit — ds-platform-utils

|                    |                                                                |
| ------------------ | -------------------------------------------------------------- |
| **Audit date**     | 2026-09-11                                                      |
| **Auditor**        | Claude — gauge-repo skill                                       |
| **Rubric version** | `item-credit-v1` — 2026-09-04 (49-item engineering checklist)    |
| **Commit audited** | `60ea68e` (`main`)                                              |

## Repo profile

`ds-platform-utils` is a **published Python library** — a `src/`-layout package
(`src/ds_platform_utils/`, ~3.4k lines) built with `hatchling` and consumed by Pattern Data
Science Metaflow flows. It is not a deployed service: there is no Dockerfile, no Terraform, no
`main()` entrypoint, and no HTTP or RPC surface. CI builds a wheel and pushes a `vX.Y.Z` git tag;
consumers install it as a `git+https://` dependency resolved through `uv.lock`
([`docs/metaflow/pypi_packages.md`](metaflow/pypi_packages.md)).

The stack is Python 3.10 (`.python-version`) with `uv` for resolution/locking, `ruff` for lint and
format, `pytest` (+ `pytest-cov`, `pytest-xdist`) for tests, and `poethepoet` for task running. The
library's external surfaces are **Snowflake** (via the Metaflow `snowflake-default` integration),
**S3** (via Metaflow's S3 client), and the **Outerbounds/Metaflow** platform. It owns no database
schema of its own — `write_audit_publish` performs DDL on tables the calling flow owns — and it
owns no browser UI, CLI binary, or AWS infrastructure of its own. Credentials are never repo-local:
CI authenticates to Outerbounds with a GitHub OIDC service principal, and Snowflake roles are
resolved per perimeter (`default` vs `prod`) at runtime.

Team signals: seven contributors in the GitHub contributor list (`vinay79n`, `amitvikramraj`,
`abhishek-pattern`, `tanay-pattern`, `phitoduck`, plus a CI runner and Copilot) — a small team, not a
solo maintainer. GitHub ownership was verified as `patterninc/ds-platform-utils`
(`gh repo view --json nameWithOwner`), so Pattern's inherited Wiz and Toolsmith controls apply; Wiz
check runs (SAST, Secret, Vulnerability, IaC, Data, Software Management scanners) were observed on
PR #34's head commit. The repo is **public**, has a Backstage component entry
(`backstage.yaml`, owner `data_science-sre-updates`, cost center `DATASCIENCE`), and carries no
`LICENSE` file or `license` field in `pyproject.toml`.

That profile — a published, dependency-only library with no deployed runtime, no owned schema, and
no UI — is what justifies every "Not applicable" verdict below.

## Scorecard

| Metric                  | Value      |
| ----------------------- | ---------- |
| **Critical gates**      | **RED**    |
| **Adjusted compliance** | **46.3%**  |

Critical gates are **RED**: items 2 (AGENTS.md) and 6 (README setup & run instructions) are Gaps,
and item 16 (required CI checks before merge) is Partial. The remaining applicable gates — 15, 19,
20, 23, 24, 40, 48 — are Met.

Adjusted compliance is calculated independently of the gates:

`(14 Met + 0.5 × 9 Partial) / (49 total − 9 justified N/A) = 18.5 / 40 = 46.3%`

### Status totals

| Status    | Items  |
| --------- | -----: |
| Met       |     14 |
| Partial   |      9 |
| Gap       |     17 |
| N/A       |      9 |
| **Total** | **49** |

### Per-category breakdown

| Category                  |    Met | Partial |    Gap |    N/A |
| ------------------------- | -----: | ------: | -----: | -----: |
| Documentation & Context   |      1 |       0 |      6 |      2 |
| Guardrails & Enforcement  |      6 |       4 |      3 |      0 |
| Testing & Feedback Loops  |      3 |       3 |      4 |      3 |
| Environment & Tooling     |      4 |       2 |      3 |      4 |
| Agent dispatch            |      0 |       0 |      1 |      0 |
| **Total**                 | **14** |   **9** | **17** |  **9** |

## Documentation & Context

| #   | Practice                    | Status               | Evidence                                                                                                                                                                        | Recommendation / rationale                                                                                                                                                                    |
| --- | --------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Skills / prompt workflows   | **Gap**              | No `.claude/skills/`, `.claude/commands/`, or tracked `.cursor/rules/`; `git ls-files` returns 60 files, none agent-facing                                                        | Add repo-local skills for the recurring tasks here — "add a Metaflow helper + docs page", "cut a release (bump `pyproject.toml` version, let CI tag)", "run the slow functional tests safely". |
| 2   | AGENTS.md                   | **Gap** *(critical)* | No `AGENTS.md`, `CLAUDE.md`, or `.cursorrules` on `main`                                                                                                                          | Add `AGENTS.md` covering: `uv sync`, `poe lint`, `poe test`, the ruff ignore list rationale, the `src/` layout, `_`-prefixed internal modules, and the "bump `project.version` or CI fails" rule. |
| 3   | Architecture decision records | **Gap**            | `docs/` holds API pages only; no `docs/adr/` or `docs/decisions/`                                                                                                                 | Record the decisions already embedded in code comments as dated ADRs: why `uv.lock` (not `pyproject.toml`) drives `@pypi` environments, why write-audit-publish clones to `DATA_SCIENCE_STAGE`, why branch coverage is disabled. |
| 4   | Runbooks                    | **Met**              | [`docs/metaflow/private_repo_access.md`](metaflow/private_repo_access.md) (setup, token rotation, troubleshooting table); [`docs/metaflow/table_ownership_registry.md`](metaflow/table_ownership_registry.md) (refresh cadence, lag, query) | —                                                                                                                                                                                             |
| 5   | API contract docs           | **Not applicable**   | No HTTP/gRPC/GraphQL surface — the package is imported directly                                                                                                                  | The wire shape is a Python API: it is typed (`src/ds_platform_utils/py.typed`), documented per helper under `docs/metaflow/`, and exercised by unit tests. OpenAPI/protobuf would describe nothing. |
| 6   | README with setup & run     | **Gap** *(critical)* | `README.md` is 16 lines: a title plus two link lists. No description, install, `uv sync`, `poe test`, or contribution steps                                                        | Expand `README.md` with what the library is, how to install it into a flow project, how to set up a dev environment (`uv sync`, `pre-commit install`), and how to run lint/tests.               |
| 7   | Changelog with migration notes | **Gap**           | No `CHANGELOG.md`; `tag-version` job pushes `v$VERSION` tags (`v0.6.1` … `v0.2.2`) with no notes; `gh release list` is empty                                                       | Generate a changelog from the already-conventional PR titles (Release Please is proposed in open PR #36) and call out breaking changes for flows pinned to a commit SHA.                        |
| 8   | On-call playbooks           | **Not applicable**   | No deployed runtime; failures surface inside consumers' Metaflow runs, which their own teams own                                                                                  | Nothing pages on this repo. The one operational break-glass it does own — the Fast Bakery Git integration — is covered by the runbook in item 4.                                                |
| 9   | CODEOWNERS                  | **Gap**              | No `.github/CODEOWNERS`; ruleset 3174764 sets `require_code_owner_review: false`. Seven contributors across `metaflow/` and `_snowflake/`                                          | Add `CODEOWNERS` mapping `src/ds_platform_utils/_snowflake/` and `src/ds_platform_utils/metaflow/` to their maintainers so reviews auto-route instead of relying on manual assignment.          |

## Guardrails & Enforcement

| #   | Practice                    | Status               | Evidence                                                                                                                                                          | Recommendation / rationale                                                                                                                                                        |
| --- | --------------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 10  | Linters                     | **Met**              | `pyproject.toml` `[tool.ruff.lint]` extends `E`, `B`, `PL`, `I`, `D`, `N`; ruff hook in `.pre-commit-config.yaml`; CI job *Lint, Format, and Static Code Quality Checks* | —                                                                                                                                                                                 |
| 11  | Formatters                  | **Met**              | `ruff-format` hook pinned at `v0.9.6` in `.pre-commit-config.yaml`, run in CI via `poe lint`; `line-length = 119`                                                    | —                                                                                                                                                                                 |
| 12  | Type checking               | **Partial**          | `py.typed` ships type information, annotations are used throughout, `ms-python.mypy-type-checker` is recommended in `.vscode/extensions.json`, and `# type: ignore[attr-defined]` appears in `snowflake_connection.py` — but no mypy/pyright config, no dev dependency, no hook, no CI job | Add `mypy` (or `pyright`) to the `dev` dependency group with config in `pyproject.toml`, and run it in `poe lint` so the shipped `py.typed` promise is actually verified.           |
| 13  | Pre-commit hooks            | **Partial**          | `.pre-commit-config.yaml` has 12 hooks (merge-conflict, large files, `detect-private-key`, `no-commit-to-branch`, ruff, ruff-format) and CI runs them — but nothing installs the git hook: no `pre-commit install` in any docs or poe task | Add a `poe install-hooks` task (`uvx pre-commit install`) and reference it in the README setup section, so the hooks run at commit time and not only in CI.                        |
| 14  | Commit message conventions  | **Partial**          | Merged PR titles are consistently Conventional Commits (`feat(pypi):`, `docs:`, `chore:`); a `lint-pr-title.yaml` workflow exists only on an unmerged branch, not on `main`; no commitlint config or documented convention | Merge the PR-title lint workflow and state the convention in `AGENTS.md`/`README.md` so the format is enforced rather than habitual.                                               |
| 15  | Branch protection rules     | **Met**              | Org ruleset `require-pr-review` (id 3174764, `enforcement: active`) on `~DEFAULT_BRANCH`: 1 approving review, stale reviews dismissed on push, `deletion` and `non_fast_forward` blocked, `current_user_can_bypass: never` | —                                                                                                                                                                                 |
| 16  | Required CI checks before merge | **Partial** *(critical)* | Six jobs run on every PR (verified on PR #34: Check Version, Lint/Format, Build Wheel, Run Tests, plus Wiz scanners) — but ruleset 3174764 contains **no** `required_status_checks` rule, so a red build does not block merge | Add a `required_status_checks` rule to the ruleset pinning *Lint, Format, and Static Code Quality Checks*, *Build Wheel*, *Run Tests*, and *Check Version*. This is the single highest-value fix in this audit. |
| 17  | Dependency allow/deny lists | **Gap**              | `[project].dependencies` lists 11 runtime deps with open lower bounds (`pandas`, `pyarrow`, `PyYAML` unbounded); no allow-list, deny-list, or upper-bound policy                                                          | Document (or enforce via a lint rule) which third-party packages a library consumed by every DS flow may add, and set upper bounds where a major bump would break consumers.       |
| 18  | License compliance scanning | **Gap**              | No `license` field in `pyproject.toml`, no `LICENSE` file, no license-check job — in a **public** repo                                                            | Add a `LICENSE` and `[project].license`, then add a license scan (e.g. `pip-licenses`/`uv`-based check) to CI. Wiz's Software Management scanner is not license-compliance evidence. |
| 19  | Secret scanning             | **Met**              | Inherited Pattern Wiz policy — *Wiz Secret Scanner* check run observed on PR #34's head; reinforced locally by the `detect-private-key` pre-commit hook           | —                                                                                                                                                                                 |
| 20  | SAST / static analysis gates | **Met**             | Inherited Pattern Wiz policy — *Wiz SAST Scanner* and *Wiz Vulnerability Scanner* check runs observed on PR #34's head                                            | —                                                                                                                                                                                 |
| 21  | Max complexity limits       | **Met**              | `[tool.ruff.lint.mccabe] max-complexity = 10`, plus the full `PL` (pylint-equivalent) rule set, enforced in CI                                                    | —                                                                                                                                                                                 |
| 22  | Import boundary enforcement | **Gap**              | Internal packages are signalled only by convention (`src/ds_platform_utils/_snowflake/`, `metaflow/_consts.py`); no ruff `TID` banned-api rules and no `__all__` in the empty `src/ds_platform_utils/__init__.py` | Enable ruff `flake8-tidy-imports` (`TID251`) to stop consumers and public modules from reaching into `_`-prefixed internals, keeping the public API the only thing you must keep stable. |

## Testing & Feedback Loops

| #   | Practice                    | Status             | Evidence                                                                                                                                                                             | Recommendation / rationale                                                                                                                                                    |
| --- | --------------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 23  | Unit tests                  | **Met**            | Seven modules under `tests/unit_tests/` covering SQL generation, object tags, `uv.lock` → `@pypi` resolution, and config validation; parametrized and `monkeypatch`-isolated; run in CI *Run Tests* | —                                                                                                                                                                             |
| 24  | Integration tests           | **Met**            | `tests/functional_tests/` drives real Metaflow flows via `subprocess` against live Snowflake and S3 (`test__publish.py`, `test__pandas_s3.py`, `test__warehouse.py`); CI authenticates with `outerbounds service-principal-configure` and runs them (no `-m "not slow"` deselection) | —                                                                                                                                                                             |
| 25  | Snapshot / golden-file tests | **Gap**           | No snapshot library or `testdata`/golden directory; generated SQL is asserted against strings inline in the test bodies                                                              | The library's main output *is* generated SQL. Store the expected write-audit-publish and tag statements as golden files so a diff is reviewable instead of buried in assertions. |
| 26  | Contract tests              | **Not applicable** | No service boundary — consumers `import ds_platform_utils` in-process                                                                                                                | There is no independent producer/consumer pair to verify. The equivalent guarantee is the typed public API plus the functional tests that run real flows against it.            |
| 27  | End-to-end tests            | **Not applicable** | No browser UI                                                                                                                                                                        | The full-flow equivalent already exists: `tests/functional_tests/` executes complete Metaflow flows end to end.                                                                |
| 28  | Visual regression tests     | **Not applicable** | No rendered visual surface of any kind                                                                                                                                               | Nothing to screenshot-diff.                                                                                                                                                   |
| 29  | Test coverage thresholds    | **Partial**        | `--cov-fail-under=30` in `[tool.pytest.ini_options].addopts`, with the in-repo comment "THIS SHOULD BE ATLEAST 90% in the future" — but CI overrides it with `--no-cov` to dodge a `DataError` when combining branch and statement data | Fix the coverage-data conflict (the `branch = true` / `parallel = true` combination under `-n auto`), re-enable coverage in CI, and ratchet the floor upward from its current 30%. |
| 30  | Mutation testing            | **Gap**            | No `mutmut`/`cosmic-ray` config                                                                                                                                                      | Low priority, but worth pointing `mutmut` at `_snowflake/` — identifier validation and SQL templating are exactly where a passing-but-vacuous test is expensive.                |
| 31  | Load / performance benchmarks | **Gap**          | No `pytest-benchmark` or timing assertions, despite `batch_inference_pipeline.py` existing to parallelize large batch workloads                                                       | Add a benchmark for the batch-inference fan-out so a regression in worker/batch partitioning is caught before flows slow down in production.                                    |
| 32  | Flaky test quarantine       | **Gap**            | A `slow` marker is registered and applied to six functional tests, but it is only a deselect switch — no quarantine marker, no retry policy, no tracking of live-service flakes       | Quarantine the live-Snowflake/Outerbounds tests behind a dedicated marker (or `pytest-rerunfailures`) so an infrastructure hiccup does not block unrelated PRs.                  |
| 33  | Structured CI output        | **Partial**        | `--junitxml=test-reports/report.xml` and `--cov-report=xml` are configured in `addopts`, but the *Run Tests* job never uploads or publishes them, and `--no-cov` suppresses the coverage half | Upload `test-reports/` as an artifact and surface the JUnit XML as check annotations so failures are readable without opening raw logs.                                        |
| 34  | Deterministic test fixtures | **Partial**        | Unit tests are hermetic — inline `textwrap.dedent` lockfile literals, `tmp_path`, `monkeypatch` on `sys.argv`/env — but `tests/fixtures/general_fixtures.py` is still the untouched `sample_fixture` placeholder and functional tests depend on live Snowflake/S3 state with no recorded fixtures | Record or stub the Snowflake responses the functional tests depend on so the same test run is reproducible off-platform, and replace the placeholder fixture module with the real shared fixtures. |
| 35  | Smoke tests for deploys     | **Met**            | The *Run Tests* job installs the built artifact (`uv pip install ./dist/*whl`) and runs the suite against the installed wheel — resolving `COVERAGE_DIR` from the installed package — before *Tag Version* publishes the release tag | —                                                                                                                                                                             |

## Environment & Tooling

| #   | Practice                    | Status             | Evidence                                                                                                                                                       | Recommendation / rationale                                                                                                                                              |
| --- | --------------------------- | ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 36  | Devcontainer config         | **Gap**            | No `.devcontainer/`; environment is reproduced ad hoc from `.python-version` + `uv.lock` (a Cloud Agent env config is proposed in unmerged PR #35)              | Add a `.devcontainer/` (or merge the proposed agent environment config) pinning Python 3.10 and `uv` so humans, CI, and coding agents share one environment.             |
| 37  | One-command setup           | **Partial**        | `poe lint`, `poe test`, `poe clean`, and `poe serve-coverage-report` are defined in `[tool.poe.tasks]` with `executor.type = "uv"` — but there is no bootstrap task and no documented `uv sync` step | Add a `poe setup` task that runs `uv sync` plus `pre-commit install`, and document it as the single onboarding command.                                                  |
| 38  | Seed scripts for local DBs  | **Not applicable** | No local database — Snowflake is an external managed warehouse reached through the Metaflow `snowflake-default` integration                                     | There is nothing local to seed; functional tests create and clean up their own throwaway tables in `DATA_SCIENCE_STAGE`.                                                  |
| 39  | MCP servers for external tools | **Met**         | Toolsmith-managed MCP access (inherited Pattern control); no repo-local `.mcp.json` required                                                                    | —                                                                                                                                                                       |
| 40  | Scoped secrets per environment | **Met**         | No credentials in the repo; CI mints Outerbounds access via GitHub OIDC (`id-token: write` + `outerbounds service-principal-configure`); Snowflake roles resolve per perimeter (`default` vs `prod`) in `snowflake_connection.py`; `detect-private-key` hook guards commits | —                                                                                                                                                                       |
| 41  | Preview environments per PR | **Not applicable** | Nothing is deployed — the release artifact is a wheel plus a git tag                                                                                            | The equivalent already runs on every PR: *Build Wheel* produces the artifact and *Run Tests* exercises the installed wheel.                                              |
| 42  | Hot-reload / watch mode     | **Gap**            | No watch task; the inner loop is a full `poe test` invocation (`pytest-xdist` is available for parallelism, but nothing re-runs on save)                        | Add a `poe test-watch` task (`pytest-watcher` or `pytest --looponfail`) scoped to `tests/unit_tests/` for a tight local loop.                                            |
| 43  | Structured logging (JSON)   | **Gap**            | The library uses bare `print()` throughout — 20+ call sites across `batch_inference_pipeline.py`, `s3_stage.py`, `sql_utils.py`, several with emoji prefixes — and a `DEBUG_QUERY` env-var toggle in `snowflake_connection.py`; `logging` is never imported | Move to the `logging` module with a library-appropriate `NullHandler`, so consuming flows control verbosity and Metaflow step logs are queryable instead of free text.    |
| 44  | Observable traces and metrics | **Partial**      | Snowflake sessions are attributed — `QUERY_TAG` is set from `current.project_name` and per-statement comments are injected (`sql_utils.py`) — and Metaflow/Outerbounds provides run-level observability, but the library emits no metrics or traces of its own (no timing, row counts, or failure counters) | Emit basic counters/timings for the operations the library owns (rows published, WAP stage durations, S3 bytes staged) so a slow flow can be attributed to a helper rather than guessed at. |
| 45  | Feature flags with local overrides | **Not applicable** | No runtime toggles: behavior is selected by explicit function arguments (`is_test=`, `use_utc=`, `warehouse=`) chosen by the calling flow                | A library imported into a flow has no independent runtime to toggle; the caller already controls every branch at call time.                                              |
| 46  | Database migration tooling  | **Not applicable** | The repo owns no persistent schema; `write_audit_publish` clones, writes, audits, and swaps tables that the *calling flow* defines and owns                     | Versioned migrations belong to the repos that own those tables. The library's contribution is the write-audit-publish safety pattern itself.                             |
| 47  | Dependency update automation | **Met**           | Org-wide Wiz coverage for verified Pattern repos (inherited); GitHub's `dependabot/update-graph` dependency-graph workflow is also active on the repo           | —                                                                                                                                                                       |
| 48  | Reproducible builds (lockfiles) | **Met**        | `uv.lock` committed (440 KB); `.python-version` pins 3.10; CI caches on `cache-dependency-glob: uv.lock`; `hatchling` build backend; pre-commit hook revs pinned to exact tags | —                                                                                                                                                                       |

## Agent dispatch

| #   | Practice                 | Status  | Evidence                                                                                                                                            | Recommendation / rationale                                                                                                                                                                    |
| --- | ------------------------ | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 49  | Agent-dispatch manifest  | **Gap** | No `.agents/pattern-agents.json` (or `.yml`/`.yaml`) anywhere in the tree. Ownership metadata exists only in `backstage.yaml` (owner `data_science-sre-updates`, cost center `DATASCIENCE`) | Add `.agents/pattern-agents.json` with `schema_version`, `github.repo: patterninc/ds-platform-utils`, the DS ClickUp list id, the team Slack channel, and `skills.plugins`. No `aws[]` array is needed — the repo deploys nothing to AWS. |

## Prioritized recommendations

1. **[S] Partial — required CI checks (critical gate):** Add a `required_status_checks` rule to org ruleset `require-pr-review` pinning *Lint, Format, and Static Code Quality Checks*, *Build Wheel*, *Run Tests*, and *Check Version* on `main`.
2. **[S] Gap — AGENTS.md (critical gate):** Add `AGENTS.md` documenting `uv sync`, `poe lint`, `poe test`, the `src/` layout, `_`-prefixed internals, and the "bump `project.version` or CI fails" release rule.
3. **[S] Gap — README setup & run (critical gate):** Expand `README.md` beyond its link index with what the library is, how to install it into a flow project, and how to set up and run dev checks.
4. **[S] Gap — LICENSE and license compliance:** Add a `LICENSE` file and `[project].license` to `pyproject.toml` for this public repo, then add a license-compatibility check to CI.
5. **[S] Gap — agent-dispatch manifest:** Add `.agents/pattern-agents.json` with GitHub repo, ClickUp list, Slack channel, and skills metadata so dispatched agents self-configure.
6. **[S] Gap — CODEOWNERS:** Map `src/ds_platform_utils/_snowflake/` and `src/ds_platform_utils/metaflow/` to their maintainers so reviews auto-route.
7. **[S] Partial — pre-commit install target:** Add a `poe install-hooks` task and document it, so the 12 configured hooks run at commit time and not only in CI.
8. **[M] Partial — coverage in CI:** Resolve the branch/statement coverage `DataError`, drop `--no-cov` from the *Run Tests* job, and ratchet `--cov-fail-under` up from 30%.
9. **[M] Partial — type checking:** Add `mypy` to the `dev` group with config in `pyproject.toml` and run it in `poe lint`, so the shipped `py.typed` marker is backed by a real check.
10. **[M] Gap — structured logging:** Replace the 20+ bare `print()` call sites with the `logging` module plus a library `NullHandler`.
11. **[S] Partial — structured CI output:** Upload `test-reports/` and surface the JUnit XML as check annotations.
12. **[M] Gap — changelog:** Generate release notes from the already-conventional PR titles (Release Please is proposed in open PR #36) and flag breaking changes for SHA-pinned consumers.
13. **[S] Partial — commit conventions:** Merge the PR-title lint workflow and state the convention in `AGENTS.md`.
14. **[M] Gap — skills / reusable prompt workflows:** Add repo-local skills for adding a helper + docs page, cutting a release, and running the slow functional tests.
15. **[S] Gap — import boundaries:** Enable ruff `flake8-tidy-imports` (`TID251`) so `_`-prefixed internals stay internal.
16. **[M] Gap — golden files for generated SQL:** Move the inline expected-SQL assertions into golden files under `tests/`.
17. **[M] Gap — flaky test quarantine:** Put the live-Snowflake/Outerbounds tests behind a quarantine marker or retry policy.
18. **[M] Partial — deterministic fixtures:** Replace the `sample_fixture` placeholder in `tests/fixtures/general_fixtures.py` with the real shared fixtures, and record or stub the Snowflake responses the functional tests depend on.
19. **[M] Gap — devcontainer:** Add `.devcontainer/` (or merge PR #35's agent environment config) pinning Python 3.10 and `uv`.
20. **[S] Partial — one-command setup:** Add a `poe setup` task running `uv sync` and `pre-commit install`.
21. **[M] Gap — ADRs:** Record the `uv.lock`-drives-`@pypi`, WAP-staging, and coverage decisions as dated ADRs under `docs/adr/`.
22. **[M] Partial — observability:** Emit counters/timings for rows published, WAP stage durations, and S3 bytes staged.
23. **[S] Gap — dependency policy:** Set upper bounds and document which packages this widely-imported library may depend on.
24. **[S] Gap — watch mode:** Add a `poe test-watch` task scoped to `tests/unit_tests/`.
25. **[L] Gap — performance benchmarks:** Benchmark the batch-inference fan-out to catch partitioning regressions.
26. **[L] Gap — mutation testing:** Point `mutmut` at `_snowflake/`, where a vacuous test is most expensive.

## Declined practices

| #   | Practice                       | Rationale                                                                                                                                                     |
| --- | ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 5   | API contract docs              | No HTTP/gRPC/GraphQL surface — consumers import the package. The contract is the typed Python API (`py.typed`), documented per helper under `docs/metaflow/`.  |
| 8   | On-call playbooks              | Nothing is deployed and nothing pages on this repo; failures surface in consumers' Metaflow runs. The one owned operational procedure has a runbook (item 4).  |
| 26  | Contract tests                 | No service boundary to verify independently; functional tests run real flows against the real public API.                                                     |
| 27  | End-to-end tests               | No browser UI. `tests/functional_tests/` already drives complete Metaflow flows end to end.                                                                   |
| 28  | Visual regression tests        | No rendered visual surface.                                                                                                                                   |
| 38  | Seed scripts for local DBs     | No local database; Snowflake is external and managed, and functional tests create their own throwaway tables in `DATA_SCIENCE_STAGE`.                          |
| 41  | Preview environments per PR    | Nothing is deployed — the artifact is a wheel plus a git tag, and CI already builds and tests that wheel on every PR.                                          |
| 45  | Feature flags with local overrides | Behavior is selected by explicit call-site arguments (`is_test=`, `use_utc=`, `warehouse=`); an imported library has no independent runtime to toggle.     |
| 46  | Database migration tooling     | The repo owns no persistent schema; `write_audit_publish` operates on tables the calling flow defines and owns, so migrations belong to those repos.           |

## Beyond the checklist

- **The release gate is the build artifact, not the source tree.** *Run Tests* installs the built wheel and runs the suite against the installed package, resolving the coverage path from `ds_platform_utils.__path__`. Packaging mistakes (a missing `py.typed`, an unexported module) fail CI rather than reaching consumers.
- **Version bumps are enforced, not remembered.** The *Check Version* job runs `git tag "v$VERSION"` on a full-history checkout, so a PR that forgets to bump `project.version` fails on a duplicate tag before it can merge.
- **Reproducibility extends to the consumer's environment.** `uv_pypi_base` derives Metaflow `@pypi` environments directly from `uv.lock`, pinning git dependencies to resolved commit SHAs — the lockfile discipline in this repo propagates into every flow that uses it.
- **Lint exemptions carry written justifications.** Every entry in the ruff `ignore` list has an inline comment explaining why (for example `PLR2004` "Is problem for tests", `PLW2901` with the exact file and line that triggered it), so an agent or a new contributor can tell a deliberate exemption from an accumulated one.
- **Known problems are documented in place instead of silently worked around.** The coverage `DataError`, the Outerbounds DNS flakiness behind the connection singleton, and the "silently fails to set the warehouse" Snowflake quirk are all explained at their call sites — high-value context that no checklist item captures.
- **Editor tooling is pinned as a first-class artifact.** `.vscode/extensions.json` recommends the exact toolchain (ruff, mypy, Outerbounds, TOML/YAML/Jinja support), which keeps local feedback aligned with CI even though the type checker is not yet enforced.
