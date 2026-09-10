# ds-platform-utils

## Metaflow API Docs

- [BatchInferencePipeline](docs/metaflow/batch_inference_pipeline.md)
- [uv_pypi_base / uv_pypi](docs/metaflow/pypi_packages.md)
- [make_pydantic_parser_fn](docs/metaflow/make_pydantic_parser_fn.md)
- [publish](docs/metaflow/publish.md)
- [publish_pandas](docs/metaflow/publish_pandas.md)
- [query_pandas_from_snowflake](docs/metaflow/query_pandas_from_snowflake.md)
- [restore_step_state](docs/metaflow/restore_step_state.md)

## Admin / Setup

- [Private repo access in Fast Bakery](docs/metaflow/private_repo_access.md)
- [Table-ownership registry](docs/metaflow/table_ownership_registry.md)

## Repo Health

- [Engineering best-practices audit](docs/engineering-best-practices-audit.md)

## Releasing

Versions are bumped automatically by [Release Please](https://github.com/googleapis/release-please) from your **PR title**, using [Conventional Commits](https://www.conventionalcommits.org/) and [Semantic Versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`).

Do **not** edit `project.version` in `pyproject.toml` (or `uv.lock`) on a feature PR. Squash-merge so the squash commit equals the PR title. CI fails if the title has no conventional prefix.

Ask: *could an existing caller keep their code unchanged and still get the same behavior?* Then pick PATCH, MINOR, or MAJOR below.

### How to set the PR title

Format: `type(optional-scope): short description`

Examples: `fix: use the new Snowflake warehouse in publish tests`, `feat(tags): stamp LAST_UPDATED on published tables`, `feat!: drop support for unquoted identifiers`.

#### PATCH (`fix:`) — bug fix, same API

Bump PATCH (`0.6.1` → `0.6.2`). Callers do not change their code. You corrected wrong behavior or accepted more valid inputs without changing meaning.

Use `fix:` when you:

- Correct a bug (`publish`, Snowflake queries, tags, `@pypi` extras)
- Tighten validation or fix a wrong SQL/warehouse/identifier
- Make an existing argument accept more valid values

```
fix: update warehouse name in test_publish_pandas_with_warehouse
fix: wrap ds.owner flow tag as ds-<owner>-team alias
```

Do **not** use `fix:` for a new public function or a new optional argument — that is MINOR.

#### MINOR (`feat:`) — new capability, still compatible

Bump MINOR (`0.6.1` → `0.7.0`). You add something callers *may* use. Old flows keep working with the same decorators and helpers.

Use `feat:` when you:

- Add an optional argument (`warehouse=`, extra table tags)
- Add a new helper (`uv_pypi`, `restore_step_state`, tag builders)
- Extend behavior behind a default that preserves today's results

```
feat(pypi): derive Metaflow @pypi environments from uv.lock
feat(tags): add LAST_UPDATED tag with auto-stamped timestamp
```

If old call sites would break or change meaning, that is **not** MINOR — use MAJOR (`!`).

#### MAJOR (`feat!:` / `fix!:` / `BREAKING CHANGE:`) — incompatible API

Incompatible change to the public API: `publish`, `publish_pandas`, `uv_pypi`, tag helpers, Snowflake helpers, or required arguments.

Use a bang on the type, or a `BREAKING CHANGE:` footer in the PR body (footer must be at the end):

```
feat!: require quoted Snowflake identifiers

fix!: rename execute() to execute_string()

feat: tag all published tables by default

BREAKING CHANGE: dev tables are tagged as well as prod; pass tag_dev=False to keep the old prod-only behavior.
```

Examples of breaking:

- Rename/remove a public function, argument, or return shape
- Change a default so existing flows write different tables/tags/SQL
- Drop a Python version or make a previously optional argument required

While the package is `0.y.z`, a breaking title bumps **MINOR** (`0.6.1` → `0.7.0`), not `1.0.0`. After `1.0.0`, the same title bumps MAJOR (`1.2.3` → `2.0.0`). Cut `1.0.0` on purpose with:

```shell
git commit --allow-empty -m "chore: release 1.0.0" -m "Release-As: 1.0.0"
```

#### Titles that should not bump the library version

| Prefix | Use for |
| ------ | ------- |
| `chore:` | deps, lockfile, formatting with no user-facing change |
| `ci:` | this repo's GitHub Actions only |
| `test:` | tests only |
| `refactor:` | same behavior, internal structure |
| `style:` | formatting |
| `build:` | packaging / build tooling |

`docs:` is treated as PATCH by the Python Release Please strategy (user-facing docs).

If a refactor or chore **does** break callers, put `!` on it (`refactor!: ...`) or add a `BREAKING CHANGE:` footer.

### Quick chooser

1. Would existing flow code fail or silently change meaning? → `feat!:` / `fix!:` / `BREAKING CHANGE:`
2. Else, did you add optional API or new helpers? → `feat:`
3. Else, did you fix incorrect behavior? → `fix:`
4. Else (tests, internal CI, tidy-up) → `chore:` / `ci:` / `test:` / `refactor:`

Wrong prefix = wrong version. `feat:` on a one-line bugfix cuts a MINOR. `fix:` on a new public helper hides a MINOR as a PATCH.

### After you merge

1. Release Please opens or updates a release PR (`pyproject.toml`, `uv.lock`, `CHANGELOG.md`).
2. Merge that PR when you want to publish. It tags `vX.Y.Z` and creates a GitHub Release.

Enable **Allow GitHub Actions to create and approve pull requests** under **Settings → Actions → General**. To run CI on the release PR, point `release-please.yaml` at a PAT with `contents` and `pull-requests` instead of `GITHUB_TOKEN`.

### FAQ

**If both a `fix:` and a `feat:` land before we merge the release PR, which version do we get?**

The **highest** bump wins. They are not applied one after another.

Example: library is `1.2.3`. Someone merges `fix: …` (would be `1.2.4`). Before the release PR is merged, someone else merges `feat: …`. The release PR becomes **`1.3.0`**, not `1.2.4` and not `1.3.1`. The bugfix is still listed in that `1.3.0` changelog.

Same idea with breaking: `fix:` + `feat:` + `feat!:` → MAJOR (`2.0.0` once you are past `1.0.0`; while on `0.y.z` that breaking change is a MINOR).

**Do I change `pyproject.toml` in my PR?**

No. CI will fail if a feature PR edits `project.version`. Only the Release Please PR may bump it.

**Why didn’t merging my PR create a `v*` tag?**

Tags are created when the **release PR** is merged, not when your feature PR is merged. `chore:` / `ci:` / `test:` / `refactor:` (without `!`) also do not open a release.

**Why is there a second PR?**

Your PR is the code change. The bot PR is the version + changelog. You choose when to cut the release by merging the bot PR. Until then, more `fix:` / `feat:` PRs can pile into the same upcoming version.

**I already merged with the wrong title (`feat:` on a bugfix). What now?**

You cannot rewrite a merged squash commit. On the merged PR, add an override so the next Release Please run uses a different message:

```
BEGIN_COMMIT_OVERRIDE
fix: use the new Snowflake warehouse in publish tests
END_COMMIT_OVERRIDE
```

This only works with **squash-merge**. If the release PR is already open, it should update after the next run (or re-run the Release Please workflow).

**Can one PR include both a bugfix and a new feature?**

Prefer two PRs so each title is honest. If they must ship together, title it for the **highest** bump (`feat:` if there is a new helper; `feat!:` if anything breaks). Extra conventional lines in the squash commit body can show up as extra changelog entries, but the version still follows the highest type.

**Does `(tags)` or another scope change the version?**

No. `feat(tags):` is still MINOR. `fix(snowflake):` is still PATCH. The word before `:` (and `!`) decides the bump.

**We used “Create a merge commit” instead of squash. Does the title still count?**

Unreliable. Release Please then reads **each commit** on the branch. Squash-merge is required so the PR title is the one commit on `main`.

**Why did `docs:` bump the version?**

For Python, Release Please treats `docs:` as PATCH. Use `chore:` if the README/docs change should not release.

**We are on `0.6.x`. Will `feat!:` jump to `1.0.0`?**

No. Breaking changes bump MINOR until you intentionally release `1.0.0` (`Release-As: 1.0.0`). After `1.0.0`, `!` / `BREAKING CHANGE:` bump MAJOR.

**How do I force a specific version (e.g. skip to `0.8.0` or `1.0.0`)?**

Empty commit on `main` (or a `chore:` PR) with a `Release-As:` footer:

```shell
git commit --allow-empty -m "chore: release 1.0.0" -m "Release-As: 1.0.0"
```

**The release PR has no CI checks.**

`GITHUB_TOKEN` PRs often do not trigger other workflows. That is expected. Checks already ran on the feature PRs. To run CI on the release PR, use a PAT in `release-please.yaml`.

**Should I wait for the release PR before merging more work?**

No. Merge more feature PRs; the bot updates the same release PR. Merge the release PR when you want users to get a new tag.

**My feature PR failed `Disallow manual version bumps`.**

Revert the `version =` change in `pyproject.toml`. Leave versioning to the bot.

