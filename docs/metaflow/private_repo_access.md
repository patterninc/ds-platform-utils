# Private repo access in Fast Bakery

Flows that depend on a private `github.com/patterninc` repo need Outerbounds to hold Git
credentials, because **Fast Bakery runs `pip install` inside the bake**, not on your laptop.
Your local `git` credentials never reach it.

This is a **one-time admin setup step per perimeter**, not part of the Python API.

## Why it is needed

[`@uv_pypi_base`](pypi_packages.md) turns a `uv.lock` git dependency into a PEP 508 direct
reference pinned to the resolved commit:

```python
{"ds-platform-utils": "@ git+https://github.com/patterninc/ds-platform-utils.git@06ead9f0..."}
```

Fast Bakery hands that to pip, pip runs `git clone`, and GitHub refuses an anonymous clone of a
private repo. Without the integration the bake fails at image build time with an authentication
error — before any step runs.

A `GIT_PYPI_REPOSITORY` integration supplies the credential Fast Bakery uses for matching
repository URLs.

## Current state

As of **2026-08-12**:

| Perimeter | Integration | Repositories covered | Status |
| --- | --- | --- | --- |
| `default` | `private-repo-access` | `https://github.com/patterninc` | ✅ Configured |
| `prod` | — | — | ❌ **Not yet configured** |

Verify at any time with:

```bash
outerbounds integrations list --perimeter default
outerbounds integrations list --perimeter prod
```

### What the existing entry covers

The registered URL is the **organization root**, `https://github.com/patterninc` — not a single
repo. Fast Bakery matches by URL prefix, so every repo under the org is already covered by the
`default` perimeter entry, including:

- `ds-platform-utils`
- `ds-dqv-tool`
- any new `patterninc` repo, with **no integration change needed**

That is the reason to keep registering the org root rather than individual repos: a new private
dependency works without touching Outerbounds.

### Still to be added

**`prod` perimeter.** A flow deployed to `prod` that depends on a private repo will fail to bake
until the same integration exists there. Perimeters are fully isolated — an integration in
`default` grants nothing in `prod`. Run the [setup](#setup) below with `--perimeter prod`.

## Prerequisites

- A GitHub **fine-grained personal access token** (`github_pat_…`) or classic PAT, scoped to
  read `patterninc` repository contents. Read-only is sufficient — Fast Bakery only clones.
- Admin rights on the Outerbounds perimeter you are configuring.
- The `outerbounds` CLI, which ships with this package's dependencies.

> **Never commit a token, and never paste one into a shared doc or ticket.** The commands below
> read it from an environment variable so it does not land in your shell history. Prefer a token
> owned by a service account over a personal one, so access survives someone leaving.

## Setup

Put the token in your shell without recording it in history — note the leading space, and use
`read -s` so it is never echoed:

```bash
 read -rs GITHUB_PAT && export GITHUB_PAT
```

### 1. Confirm which perimeters exist

```bash
outerbounds perimeter list
outerbounds perimeter show-current
```

### 2. Create the integration in `default`

Already done — skip unless you are rebuilding it. Use `update` instead if it exists (see
[Rotating the token](#rotating-the-token)).

```bash
outerbounds integrations git-pypi-repository create private-repo-access \
    --description "Patterninc repos" \
    --repository-url https://github.com/patterninc \
    --username oauth2 \
    --password "$GITHUB_PAT" \
    --perimeter default
```

### 3. Create the integration in `prod`

This is the outstanding step. Same command, different perimeter:

```bash
outerbounds integrations git-pypi-repository create private-repo-access \
    --description "Patterninc repos" \
    --repository-url https://github.com/patterninc \
    --username oauth2 \
    --password "$GITHUB_PAT" \
    --perimeter prod
```

`--username oauth2` is a placeholder. GitHub authenticates on the token alone and ignores the
username, but the CLI requires the flag.

### 4. Verify

```bash
outerbounds integrations get private-repo-access --perimeter prod
```

Expect `"integration_status": "CREATED"` and your URL under `repository_urls`. The response never
returns the token.

Then confirm end to end by baking a flow that has a private git dependency:

```bash
python flows/my_flow.py --environment=pypi run
```

## Maintenance

### Adding another Git host or org

`--repository-url` is repeatable, so one integration can cover several roots. `update` **replaces**
the list rather than appending — pass every URL you want to keep:

```bash
outerbounds integrations git-pypi-repository update private-repo-access \
    --repository-url https://github.com/patterninc \
    --repository-url https://github.com/some-other-org \
    --perimeter default
```

### Rotating the token

Run `update` with the new token in **each** perimeter. Do this before the old token expires — an
expired credential surfaces as a bake failure, not a warning:

```bash
for p in default prod; do
    outerbounds integrations git-pypi-repository update private-repo-access \
        --username oauth2 --password "$GITHUB_PAT" --perimeter "$p"
done
```

Then clear the variable: `unset GITHUB_PAT`.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| Bake fails cloning a `patterninc` repo | No integration in the perimeter you are running against. Check with `outerbounds integrations list --perimeter <p>`. |
| Works locally, fails on deploy | The `prod` perimeter is missing the integration — see [step 3](#3-create-the-integration-in-prod). |
| Worked before, now `authentication failed` | Token expired or was revoked. Rotate it in every perimeter. |
| A newly added repo fails | Only if it lives outside `github.com/patterninc`; anything under the org root is already matched. |
| `pip` resolves but installs the wrong commit | Not a credentials problem — the lockfile is stale. Run `uv lock`. |

Related: [`uv_pypi_base` / `uv_pypi`](pypi_packages.md) for how the git reference is produced.
