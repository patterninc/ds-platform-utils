# `get_packages_from_pyproject` / `get_packages_from_uv_lock`

Source: `ds_platform_utils.metaflow.pypi_packages`

Derives the `packages={...}` map for Metaflow's `@pypi` / `@pypi_base` from your flow repo's
own dependency declarations, so the decorator cannot drift from what the project installs.

## Which one to use

| | Reads | Emits | Use when |
| --- | --- | --- | --- |
| `get_packages_from_uv_lock` | `uv.lock` | resolved versions (`"2.3.2"`), git commit SHAs | **Preferred.** The repo has a lockfile and you want reproducible bakes. |
| `get_packages_from_pyproject` | `pyproject.toml` | declared constraints (`">=2"`) | No lockfile, or you deliberately want the loose constraint re-resolved at bake time. |

The lockfile variant is the safer default: a `>=` constraint in `pyproject.toml` — or a git
dependency pinned to `rev = "main"` — means two bakes a week apart can produce different
images. `uv.lock` records what was actually resolved, including the commit SHA.

## Signature

```python
get_packages_from_uv_lock(
    groups: Optional[Union[str, list]] = None,
    project_root: Optional[Union[str, Path]] = None,
) -> dict

get_packages_from_pyproject(
    groups: Optional[Union[str, list]] = None,
    project_root: Optional[Union[str, Path]] = None,
) -> dict
```

## Parameters

| Parameter      | Type                              | Required | Description                                                                                                            |
| -------------- | --------------------------------- | -------: | ---------------------------------------------------------------------------------------------------------------------- |
| `groups`       | `str \| list[str]`                |       No | Dependency groups to add on top of the runtime dependencies, e.g. `["dev"]`. Excluded by default — groups are optional. |
| `project_root` | `str \| Path`                     |       No | Directory holding `uv.lock` / `pyproject.toml`. Defaults to searching upward from the launch directory.                 |

**Returns:** `dict` of package name → version, ready for `@pypi(packages=...)`.

## Typical usage

```python
from metaflow import FlowSpec, pypi_base, step

from ds_platform_utils.metaflow import get_packages_from_uv_lock


@pypi_base(python="3.11", packages=get_packages_from_uv_lock())
class MyFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass
```

## What it does

- Emits the **root project's direct dependencies only**, not the full transitive closure.
  Lock entries are marker-gated per platform (`appnope` on darwin, `colorama` on win32), so
  pinning the whole graph would break a bake on any platform but the one that resolved it.
  `@pypi` resolves transitives itself from the pinned roots.
- Renders non-PyPI dependencies as PEP 508 direct references (`@ git+https://...@<sha>`),
  which `@pypi` passes through to pip verbatim. `[tool.uv.sources]` tables and uv.lock's
  single-URL git form are both flattened to what pip understands.
- Leaves a dependency **unpinned** (`""`) when the lock holds it at more than one version
  behind different resolution markers — pinning either one would break the other platform.
- Follows `{include-group = ...}` chains in `[dependency-groups]` (the lockfile variant needs
  no such handling; uv flattens groups when it writes the lock).
- Returns `{}` when the file cannot be found, which is what makes remote tasks work: a remote
  task re-imports the flow module — re-evaluating the decorator — inside a container whose
  code package holds only `.py` files. The image was already baked from the map resolved on
  the client, so nothing is lost.

## Finding the project root

Because this ships as an installed package, it cannot resolve your repo from `__file__` —
that points into `site-packages`. Instead it walks up from the directory the flow was launched
from, so both of these work:

```bash
python flows/my_flow.py run     # from the repo root
cd flows && python my_flow.py run
```

Pass `project_root=` explicitly if the flow is launched from outside the repo.

## Errors

Both functions raise `ValueError` rather than silently emitting a map that will fail at bake
time:

- a requested group is not declared / recorded
- a `[tool.uv.sources]` or lock `source` is `path` or `workspace` — local-only, so a remote
  task cannot fetch it
- a root dependency is missing from the lock (stale lockfile — run `uv lock`)
- `[dependency-groups]` contains a circular `include-group` chain
