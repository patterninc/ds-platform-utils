# `uv_pypi_base` / `uv_pypi`

Source: `ds_platform_utils.metaflow.pypi_packages`

Builds a Metaflow `@pypi` / `@pypi_base` environment from your flow repo's own dependency
declarations, so the decorator cannot drift from what the project installs.

## Start here

`@uv_pypi_base` is `@pypi_base` with the Python version and packages filled in from `uv.lock`:

```python
from metaflow import FlowSpec, step

from ds_platform_utils.metaflow import uv_pypi_base


@uv_pypi_base
class MyFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass
```

That's the whole environment — no `python=`, no `packages=`, nothing to keep in sync. Call it
when there is something to configure:

```python
@uv_pypi_base                    # derive everything
@uv_pypi_base(dependency_groups=["dev"])    # add a dependency group
@uv_pypi_base(python="3.11")     # override the derived interpreter
@uv_pypi_base(disabled=True)     # skip environment creation
```

### One step instead of the whole flow

`@uv_pypi` is the same thing scoped to a step, delegating to `@pypi`:

```python
class MyFlow(FlowSpec):
    @uv_pypi(dependency_groups=["train"])
    @step
    def train(self): ...
```

### What it prints

Both decorators print the environment they resolved, so a run records what it actually built:

```
@uv_pypi_base on MyFlow: python 3.10, 10 package(s) from uv.lock
  jinja2                      3.1.6
  outerbounds                 0.12.39
  pandas                      2.3.3
  polars                      (unpinned)
  snowflake-connector-python  4.7.2
  ...
```

`(unpinned)` means the lock could not be narrowed to one version, so `@pypi` resolves it —
see [Resolving a universal lockfile](#resolving-a-universal-lockfile). A `disabled=True`
environment is flagged in the header.

This happens at **import** time, so it prints on any command that loads the flow module, not
only `run`. Two things suppress it:

| Condition | Why |
| --- | --- |
| `current.is_running_flow` is true | The flow module was re-imported while a run is already in progress — one flow triggering another — so the environment has already been reported. |
| No `uv.lock` found | The remote task, re-importing the module inside an already-baked image. Nothing was resolved, so there is nothing to report. |

### Where the Python version comes from

Checked in order of how concrete each source is:

| | Source | Example |
| --: | --- | --- |
| 1 | `.python-version` — the interpreter uv pinned | `3.11`, or `cpython@3.11` |
| 2 | `requires-python` in `uv.lock`, else `pyproject.toml` — a range, so its **floor** is used | `">=3.11,<3.13"` → `3.11` |
| 3 | The running interpreter | |

Upper bounds are ignored: `<3.13` says nothing about what the project runs *on*. Pass
`python=` to bypass all three.

> **Note:** whatever the repo pins is what the flow bakes on — not the version you may have
> hand-written in a decorator before. That is the intended behavior, and it means re-pinning
> `.python-version` moves the flow with it, with no decorator edit.

The resolved version also decides **which packages** you get; see below.

## Signatures

```python
uv_pypi_base(
    flow=None,                                          # supplied by Python in the bare form
    *,
    dependency_groups: Optional[Union[str, list]] = None,
    python: Optional[str] = None,
    project_root: Optional[Union[str, Path]] = None,
    disabled: Optional[bool] = None,
)                  # the decorated flow, or a decorator

uv_pypi(
    step=None,
    *,
    dependency_groups: Optional[Union[str, list]] = None,
    python: Optional[str] = None,
    project_root: Optional[Union[str, Path]] = None,
    disabled: Optional[bool] = None,
)                  # the decorated step, or a decorator
```

## Parameters

| Parameter      | Type               | Required | Description                                                                                                             |
| -------------- | ------------------ | -------: | ----------------------------------------------------------------------------------------------------------------------- |
| `dependency_groups` | `str \| list[str]` |       No | Dependency groups to add on top of the runtime dependencies, e.g. `["dev"]`. Excluded by default — groups are optional.  |
| `python`       | `str`              |       No | Overrides the version derived from the project, e.g. `"3.11"`.                                                          |
| `project_root` | `str \| Path`      |       No | Directory holding the project files. Defaults to searching upward from the launch directory.                             |
| `disabled`     | `bool`             |       No | Forwarded to Metaflow to skip environment creation without removing the decorator.                                      |

## How the packages are derived

- Emits the **root project's direct dependencies only**, not the full transitive closure.
  Lock entries are marker-gated per platform (`appnope` on darwin, `colorama` on win32), so
  pinning the whole graph would break a bake on any platform but the one that resolved it.
  `@pypi` resolves transitives itself from the pinned roots.
- Renders non-PyPI dependencies as PEP 508 direct references (`@ git+https://...@<sha>`), which
  `@pypi` passes through to pip verbatim. uv records a git source as one URL carrying the ref in
  the query string and the resolved commit in the fragment; that gets taken apart and
  reassembled around the **commit SHA**, which is what makes the build repeatable.
- **Resolves markers** against the environment being built — see below.
- Emits **nothing** when `uv.lock` cannot be found, which is what makes remote tasks work: a
  remote task re-imports the flow module — re-evaluating the decorator — inside a container
  whose code package holds only `.py` files. The image was already baked from the environment
  resolved on the client, so nothing is lost.

Everything above comes from `uv.lock`, so run `uv lock` after changing a dependency or the
flow will bake the previous version.

## Resolving a universal lockfile

`uv.lock` is a *universal* resolution: it holds the answer for every Python version and platform
in range at once, each tagged with the marker it applies to. That is why switching OS never
requires a re-lock. But `@pypi` takes a flat name → version map with **nowhere to put a
marker**, so the markers have to be resolved here rather than passed along.

A dependency that no single version covers appears once per marker region, each naming its own
version:

```toml
{ name = "pandas", version = "2.3.3", marker = "python_full_version < '3.11'" }
{ name = "pandas", version = "3.0.5", marker = "python_full_version >= '3.11'" }
```

So the same unchanged lockfile yields different packages depending on the interpreter — which is
why the Python version is resolved *first* and then used to pick the packages:

```python
>>> # .python-version = 3.10
>>> uv_pypi_base ...  # pandas 2.3.3
>>> # .python-version = 3.11
>>> uv_pypi_base ...  # pandas 3.0.5
```

Three outcomes per dependency:

| Lock state | Emitted |
| --- | --- |
| One entry, or several with a marker that selects one | that exact version |
| Marker excludes this environment (e.g. `sys_platform == 'darwin'` on a Linux bake) | **omitted** — `@pypi` cannot express the condition, so installing it would break the bake |
| Several entries with no marker or version to tell them apart | `""`, left for `@pypi` to resolve |

**Platform assumption:** markers are evaluated against **Linux**, which is what Metaflow builds
for a remote task. A dependency gated to macOS is therefore dropped. If a flow only ever runs
locally on a Mac and needs such a dependency, the packages helper takes `sys_platform="darwin"`.

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

Decoration raises `ValueError` rather than silently building an environment that will fail at
bake time, so these surface the moment the flow module is imported:

- a requested group is not recorded in the lock
- a lock `source` is `path` or `workspace` — local-only, so a remote task cannot fetch it
- a root dependency is missing from the lock (stale lockfile — run `uv lock`)

Metaflow raises `BadFlowDecoratorException` if `@uv_pypi_base` is applied to something that is
not a `FlowSpec`, and there is a matching check for `@uv_pypi` on steps.
