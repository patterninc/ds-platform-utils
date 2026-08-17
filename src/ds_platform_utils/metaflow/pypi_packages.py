"""Build a Metaflow ``@pypi`` environment from a flow repo's dependency declarations.

Describing the environment in one place means a flow cannot drift from what the project
actually installs.

[`uv_pypi_base`][ds_platform_utils.metaflow.uv_pypi_base] is the whole thing -- Metaflow's
`@pypi_base` with the Python version and packages filled in from uv.lock:

```python
@uv_pypi_base
class MyFlow(FlowSpec): ...
```

[`uv_pypi`][ds_platform_utils.metaflow.uv_pypi] is its step-level counterpart, delegating to
`@pypi`. Those two decorators are the entire public surface of this module.

Everything below them is internal: `_get_pypi_kwargs` assembles the arguments both decorators
pass on, over `_get_packages_from_uv_lock` for the packages and `_find_python_version` for the
interpreter.

Versions come from `uv.lock` rather than the constraints in pyproject.toml, so every one is
the version uv actually resolved -- including exact commit SHAs for git dependencies. That is
what makes a bake reproducible instead of tracking whatever `main` points at today.
"""

import sys

# Python versions 3.11+ ship with a version of Tomli: the tomllib standard library module.
# https://pypi.org/project/tomli/
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import os
from pathlib import Path
from typing import Optional, Tuple, Union
from urllib.parse import parse_qs, urlsplit, urlunsplit

#: uv.lock `source` keys that mean "this is the local project, not something to install".
_LOCAL_SOURCE_KEYS = ("virtual", "editable", "directory")

#: Metaflow subcommands that mean "this process is running one task", not the client invocation
#: that launched it. Metaflow spawns one such process per task -- locally, and again inside the
#: container -- and each re-imports the flow module, re-evaluating these decorators.
_TASK_COMMANDS = ("step", "spin-step")

#: Exported by Metaflow's mflog helper into every *remote* task command, so it is set in a
#: Kubernetes pod, Batch job, Argo or Step Functions container -- but not in the local worker,
#: whose environment is a plain copy of the client's.
_REMOTE_TASK_ENV_VAR = "MF_PATHSPEC"

#: Metaflow bakes remote task images for Linux, so that is the platform markers are evaluated
#: against. A flow that only ever runs locally on a Mac can override it.
_DEFAULT_SYS_PLATFORM = "linux"

#: Marker variables that follow from the target platform once `sys_platform` is known.
_PLATFORM_MARKERS = {
    "linux": {"platform_system": "Linux", "os_name": "posix", "platform_machine": "x86_64"},
    "darwin": {"platform_system": "Darwin", "os_name": "posix", "platform_machine": "arm64"},
    "win32": {"platform_system": "Windows", "os_name": "nt", "platform_machine": "AMD64"},
}


def _marker_environment(python_version: str, sys_platform: str) -> dict:
    """Describe the environment being built, in the variables PEP 508 markers are written in.

    Args:
        python_version: the interpreter the flow will run on, e.g. `"3.11"` or `"3.11.5"`
        sys_platform: the platform the image is baked for, e.g. `"linux"`

    Returns:
        A partial marker environment. Variables left out of it fall back to the running
        interpreter's values, which is fine for the ones no lockfile discriminates on.

    """
    from packaging.version import Version

    # markers compare `python_full_version` as a three-part version, so "3.11" has to be
    # widened to "3.11.0" or `python_full_version < '3.11.1'` would not evaluate.
    release = (Version(python_version).release + (0, 0, 0))[:3]
    environment = {
        "python_version": f"{release[0]}.{release[1]}",
        "python_full_version": ".".join(str(part) for part in release),
        "sys_platform": sys_platform,
        "implementation_name": "cpython",
        "platform_python_implementation": "CPython",
    }
    environment.update(_PLATFORM_MARKERS.get(sys_platform, {}))
    return environment


def _dependency_applies(dep: dict, environment: dict) -> bool:
    """Say whether a locked root dependency is wanted in the environment being built.

    uv records one entry per marker region, so a dependency gated to another platform or
    Python version is present in the lock but must not be installed here. `@pypi` takes a flat
    name -> version map with nowhere to put a condition, so the marker has to be resolved now
    rather than passed along.

    Args:
        dep: an entry from the root project's `dependencies` list
        environment: the marker environment from `_marker_environment`

    """
    marker = dep.get("marker")
    if marker is None:
        return True

    from packaging.markers import Marker

    return Marker(marker).evaluate(environment)


def _select_locked_package(dep: dict, locked: list) -> Optional[dict]:
    """Pick the one lock entry a dependency resolves to, or `None` if it cannot be narrowed.

    Args:
        dep: an entry from the root project's `dependencies` list, whose `version` key is
            uv's own statement of which entry applies once markers are accounted for
        locked: every `[[package]]` entry recorded under that name

    Returns:
        The matching entry, or `None` when the name is locked several times with nothing to
        tell them apart -- the caller then leaves it unpinned for `@pypi` to resolve.

    """
    if len(locked) == 1:
        return locked[0]
    version = dep.get("version")
    if version is None:
        return None
    return next((package for package in locked if package["version"] == version), None)


def _find_project_file(filename: str, project_root: Optional[Union[str, Path]] = None) -> Optional[Path]:
    """Locate a dependency-declaration file in the flow repo calling this function.

    An installed package cannot resolve the project root from `__file__` -- that points into
    `site-packages`, not the flow repo. So the search starts at the directory Metaflow was
    launched from and walks up, which finds the file whether the flow was run as
    `python flows/my_flow.py run` from the repo root or from inside `flows/` itself.

    Args:
        filename: file to look for, e.g. `"uv.lock"`
        project_root: directory to look in, skipping the upward search. Pass this when the
            flow is launched from outside the repo.

    Returns:
        The path to the file, or `None` when it is nowhere to be found.

    """
    if project_root is not None:
        candidate = Path(project_root) / filename
        return candidate if candidate.is_file() else None

    start = Path.cwd().resolve()
    for directory in (start, *start.parents):
        candidate = directory / filename
        if candidate.is_file():
            return candidate
    return None


def _load_toml(path: Path) -> dict:
    """Parse a TOML file.

    Args:
        path: the file to read

    """
    with open(path, "rb") as f:
        return tomllib.load(f)


def _requires_python_floor(project_root: Optional[Union[str, Path]] = None) -> Optional[str]:
    """Read the lowest Python version the project claims to support.

    `requires-python` is a range, not a version, so the floor is the only concrete thing in
    it -- `">=3.11,<3.13"` means the project is built to run on 3.11. uv.lock is checked
    before pyproject.toml because the lock records the range the dependency graph was
    actually resolved against.

    Args:
        project_root: directory holding the project files. Defaults to searching upward from
            the directory the flow was launched from.

    Returns:
        A `"<major>.<minor>"` string, or `None` when neither file declares a floor.

    """
    from packaging.specifiers import SpecifierSet
    from packaging.version import Version

    requires = None
    lock_path = _find_project_file("uv.lock", project_root)
    if lock_path is not None:
        requires = _load_toml(lock_path).get("requires-python")
    if not requires:
        toml_path = _find_project_file("pyproject.toml", project_root)
        if toml_path is not None:
            requires = _load_toml(toml_path).get("project", {}).get("requires-python")
    if not requires:
        return None

    # ">=3.11" and "~=3.11" pin a floor; "==3.11.*" pins it exactly. "<3.13" says nothing
    # about what the project runs on, so upper bounds are ignored.
    floors = [
        Version(spec.version.rstrip(".*")) for spec in SpecifierSet(requires) if spec.operator in (">=", "==", "~=")
    ]
    if not floors:
        return None
    floor = max(floors)
    return f"{floor.major}.{floor.minor}"


def _find_python_version(project_root: Optional[Union[str, Path]] = None) -> str:
    """Work out which Python version the flow's environment should be built on.

    Checked in order of how concrete each source is:

    1. `.python-version` -- the exact interpreter uv pinned and built the venv from.
    2. `requires-python` in uv.lock or pyproject.toml -- a range, so the floor is used.
    3. The running interpreter, which on a remote task is the one already baked into the
       image, so falling through to it is harmless.

    Args:
        project_root: directory holding the project files. Defaults to searching upward from
            the directory the flow was launched from.

    Returns:
        A version string such as `"3.11"`, ready to hand to `@pypi(python=...)`.

    """
    pin_path = _find_project_file(".python-version", project_root)
    if pin_path is not None:
        for line in pin_path.read_text().splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                # uv allows an implementation prefix, e.g. "cpython@3.11" or "pypy@3.10"
                return line.rpartition("@")[2]

    return _requires_python_floor(project_root) or f"{sys.version_info.major}.{sys.version_info.minor}"


def _lock_source_to_direct_reference(name: str, source: dict) -> str:
    """Render a uv.lock `source` table as a PEP 508 direct reference.

    uv stores a git source as a single URL carrying the ref in the query string and the
    resolved commit in the fragment:

    ```
    https://github.com/patterninc/ds-platform-utils.git?rev=main#06ead9f0189289...
    ```

    pip wants `git+<url>@<ref>`, so the URL is taken apart and reassembled around the commit
    SHA -- the SHA, not `main`, because that is what makes the build repeatable.

    Args:
        name: the package name, used in the error message
        source: the lock's `source` table, e.g. `{"git": "https://...#<sha>"}`

    """
    if "git" in source:
        parts = urlsplit(source["git"])
        # fragment holds the resolved commit; fall back to the requested ref if a lock
        # was written without one.
        query = parse_qs(parts.query)
        ref = parts.fragment or next(
            (query[key][0] for key in ("rev", "tag", "branch") if key in query),
            None,
        )
        url = "git+" + urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        if ref:
            url = f"{url}@{ref}"
        if "subdirectory" in query:
            url = f"{url}#subdirectory={query['subdirectory'][0]}"
        return f"@ {url}"
    if "url" in source:
        return f"@ {source['url']}"
    raise ValueError(
        f"uv.lock source for {name!r} cannot be installed by @pypi: {source!r}. Only git and url sources "
        "are fetchable from a remote task -- path and workspace sources are local-only."
    )


def _split_lock_packages(lock: dict, lock_path: Path) -> Tuple[dict, dict]:
    """Split a parsed uv.lock into its root project entry and its installable packages.

    Args:
        lock: the parsed lock
        lock_path: the file it came from, used in the error message

    Returns:
        `(root, entries)`, where `entries` maps a name to the *list* of lock entries under
        it -- a name resolved differently per platform appears more than once.

    """
    entries: dict = {}
    root = None
    for package in lock.get("package", []):
        source = package.get("source", {})
        if any(key in source for key in _LOCAL_SOURCE_KEYS):
            # the repo itself -- it is the thing depending on everything else, not a dep.
            root = package
            continue
        # a name locked at several versions (different resolution markers) is ambiguous;
        # keep them all so it can be left unpinned rather than pinned to an arbitrary one.
        entries.setdefault(package["name"], []).append(package)

    if root is None:
        raise ValueError(f"no root project entry found in {lock_path} -- expected a package with a local source")
    return root, entries


def _get_packages_from_uv_lock(
    dependency_groups: Optional[Union[str, list]] = None,
    project_root: Optional[Union[str, Path]] = None,
    python: Optional[str] = None,
    sys_platform: str = _DEFAULT_SYS_PLATFORM,
) -> dict:
    """Build the `@pypi` packages map from a flow repo's `uv.lock`.

    Emits the root project's direct runtime dependencies pinned to their locked versions, so
    the image Metaflow bakes matches the environment `uv sync` gives you locally. Dependency
    groups are excluded unless named in `dependency_groups`, since uv keeps them in a separate table and
    they are optional by definition.

    A uv.lock is a *universal* resolution: it holds the answer for every Python version and
    platform in range at once, each tagged with the marker it applies to. `@pypi` takes a flat
    name -> version map with nowhere to put a marker, so this resolves them against the
    environment actually being built -- a dependency gated to another platform is dropped, and
    a name locked at two versions collapses to whichever one `python` selects. That is why
    `pandas` can be `2.3.3` on 3.10 and `3.0.5` on 3.11 from one unchanged lockfile.

    Deliberately *not* the full transitive closure: `@pypi` resolves transitives itself from
    these pinned roots, and per-platform wheel availability is its job rather than this one's.

    Raises when `uv.lock` cannot be found, since an environment silently resolving to nothing is
    worse than a failure that names the directory it searched. The exception is a process already
    running a task: it re-imports the flow module inside an already-baked image, where metaflow's
    code package carries only `.py` files, so the lock is legitimately absent and an empty map is
    the right answer.

    Args:
        dependency_groups: names of dependency groups to add on top of the runtime dependencies, e.g.
            `["dev"]`. uv resolves `include-group` references when it writes the lock, so the
            groups recorded here are already flat.
        project_root: directory holding `uv.lock`. Defaults to searching upward from the
            directory the flow was launched from.
        python: the Python version the flow will run on, used to resolve markers. Defaults to
            the same value the decorators derive, so the packages always agree with the
            interpreter they are installed against.
        sys_platform: the platform the image is baked for. Defaults to Linux, which is what
            Metaflow builds for a remote task.

    Returns:
        A map of package name -> locked version, ready to hand to `@pypi(packages=...)`.

    """
    if isinstance(dependency_groups, str):
        # a bare string would otherwise iterate character by character
        dependency_groups = [dependency_groups]

    lock_path = _find_project_file("uv.lock", project_root)
    if lock_path is None:
        if _is_running_task():
            # a task re-imports the flow module inside an already-baked image, and metaflow's
            # code package carries only .py files -- so the lock is legitimately absent here.
            # The image was built from the map resolved on the client, so nothing is lost.
            return {}
        raise FileNotFoundError(
            "no uv.lock found, so there is nothing to build a @pypi environment from. Looked in "
            + (f"{Path(project_root)}" if project_root is not None else f"{Path.cwd()} and its parents")
            + ". Run `uv lock` if the project has no lockfile yet, launch the flow from inside the "
            "repo, or pass project_root= to point at the directory holding it."
        )

    lock = _load_toml(lock_path)

    root, entries = _split_lock_packages(lock, lock_path)

    dependencies = list(root.get("dependencies", []))
    if dependency_groups:
        declared = root.get("dev-dependencies", {})
        for group in dependency_groups:
            try:
                dependencies.extend(declared[group])
            except KeyError:
                raise ValueError(
                    f"dependency group {group!r} is not recorded in {lock_path}. "
                    f"Groups present: {', '.join(sorted(declared)) or '(none)'}"
                ) from None

    environment = _marker_environment(python or _find_python_version(project_root), sys_platform)

    packages = {}
    for dep in dependencies:
        name = dep["name"]
        if not _dependency_applies(dep, environment):
            # gated to a platform or Python version this image is not being built for
            continue
        locked = entries.get(name, [])
        if not locked:
            raise ValueError(f"{name!r} is a dependency of the root project but is missing from {lock_path}")
        package = _select_locked_package(dep, locked)
        if package is None:
            # locked several times with nothing to tell the entries apart -- let @pypi resolve.
            packages[name] = ""
            continue
        source = package.get("source", {})
        if "registry" in source:
            # metaflow prepends "==" to a bare version.
            packages[name] = package["version"]
        else:
            packages[name] = _lock_source_to_direct_reference(name, source)
    return packages


def _get_pypi_kwargs(
    dependency_groups: Optional[Union[str, list]] = None,
    python: Optional[str] = None,
    project_root: Optional[Union[str, Path]] = None,
    sys_platform: str = _DEFAULT_SYS_PLATFORM,
) -> dict:
    """Build every argument `@pypi_base` needs from a flow repo's uv.lock.

    Splat the result into the decorator and the flow's environment is described in exactly
    one place -- neither the Python version nor the dependency list can drift from what
    `uv sync` gives you locally:

    ```python
    @pypi_base(**_get_pypi_kwargs())
    ```

    Packages come from
    `_get_packages_from_uv_lock`, so
    the same guarantees apply: direct dependencies only, pinned to their locked versions,
    with git dependencies pinned to their resolved commit SHA.

    The Python version is whatever the project builds its own environment on, checked in
    order of how concrete each source is:

    1. `.python-version` -- the interpreter uv pinned, e.g. `"3.11"`.
    2. `requires-python` in uv.lock or pyproject.toml -- a range, so its floor is used.
    3. The running interpreter.

    Example usage:

    ```python
    from metaflow import FlowSpec, pypi_base, step

    from ds_platform_utils.metaflow import _get_pypi_kwargs


    @pypi_base(**_get_pypi_kwargs())
    class MyFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass
    ```

    Args:
        dependency_groups: dependency groups to add on top of the runtime dependencies, e.g. `["dev"]`.
            Excluded by default, since groups are optional by definition.
        python: Python version to use instead of the one derived from the project, e.g.
            `"3.11"`. Reach for this when the flow has to run on a different interpreter than
            the repo develops against.
        project_root: directory holding the project files. Defaults to searching upward from
            the directory the flow was launched from.
        sys_platform: the platform the image is baked for. Defaults to Linux, which is what
            Metaflow builds for a remote task.

    Returns:
        `{"python": ..., "packages": {...}}`, ready to splat into `@pypi_base`.

    """
    # resolve the interpreter first, then hand it down: the packages a universal lock selects
    # depend on the Python version, so the two halves of the environment have to agree.
    python = python or _find_python_version(project_root)
    return {
        "python": python,
        "packages": _get_packages_from_uv_lock(
            dependency_groups=dependency_groups, project_root=project_root, python=python, sys_platform=sys_platform
        ),
    }


def _is_running_task() -> bool:
    """Say whether this process is running a task rather than launching one.

    Metaflow re-imports the flow module -- so re-evaluates these decorators -- in every process
    that touches a task. Two things follow from that, and both need this answer: the resolved
    environment is worth reporting once per run rather than once per task, and a missing
    lockfile is a mistake on the client but expected inside a task.

    Three checks, because no single one covers every context:

    1. `MF_PATHSPEC`, which Metaflow's mflog helper exports into every *remote* task command.
       Present in a Kubernetes pod, Batch job, Argo or Step Functions container; absent from the
       local worker, whose environment is a plain copy of the client's.
    2. The task subcommand in `sys.argv`. Every backend builds its task command around the same
       subcommand, so this one covers the local worker Metaflow spawns per task *as well as* the
       container it launches.
    3. `current.is_running_flow`, which only becomes true once the task runtime has started --
       after this decorator ran. It therefore cannot see cases 1 or 2 at all, and catches a
       different one: a flow module re-imported while a run is already in progress, such as one
       flow triggering another.

    None of these is load-bearing for the logging. If a future Metaflow renames a subcommand or
    drops a variable, the worst case there is a duplicated log block. A false *positive* would
    matter more, since it would turn a missing lockfile into an empty environment instead of an
    error -- but all three only become true in processes Metaflow itself started.

    Returns:
        `True` when this process is executing a task.

    """
    if os.environ.get(_REMOTE_TASK_ENV_VAR):
        return True
    if any(command in sys.argv for command in _TASK_COMMANDS):
        return True

    from metaflow import current

    return current.is_running_flow


def _format_pypi_environment(label: str, pypi_kwargs: dict) -> str:
    """Render a resolved environment as an aligned block, for printing at decoration time.

    Args:
        label: what is being decorated, e.g. `"@uv_pypi_base on MyFlow"`
        pypi_kwargs: the `python` / `packages` map about to be handed to Metaflow

    Returns:
        A multi-line string, package names sorted so two runs are comparable by eye.

    """
    packages = pypi_kwargs["packages"]
    header = f"{label}: python {pypi_kwargs['python']}, {len(packages)} package(s) from uv.lock"

    width = max(len(name) for name in packages)
    lines = [header]
    for name in sorted(packages):
        # an empty version is a deliberate "let @pypi resolve it", not a missing value, so
        # say so rather than printing a blank column
        lines.append(f"  {name.ljust(width)}  {packages[name] or '(unpinned)'}")
    return "\n".join(lines)


def _apply_uv_pypi(decorator, target, label, **kwargs):
    """Wrap a Metaflow pypi decorator so its environment comes from the project.

    Supports both the bare (`@uv_pypi_base`) and called (`@uv_pypi_base(dependency_groups=["dev"])`)
    forms: in the bare form the decorated object arrives as `target`, in the called form
    `target` is `None` and the returned closure receives it instead.

    The environment is resolved when the decorator is applied, not when this function is
    called, so nothing is read off disk until a flow module is imported.

    The resolved environment is printed, so a run records what it actually built rather than
    leaving you to re-derive it -- but only from a process that is not itself executing a task,
    per `_should_log_environment`. Nothing is printed when the map came back empty either, that
    being the remote task re-importing the flow module inside an already-baked image, where
    there is no lockfile to read and nothing worth reporting.

    Args:
        decorator: the Metaflow decorator to delegate to, `pypi_base` or `pypi`
        target: the flow or step being decorated, or `None` in the called form
        label: the decorator's own name, used to say which one resolved the environment
        **kwargs: `dependency_groups`, `python` and `project_root`, forwarded to `_get_pypi_kwargs`

    """

    def decorate(obj):
        pypi_kwargs = _get_pypi_kwargs(**kwargs)
        if pypi_kwargs["packages"] and not _is_running_task():
            named = f"{label} on {obj.__name__}" if hasattr(obj, "__name__") else label
            print(_format_pypi_environment(named, pypi_kwargs))
        return decorator(**pypi_kwargs)(obj)

    return decorate if target is None else decorate(target)


def uv_pypi_base(
    flow=None,
    *,
    dependency_groups: Optional[Union[str, list]] = None,
    python: Optional[str] = None,
    project_root: Optional[Union[str, Path]] = None,
):
    """Metaflow's `@pypi_base`, with the flow's environment filled in from uv.lock.

    Delegates to `@pypi_base` after deriving `python` and `packages` via
    `_get_pypi_kwargs`, so the flow's
    environment is described only once -- in the project's own dependency files -- and cannot
    drift from what `uv sync` gives you locally.

    Use [`uv_pypi`][ds_platform_utils.metaflow.uv_pypi] for a single step.

    Example usage:

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

    Both forms work; call it when there is something to configure:

    ```python
    @uv_pypi_base                       # equivalent to @pypi_base(**_get_pypi_kwargs())
    @uv_pypi_base(dependency_groups=["dev"])       # add a dependency group
    @uv_pypi_base(python="3.11")        # override the derived interpreter
    ```

    Args:
        flow: the decorated `FlowSpec`, supplied by Python in the bare form. Never pass this
            yourself.
        dependency_groups: dependency groups to add on top of the runtime dependencies, e.g. `["dev"]`.
            Excluded by default, since groups are optional by definition.
        python: Python version to use instead of the one derived from the project, e.g.
            `"3.11"`.
        project_root: directory holding the project files. Defaults to searching upward from
            the directory the flow was launched from.

    Returns:
        The decorated flow, or a decorator when called with keyword arguments.

    """
    from metaflow import pypi_base

    return _apply_uv_pypi(
        pypi_base,
        flow,
        "@uv_pypi_base",
        dependency_groups=dependency_groups,
        python=python,
        project_root=project_root,
    )


def uv_pypi(
    step=None,
    *,
    dependency_groups: Optional[Union[str, list]] = None,
    python: Optional[str] = None,
    project_root: Optional[Union[str, Path]] = None,
):
    """Metaflow's `@pypi`, with a single step's environment filled in from uv.lock.

    The step-level counterpart to
    [`uv_pypi_base`][ds_platform_utils.metaflow.uv_pypi_base] -- same derived environment,
    scoped to one step instead of the whole flow. Both Metaflow decorators accept the same
    `python` and `packages` arguments, so the two behave identically apart from
    where they attach.

    Example usage:

    ```python
    from metaflow import FlowSpec, step

    from ds_platform_utils.metaflow import uv_pypi


    class MyFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.train)

        @uv_pypi(dependency_groups=["train"])
        @step
        def train(self):
            self.next(self.end)

        @step
        def end(self):
            pass
    ```

    Args:
        step: the decorated step function, supplied by Python in the bare form. Never pass
            this yourself.
        dependency_groups: dependency groups to add on top of the runtime dependencies, e.g. `["dev"]`.
            Excluded by default, since groups are optional by definition.
        python: Python version to use instead of the one derived from the project, e.g.
            `"3.11"`.
        project_root: directory holding the project files. Defaults to searching upward from
            the directory the flow was launched from.

    Returns:
        The decorated step, or a decorator when called with keyword arguments.

    """
    from metaflow import pypi

    return _apply_uv_pypi(
        pypi, step, "@uv_pypi", dependency_groups=dependency_groups, python=python, project_root=project_root
    )
