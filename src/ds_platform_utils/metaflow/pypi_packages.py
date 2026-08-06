"""Derive a Metaflow ``@pypi`` packages map from a flow repo's dependency declarations.

Keeping the dependency list in one place means a flow's `@pypi_base` cannot drift from what
the project actually installs. Two entry points, same output contract, differing only in
where the versions come from:

- [`get_packages_from_pyproject`][ds_platform_utils.metaflow.get_packages_from_pyproject]
  reads `pyproject.toml`, so a dependency is pinned exactly as loosely as it was declared.
- [`get_packages_from_uv_lock`][ds_platform_utils.metaflow.get_packages_from_uv_lock] reads
  `uv.lock`, so every version is the resolved one -- including exact commit SHAs for git
  dependencies, which makes a bake reproducible instead of tracking whatever `main` points
  at today.
"""

import sys

# Python versions 3.11+ ship with a version of Tomli: the tomllib standard library module.
# https://pypi.org/project/tomli/
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from pathlib import Path
from typing import Optional, Tuple, Union
from urllib.parse import parse_qs, urlsplit, urlunsplit

#: uv.lock `source` keys that mean "this is the local project, not something to install".
_LOCAL_SOURCE_KEYS = ("virtual", "editable", "directory")


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


def _uv_source_to_direct_reference(name: str, source: dict) -> str:
    """Render a `[tool.uv.sources]` entry as a PEP 508 direct reference.

    uv's table syntax (`{ git = "...", rev = "main" }`) means nothing to pip, so it has to be
    flattened into the `git+<url>@<ref>` form pip understands.

    Args:
        name: the dependency name the source is declared for, used in the error message
        source: the source table, e.g. `{"git": "https://...", "rev": "main"}`

    """
    if "git" in source:
        url = f"git+{source['git']}"
        # uv allows rev / tag / branch; all three land on the same pip fragment.
        ref = source.get("rev") or source.get("tag") or source.get("branch")
        if ref:
            url = f"{url}@{ref}"
    elif "url" in source:
        url = source["url"]
    else:
        raise ValueError(
            f"[tool.uv.sources] entry for {name!r} cannot be installed by @pypi: {source!r}. Only git and "
            "url sources are fetchable from a remote task -- path and workspace sources are local-only."
        )
    if source.get("subdirectory"):
        url = f"{url}#subdirectory={source['subdirectory']}"
    # metaflow passes a version through verbatim when it starts with "@", which is
    # exactly the PEP 508 direct-reference separator.
    return f"@ {url}"


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


def _flatten_group(group: str, declared: dict, path: Tuple[str, ...] = ()) -> list:
    """Expand one PEP 735 dependency group into a flat list of requirement strings.

    A group entry is normally a requirement string, but it can also be
    `{include-group = "other"}`, which pulls in another group wholesale -- so the expansion
    recurses.

    Args:
        group: name of the group to expand
        declared: the whole `[dependency-groups]` table
        path: groups already being expanded further up the recursion, used to catch an
            include cycle instead of blowing the stack

    """
    if group in path:
        raise ValueError("circular include-group chain in [dependency-groups]: " + " -> ".join(path + (group,)))
    try:
        entries = declared[group]
    except KeyError:
        raise ValueError(
            f"dependency group {group!r} is not declared in [dependency-groups]. "
            f"Declared groups: {', '.join(sorted(declared)) or '(none)'}"
        ) from None

    requirements = []
    for entry in entries:
        if isinstance(entry, dict):
            try:
                included = entry["include-group"]
            except KeyError:
                raise ValueError(f"unsupported entry in dependency group {group!r}: {entry!r}") from None
            requirements.extend(_flatten_group(included, declared, path + (group,)))
        else:
            requirements.append(entry)
    return requirements


def get_packages_from_pyproject(
    groups: Optional[Union[str, list]] = None,
    project_root: Optional[Union[str, Path]] = None,
) -> dict:
    """Build the `@pypi` packages map from a flow repo's `pyproject.toml`.

    Dependencies declared in `[tool.uv.sources]` are not on PyPI, so they are emitted as git
    direct references and built from source by pip rather than resolved by name.

    Prefer [`get_packages_from_uv_lock`][ds_platform_utils.metaflow.get_packages_from_uv_lock]
    when the repo has a lockfile: this function can only pass on what was declared, so an
    open-ended `>=` constraint stays open-ended and two bakes a week apart may not match.

    Returns an empty map when `pyproject.toml` cannot be found. A remote task re-imports the
    flow module -- and therefore re-evaluates the decorator -- inside a container whose code
    package holds only `.py` files, so the file is missing there. By that point the image has
    already been baked from the map resolved on the client, so nothing needs to be
    re-derived; without this the remote task dies on `FileNotFoundError`.

    Example usage:

    ```python
    from metaflow import FlowSpec, pypi_base, step

    from ds_platform_utils.metaflow import get_packages_from_pyproject


    @pypi_base(python="3.11", packages=get_packages_from_pyproject())
    class MyFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass
    ```

    Args:
        groups: names of `[dependency-groups]` to add on top of the project's runtime
            dependencies, e.g. `["dev"]`. Groups are optional by definition, so none are
            included unless asked for. `{include-group = ...}` references are followed. A
            name declared in more than one requested group takes its last-seen constraint.
        project_root: directory holding `pyproject.toml`. Defaults to searching upward from
            the directory the flow was launched from.

    Returns:
        A map of package name -> version constraint, ready to hand to `@pypi(packages=...)`.

    """
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    if isinstance(groups, str):
        # a bare string would otherwise iterate character by character
        groups = [groups]

    toml_path = _find_project_file("pyproject.toml", project_root)
    if toml_path is None:
        return {}

    with open(toml_path, "rb") as f:
        pyproject = tomllib.load(f)

    sources = {
        canonicalize_name(name): (name, source)
        for name, source in pyproject.get("tool", {}).get("uv", {}).get("sources", {}).items()
    }

    requirements = list(pyproject["project"]["dependencies"])
    if groups:
        declared = pyproject.get("dependency-groups", {})
        for group in groups:
            requirements.extend(_flatten_group(group, declared))

    packages = {}
    for dep in requirements:
        req = Requirement(dep)
        source = sources.get(canonicalize_name(req.name))
        if source:
            packages[req.name] = _uv_source_to_direct_reference(*source)
            continue
        specifiers = list(req.specifier)
        # metaflow prepends "==" to a bare version but passes <, >, !, ~, @ through
        # verbatim, so an "==" pin has to be handed over as the bare version.
        if len(specifiers) == 1 and specifiers[0].operator == "==":
            packages[req.name] = specifiers[0].version
        else:
            packages[req.name] = str(req.specifier)
    return packages


def get_packages_from_uv_lock(
    groups: Optional[Union[str, list]] = None,
    project_root: Optional[Union[str, Path]] = None,
) -> dict:
    """Build the `@pypi` packages map from a flow repo's `uv.lock`.

    Emits the root project's direct runtime dependencies pinned to their locked versions, so
    the image Metaflow bakes matches the environment `uv sync` gives you locally. Dependency
    groups are excluded unless named in `groups`, since uv keeps them in a separate table and
    they are optional by definition.

    Deliberately *not* the full transitive closure: lock entries are marker-gated per
    platform (`appnope` on darwin, `colorama` on win32) and one name can be locked at two
    versions behind different resolution markers, so pinning the whole graph would break a
    bake for any platform but this one. `@pypi` resolves transitives itself from these pinned
    roots. A name locked more than once is emitted unpinned for the same reason.

    Returns an empty map when `uv.lock` cannot be found, for the same reason
    [`get_packages_from_pyproject`][ds_platform_utils.metaflow.get_packages_from_pyproject]
    does when `pyproject.toml` is missing -- a remote task re-imports the flow module inside a
    container whose code package holds only `.py` files. The image is already baked from the
    map resolved on the client by then, so nothing is lost.

    Example usage:

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

    Args:
        groups: names of dependency groups to add on top of the runtime dependencies, e.g.
            `["dev"]`. uv resolves `include-group` references when it writes the lock, so the
            groups recorded here are already flat.
        project_root: directory holding `uv.lock`. Defaults to searching upward from the
            directory the flow was launched from.

    Returns:
        A map of package name -> locked version, ready to hand to `@pypi(packages=...)`.

    """
    if isinstance(groups, str):
        # a bare string would otherwise iterate character by character
        groups = [groups]

    lock_path = _find_project_file("uv.lock", project_root)
    if lock_path is None:
        return {}

    with open(lock_path, "rb") as f:
        lock = tomllib.load(f)

    root, entries = _split_lock_packages(lock, lock_path)

    dependencies = list(root.get("dependencies", []))
    if groups:
        declared = root.get("dev-dependencies", {})
        for group in groups:
            try:
                dependencies.extend(declared[group])
            except KeyError:
                raise ValueError(
                    f"dependency group {group!r} is not recorded in {lock_path}. "
                    f"Groups present: {', '.join(sorted(declared)) or '(none)'}"
                ) from None

    packages = {}
    for dep in dependencies:
        name = dep["name"]
        locked = entries.get(name, [])
        if not locked:
            raise ValueError(f"{name!r} is a dependency of the root project but is missing from {lock_path}")
        if len(locked) > 1:
            # resolved differently per platform -- hand it to @pypi unpinned.
            packages[name] = ""
            continue
        package = locked[0]
        source = package.get("source", {})
        if "registry" in source:
            # metaflow prepends "==" to a bare version.
            packages[name] = package["version"]
        else:
            packages[name] = _lock_source_to_direct_reference(name, source)
    return packages
