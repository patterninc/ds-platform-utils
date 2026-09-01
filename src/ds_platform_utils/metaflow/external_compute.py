"""Run a whole Metaflow step on AWS compute by decorating the step itself.

    @step
    @remote_step(BACKEND, cpu=8, memory=32)
    def train(self):
        df = query_pandas_from_snowflake(query=SQL)   # module-level imports, normal self.*
        self.model = fit(df)
        self.next(self.end)

The step body is written exactly as it would be for EKS. No in-body imports, no extracting the
work into a separate function, no manual serialization. The step's Metaflow task still runs on
EKS -- it only submits and waits -- so it can be declared small while the body runs elsewhere.

That is the point: Outerbounds bills by the task size a step declares, so heavy work moved off the
pod stops being billed at the medium or large band.

How the four hard parts are handled
-----------------------------------

1. ``self`` cannot travel. The body is parsed (AST) to find exactly which ``self.X`` it reads and
   which it writes, so only the artifacts actually used are shipped, not the whole flow state. A
   proxy stands in for ``self`` inside the container; the attributes it wrote are copied back onto
   the real flow afterwards.

2. ``metaflow`` is not installed in the container. We never import the flow module remotely -- the
   function is pickled *by value*, and cloudpickle ships only the globals the body references.
   ``from metaflow import ...`` lives at module scope, not in the body, so it never travels. Every
   Python package sitting beside the flow file is uploaded, so ``helpers``, ``ads_utils``,
   ``meridian`` or anything else the body imports resolves in the container.

3. ``current`` and ``ds_platform_utils`` do not work there. Anything the body references that is
   unusable remotely is swapped in the function's globals before pickling: ``current`` becomes a
   picklable snapshot, and the ``ds_platform_utils`` query helpers become the container-aware
   shims. The body sees the same names and does not know the difference.

4. Artifacts get pickled blindly. DataFrames are encoded as parquet in both directions, so the
   pandas version on either side stops mattering -- which is the failure this otherwise invites.

Limits
------

- Attributes reached dynamically (``getattr(self, name)``) are invisible to the AST pass. Pass
  ``extra_inputs=["name"]`` / ``extra_outputs=["name"]`` for those.
- The body must be picklable by value: no closures over unpicklable objects.
- Writes are copied back only after the job succeeds, so a partial failure leaves the flow
  untouched. That is intentional -- a half-applied step is worse than a failed one.
"""

from __future__ import annotations

import ast
import functools
import inspect
import io
import sys
import tempfile
import textwrap
import time
import types
import zipfile
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from .compute_backends import BACKEND_PACKAGES, ComputeBackend, RemoteCall, Resources
from .remote_runtime import PYTHON_VERSION, await_job, runtime_fingerprint
from .snowflake_access import bootstrap_ds_platform_utils

# Re-exported so a flow needs one import line for everything it touches: the decorator, the Python
# pin for @pypi_base, the submitting-side packages, and the fingerprint helper for comparing a
# decorated step against an ordinary one. Knowing which helper module each lives in is overhead a
# flow author should not carry.
__all__ = [
    "BACKEND_PACKAGES",
    "PYTHON_VERSION",
    "remote_step",
    "runtime_fingerprint",
]

# Metaflow *methods* on `self`. Reading one is never a request to ship an artifact.
#
# `input` and `index` are deliberately NOT here: inside a foreach they hold the item being
# processed, which is exactly the data a remote body needs, so they must be shipped like any other.
_METAFLOW_ATTRS = frozenset({"next", "foreach_stack", "merge_artifacts"})

_PARQUET_MARKER = "__remote_step_parquet__"

# Directory names never worth uploading with a path bundle.
_BUNDLE_EXCLUDES = frozenset({"__pycache__", ".git", ".venv", "test-reports", ".ipynb_checkpoints"})

# A path bundle rides along as a job argument, so it must stay small. SQL and YAML are text; if a
# body references a Path whose tree is large (a data directory), that is worth failing on.
MAX_PATH_BUNDLE_BYTES = 25 * 1024 * 1024


# ---------------------------------------------------------------------------
# Static analysis of the step body
# ---------------------------------------------------------------------------


def analyze_self_access(fn: Callable) -> tuple[list[str], list[str]]:
    """Find which ``self.X`` attributes a step body reads and which it writes.

    This is what keeps the decorator from shipping the entire flow state: only the artifacts the
    body actually touches cross the wire. Metaflow's own API names are excluded, as is anything
    written before it is read (a local result, not an input).

    :param fn: The undecorated step function.
    :return: ``(reads, writes)``, both sorted.
    """
    try:
        visitor = _visit_body(fn)
    except OSError as exc:
        # Only happens when the body has no source on disk (exec'd, or a REPL). Flow files always
        # do, so this is a wrong-usage signal rather than something to work around.
        raise RuntimeError(
            f"@remote_step could not read the source of '{fn.__name__}', so it cannot work out "
            f"which self.* attributes to ship. It must decorate a step defined in a flow file."
        ) from exc

    return sorted(visitor.inputs), sorted(visitor.writes)


def _visit_body(fn: Callable) -> "_SelfAccessVisitor":
    """Parse a step body once and return the visitor that walked it.

    :param fn: The undecorated step function.
    :return: The visitor, with reads, writes and conditional writes populated.
    """
    source = textwrap.dedent(inspect.getsource(fn))
    visitor = _SelfAccessVisitor()
    visitor.visit(ast.parse(source))
    return visitor


def conditional_writes(fn: Callable) -> set:
    """Attributes the body only assigns inside a branch, so may legitimately never be written.

    The difference matters when reporting what came back. An unconditional write that is missing
    means something went wrong -- the body took an early return, or never got that far. A
    conditional one missing is ordinary Python doing what it was told. Reporting both the same way
    would make the warning noisy on flows that branch, and a noisy warning is an ignored one.

    :param fn: The undecorated step function.
    :return: Names assigned only under ``if`` / ``for`` / ``while`` / ``try``.
    """
    return set(_visit_body(fn).conditional_writes)


class _SelfAccessVisitor(ast.NodeVisitor):
    """Walk a step body in execution order, separating inputs from locally-produced values.

    Order is the whole point, so ``ast.walk`` (breadth-first) will not do. ``self.model = fit(...)``
    followed by ``self.model.predict(...)`` must classify ``model`` as a write only -- reading it
    back is not a request to ship anything.

    A caveat comes with that: a write inside a conditional branch is treated as having happened, so
    an attribute assigned in an ``if`` and read afterwards is not recognised as an input. If that
    ever bites, the proxy raises an ``AttributeError`` naming the attribute and pointing at
    ``extra_inputs``, rather than failing silently.
    """

    def __init__(self) -> None:
        self.inputs: set[str] = set()
        self.writes: set[str] = set()
        self.conditional_writes: set[str] = set()
        self._branch_depth = 0

    def _visit_branch(self, node: ast.AST, *fields: str) -> None:
        """Visit the branching parts of a node with the conditional flag raised."""
        self._branch_depth += 1
        try:
            for field in fields:
                for child in getattr(node, field, []) or []:
                    self.visit(child)
        finally:
            self._branch_depth -= 1

    def visit_If(self, node: ast.If) -> None:  # noqa: N802
        """The test runs unconditionally; the branches do not."""
        self.visit(node.test)
        self._visit_branch(node, "body", "orelse")

    def visit_For(self, node: ast.For) -> None:  # noqa: N802
        """A loop body may run zero times."""
        self.visit(node.iter)
        self.visit(node.target)
        self._visit_branch(node, "body", "orelse")

    def visit_While(self, node: ast.While) -> None:  # noqa: N802
        """Same as a for loop: the body is not guaranteed."""
        self.visit(node.test)
        self._visit_branch(node, "body", "orelse")

    def visit_Try(self, node: ast.Try) -> None:  # noqa: N802
        """Anything after a raise point may be skipped."""
        self._visit_branch(node, "body", "handlers", "orelse", "finalbody")

    def _attr_name(self, node: ast.AST) -> Optional[str]:
        """Return the attribute name if the node is ``self.<attr>``, else None."""
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "self":
            return node.attr
        return None

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        """Visit the assigned value before the targets, matching evaluation order."""
        self.visit(node.value)
        for target in node.targets:
            self.visit(target)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:  # noqa: N802
        """``self.x += 1`` reads before it writes, so count it as both."""
        self.visit(node.value)
        name = self._attr_name(node.target)
        if name and name not in self.writes and name not in _METAFLOW_ATTRS:
            self.inputs.add(name)
        if name:
            self.writes.add(name)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        """Skip ``self.<step>`` arguments of ``self.next(...)`` -- those are steps, not data."""
        if self._attr_name(node.func) == "next":
            for argument in node.args:
                if self._attr_name(argument) is None:
                    self.visit(argument)
            for keyword in node.keywords:
                self.visit(keyword.value)
            return
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802
        """Record a read or a write of ``self.<attr>``."""
        name = self._attr_name(node)
        if name is not None:
            if isinstance(node.ctx, ast.Store):
                self.writes.add(name)
                if self._branch_depth:
                    self.conditional_writes.add(name)
            elif name not in self.writes and name not in _METAFLOW_ATTRS:
                self.inputs.add(name)
        self.generic_visit(node)


# ---------------------------------------------------------------------------
# The stand-in for `self` inside the container
# ---------------------------------------------------------------------------


class StepRef:
    """A reference to another step, produced by ``self.<step_name>`` inside a remote body."""

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


class SelfProxy:
    """Stands in for the ``FlowSpec`` while the body runs remotely.

    Reads come from the shipped artifacts. Writes are collected for copying back. ``self.next(...)``
    is recorded rather than executed -- the real transition is performed by the Metaflow task once
    the job returns, because only it can actually advance the graph.
    """

    def __init__(self, inputs: dict[str, Any], step_names: Iterable[str]) -> None:
        object.__setattr__(self, "_data", dict(inputs))
        object.__setattr__(self, "_step_names", frozenset(step_names))
        object.__setattr__(self, "_next_call", None)

    def __getattr__(self, name: str) -> Any:
        """Resolve a shipped artifact, or a step name, or explain what is missing."""
        data = object.__getattribute__(self, "_data")
        if name in data:
            return data[name]
        if name in object.__getattribute__(self, "_step_names"):
            return StepRef(name)
        raise AttributeError(
            f"'self.{name}' was not shipped to the remote job. The decorator ships only the "
            f"attributes it can see in the step body; if this one is reached dynamically, add "
            f"extra_inputs=['{name}'] to @remote_step."
        )

    def __setattr__(self, name: str, value: Any) -> None:
        """Collect a write for copying back onto the real flow."""
        object.__getattribute__(self, "_data")[name] = value

    def next(self, *steps: Any, **kwargs: Any) -> None:
        """Record the transition instead of performing it."""
        object.__setattr__(
            self,
            "_next_call",
            {"steps": [step.name for step in steps if isinstance(step, StepRef)], "kwargs": kwargs},
        )


# ---------------------------------------------------------------------------
# Version-proof encoding across the boundary
# ---------------------------------------------------------------------------


def encode_value(value: Any) -> Any:
    """Encode a value so it survives a pandas version gap between the two environments.

    DataFrames become parquet bytes; everything else is left for the normal pickle path.

    :param value: Any artifact value.
    :return: The value, or a marker dict wrapping parquet bytes.
    """
    try:
        import pandas as pd
    except ImportError:
        return value

    if isinstance(value, pd.DataFrame):
        return {_PARQUET_MARKER: value.to_parquet(index=False)}
    return value


def decode_value(value: Any) -> Any:
    """Reverse :func:`encode_value`.

    :param value: A possibly-encoded artifact value.
    :return: The original object.
    """
    if isinstance(value, dict) and _PARQUET_MARKER in value:
        import io

        import pandas as pd

        return pd.read_parquet(io.BytesIO(value[_PARQUET_MARKER]))
    return value


def _encode_all(values: dict[str, Any]) -> dict[str, Any]:
    return {key: encode_value(value) for key, value in values.items()}


def _decode_all(values: dict[str, Any]) -> dict[str, Any]:
    return {key: decode_value(value) for key, value in values.items()}


# ---------------------------------------------------------------------------
# Globals rewriting -- how metaflow/ds_platform_utils are kept out of the container
# ---------------------------------------------------------------------------


def requirements_from_packages(packages: Optional[dict[str, str]]) -> list[str]:
    """Turn a ``@pypi``-style package mapping into pip requirement strings.

    Same shape flows already use for ``@pypi``, so there is one way to express a dependency::

        {"xgboost": "2.0.3", "shap": ""}  ->  ["xgboost==2.0.3", "shap"]

    An empty version means unpinned. A value that already looks like a specifier (``>=1.2``) is
    used as written rather than forced to ``==``.

    :param packages: Package name to version, or None.
    :return: pip requirement strings.
    """
    if not packages:
        return []

    requirements = []
    for name, version in packages.items():
        if not version:
            requirements.append(name)
        elif version[0] in "=<>!~@":
            requirements.append(f"{name}{version}")
        else:
            requirements.append(f"{name}=={version}")
    return requirements


def packages_from_flow(flow: Any, step_name: str) -> dict[str, str]:
    """Read the packages the flow already declares, so the container gets the same ones.

    A flow's ``@pypi_base`` (and any step-level ``@pypi``) is the existing, obvious answer to
    "what does this code need to import". Without this the remote container is a second,
    invisible dependency list that drifts: every package the flow adds works everywhere except
    inside a decorated step, where it fails with ``ModuleNotFoundError`` on a machine the author
    cannot see. Inheriting means a step body has the same imports available wherever it runs.

    Step-level ``@pypi`` wins over ``@pypi_base`` for the same package, matching Metaflow.

    :param flow: The running FlowSpec instance.
    :param step_name: Name of the decorated step, for its own ``@pypi``.
    :return: Package name to version, in ``@pypi`` shape.
    """
    packages: dict[str, str] = {}

    # Flow level. `_flow_decorators` maps a decorator name to a list of instances.
    for decorator in (getattr(flow, "_flow_decorators", None) or {}).get("pypi_base", []):
        packages.update(getattr(decorator, "attributes", {}).get("packages") or {})

    # Step level, which Metaflow lets override the base.
    for decorator in getattr(getattr(type(flow), step_name, None), "decorators", []):
        if getattr(decorator, "name", None) == "pypi":
            packages.update(getattr(decorator, "attributes", {}).get("packages") or {})

    return packages


def assert_python_versions_match(submitting_version: tuple[int, int]) -> None:
    """Refuse to unpickle a body compiled by a different Python minor version.

    A function pickled *by value* carries its compiled bytecode, and bytecode is not portable
    across minor versions. Both sides are 3.11 today, but the container image is a separate
    artifact: the day it is rebuilt on 3.12 while ``PYTHON_VERSION`` stays 3.11, the failure would
    surface as an opaque unpickling error minutes into a job. This names the cause instead.

    Patch versions are fine -- 3.11.0 and 3.11.14 share bytecode -- so only major.minor is compared.

    :param submitting_version: ``(major, minor)`` captured where the body was pickled.
    :raises RuntimeError: If the container's Python minor version differs.
    """
    container_version = sys.version_info[:2]
    if tuple(submitting_version) != container_version:
        submitted = ".".join(str(part) for part in submitting_version)
        running = ".".join(str(part) for part in container_version)
        raise RuntimeError(
            f"The step body was pickled on Python {submitted} but this container runs {running}. "
            f"cloudpickle ships the body as bytecode, which is not portable across minor versions. "
            f"Align PYTHON_VERSION in helpers/remote_runtime.py with the container image."
        )


def discover_importable_packages(fn: Callable) -> list[tuple[str, str]]:
    """Find the Python packages sitting beside the flow file, so the body's imports resolve remotely.

    Derived from the *flow's* location rather than this module's, because projects do not all keep
    their code in ``helpers``: ``ads_utils``, ``meridian`` and ``configs`` are all real packages in
    this repo, and a body importing from one of those needs it shipped too. Deriving it from the
    flow also keeps working once these helpers move into ``ds-platform-utils``.

    :param fn: The undecorated step function.
    :return: ``[(directory, import_name)]`` pairs for ``imports=``.
    """
    flow_file = fn.__globals__.get("__file__")
    if not flow_file:
        # No module file (an exec'd body). Nothing to discover, and nothing needs shipping --
        # ds_platform_utils itself is installed in the image.
        return []

    src_dir = Path(flow_file).resolve().parent
    return [(str(child), child.name) for child in sorted(src_dir.iterdir()) if _is_importable_dir(child)]


def _is_importable_dir(path: Path) -> bool:
    """Decide whether a directory beside the flow holds importable code.

    An ``__init__.py`` is not required: Python 3 namespace packages work without one, and this repo
    has real examples -- ``operations/excess-inventory`` does ``from configs.configs import ...``
    against a directory that has no ``__init__.py``. Requiring one would miss those entirely, so
    any directory holding ``.py`` files counts.

    :param path: A candidate directory.
    :return: True if it should be shipped as an importable package.
    """
    if not path.is_dir() or path.name in _BUNDLE_EXCLUDES or path.name.startswith("."):
        return False
    return (path / "__init__.py").exists() or any(path.glob("*.py"))


def collect_path_globals(fn: Callable) -> dict[str, Path]:
    """Find ``Path``-valued globals the body references, e.g. ``SQL_DIR`` or ``THIS_DIR``.

    These are the ones whose *files* have to travel: cloudpickle happily ships the ``Path`` object
    itself, but the path it points at does not exist in the container, so reading it would fail.

    :param fn: The undecorated step function.
    :return: ``{global_name: path}`` for referenced paths that exist on disk.
    """
    referenced = set(fn.__code__.co_names)
    found: dict[str, Path] = {}
    for name in sorted(referenced):
        value = fn.__globals__.get(name)
        if not isinstance(value, Path):
            continue
        if value.exists():
            found[name] = value
        else:
            # Almost always means Metaflow's code package left the files behind: it ships .py only
            # unless --package-suffixes names the others. Silently skipping turns that into a
            # FileNotFoundError inside the container, minutes later, after the pool has spun up.
            print(
                f"[remote_step] WARNING: '{name}' points at {value}, which does not exist here, "
                f"so its files cannot be bundled. If the body reads from it, the job will fail with "
                f"FileNotFoundError. Add --package-suffixes='.csv,.sql,.json,.toml,.yaml,.yml,.txt' "
                f"so Metaflow packages them."
            )
    return found


def bundle_paths(path_globals: dict[str, Path]) -> tuple[bytes, dict[str, str]]:
    """Zip up the files behind referenced path globals so they can travel with the job.

    Each global gets its own namespace inside the archive, so two globals pointing at
    similarly-named directories cannot collide.

    :param path_globals: Output of :func:`collect_path_globals`.
    :return: ``(zip_bytes, {global_name: path_inside_archive})``.
    :raises RuntimeError: If the bundle exceeds :data:`MAX_PATH_BUNDLE_BYTES`.
    """
    if not path_globals:
        return b"", {}

    buffer = io.BytesIO()
    mapping: dict[str, str] = {}

    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, path in sorted(path_globals.items()):
            if path.is_file():
                mapping[name] = f"{name}/{path.name}"
                archive.write(path, mapping[name])
                continue

            mapping[name] = name
            for item in sorted(path.rglob("*")):
                if item.is_dir() or _BUNDLE_EXCLUDES.intersection(item.parts):
                    continue
                archive.write(item, f"{name}/{item.relative_to(path)}")

    bundle = buffer.getvalue()
    if len(bundle) > MAX_PATH_BUNDLE_BYTES:
        raise RuntimeError(
            f"@remote_step would upload {len(bundle) / 1e6:.1f} MB of files for path globals "
            f"{sorted(path_globals)}, over the {MAX_PATH_BUNDLE_BYTES / 1e6:.0f} MB limit. Point "
            f"the body at a narrower directory (e.g. a specific sql/ subfolder rather than the "
            f"whole src/), or read the file in the step and pass its contents instead."
        )
    return bundle, mapping


def unbundle_paths(bundle: bytes, mapping: dict[str, str]) -> dict[str, Path]:
    """Extract a path bundle in the container and return the rebased globals.

    :param bundle: Zip bytes from :func:`bundle_paths`.
    :param mapping: ``{global_name: path_inside_archive}``.
    :return: ``{global_name: extracted_path}`` for patching into the body's globals.
    """
    if not bundle:
        return {}

    destination = Path(tempfile.mkdtemp(prefix="remote_step_paths_"))
    with zipfile.ZipFile(io.BytesIO(bundle)) as archive:
        archive.extractall(destination)
    return {name: destination / relative for name, relative in mapping.items()}


def snapshot_current() -> types.SimpleNamespace:
    """Capture the picklable parts of Metaflow's ``current`` so the body can still read it remotely.

    :return: A namespace exposing the commonly used ``current`` fields.
    """
    from metaflow import current

    # Covers every `current.*` attribute used across the repo's flows except `current.card`,
    # which is a live object and cannot travel -- build cards outside a @remote_step body.
    fields = (
        "is_production",
        "project_name",
        "flow_name",
        "run_id",
        "step_name",
        "origin_run_id",
        "username",
        "pathspec",
        "task_id",
        "retry_count",
    )
    captured = {field: getattr(current, field, None) for field in fields}
    captured["tags"] = list(getattr(current, "tags", []) or [])
    captured["is_running_flow"] = True
    return types.SimpleNamespace(**captured)


def rebind_globals(fn: Callable) -> Callable:
    """Return a copy of ``fn`` whose globals are safe to ship.

    ``current`` is replaced with a picklable snapshot, and only when the body actually references
    it -- cloudpickle ships referenced globals only, so an untouched name costs nothing.
    ``ds_platform_utils`` needs no swapping here: the library itself is repointed inside the
    container by :func:`ds_platform_utils.metaflow.snowflake_access.bootstrap_ds_platform_utils`.

    :param fn: The undecorated step function.
    :return: An equivalent function bound to rewritten globals.
    """
    referenced = set(fn.__code__.co_names)
    new_globals = dict(fn.__globals__)

    if "current" in referenced:
        new_globals["current"] = snapshot_current()

    return types.FunctionType(fn.__code__, new_globals, fn.__name__, fn.__defaults__, fn.__closure__)


# ---------------------------------------------------------------------------
# The remote half
# ---------------------------------------------------------------------------


def execute_remote_step(  # noqa: PLR0913
    fn_bytes: bytes,
    encoded_inputs: dict[str, Any],
    write_names: list[str],
    step_names: list[str],
    path_bundle: bytes = b"",
    path_map: Optional[dict[str, str]] = None,
    submitting_python: tuple[int, int] = (),
) -> bytes:
    """Run a pickled step body against a proxy ``self``. This executes inside the job container.

    :param fn_bytes: The step body, cloudpickled by value.
    :param encoded_inputs: Shipped artifacts, parquet-encoded where applicable.
    :param write_names: Attributes to collect afterwards.
    :param step_names: Flow step names, so ``self.<step>`` resolves in ``self.next(...)``.
    :param path_bundle: Zipped files behind the body's ``Path`` globals.
    :param path_map: ``{global_name: path_inside_archive}``.
    :param submitting_python: ``(major, minor)`` of the Python that pickled the body, checked
        before unpickling because bytecode does not cross minor versions.
    :return: A cloudpickled dict of writes, the recorded transition, and where this ran. Returned
        as opaque bytes on purpose -- see the note in the return statement.
    """
    import cloudpickle

    if submitting_python:
        assert_python_versions_match(submitting_python)

    body = cloudpickle.loads(fn_bytes)

    bootstrap = bootstrap_ds_platform_utils(body.__globals__.get("current"))
    print(f"[remote_step] ds_platform_utils patched for container use: {bootstrap}")

    # Repoint Path globals at the extracted copies. The originals are absolute paths from the
    # submitting side and do not exist here, so this has to happen before the body runs.
    for name, extracted in unbundle_paths(path_bundle, path_map or {}).items():
        body.__globals__[name] = extracted

    proxy = SelfProxy(_decode_all(encoded_inputs), step_names)
    body_started = time.time()
    body(proxy)
    body_seconds = time.time() - body_started

    data = object.__getattribute__(proxy, "_data")
    writes = {name: data[name] for name in write_names if name in data}

    # Names the body was expected to set but never did. Reported rather than raised: a branch that
    # did not run is ordinary Python. Carried back so the *submitting* side can say so, because
    # otherwise the first sign is an AttributeError in a later step, far from the cause.
    missing = [name for name in write_names if name not in data]

    # Returned as a single pickled blob rather than a plain dict. A backend's result protocol
    # walks returned containers recursively and assumes every dict key is a string
    # (`k.startswith(...)`), so a perfectly ordinary artifact like `{7: 0.1, 14: 0.2}` fails with
    # "'int' object has no attribute 'startswith'" *after* the job has already succeeded. Handing
    # back bytes means it never inspects user data, and any picklable artifact survives.
    return cloudpickle.dumps(
        {
            "writes": _encode_all(writes),
            "missing_writes": missing,
            "next": object.__getattribute__(proxy, "_next_call"),
            # Named for the decorator, not the backend: this runs on whichever compute was
            # chosen, and a label naming the wrong service is how a wrong finding gets written down.
            "runtime": runtime_fingerprint("remote step"),
            "body_seconds": round(body_seconds, 2),
        }
    )


# ---------------------------------------------------------------------------
# The decorator
# ---------------------------------------------------------------------------


def _already_finished(job) -> bool:
    """Report whether the job reached a terminal state without our help.

    :param job: A handle satisfying :class:`~ds_platform_utils.metaflow.compute_backends.JobHandle`.
    :return: True if it is already terminal. False when unreadable -- an unknown state is better
        treated as still running, since a needless stop call is cheaper than a stranded job.
    """
    try:
        return str(job.status) in job.terminal_statuses
    except Exception:
        return False


def await_job_or_cancel(job, label: str):
    """Wait for a job, and stop it if this process is going away instead.

    A submitted job has no idea whether anyone is still waiting: interrupt the step, evict the pod,
    and it runs on to completion or to its stopping condition, billing the whole time. Nothing else
    would ever stop it, because the only thing holding a reference to it is this process.

    SIGTERM is handled explicitly because that is how Kubernetes evicts a pod, and Python's default
    action for it is to die without unwinding -- no ``finally``, no cleanup. The handler is
    installed only for the wait and restored afterwards, so it cannot affect anything else. SIGKILL
    remains unreachable by definition; ``max_runtime_seconds`` on the job is the backstop for that.

    :param job: A handle satisfying :class:`~ds_platform_utils.metaflow.compute_backends.JobHandle`.
    :param label: Step name, for log lines.
    :return: The pickled result.
    """
    import signal

    def on_terminate(_signum, _frame):
        raise SystemExit(f"{label}: terminated while waiting on {job.id}")

    try:
        previous = signal.signal(signal.SIGTERM, on_terminate)
    except ValueError:
        previous = None  # not on the main thread; the try/except below still covers interrupts

    try:
        return await_job(job, label=label)
    except BaseException:
        # BaseException, not Exception: KeyboardInterrupt and SystemExit are exactly the cases
        # where a job would otherwise be left running with nobody watching.
        #
        # A body that raised is *not* one of those. The job already ended on its own and AWS
        # marked it Failed, so asking to stop it would report an error about a job that did
        # exactly what it should have -- noise on top of the real traceback.
        if not _already_finished(job):
            print(f"[remote_step] {label}: stopping {job.id} -- nothing is waiting for it any more")
            cancel = getattr(job, "cancel", None)
            if cancel is not None:
                try:
                    cancel()
                except Exception as exc:  # cleanup must never mask the original failure
                    print(f"[remote_step] {label}: cancel failed: {exc}")
        raise
    finally:
        if previous is not None:
            signal.signal(signal.SIGTERM, previous)


# The names a flow can pass instead of building a backend itself. A string keeps the choice
# explicit -- which was always the argument for requiring it -- while removing the import, the
# module-level object, and the need to know which factory lives where.
BACKENDS = ("sagemaker", "batch")


def resolve_backend(backend, snowflake: bool = False, warm_seconds: int = 0):
    """Turn a backend name into a backend, or pass an already-built one straight through.

    :param backend: ``"sagemaker"``, ``"batch"``, or a :class:`ComputeBackend` for anything the
        names cannot express -- another account, a different queue, a custom image.
    :param snowflake: Stage Snowflake credentials for the container.
    :param warm_seconds: Keep the instance alive this long for the next job. SageMaker only;
        Fargate has no equivalent, so asking for it on Batch is refused rather than ignored.
    :return: A backend.
    :raises ValueError: On an unknown name, or a warm pool where none exists.
    """
    if not isinstance(backend, str):
        return backend

    # Imported here rather than at module scope: aws_env reads account configuration, and a flow
    # that passes its own backend object should not need that to be resolvable.
    from . import aws_env

    if backend == "sagemaker":
        return aws_env.sagemaker_backend(with_snowflake=snowflake, keep_alive_seconds=warm_seconds)
    if backend == "batch":
        if warm_seconds:
            raise ValueError(
                "warm_seconds is a SageMaker feature: Fargate has no warm pools, so a Batch job "
                "always pays a cold start. Drop it, or use backend='sagemaker'."
            )
        return aws_env.batch_backend(with_snowflake=snowflake)
    raise ValueError(f"unknown backend {backend!r}; expected one of {BACKENDS} or a ComputeBackend")


def remote_step(  # noqa: PLR0913
    backend: "str | ComputeBackend",
    *,
    cpu: int = 0,
    memory: int = 0,
    gpu: int = 0,
    instances: int = 1,
    snowflake: bool = False,
    warm_seconds: int = 0,
    packages: Optional[dict[str, str]] = None,
    fingerprint: bool = True,
    extra_inputs: Iterable[str] = (),
    extra_outputs: Iterable[str] = (),
    extra_imports: Iterable[tuple[str, str]] = (),
    pip_requirements: Optional[list[str]] = None,
    inherit_flow_packages: bool = True,
) -> Callable:
    """Run the decorated step's body on AWS compute instead of the Metaflow pod.

    Apply it beneath ``@step``, so Metaflow still sees the step::

        BACKEND = SageMakerBackend(role_arn=..., image_uri=..., s3_prefix=...)

        @step
        @remote_step(BACKEND, cpu=8, memory=32)
        def train(self):
            ...

    ``cpu``/``memory``/``gpu`` size the job. The backend picks the smallest instance that satisfies
    the request, per job -- nothing has to be provisioned in advance, so asking for more simply
    costs more for the minutes it runs.

    ``backend`` is required rather than defaulted. Where a job runs depends on an account, a role
    and an image, and guessing any of those would be worse than saying so.

    ``packages`` is usually unnecessary. The step inherits the flow's ``@pypi_base`` packages, so
    whatever the flow already declares is available in the container too -- the body imports the
    same names wherever it runs. Use ``packages`` only for something the flow itself does not need.

    Installs happen at job start and are wiped on exit. A backend whose image already ships a
    package skips it in seconds, so baking the common ones into an image is how this stays fast;
    it is a cache, not the contract.

    **Joining a foreach.** The decorator records evidence on ``self.remote_runtimes``, so each
    branch of a foreach arrives at the join with a different value and ``merge_artifacts`` refuses
    to choose. Set it explicitly in the join and exclude it -- see ``join_probe`` in
    ``src/oos_replica_flow.py``.

    :param backend: ``"sagemaker"``, ``"batch"``, or a built ``ComputeBackend``.
    :param cpu: Minimum CPU cores.
    :param memory: Minimum memory in GB.
    :param gpu: Minimum GPUs.
    :param instances: Nodes to run the job on.
    :param snowflake: Stage short-lived Snowflake credentials, so the body can read and write it
        through ``ds_platform_utils`` as it would on EKS.
    :param warm_seconds: Keep the instance alive this long for the next job, cutting the next
        step's startup from ~169s to ~31s. SageMaker only; refused on Batch rather than ignored.
        The instance bills while idle, so this is a trade, not free.
    :param packages: Extra packages for the container, ``{"name": "version"}`` like ``@pypi``.
        Merged over the flow's inherited ones, so it can also re-pin a version.
    :param fingerprint: Record where the body ran on ``self.remote_runtimes``. On by default --
        it is how you prove the work moved -- and worth turning off only to keep a foreach join
        simple.
    :param extra_inputs: Attributes to ship that the AST pass cannot see (dynamic access).
    :param extra_outputs: Attributes to copy back that the AST pass cannot see.
    :param extra_imports: Additional ``(directory, import_name)`` pairs to ship, for packages
        that do not live beside the flow file.
    :param pip_requirements: Extra packages for the container.
    :param inherit_flow_packages: Install the flow's ``@pypi_base``/``@pypi`` packages in the
        container. Turn off only for a flow whose submitting side needs packages the body does
        not, where installing them is pure cost.
    :return: The wrapped step function.
    """
    step_backend = resolve_backend(backend, snowflake=snowflake, warm_seconds=warm_seconds)

    def decorate(fn: Callable) -> Callable:
        reads, writes = analyze_self_access(fn)
        branch_writes = conditional_writes(fn)
        read_names = sorted(set(reads) | set(extra_inputs))
        write_names = sorted(set(writes) | set(extra_outputs))

        @functools.wraps(fn)
        def wrapper(self):
            import cloudpickle

            step_names = [
                name for name in dir(type(self)) if getattr(getattr(type(self), name, None), "is_step", False)
            ]
            inputs = {name: getattr(self, name) for name in read_names if hasattr(self, name)}
            path_bundle, path_map = bundle_paths(collect_path_globals(fn))
            code_dirs = [*discover_importable_packages(fn), *extra_imports]

            # The flow's own packages, plus anything this step adds on top. Read here rather than
            # at decoration because the flow decorators are only attached once the class exists.
            inherited = packages_from_flow(self, fn.__name__) if inherit_flow_packages else {}
            step_packages = {**inherited, **(packages or {})}

            # ds_platform_utils is always installed: it is how flows read and write Snowflake, and
            # measurement put the install at ~9.5s against ~94s of unavoidable per-job overhead.
            call = RemoteCall(
                step_name=fn.__name__,
                fn_bytes=cloudpickle.dumps(rebind_globals(fn)),
                inputs=_encode_all(inputs),
                write_names=write_names,
                step_names=step_names,
                python_version=sys.version_info[:2],
                code_dirs=code_dirs,
                pip_requirements=[*(pip_requirements or []), *requirements_from_packages(step_packages)],
                path_bundle=path_bundle,
                path_map=path_map,
                resources=Resources(cpu=cpu, memory=memory, gpu=gpu, instances=instances),
            )

            print(f"[remote_step] {fn.__name__}: {step_backend.describe(call)}")
            print(
                f"[remote_step] {fn.__name__}: shipping {len(inputs)} artifact(s) "
                f"{read_names or '[]'}, expecting back {write_names or '[]'}"
            )
            print(f"[remote_step] {fn.__name__}: shipping package(s) {[name for _, name in code_dirs]}")
            if step_packages:
                # What the container is asked for, not what it installs -- each backend drops what
                # its own runtime already ships, and says so in its own line further down.
                print(f"[remote_step] {fn.__name__}: container needs {sorted(step_packages)}")
            if path_map:
                print(
                    f"[remote_step] {fn.__name__}: bundling {sorted(path_map)} "
                    f"({len(path_bundle) / 1024:.0f} KB of files)"
                )

            job = step_backend.submit(call)
            # Where it went is already in the describe() line above; this one is just the handle.
            print(f"[remote_step] {fn.__name__}: submitted job {job.id}")
            submitted_at = time.time()
            result = cloudpickle.loads(await_job_or_cancel(job, label=fn.__name__))
            total_seconds = time.time() - submitted_at

            # Everything that is not the body: queueing for a pool node, container start, and the
            # pip install when one is needed. Comparing a step that installs against one that does
            # not is what turns "should we bake an image?" into a number.
            body_seconds = result.get("body_seconds", 0.0)
            print(
                f"[remote_step] {fn.__name__}: {total_seconds:.1f}s total = "
                f"{body_seconds:.1f}s body + {total_seconds - body_seconds:.1f}s overhead "
                f"(queue + container start + pip install)"
            )

            # Applied only on success, so a failed job leaves the flow's state untouched.
            for name, value in _decode_all(result["writes"]).items():
                setattr(self, name, value)

            # An attribute the body was supposed to set but did not simply will not exist, and the
            # next step to touch it raises AttributeError with no hint of where it came from. Say
            # it here instead, and separate the two cases: a branch that did not run is expected,
            # an unconditional assignment going missing means the body did not reach the end.
            for name in result.get("missing_writes", []):
                if name in branch_writes:
                    print(f"[remote_step] {fn.__name__}: '{name}' not set -- its branch did not run")
                else:
                    print(
                        f"[remote_step] {fn.__name__}: WARNING '{name}' is assigned unconditionally "
                        f"but came back unset; the body may not have run to completion"
                    )

            # Keyed by step so a later remote step does not overwrite an earlier one's evidence.
            if fingerprint:
                runtimes = dict(getattr(self, "remote_runtimes", {}))
                runtimes[fn.__name__] = result["runtime"]
                self.remote_runtimes = runtimes

            transition = result["next"]
            if transition:
                self.next(*[getattr(self, name) for name in transition["steps"]], **transition["kwargs"])

        return wrapper

    return decorate
