"""@remote_step — the Metaflow StepDecorator that offloads to Kubernetes.

Hooks used:
  step_init         — validate at flow-init, resolve resources, adjust siblings
  task_pre_step     — capture the Metaflow code-package URL from the datastore
  task_decorate     — replace the user's step body with a driver body

The driver body:
  1. Reads sibling attrs off `self` (mostly RemoteArtifact refs).
  2. Builds spec.json + uploads to the payload bucket.
  3. Creates a Kubernetes Job, queued through Kueue.
  4. Blocks on poll.wait, streaming the pod's log to stderr.
  5. Reads output-manifest.json.
  6. Assigns RemoteArtifact refs back onto `self`.

Metaflow then persists those tiny refs as normal artifacts at task end.
"""

from __future__ import annotations

import getpass
import json
import os
import subprocess
import sys
import threading
import time

try:
    # ob-metaflow / metaflow — same import path.
    from metaflow.decorators import StepDecorator
except ImportError:  # pragma: no cover - metaflow always present in prod
    StepDecorator = object  # type: ignore[assignment,misc]

from remote_step.artifact import RemoteArtifact
from remote_step import keys
from remote_step.code_package import resolve_code_package
from remote_step.config import (
    RemoteStepConfig,
    check_team,
    load as load_config,
)
from remote_step.eks_auth import acquire as eks_acquire, api_client as eks_api_client
from remote_step.errors import (
    ConfigError,
    RemoteStepError,
    RunnerError,
    SizingError,
)
from remote_step.manifest import read as read_manifest
from remote_step.payload import DriverContext, build_and_upload
from remote_step.poll import wait as poll_wait
from remote_step.submit import (
    StepResources,
    format_resources,
    resolve,
    submit as k8s_submit,
)


DEFAULT_DRIVER_CPU = 2
DEFAULT_DRIVER_MEMORY_MB = 8192
# The driver only holds a poll loop, so it never needs the step's scratch space.
DEFAULT_DRIVER_DISK_MB = 10240
DEFAULT_GITHUB_SECRET_SOURCE = "outerbounds.remote-step-github"
CACHED_ENV_FILENAME = ".remote_step_env.json"
# `--tag ds.domain:<team>` can stand in for team= on the decorator, since
# flows already label their owning domain this way.
TEAM_TAG_PREFIX = "ds.domain:"
# Namespace used when neither the decorator nor a tag names a team. Every
# scheduled flow tags its domain, so an untagged run is ad-hoc by definition
# and belongs on a small shared quota rather than being refused. Named
# `sandbox` rather than `default` so a pod that lands here by accident reads
# as obviously misplaced.
FALLBACK_TEAM = "sandbox"
# The Metaflow mflog sidecar uploads task stdout to the datastore on a
# sigmoid schedule that slows to a ~30 s cadence for long-running steps.
# The Outerbounds UI reads the task's stdout from that upload, so users
# see the driver's log tail lag by that much. We force a save_logs call
# every ``MFLOG_FORCE_UPLOAD_INTERVAL_SEC`` seconds so the UI is never
# behind by more than that regardless of the sidecar's own cadence.
MFLOG_FORCE_UPLOAD_INTERVAL_SEC = 3.0


class _MflogPusher:
    """Force `metaflow.mflog.save_logs` to run every N s from the driver.

    Metaflow's built-in ``save_logs_periodically`` sidecar backs off to a
    ~30 s cadence for long-running tasks (a sigmoid on task age). The
    Outerbounds UI reads the driver's stdout from those uploads, so at
    the sidecar's slow end users only see fresh log output tens of
    seconds after the container wrote it. Running the save_logs subprocess
    ourselves on a tight cadence keeps the UI within a few seconds of the
    stream regardless of the sidecar's backoff.

    Only starts if the mflog env vars are set — i.e. we're running inside
    a Metaflow task pod that has a stdout capture file. Locally the vars
    are absent and the pusher is a no-op.
    """

    def __init__(self, interval: float = MFLOG_FORCE_UPLOAD_INTERVAL_SEC) -> None:
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not os.environ.get("MFLOG_STDOUT"):
            return
        self._thread = threading.Thread(target=self._run, name="remote-step-mflog-pusher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        # Small initial delay so the very first stdout writes are buffered
        # into the mflog file before we ask for an upload.
        if self._stop.wait(1.0):
            return
        while not self._stop.is_set():
            try:
                subprocess.run(
                    ["python", "-m", "metaflow.mflog.save_logs"],
                    check=False,
                    capture_output=True,
                    timeout=15,
                )
            except (subprocess.SubprocessError, OSError):
                pass
            if self._stop.wait(self._interval):
                return


def _cached_env_path() -> str | None:
    """Absolute path to the cached env JSON, next to the flow module.

    Used by the *writer* on the user's laptop at argo-workflows-create
    time — the flow module is a real file next to the project's
    ``uv.lock``/``pyproject.toml``, so this always resolves.
    """
    import __main__

    flow_file = getattr(__main__, "__file__", None)
    if not flow_file:
        return None
    return os.path.join(os.path.dirname(os.path.abspath(flow_file)), CACHED_ENV_FILENAME)


def _cached_env_read_candidates() -> list[str]:
    """Directories to probe for the cached env JSON at read time.

    Metaflow's packager routes ``add_to_package``'s CODE_CONTENT files
    under ``.mf_code/``, but user-code walked from the flow directory
    (subject to ``--package-suffixes``) lands at the archive root. Which
    of the two paths the file ends up on depends on the exact CLI flags
    passed at ``argo-workflows create``, so at read time we probe every
    candidate location instead of relying on a single "correct" one.
    """
    import __main__

    dirs: list[str] = []

    def _add(d: str | None) -> None:
        if d and d not in dirs:
            dirs.append(d)

    flow_file = getattr(__main__, "__file__", None)
    if flow_file:
        flow_dir = os.path.dirname(os.path.abspath(flow_file))
        _add(flow_dir)
        _add(os.path.join(flow_dir, ".mf_code"))

    # On an Argo pod Metaflow's bootstrap sets METAFLOW_EXTRACTED_ROOT to
    # the directory it extracted the code package into. The .mf_code
    # sub-directory holds everything added via ``add_to_package``.
    mf_root = os.environ.get("METAFLOW_EXTRACTED_ROOT")
    if mf_root:
        _add(mf_root)
        _add(os.path.join(mf_root, ".mf_code"))

    cwd = os.getcwd()
    _add(cwd)
    _add(os.path.join(cwd, ".mf_code"))
    return dirs


def _write_cached_env(env_spec: dict) -> None:
    """Write env_spec to `<flow_dir>/.remote_step_env.json`."""
    import json

    path = _cached_env_path()
    if not path:
        return
    try:
        with open(path, "w") as f:
            json.dump(env_spec, f)
    except Exception:  # noqa: BLE001
        pass


def _read_cached_env() -> dict | None:
    """Read env_spec from the JSON file if present.

    Probes every candidate location in ``_cached_env_read_candidates()``
    and returns the first one whose parsed body has non-empty
    ``packages``. Silently skips unreadable / empty entries so a stale
    file next to the flow doesn't shadow a fresh one in ``.mf_code/``.
    """
    import json

    for d in _cached_env_read_candidates():
        path = os.path.join(d, CACHED_ENV_FILENAME)
        if not os.path.isfile(path):
            continue
        try:
            with open(path) as f:
                body = json.load(f)
        except Exception:  # noqa: BLE001
            continue
        if isinstance(body, dict) and body.get("packages"):
            return body
    return None


def _is_argo_context() -> bool:
    """Whether this process is deploying to, or running under, Argo.

    Signals, first-hit wins:
      1. `argo-workflows` in sys.argv — deploy time, on the operator's machine
      2. `ARGO_WORKFLOW_NAME` — set on every Argo task pod by
         argo_workflows.py. This is the signal that identifies a *running*
         Argo task, and it is deliberately not `METAFLOW_KUBERNETES_WORKLOAD`:
         that one is also set on a pod launched by Metaflow's plain kubernetes
         launcher, so using it would make `run --with kubernetes` submit to
         EKS from inside the Outerbounds pod, which is not what it asks for.
      3/4. `METAFLOW_ARGO_WORKFLOWS` / `ARGO_TEMPLATE` — neither is set by
         Metaflow itself, but Argo's executor sets ARGO_TEMPLATE in some
         versions. Kept because a false negative here means production
         silently running the step body in the wrong place.
    """
    if any("argo-workflows" in arg for arg in sys.argv):
        return True
    if os.environ.get("ARGO_WORKFLOW_NAME"):
        return True
    if os.environ.get("METAFLOW_ARGO_WORKFLOWS"):
        return True
    if os.environ.get("ARGO_TEMPLATE"):
        return True
    return False


def _is_k8s_task_runtime() -> bool:
    """Whether this process *is* the task, already executing inside a pod.

    Used only to decide whether injecting @kubernetes would record useful pod
    metadata — METAFLOW_KUBERNETES_WORKLOAD is the same flag
    KubernetesDecorator.task_pre_step gates on. It deliberately says nothing
    about where a step body runs; see the note in _is_argo_context().
    """
    return bool(os.environ.get("METAFLOW_KUBERNETES_WORKLOAD"))


def _cli_option_values(name: str) -> list[str]:
    """Every value given for a repeatable CLI option, from sys.argv.

    Handles both `--name value` and `--name=value`.
    """
    argv = sys.argv
    out: list[str] = []
    flag, eq = f"--{name}", f"--{name}="
    for i, arg in enumerate(argv):
        if arg == flag and i + 1 < len(argv):
            out.append(argv[i + 1])
        elif arg.startswith(eq):
            out.append(arg.split("=", 1)[1])
    return out


def _team_from_tags() -> str | None:
    """Team named by a `--tag ds.domain:<team>` on the command line.

    Flows already label their owning domain that way, so it can stand in for
    team= rather than repeating the same string on every step.

    Read from sys.argv rather than `metaflow.current.tags`, for two reasons.
    The requirement is validated at flow init, and `current` is only populated
    once a task is running — by which point failing is far less useful. And
    argv is the one source that agrees between a laptop and an Argo pod:
    argo_workflows.py re-emits every run tag into each step's baked command,
    the same way it re-emits `--with`.

    Raises when two such tags name different teams; silently picking one would
    put a step in a namespace nobody asked for.
    """
    found: list[str] = []
    for val in _cli_option_values("tag"):
        if val.startswith(TEAM_TAG_PREFIX):
            team = val[len(TEAM_TAG_PREFIX) :].strip()
            if team and team not in found:
                found.append(team)
    if not found:
        return None
    if len(found) > 1:
        raise SizingError(
            f"more than one {TEAM_TAG_PREFIX} tag, naming different teams "
            f"({', '.join(found)}). Pass team= on the decorator to say which "
            f"one this step belongs to."
        )
    return found[0]


def _attached_via_with() -> bool:
    """Whether @remote_step was applied by `--with`, not written in source.

    Metaflow's `--with` attaches a decorator to *every* step, so `start` and
    `end` receive it too. Written by hand on those steps that is a mistake
    worth failing on — they run in the Metaflow scheduler and there is nothing
    to offload. Attached in bulk it is simply the two steps a sweep cannot
    cover, and refusing would make `--with remote_step:team=...` unusable,
    which is the whole point of the flag.

    Read from sys.argv because that is what Metaflow itself propagates:
    `--with` travels in `top_level_options`, so the same flag appears on the
    command built for a remote step and the answer is the same on a laptop and
    inside a pod. Metaflow exposes no public API for the parsed decospecs.
    """
    # A decospec is `name` or `name:k=v,k2=v2`.
    return any(spec.split(":", 1)[0].strip() == "remote_step" for spec in _cli_option_values("with"))


def _outerbounds_config() -> dict:
    """The Metaflow config Outerbounds serves, or {} if unavailable.

    `~/.metaflowconfig/config.json` holds only a pointer —
    OBP_METAFLOW_CONFIG_URL plus an auth key — and the real settings are
    fetched from that URL. init_config() performs and caches that fetch, so it
    is the only way to see keys like OBP_INTEGRATIONS_URL from a laptop, where
    they never appear in the environment.

    Returns {} rather than raising: a caller with no Outerbounds config should
    degrade to whatever the environment provides.
    """
    try:
        from metaflow_extensions.outerbounds.remote_config import init_config

        conf = init_config()
        return conf if isinstance(conf, dict) else {}
    except Exception:  # noqa: BLE001 - no OB extension, or nothing to fetch
        return {}


# Metaflow's own @kubernetes(disk=...) default, in MB. A bare @kubernetes
# carries it whether the user thought about disk or not, so it must not be
# read as a deliberate ask — otherwise adding @kubernetes(cpu=8) would shrink
# the pod's scratch space from our 40 GB default to 10.
_K8S_DEFAULT_DISK_MB = 10240


def _find_resources(decorators) -> tuple[int, int, int, int, int]:
    """cpu, memory (MB), gpu, disk (MB), shared_memory (MB) from siblings.

    Both @resources and @kubernetes are read and the max of each dimension
    wins, which is how Metaflow reconciles them itself. Reading only
    @resources silently under-provisions a step that stated its ask on
    @kubernetes — the fallback below would hand a step declaring
    `@kubernetes(cpu=3, memory=29000)` a 1 vCPU / 4 GB pod, and the resulting
    OOM is indistinguishable from one in the step's own code.

    `disk` and `shared_memory` used to be dropped here, which was the same
    bug one layer down. `@kubernetes(disk=200000)` on a step unpacking a
    120 GB dataset produced a pod with a 40 GiB ephemeral-storage *limit*, so
    the kubelet evicted it partway through — and an eviction reads as node
    loss, so it looked like an infrastructure blip rather than a sizing
    mistake. `shared_memory` was silently unsupported, leaving /dev/shm at
    the container default of 64 MB, which is what makes a torch DataLoader
    with workers die on a bus error.
    """
    found: dict[str, int] = {}
    for d in decorators:
        if getattr(d, "name", "") not in ("resources", "kubernetes"):
            continue
        attrs = getattr(d, "attributes", {}) or {}
        for key in ("cpu", "memory", "gpu", "disk", "shared_memory"):
            raw = attrs.get(key)
            if raw in (None, ""):
                continue
            try:
                # float() first: @kubernetes accepts "2" and 2.0 as well as 2.
                val = int(float(raw))
            except (TypeError, ValueError):
                continue
            found[key] = max(found.get(key, 0), val)
    return (
        found.get("cpu", 1),
        found.get("memory", 4000),
        found.get("gpu", 0),
        found.get("disk", 0),
        found.get("shared_memory", 0),
    )


def _find_pypi_env(flow, decorators) -> dict:
    """Merge @pypi_base (on the flow class) and @pypi (on the step).

    Metaflow stores flow-level decorators in `_flow_decorators` — the shape
    varies across versions (list vs dict). We probe defensively.
    """
    base_python = "3.12"
    base_packages: dict[str, str] = {}

    def iter_flow_decos(f):
        raw = getattr(type(f), "_flow_decorators", None)
        if raw is None:
            return
        raw2 = getattr(f, "_flow_decorators", raw)
        if isinstance(raw2, dict):
            for v in raw2.values():
                if isinstance(v, list):
                    yield from v
                else:
                    yield v
            return
        try:
            yield from raw2
        except TypeError:
            return

    for d in iter_flow_decos(flow):
        if getattr(d, "name", "") == "pypi_base":
            attrs = getattr(d, "attributes", {}) or {}
            base_python = attrs.get("python") or base_python
            base_packages.update(attrs.get("packages") or {})

    step_python: str | None = None
    step_packages: dict[str, str] = {}
    for d in decorators:
        if getattr(d, "name", "") == "pypi":
            attrs = getattr(d, "attributes", {}) or {}
            step_python = attrs.get("python") or step_python
            step_packages.update(attrs.get("packages") or {})

    merged = {**base_packages, **step_packages}
    return {"python": step_python or base_python, "packages": merged}


# Distributions that provide the `metaflow` module. Outerbounds ships a fork
# under a different distribution name, so checking for "metaflow" alone would
# miss it and pin a second, conflicting copy.
_METAFLOW_DISTS = ("ob-metaflow", "metaflow")


def _ensure_metaflow_in_env(env_spec: dict) -> dict:
    """Guarantee the runner can import the flow module.

    The runner imports the user's flow module to get at the step function,
    and any real Metaflow flow does `from metaflow import step, ...` at
    module scope. Without metaflow in the runner's venv that import raises,
    and a flow whose fallback defines only some names then fails at
    class-definition time with e.g.

        NameError: name 'pypi_base' is not defined

    Most flows carry metaflow transitively (ds-platform-utils -> outerbounds
    -> ob-metaflow) so their resolved package set already has it. A flow that
    declares no packages does not, which is a legitimate configuration — so
    pin the driver's own version rather than leaving it to chance. Matching
    the driver also avoids the two sides disagreeing about artifact formats.
    """
    packages = dict(env_spec.get("packages") or {})
    if any(d in packages for d in _METAFLOW_DISTS):
        return env_spec

    # Installed-distribution metadata first: correct when the driver runs in
    # an environment where metaflow was pip-installed.
    import importlib.metadata as _md

    for dist in _METAFLOW_DISTS:
        try:
            packages[dist] = _md.version(dist)
            return {**env_spec, "packages": packages}
        except Exception:  # noqa: BLE001
            continue

    # On an Argo pod metaflow is not a distribution at all — it is shipped
    # as CODE under /metaflow/.mf_code/metaflow/, so importlib.metadata sees
    # nothing and the loop above finds no version. Read the module instead.
    try:
        import metaflow as _mf

        version = str(getattr(_mf, "__version__", "") or "").split("+")[0].strip()
    except Exception:  # noqa: BLE001
        return env_spec
    if not version:
        return env_spec

    # Pick the right distribution name. Outerbounds publishes its fork as
    # `ob-metaflow`; pinning plain `metaflow` next to the fork's extensions
    # installs an incompatible core, so getting this wrong is worse than
    # doing nothing.
    #
    # `metaflow.__version__` is the bare number on both, so it cannot
    # discriminate. `metaflow_version.get_version()` carries the decorated
    # string the CLI banner prints —
    #   2.19.37.2+obcheckpoint(0.2.10);<unk>(<unk>);ob(v1)
    # — which names the fork explicitly.
    decorated = ""
    try:
        from metaflow.metaflow_version import get_version as _get_version

        decorated = str(_get_version() or "")
    except Exception:  # noqa: BLE001
        pass

    is_ob = "ob(" in decorated or "obcheckpoint" in decorated
    if not is_ob:
        # Second signal: the fork ships this extension module.
        try:
            import metaflow_extensions.obcheckpoint  # noqa: F401

            is_ob = True
        except Exception:  # noqa: BLE001
            pass

    dist = "ob-metaflow" if is_ob else "metaflow"
    packages[dist] = version
    return {**env_spec, "packages": packages}


def _retarget_kubernetes(decorators) -> list[dict]:
    """Resize any sibling @kubernetes to driver scale, IN PLACE.

    Only called when the step body is going to EKS, where a @kubernetes
    decorator would otherwise size the *driver* pod to the step's full ask.
    Returns a snapshot of what each one asked for, so the caller can report
    the parts that do not carry over -- Outerbounds-specific ones like
    compute_pool vanishing in silence makes the pod look mysteriously
    misplaced.

    IN PLACE, and never removed, because Metaflow hands `step_init` the very
    list it is iterating:

        for deco in step.decorators:
            deco.step_init(flow, graph, step.__name__, step.decorators, ...)

    Removing an element at a lower index shifts the list left under that
    iterator, so it skips the element that slides into the vacated slot --
    the decorator written directly above @remote_step. For

        @card
        @remote_step(team="content")
        @kubernetes(cpu=3, memory=29000)
        @step

    the list is [kubernetes, remote_step, card]; dropping kubernetes and
    appending a driver-sized one made the iterator visit
    [kubernetes, remote_step, kubernetes-driver] and `card.step_init` never
    ran at all, so the card was silently never registered. Which decorator
    got skipped depended on how many were written above @remote_step, which
    is no way to run anything.

    Mutating attributes keeps every position stable, so every sibling still
    gets its step_init. Metaflow's own KubernetesDecorator.step_init only
    records internal state and validates -- nothing it derives depends on
    cpu/memory -- and the sizing is read from `attributes` at task-render
    time, so overwriting them afterwards is safe. It also means Metaflow
    imputes the image itself, rather than us having to.
    """
    snapshots: list[dict] = []
    for d in decorators:
        if getattr(d, "name", "") != "kubernetes":
            continue
        attrs = getattr(d, "attributes", None)
        if attrs is None:
            continue
        snapshots.append(dict(attrs))
        # Size only. Everything that says *where* the driver runs --
        # compute_pool, node_selector, namespace, tolerations, image -- is
        # already on this decorator and is deliberately left alone.
        attrs["cpu"] = DEFAULT_DRIVER_CPU
        attrs["memory"] = DEFAULT_DRIVER_MEMORY_MB
        attrs["gpu"] = 0
        if attrs.get("disk"):
            attrs["disk"] = DEFAULT_DRIVER_DISK_MB
    return snapshots


def _shrink_resources(decorators) -> None:
    """Overwrite sibling @resources with driver-sized values.

    Metaflow reconciles @resources with @kubernetes at task-render time and
    picks the max of each dimension. If we leave the user's ask on
    @resources (say cpu=20, memory=65000), Metaflow builds a Large pod for
    the driver — same OBC tier the flow already had. Rewriting @resources
    with (cpu=1, memory=2000, gpu=0) keeps the driver at Small tier. The
    original ask was already captured on `self._resources` for the pod.
    """
    for d in decorators:
        if getattr(d, "name", "") == "resources":
            attrs = getattr(d, "attributes", None)
            if attrs is None:
                continue
            attrs["cpu"] = DEFAULT_DRIVER_CPU
            attrs["memory"] = DEFAULT_DRIVER_MEMORY_MB
            attrs["gpu"] = 0
            return


# Attributes carried from a user-supplied @kubernetes onto the driver's own.
# All of them place the pod; none of them size it. compute_pool names an
# Outerbounds pool, and the driver *does* run on Outerbounds, so honouring it
# is right — the step body is what runs elsewhere. Outerbounds derives
# node_selector from compute_pool, so the two travel together.
DRIVER_PLACEMENT_ATTRS = (
    "compute_pool",
    "node_selector",
    "namespace",
    "tolerations",
    # The image says what the driver runs in, not how big it is, so it carries
    # over like the rest. Dropping it left the driver's @kubernetes with
    # image=None, and under --environment=fast-bakery that is a second bake
    # request with no base image, which the bakery rejects:
    #   Bake [#02] failed: Server error:
    "image",
    "image_pull_policy",
    "image_pull_secrets",
)


# Subcommands that neither run a task nor render a template. They act on a
# deployment that already exists, so the step's own configuration is not
# consulted and need not resolve. `argo-workflows create` is deliberately
# absent: it renders the template and does need everything.
DISPATCH_ONLY_COMMANDS = frozenset(
    {
        "trigger",
        "delete",
        "terminate",
        "suspend",
        "unsuspend",
        "status",
        "list-runs",
        "logs",
        "card",
        "dump",
        "tag",
    }
)


def _is_dispatch_only_command() -> bool:
    """Whether this invocation only acts on an existing deployment."""
    argv = sys.argv[1:]
    # Read the first bare word: options and their values are skipped, so
    # `--with remote_step ... argo-workflows trigger` still resolves.
    for i, arg in enumerate(argv):
        if arg.startswith("-"):
            continue
        # A value belonging to the preceding option is not a subcommand.
        if i and argv[i - 1].startswith("--") and "=" not in argv[i - 1]:
            continue
        if arg in ("argo-workflows", "step-functions", "argo-workflows-legacy"):
            continue
        return arg in DISPATCH_ONLY_COMMANDS
    return False


def _default_kubernetes_image() -> str:
    """The image Metaflow would impute for a @kubernetes without one.

    Mirrors KubernetesDecorator.step_init: the configured container image if
    there is one, else a vanilla Python image matching the interpreter, with
    the configured registry prefixed when the name carries none.
    """
    import platform

    try:
        from metaflow.metaflow_config import (
            KUBERNETES_CONTAINER_IMAGE,
            KUBERNETES_CONTAINER_REGISTRY,
        )
        from metaflow.plugins.kubernetes.kubernetes_decorator import get_docker_registry
    except ImportError:  # pragma: no cover
        major, minor = platform.python_version_tuple()[:2]
        return f"python:{major}.{minor}"

    image = KUBERNETES_CONTAINER_IMAGE
    if not image:
        major, minor = platform.python_version_tuple()[:2]
        image = f"python:{major}.{minor}"
    if not get_docker_registry(image) and KUBERNETES_CONTAINER_REGISTRY:
        image = f"{KUBERNETES_CONTAINER_REGISTRY.rstrip('/')}/{image}"
    return image


def _inject_driver_kubernetes(decorators, dropped: list[dict] | None = None) -> None:
    """Give the driver a small @kubernetes so its pod is Small tier.

    Three triggers, each for a different reason:

      _is_argo_context()      deploy time — sizes the Argo template's pod
      _is_k8s_task_runtime()  task time — runs KubernetesDecorator's
                              task_pre_step, which records
                              kubernetes-pod-name / -pod-id / -node-ip as
                              task metadata. Outerbounds joins its per-task
                              CPU/memory panel to cluster metrics through
                              those keys, so without this a driver task
                              reports no resource usage at all.
      dropped                 legacy path, kept for callers that remove a
                              @kubernetes themselves. step_init no longer
                              does: _retarget_kubernetes resizes the sibling
                              in place instead, because removing it from the
                              list Metaflow is iterating skipped whichever
                              decorator was written above @remote_step.

    `dropped` also supplies placement: the driver inherits where to run but
    never how big to be. A step asking for a 29 GB pool gets its driver on
    that pool at driver size, not a 29 GB pod holding a poll loop.
    """
    if not (_is_argo_context() or _is_k8s_task_runtime() or dropped):
        return
    try:
        from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator
    except ImportError:  # pragma: no cover
        return
    for d in decorators:
        if getattr(d, "name", "") == "kubernetes":
            return
    # Merge with the class defaults so Metaflow's own attr checks pass.
    # Some defaults are None but Metaflow's step_init dereferences them —
    # force sensible fallbacks for the ones we've hit in practice.
    attrs = {**KubernetesDecorator.defaults}
    # Placement first, so the size overrides below always win.
    for removed in dropped or []:
        for key in DRIVER_PLACEMENT_ATTRS:
            val = removed.get(key)
            if val not in (None, "", {}, []):
                attrs[key] = val
    attrs["cpu"] = DEFAULT_DRIVER_CPU
    attrs["memory"] = DEFAULT_DRIVER_MEMORY_MB
    attrs["gpu"] = 0
    if attrs.get("gpu_vendor") is None:
        attrs["gpu_vendor"] = "nvidia"
    if attrs.get("disk") is None:
        attrs["disk"] = 10240
    # KubernetesDecorator imputes a missing image in its own step_init, which
    # never runs for a decorator appended from inside another step_init. So we
    # impute it here the same way it does, leaving nothing at None.
    if not attrs.get("image"):
        attrs["image"] = _default_kubernetes_image()
    driver_deco = KubernetesDecorator(attributes=attrs)
    decorators.append(driver_deco)


def _inject_secrets(decorators, source_name: str) -> None:
    """Add @secrets(sources=[source_name]) to the step's decorator list."""
    try:
        from metaflow.plugins.secrets.secrets_decorator import SecretsDecorator
    except ImportError:  # pragma: no cover
        return
    for d in decorators:
        if getattr(d, "name", "") == "secrets":
            attrs = getattr(d, "attributes", {}) or {}
            sources = attrs.get("sources") or []
            if source_name in sources:
                return
            attrs["sources"] = list(sources) + [source_name]
            return
    decorators.append(SecretsDecorator(attributes={"sources": [source_name]}))


class RemoteStepDecorator(StepDecorator):
    """`@remote_step` — offload one step's compute to Kubernetes.

    Kwargs:
        team: namespace to run in. Selects the team's Kueue ClusterQueue and
            therefore its quota. Optional: `--tag ds.domain:<team>` on the run
            supplies it for every step, and with neither the step lands in the
            `sandbox` namespace on its small shared quota. Name the team for
            anything scheduled — sandbox quota is not sized for production.
        cpu_arch: 'arm64' (default) | 'x86_64'. arm64 is Graviton — cheaper
            and usually faster for ML CPU kernels. A step asking for a GPU
            falls back to x86_64 on its own, since the GPU NodePool is amd64
            only; set 'x86_64' explicitly for a dependency with no arm64 wheel.
        priority: 'low' | 'normal' (default) | 'high' — WorkloadPriorityClass
            used for preemption within the team's own queue.
        ephemeral_gb: pod scratch space; raise it if the step unpacks large
            wheels or writes big temp files.
        pending_timeout_minutes: how long to wait for Kueue admission plus
            Karpenter provisioning before giving up.
    """

    def add_to_package(self):
        """Bundle uv.lock + pyproject.toml alongside the flow file.

        @uv_pypi_base reads these at class-load time to derive `packages`
        for the flow. When Metaflow re-imports the flow module inside the
        Argo driver pod, these files must be present at (or above) the
        flow's directory — otherwise `packages` comes back empty and the
        runner pod can't install project deps.

        Metaflow's V1 packager wants (path, arcname) tuples where arcname
        is what goes on the wire. We yield each wanted file with a bare
        arcname (e.g. "uv.lock") so it lands next to the flow module in
        `.mf_code/`.
        """
        try:
            flow_file = self._flow_file_path()
        except Exception:  # noqa: BLE001
            return
        if not flow_file:
            return
        start = os.path.dirname(os.path.abspath(flow_file))
        wanted = ("uv.lock", "pyproject.toml", ".python-version", CACHED_ENV_FILENAME)
        seen: set[str] = set()
        cur = start
        for _ in range(6):
            for name in wanted:
                if name in seen:
                    continue
                p = os.path.join(cur, name)
                if os.path.isfile(p):
                    seen.add(name)
                    yield p, name
            parent = os.path.dirname(cur)
            if parent == cur:
                break
            cur = parent

    def _effective_cpu_arch(self, gpu: int, step_name: str) -> str:
        """The architecture to run on, after a GPU ask overrides the default.

        arm64 is the default because it is cheaper and usually faster, but the
        gpu NodePool is amd64 only. A step that asks for a GPU therefore falls
        back to x86_64 rather than failing on a default it never chose.

        An *explicit* `cpu_arch="arm64"` with a GPU is left to fail: that is a
        request for something that cannot exist, and silently ignoring it would
        hide the mistake.
        """
        arch = self.attributes["cpu_arch"]
        if not gpu or arch != "arm64":
            return arch
        if "cpu_arch" in (getattr(self, "_user_defined_attributes", None) or set()):
            return arch  # resolve() raises, with the message it already has
        sys.stderr.write(
            f"[remote_step] {step_name}: gpu={gpu} requested, so running on x86_64 — the GPU NodePool is amd64 only.\n"
        )
        return "x86_64"

    def _flow_file_path(self) -> str | None:
        """Best-effort location of the flow's file for add_to_package."""
        import __main__

        return getattr(__main__, "__file__", None)

    name = "remote_step"
    defaults = {
        # Kubernetes namespace == team. Resolved at step_init from this, then
        # `--tag ds.domain:<team>`, then FALLBACK_TEAM.
        "team": None,
        "ttl_hours": 24,
        # Outerbounds custom-secret carrying GITHUB_TOKEN for cloning
        # private git dependencies inside the runner pod. Set to None to
        # skip if the driver env already has GITHUB_TOKEN some other way.
        "github_secret_source": DEFAULT_GITHUB_SECRET_SOURCE,
        "job_timeout_minutes": 240,
        # Covers Kueue admission + Karpenter node provisioning. A cold GPU
        # node pulling a large image is the slow case.
        "pending_timeout_minutes": 20,
        # Kueue WorkloadPriorityClass. Only affects preemption inside the
        # team's own ClusterQueue, not across teams.
        "priority": "normal",
        # Pod scratch space (ephemeral-storage request and limit).
        "ephemeral_gb": 40,
        # CPU architecture. "arm64" (default) or "x86_64".
        #
        # arm64 lands on the Graviton NodePool (c9g/m9g/r9g/x8g) and picks the
        # arm64 variant of the multi-arch runner image — ~20% cheaper and often
        # faster for ML CPU kernels, which is why it is the default.
        #
        # A GPU ask overrides it: the gpu NodePool is amd64 only, so a step
        # requesting a GPU falls back to x86_64 automatically. Setting
        # cpu_arch="arm64" *explicitly* alongside a GPU is still an error, since
        # that asks for something impossible rather than leaving it to us.
        #
        # Set "x86_64" for a dependency with no arm64 wheel.
        "cpu_arch": "arm64",
    }

    _resources: StepResources
    _env_spec: dict
    _config: RemoteStepConfig

    def step_init(
        self,
        flow,
        graph,
        step_name,
        decorators,
        environment,
        flow_datastore,
        logger,
    ):
        """Runs once at flow init. Fails fast on refusals."""
        if _is_dispatch_only_command():
            # Nothing about the step is decided here, so nothing about it needs
            # to be resolvable. Notably `argo-workflows trigger` takes no
            # --tag, so a team that comes from one cannot be supplied to it --
            # and demanding one made a tag-derived team unusable with Argo.
            self._submit = False
            return
        if step_name in ("start", "end"):
            if _attached_via_with():
                # `--with remote_step:team=...` sweeps every step. Skip these
                # two quietly rather than failing the whole flow: they run in
                # the Metaflow scheduler and have nothing to offload.
                self._submit = False
                return
            raise SizingError(
                f"@remote_step on '{step_name}' — heavy compute must move to a "
                f"downstream step; start/end run in the Metaflow scheduler.\n"
                f"  To apply it to every other step at once, use "
                f"`--with remote_step:team=<team>` instead of decorating "
                f"by hand.",
                step_name=step_name,
            )
        for d in decorators:
            if getattr(d, "name", "") == "batch":
                raise SizingError(
                    f"@remote_step conflicts with @batch on step '{step_name}'. "
                    f"Pick one — @batch runs the whole Metaflow task on Batch; "
                    f"@remote_step keeps the driver on Outerbounds.",
                    step_name=step_name,
                )
            if getattr(d, "name", "") == "parallel":
                raise SizingError(
                    f"@remote_step + @parallel not yet supported (step '{step_name}').",
                    step_name=step_name,
                )
            if _declares_conda_packages(d):
                # The runner builds its venv from @pypi/@pypi_base packages
                # only. Accepting @conda would run the step in an environment
                # that quietly lacks its conda dependencies, so refuse and say
                # what to do instead.
                raise SizingError(
                    f"@remote_step cannot honour @conda on step '{step_name}' — "
                    f"the runner builds its environment from @pypi / @pypi_base "
                    f"(or @uv_pypi_base) only.\n"
                    f"  Declare the step's packages there instead, or drop "
                    f"@remote_step from this step so it runs where conda is set up.",
                    step_name=step_name,
                )
        # team= wins when given, so a single step can override the run's tag.
        team = self.attributes.get("team") or _team_from_tags()
        if not team:
            # Nothing named a team, so this is not a production run: every
            # scheduled flow carries `--tag ds.domain:<team>`. Fall back to the
            # sandbox namespace and its small quota rather than refusing to
            # run, which is what ad-hoc and exploratory work wants.
            #
            # Said out loud, on stderr, because the consequence is real: the
            # step spends sandbox quota and its pod lands in the sandbox
            # namespace, so a *production* flow that lost its tag runs in the
            # wrong place instead of failing.
            team = FALLBACK_TEAM
            sys.stderr.write(
                f"[remote_step] {step_name}: no team given, using "
                f"'{FALLBACK_TEAM}'. For a team's own quota, add "
                f"--tag {TEAM_TAG_PREFIX}<team> to the run or "
                f'team="<team>" to the decorator.\n'
            )
        self._team = team
        # Read here because task_decorate is not given the decorator list.
        self._env_vars = _find_env_vars(decorators)
        self._user_timeout_minutes = _find_timeout_minutes(decorators)
        self._card_attributes = _find_card_attributes(decorators)
        self._gpu_profile = _find_gpu_profile(decorators)
        if self._gpu_profile:
            # Read before dropping. The driver cannot see a GPU, and its empty
            # reading would overwrite the runner's.
            _drop_gpu_profile(decorators)
        self._model_loads = _find_model_loads(decorators)
        if self._model_loads:
            # Read before dropping, since dropping removes the attributes.
            _drop_model(decorators)
        self._hf_loads = _find_hf_loads(decorators)
        if self._hf_loads:
            _drop_hf_hub(decorators)
        cpu, memory_mb, gpu, disk_mb, shm_mb = _find_resources(decorators)
        cpu_arch = self._effective_cpu_arch(gpu, step_name)
        # An explicit ephemeral_gb on @remote_step is the most specific
        # statement and wins outright. Otherwise a deliberate
        # @kubernetes(disk=...) raises it -- never lowers it, since a bare
        # @kubernetes carries Metaflow's 10 GB default whether the user
        # thought about disk or not.
        ephemeral_gb = int(self.attributes["ephemeral_gb"])
        if "ephemeral_gb" not in getattr(self, "_user_defined_attributes", set()):
            if disk_mb and disk_mb != _K8S_DEFAULT_DISK_MB:
                ephemeral_gb = max(ephemeral_gb, -(-disk_mb // 1024))
        try:
            self._resources = resolve(
                cpu,
                memory_mb,
                gpu,
                cpu_arch=cpu_arch,
                ephemeral_gb=ephemeral_gb,
                shm_mb=shm_mb,
            )
        except SizingError:
            raise
        # Read @pypi_base/@pypi packages. Metaflow blanks these at task-run
        # time (env already baked into the argo pod image), so we cache the
        # resolved env to a JSON file alongside the flow module and ship it
        # via add_to_package — driver reads it back on the argo pod.
        env_spec = _find_pypi_env(flow, decorators)
        if not env_spec["packages"]:
            cached = _read_cached_env()
            if cached:
                env_spec = cached
        else:
            _write_cached_env(env_spec)
        # Applied after the cache round-trip so the cached file keeps the
        # user's declared set verbatim and the pin is re-derived each time.
        env_spec = _ensure_metaflow_in_env(env_spec)
        self._env_spec = env_spec
        try:
            self._config = load_config()
        except ConfigError:
            raise
        # Advisory: cfg.teams is a snapshot from terraform, so this only
        # fires on a clear typo, not on a namespace added since.
        check_team(self._config, team)

        # The one decision. Everything below branches on it, and it must come
        # out the same here and inside whatever pod the task later lands in —
        # both run step_init.
        # Only the start/end sweep skip above can clear this.
        self._submit = True

        if self._submit:
            # A sibling @kubernetes would size the *driver* pod to the step's
            # full ask, so it goes; same for @resources, which Metaflow
            # reconciles with @kubernetes at task-render time. The real ask is
            # already captured on self._resources for the Job manifest.
            dropped = _retarget_kubernetes(decorators)
            if not dropped:
                # No sibling @kubernetes to resize, so the driver may still
                # need one of its own -- see _inject_driver_kubernetes.
                _inject_driver_kubernetes(decorators, None)
            _shrink_resources(decorators)
            for attrs in dropped:
                pool = attrs.get("compute_pool")
                if pool:
                    sys.stdout.write(
                        f"[remote_step] {step_name}: driver on compute_pool "
                        f"{pool!r} at {DEFAULT_DRIVER_CPU} vCPU / "
                        f"{DEFAULT_DRIVER_MEMORY_MB // 1024} GB. The step body "
                        f"runs on our EKS cluster, so the pool sizes only the "
                        f"driver.\n"
                    )
        # When not submitting, every sibling decorator is left exactly as
        # written so Metaflow does whatever it normally would — in-process for
        # a plain `run`, an Outerbounds pod if @kubernetes is present.

        # No AWS secret is injected. The driver reaches this cluster by
        # assuming ob-submitter with the Outerbounds pod's own OIDC task
        # role, so it needs no static credentials — and injecting any would
        # be actively harmful: AWS_ACCESS_KEY_ID in the environment shadows
        # the task role, and that static identity is in no trust policy, so
        # sts:AssumeRole would fail.
        #
        # GitHub is different: uv needs a token to clone private git
        # dependencies inside the runner pod, and there is no ambient
        # equivalent.
        #
        # Gated on self._submit, not on being under Argo. The runner pod is
        # what needs the token, and it exists for a local `run` just as much
        # as for an Argo one — the driver forwards GITHUB_TOKEN out of its own
        # environment, and @secrets is what puts it there. Gating on Argo left
        # a local run with no token, so any flow with a private git dependency
        # died at STAGE=uv_pip_install with
        #
        #   fatal: could not read Username for 'https://github.com'
        #
        # Skipped for a `start`/`end` sweep skip: no submission, no runner,
        # nothing to authenticate.
        if self._submit:
            gh_src = self.attributes.get("github_secret_source")
            if gh_src:
                _inject_secrets(decorators, gh_src)

            # A user's own @secrets is fetched on the driver and does not
            # reach the pod on its own -- see _resolve_secret_env. Recorded
            # here because task_decorate is not given the decorator list.
            self._secret_sources = _find_secret_sources(decorators, injected=gh_src)
            self._secret_role = None
            for _d in decorators:
                if getattr(_d, "name", "") == "secrets":
                    self._secret_role = (getattr(_d, "attributes", {}) or {}).get("role")
                    break

        # sys.stdout.write, not logger(): Metaflow's logger stamps every
        # line with "YYYY-MM-DD HH:MM:SS.mmm ", which every other
        # [remote_step] line — all written from the driver body — does not
        # carry. Using it here made flow-init output look like a different
        # subsystem from the rest.
        sys.stdout.write(f"[remote_step] {step_name} -> {team} · {format_resources(self._resources)}\n")
        # Flush explicitly. step_init runs in the CLI process, where stdout is
        # block-buffered whenever it is a pipe rather than a tty — so without
        # this the lines sit in the buffer until interpreter exit and surface
        # *after* everything Metaflow printed, including the "triggered ...
        # (run-id ...)" line. The driver body does not need this because it
        # reconfigures stdout to line buffering first.
        sys.stdout.flush()

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        """Called just before user code — capture code-package URL."""
        # Some Metaflow versions expose the code-package URL via
        # task_datastore.ca_client or via env vars. Fall back to env.
        ds_root = os.environ.get("METAFLOW_DATASTORE_SYSROOT_S3") or ""
        self._runtime_ctx = {
            "run_id": run_id,
            "task_id": task_id,
            "attempt": retry_count,
            "flow_name": type(flow).__name__,
            "datastore_root": ds_root,
        }

    def task_decorate(self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context):
        """Wrap the user's step method with the driver body.

        Metaflow calls the returned callable with `()` (or `(inputs)` for
        join steps), NOT with the flow instance. `flow` is captured in the
        closure — same pattern @catch's `fallback_step` uses.

        Returns `step_func` untouched when this step is not going to EKS, so
        the body runs wherever Metaflow would have run it anyway.
        """
        if not getattr(self, "_submit", True):
            return step_func

        cfg = self._config
        resources = self._resources
        env_spec = self._env_spec
        team = self._team
        ctx = self._runtime_ctx
        pending_timeout = self.attributes["pending_timeout_minutes"] * 60
        step_name = getattr(step_func, "__name__", "unknown_step")
        # Capture the static graph so we can replay self.next() afterwards.
        # Preserve the transition *shape* (linear / split / split-switch /
        # foreach), not just the target names, so control-flow constructs
        # like `self.next({True: X, False: Y}, condition="run_dqv")` route
        # correctly downstream.
        node_type = "linear"
        out_funcs: list[str] = []
        switch_cases: dict = {}
        condition: str | None = None
        foreach_param: str | None = None
        num_parallel: int | None = None
        try:
            node = graph.nodes[step_name]
            node_type = getattr(node, "type", "linear") or "linear"
            out_funcs = list(node.out_funcs or [])
            switch_cases = dict(getattr(node, "switch_cases", {}) or {})
            condition = getattr(node, "condition", None)
            foreach_param = getattr(node, "foreach_param", None)
            num_parallel = getattr(node, "num_parallel", None)
        except Exception:  # noqa: BLE001
            pass

        def driver(inputs=None):
            """The remote_step driver body — small enough to run at Local tier."""
            # Line-buffer stdout so mflog sees each `[remote_step] …` line as
            # it's written — otherwise the Outerbounds UI only shows them
            # after the task exits.
            try:
                sys.stdout.reconfigure(line_buffering=True)
            except Exception:  # noqa: BLE001
                pass
            mflog_pusher = _MflogPusher()
            mflog_pusher.start()
            try:
                user = os.environ.get("METAFLOW_USER") or getpass.getuser()
                self_flow = flow
                input_attrs = _collect_flow_attrs(self_flow)
                # `self.input` is a property Metaflow computes from the
                # foreach stack, so it is not in the attrs above and has to be
                # read separately. Outside a foreach it raises, which is how
                # we tell "no foreach" from "a foreach whose value is None".
                try:
                    _foreach_input = self_flow.input
                    _has_foreach_input = True
                except Exception:  # noqa: BLE001
                    _foreach_input = None
                    _has_foreach_input = False
                # `self.index` and `self.foreach_stack()` are computed from the
                # same stack and are just as absent from the attrs above. Left
                # unshipped, the pod answered `self.index` from
                # _FakeSelf.__getattr__ with a no-op callable: `f"part-{self.index}"`
                # became an address-dependent garbage string and
                # `if self.index == 0` was silently always False.
                try:
                    _foreach_index = self_flow.index
                except Exception:  # noqa: BLE001
                    _foreach_index = None
                try:
                    _foreach_stack = self_flow.foreach_stack()
                    _has_foreach_stack = _foreach_stack is not None
                except Exception:  # noqa: BLE001
                    _foreach_stack = None
                    _has_foreach_stack = False
                sys.stdout.write(f"[remote_step] captured inputs: {list(input_attrs.keys())}\n")
                # Acquire cluster access before anything touches S3.
                #
                # The Outerbounds pod's own task role has no rights on our
                # payload bucket — it is their role in their perimeter, and
                # granting it ours is not on the table. Every S3 call the
                # driver makes therefore has to use the assumed submitter
                # role, whose PayloadBucketReadWrite policy covers exactly
                # these objects. Using ambient credentials fails with
                #
                #   assumed-role/obp-...-task is not authorized to perform:
                #   s3:PutObject ... because no identity-based policy allows
                #
                # which is why this hop happens up here rather than just
                # before the Job is created.
                access = eks_acquire(
                    cluster_name=cfg.cluster_name,
                    region=cfg.region,
                    submitter_role_arn=cfg.submitter_role_arn,
                    endpoint_hint=cfg.cluster_endpoint,
                    session_name=f"rs-{ctx['run_id']}-{step_name}",
                )
                api = eks_api_client(access)
                # Pool sized for the upload path, not left at the boto
                # default of 10. artifact._transfer_config_for asks for 32
                # concurrent parts on blobs >= 2 GB, and a 10-connection
                # pool makes those 32 threads queue on each other — the same
                # starvation the read path already avoids. Adaptive retries
                # because a large multipart upload is exactly when S3 starts
                # returning SlowDown.
                from botocore.config import Config as _BotoConfig

                driver_s3 = access.session.client(
                    "s3",
                    region_name=cfg.region,
                    config=_BotoConfig(
                        max_pool_connections=64,
                        retries={"max_attempts": 10, "mode": "adaptive"},
                    ),
                )

                perimeter = keys.resolve_perimeter()
                code_url, code_sha = resolve_code_package(
                    cfg.payload_bucket,
                    ctx["run_id"],
                    ctx["flow_name"],
                    perimeter=perimeter,
                    s3_client=driver_s3,
                )
                try:
                    from metaflow import current as _current

                    _tags = list(getattr(_current, "tags", None) or [])
                    # Include system tags too (user:X, runtime:X, project_branch:X, ...)
                    # so downstream code that filters on either kind still works.
                    _tags.extend(t for t in (getattr(_current, "system_tags", None) or []) if t not in _tags)
                except Exception:  # noqa: BLE001
                    _tags = []
                driver_ctx = DriverContext(
                    flow_module=_flow_module_name(self_flow),
                    flow_class=type(self_flow).__name__,
                    step_name=step_name,
                    flow_name=ctx["flow_name"],
                    run_id=ctx["run_id"],
                    task_id=ctx["task_id"],
                    attempt=ctx["attempt"],
                    code_package_url=code_url,
                    code_package_sha=code_sha,
                    datastore_root=ctx["datastore_root"],
                    mfconfig=_named_mfconfig(),
                    tags=_tags,
                    artifact_read_role_arn=cfg.artifact_read_role_arn,
                    perimeter=perimeter,
                    project=_project_context(),
                    foreach_input=_foreach_input,
                    has_foreach_input=_has_foreach_input,
                    foreach_index=_foreach_index,
                    foreach_stack=_foreach_stack,
                    has_foreach_stack=_has_foreach_stack,
                    is_join=(node_type == "join"),
                    join_branches=_join_branches(inputs),
                    model_loads=getattr(self, "_model_loads", None),
                    hf_loads=getattr(self, "_hf_loads", None),
                    requested={
                        "cpu": resources.cpu,
                        "memory_mb": resources.memory_mb,
                        "gpus": resources.gpus,
                    },
                    gpu_profile=bool(getattr(self, "_gpu_profile", None)),
                    gpu_profile_interval=((getattr(self, "_gpu_profile", None) or {}).get("interval") or 1),
                )
                spec_uri, spec = build_and_upload(
                    driver_ctx,
                    env_spec,
                    input_attrs,
                    cfg.payload_bucket,
                    s3_client=driver_s3,
                )
                sys.stdout.write(
                    f"[remote_step] submitted spec {spec_uri}\n[remote_step] {format_resources(resources)}\n"
                )
                # Forward the runner's own environment needs.
                #
                # GITHUB_TOKEN in particular: step_init injects
                # @secrets(sources=[...github]) which populates the DRIVER's
                # environment, but the runner is a separate pod in a separate
                # cluster and inherits nothing. Without forwarding it here,
                # `uv pip install "pkg @ git+https://github.com/..."` in the
                # runner fails to authenticate and the step dies at
                # STAGE=uv_pip_install.
                runner_env: dict[str, str] = {}
                for _k in ("GITHUB_TOKEN", "GIT_TOKEN", "GH_TOKEN"):
                    _v = os.environ.get(_k)
                    if _v:
                        runner_env[_k] = _v
                        sys.stdout.write(f"[remote_step] forwarding {_k} to the runner (len={len(_v)})\n")
                        break
                # Outerbounds runtime context, so user code that talks to
                # Outerbounds integrations (Snowflake and friends) works from
                # inside the runner pod.
                #
                # Two sources, and both are needed. On an Argo pod Outerbounds
                # materialises its config as environment variables, so the
                # environment alone is enough. Run the same flow locally and
                # those values exist only in the config fetched from
                # OBP_METAFLOW_CONFIG_URL — nothing is in os.environ to
                # forward, and the runner dies with
                #
                #   OuterboundsSnowflakeConnectorException: No integrations
                #   url set.
                #
                # because OBP_INTEGRATIONS_URL never reached it. The config is
                # read first and the environment layered on top, so a pod's
                # real values always win over anything stale on disk.
                for _k, _v in _outerbounds_config().items():
                    if _k.startswith(("METAFLOW_", "OBP_", "OUTERBOUNDS_")):
                        runner_env[_k] = str(_v)
                for _k, _v in os.environ.items():
                    if _k.startswith(("METAFLOW_", "OBP_", "OUTERBOUNDS_")):
                        runner_env[_k] = _v
                # The integrations API is authenticated, and Outerbounds' own
                # client reads the header from METAFLOW_SERVICE_HEADERS but
                # the key itself from METAFLOW_SERVICE_AUTH_KEY. On a pod the
                # header is already set; locally only the key exists, so
                # synthesise the header rather than leave the runner able to
                # find the endpoint but not call it.
                # A sibling @environment declared these for the step, and the
                # step body runs here — not on the driver Metaflow set them
                # on. Applied last so an explicit @environment wins over the
                # forwarded Outerbounds context.
                # A sibling @secrets, resolved here because the pod inherits
                # nothing from the driver's environment. Before @environment
                # so an explicit @environment(vars=...) still wins.
                _secret_env = _resolve_secret_env(
                    getattr(self, "_secret_sources", None) or [],
                    getattr(self, "_secret_role", None),
                )
                if _secret_env:
                    # Names only. The values are the secret.
                    sys.stdout.write(
                        f"[remote_step] forwarding @secrets to the runner: {', '.join(sorted(_secret_env))}\n"
                    )
                    runner_env.update(_secret_env)
                runner_env.update(getattr(self, "_env_vars", None) or {})
                if "METAFLOW_SERVICE_HEADERS" not in runner_env:
                    _auth = runner_env.get("METAFLOW_SERVICE_AUTH_KEY")
                    if _auth:
                        runner_env["METAFLOW_SERVICE_HEADERS"] = json.dumps({"x-api-key": _auth})

                result = k8s_submit(
                    cfg,
                    resources,
                    spec_uri,
                    flow_name=ctx["flow_name"],
                    run_id=ctx["run_id"],
                    step_name=step_name,
                    task_id=ctx["task_id"],
                    attempt=ctx["attempt"],
                    user=user,
                    team=team,
                    perimeter=perimeter,
                    priority=self.attributes["priority"],
                    extra_env=runner_env,
                    timeout_minutes=_job_timeout_minutes(
                        getattr(self, "_user_timeout_minutes", None),
                        self.attributes["job_timeout_minutes"],
                    ),
                    client=api,
                )
                sys.stdout.write(f"[remote_step] job {result.job_name} in {result.namespace} (queue {result.queue})\n")
                outcome = poll_wait(
                    api,
                    result.namespace,
                    result.job_name,
                    pending_timeout_sec=pending_timeout,
                )
                if not outcome.succeeded:
                    # Re-raise the step's own exception when the runner
                    # managed to save one. @catch(var="e") sits on this
                    # driver task, so without this it only ever caught a
                    # RunnerError and the user's exception type was lost.
                    _reraise_step_exception(
                        cfg.payload_bucket,
                        spec["output_prefix"],
                        step_name,
                        s3_client=driver_s3,
                    )
                    detail = "\n  ".join(outcome.events) if outcome.events else ""
                    raise RunnerError(
                        f"step '{step_name}' failed: {outcome.reason}"
                        + (f" (exit {outcome.exit_code})" if outcome.exit_code is not None else "")
                        + (f"\n  {detail}" if detail else ""),
                        exit_code=outcome.exit_code,
                        job_name=outcome.job_name,
                    )
                _apply_run_tags(cfg.payload_bucket, spec["output_prefix"], s3_client=driver_s3)
                _replay_card_components(cfg.payload_bucket, spec["output_prefix"], s3_client=driver_s3)
                outputs = read_manifest(
                    cfg.payload_bucket,
                    spec["output_prefix"],
                    s3_client=driver_s3,
                )
                # Keep zero-copy semantics: assign each ref directly onto
                # ``self``. Metaflow's artifact persistence stores the
                # tiny pickle-clean ref (a few hundred bytes) on the
                # Outerbounds datastore, and downstream consumers reach
                # through the RemoteArtifact's proxy dunders — assuming
                # the cross-account read role baked into every ref — to
                # fetch the real payload from our S3 bucket only when
                # they actually touch it.
                card_attrs = getattr(self, "_card_attributes", None) or set()
                for name, ref in outputs.items():
                    if name in card_attrs:
                        # A card reading `options={"attribute": name}` renders
                        # this on the driver, so a ref would render as
                        # `RemoteArtifact(...)` instead of the content. Load
                        # just this one; everything else stays zero-copy.
                        loaded = _hydrate_for_card(name, ref)
                        setattr(self_flow, name, loaded)
                        continue
                    setattr(self_flow, name, ref)
                sys.stdout.write(f"[remote_step] {step_name} finished, {len(outputs)} artifact(s) linked\n")
                # Replay the user step's self.next(...) so Metaflow's transition
                # tracker sees the same shape it does when the step runs
                # locally — including the transition *type* (linear / split /
                # split-switch / foreach). Passing every out_func positionally
                # would silently turn a `self.next({True: X, False: Y},
                # condition="foo")` into a parallel split that always runs
                # both branches — which is exactly how ``dqv_step_input``
                # ended up running even when ``run_dqv=False``.
                if out_funcs:
                    if node_type == "split-switch" and switch_cases and condition:
                        # Before next(): Metaflow hashes the condition value
                        # against the case keys, and a RemoteArtifact is
                        # unhashable.
                        _hydrate_condition(self_flow, condition)
                        case_map = {
                            case: getattr(self_flow, fn) for case, fn in switch_cases.items() if hasattr(self_flow, fn)
                        }
                        if case_map:
                            self_flow.next(case_map, condition=condition)
                    elif node_type == "foreach" and len(out_funcs) == 1:
                        target = out_funcs[0]
                        if hasattr(self_flow, target):
                            kwargs = {}
                            if num_parallel is not None:
                                kwargs["num_parallel"] = num_parallel
                            elif foreach_param is not None:
                                kwargs["foreach"] = foreach_param
                            self_flow.next(getattr(self_flow, target), **kwargs)
                    else:
                        next_refs = [getattr(self_flow, f) for f in out_funcs if hasattr(self_flow, f)]
                        if next_refs:
                            self_flow.next(*next_refs)
            finally:
                mflog_pusher.stop()

        driver.__name__ = step_name
        driver.__wrapped__ = step_func
        return driver


def _pickleable(v) -> bool:
    """Cheap pre-flight — reject obvious non-picklable inputs."""
    import types

    if isinstance(v, (types.ModuleType, types.FunctionType, types.MethodType)):
        return False
    if type(v).__name__ == "Parameter":  # raw Parameter class-level object
        return False
    return True


class _NullSink:
    """A write-only sink that keeps nothing.

    `pickle.dumps(val)` to prove a value is picklable materialised the entire
    pickle as a bytes object and then dropped it. On the Small-tier driver --
    2 vCPU, 8 GB -- a 3 GB DataFrame therefore peaked at ~3 GB before
    build_spec pickled it a second time to actually ship it, which is a spike
    big enough to OOM the driver on an artifact it was only inspecting.

    Pickling into this instead answers the same question in constant memory.
    The CPU is still spent twice; only the allocation is avoided.
    """

    __slots__ = ()

    def write(self, _chunk) -> None:  # noqa: D105
        return None


def _is_picklable_streaming(val) -> bool:
    """Whether `val` pickles, without keeping the result."""
    import pickle as _pickle

    try:
        _pickle.dump(val, _NullSink(), protocol=5)
    except Exception:  # noqa: BLE001
        return False
    return True


_SKIP_ATTRS = frozenset(
    {
        "next",
        "input",
        "index",
        "foreach_stack",
        "checkpoint",
        "_datastore",
        "_metadata",
        "_current_step",
        "_task",
        "_flow_state",
        "_graph",
        "_transition",
        "_flow_decorators",
        "_success",
        "_flow_state",
        "logger",
        "cards",
        "_cards",
        "_current",
    }
)


# What @project injects into `metaflow.current`. Forwarded to the runner so a
# step body sees the same values it would have on the driver.
PROJECT_CONTEXT_KEYS = (
    "project_name",
    "branch_name",
    "is_production",
    "is_user_branch",
    "project_flow_name",
)


def _project_context() -> dict:
    """@project's contribution to `metaflow.current`, if the flow uses it.

    These are not built-in properties of `current` — @project adds them with
    `_update_env` — so a flow without @project simply has none of them and
    this returns {}.
    """
    try:
        from metaflow import current
    except ImportError:  # pragma: no cover
        return {}
    out: dict[str, object] = {}
    for key in PROJECT_CONTEXT_KEYS:
        try:
            val = getattr(current, key)
        except Exception:  # noqa: BLE001
            continue
        if val is not None:
            out[key] = val
    return out


# Deliberately no slack. An earlier version added 5 minutes to the user's
# @timeout, which had it exactly backwards: it let the runner pod outlive the
# driver, which is the runaway-billing problem @timeout is supposed to prevent.
# The Job deadline is the user's value, full stop.


def _declares_conda_packages(decorator) -> bool:
    """Whether this is a *user's* @conda / @conda_base, not Metaflow's own.

    The decorator name alone is not enough. `CondaEnvironment.decospecs()`
    returns ("conda",), so `--environment=pypi|conda|fast-bakery` attaches a
    `conda` decorator to *every* step to manage the task lifecycle — and
    refusing on the name refuses every flow that uses fast-bakery, which is
    all of them.

    The lifecycle one carries the defaults, `packages={}` and `libraries={}`.
    A user asking for conda dependencies fills one of those in, and that is
    the thing we cannot honour.
    """
    if getattr(decorator, "name", "") not in ("conda", "conda_base"):
        return False
    attrs = getattr(decorator, "attributes", {}) or {}
    return bool(attrs.get("packages")) or bool(attrs.get("libraries"))


def _find_model_loads(decorators) -> dict | None:
    """A sibling @model's `load` request, or None if the step has none.

    Only the *names* travel. `@model(load=["my_model"])` resolves the model
    through `getattr(flow, "my_model")` — the reference is an ordinary flow
    artifact, a small dict, which the spec already ships as an input. So the
    pod can fetch the model itself and nothing large crosses the driver.
    """
    for d in decorators:
        if getattr(d, "name", "") != "model":
            continue
        attrs = getattr(d, "attributes", {}) or {}
        load = attrs.get("load")
        if not load:
            return None
        # Normalise to [name, ...] / [[name, path], ...] so it survives JSON.
        refs: list = []
        if isinstance(load, str):
            refs = [load]
        else:
            for item in load:
                if isinstance(item, (tuple, list)) and len(item) == 2:
                    refs.append([item[0], item[1]])
                else:
                    refs.append(item)
        return {"load": refs, "temp_dir_root": attrs.get("temp_dir_root")}
    return None


def _drop_model(decorators) -> list[dict]:
    """Remove a sibling @model so the *driver* does not download the model.

    Its `task_pre_step` downloads every `load=` model onto the task running
    it. For a remote step that is the driver — a Small-tier pod with 10 GB of
    disk that will never touch the file. Dropping it and re-loading in the pod
    puts the download next to the GPU and keeps a multi-GB model off the
    driver entirely.
    """
    removed: list[dict] = []
    for d in list(decorators):
        if getattr(d, "name", "") == "model":
            removed.append(dict(getattr(d, "attributes", {}) or {}))
            decorators.remove(d)
    return removed


def _find_hf_loads(decorators) -> dict | None:
    """A sibling @huggingface_hub's `load` request, or None.

    Same reasoning as @model: the download belongs next to the step body, not
    on the driver. Entries may be a bare repo_id or a dict carrying
    snapshot_download arguments, and both survive JSON as-is.
    """
    for d in decorators:
        if getattr(d, "name", "") != "huggingface_hub":
            continue
        attrs = getattr(d, "attributes", {}) or {}
        load = attrs.get("load")
        if not load:
            return None
        refs = [load] if isinstance(load, (str, dict)) else list(load)
        return {"load": refs, "temp_dir_root": attrs.get("temp_dir_root")}
    return None


def _drop_gpu_profile(decorators) -> list[dict]:
    """Remove a classic @gpu_profile so the driver does not also profile.

    It samples wherever it runs, which for a remote step is the driver — a pod
    with no GPU, so it reports `nvidia-smi not found`.

    **This only catches a decorator literally named `gpu_profile`.** In current
    Metaflow @gpu_profile is a StepMutator that rewrites itself into a `card`
    plus a `user_step_decorator`, and that wrapper is not in this list, so it
    keeps running on the driver and still writes `gpu_profile_data` at
    task_finished. That is why the runner writes `remote_gpu_profile` instead
    of competing for the same name.
    """
    removed: list[dict] = []
    for d in list(decorators):
        if getattr(d, "name", "") == "gpu_profile":
            removed.append(dict(getattr(d, "attributes", {}) or {}))
            decorators.remove(d)
    return removed


def _drop_hf_hub(decorators) -> list[dict]:
    """Remove a sibling @huggingface_hub so the driver does not download."""
    removed: list[dict] = []
    for d in list(decorators):
        if getattr(d, "name", "") == "huggingface_hub":
            removed.append(dict(getattr(d, "attributes", {}) or {}))
            decorators.remove(d)
    return removed


# A card attribute is rendered on the driver, so it has to be a value there.
# Cap what we are willing to pull into a Small-tier driver to do it: a report
# is kilobytes, and silently loading a 10 GB DataFrame would OOM the driver.
MAX_CARD_ATTR_BYTES = 64 * 1024 * 1024


def _find_secret_sources(decorators, injected: str | None = None) -> list:
    """Sources on a sibling @secrets, excluding one we injected ourselves.

    Kept separate from resolution so step_init can record them: by the time
    the driver submits, `task_decorate` no longer has the decorator list.
    """
    out: list = []
    for d in decorators:
        if getattr(d, "name", "") != "secrets":
            continue
        attrs = getattr(d, "attributes", {}) or {}
        for src in attrs.get("sources") or []:
            if injected and isinstance(src, str) and src == injected:
                continue
            out.append(src)
    return out


def _resolve_secret_env(sources: list, role: str | None = None) -> dict[str, str]:
    """Env vars from a sibling @secrets, for forwarding to the runner pod.

    Metaflow's @secrets fetches into the *driver's* os.environ during
    task_pre_step. The runner is a separate pod in a separate cluster and
    inherits nothing, and the forwarding list carries only GITHUB_TOKEN plus
    METAFLOW_/OBP_/OUTERBOUNDS_ prefixes -- so a value exported under its own
    name, which is the whole point of @secrets, never crossed. The step body
    then died on os.environ["..."] with a KeyError naming a variable the user
    could see was configured, or worse built an unauthenticated client.

    Resolved through Metaflow's own SecretSpec and provider rather than
    guessed at from os.environ, so the keys are exactly the ones this @secrets
    defines and nothing else rides along.

    Never fatal: a step whose secret cannot be resolved here would have failed
    in the pod anyway, and Metaflow's own task_pre_step raises on the same
    source first, so this only ever reports.
    """
    if not sources:
        return {}
    try:
        from metaflow.plugins.secrets.secrets_decorator import get_secrets_backend_provider
        from metaflow.plugins.secrets.secrets_spec import SecretSpec
    except ImportError:  # pragma: no cover
        return {}
    out: dict[str, str] = {}
    for src in sources:
        try:
            if isinstance(src, dict):
                spec = SecretSpec.secret_spec_from_dict(src, role=role)
            else:
                spec = SecretSpec.secret_spec_from_str(str(src), role=role)
            provider = get_secrets_backend_provider(spec.secrets_backend_type)
            resolved = provider.get_secret_as_dict(spec.secret_id, options=spec.options, role=spec.role)
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[remote_step] could not resolve @secrets source {src!r} for the runner: {exc}\n")
            continue
        for k, v in (resolved or {}).items():
            out[str(k)] = str(v)
    return out


def _find_card_attributes(decorators) -> set[str]:
    """Flow attributes that a sibling @card renders itself.

    `@card(type="html", options={"attribute": "html"})` reads `self.html` at
    render time, on the driver. Left as a RemoteArtifact ref that card shows
    `RemoteArtifact(kind=..., uri=...)` rather than the report.
    """
    names: set[str] = set()
    for d in decorators:
        if getattr(d, "name", "") != "card":
            continue
        options = (getattr(d, "attributes", {}) or {}).get("options") or {}
        attr = options.get("attribute")
        if attr:
            names.add(str(attr))
    return names


# A switch condition is compared against the case keys with `in`, which hashes
# it. RemoteArtifact sets __hash__ to None, so a condition left as a reference
# raises `TypeError: unhashable type: 'RemoteArtifact'` from inside Metaflow's
# own next() -- naming neither the attribute nor @remote_step.
MAX_CONDITION_ATTR_BYTES = 1 * 1024 * 1024


def _hydrate_condition(flow, condition: str) -> None:
    """Load a switch condition the step produced remotely, in place.

    `self.next({True: self.dqv, False: self.skip}, condition="run_dqv")` sends
    Metaflow to `condition_value not in switch_cases` (flowspec.py), and a
    RemoteArtifact is unhashable, so the whole run died on the transition
    after the step had already succeeded.

    A condition is a scalar by construction -- it has to equal one of the case
    keys -- so loading it costs nothing. The cap is a guard against a user
    switching on something enormous, not an expected path.
    """
    if not condition:
        return
    ref = getattr(flow, condition, None)
    if not isinstance(ref, RemoteArtifact):
        return
    size = getattr(ref, "size_bytes", 0) or 0
    if size > MAX_CONDITION_ATTR_BYTES:
        sys.stdout.write(
            f"[remote_step] switch condition '{condition}' is {size / 1024 / 1024:.0f} MB, "
            f"which is not a scalar — leaving it as a reference. The transition will "
            f"fail; switch on a small value instead.\n"
        )
        return
    try:
        setattr(flow, condition, ref.load())
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] could not load switch condition '{condition}': {exc}\n")


def _hydrate_for_card(name: str, ref):
    """The value behind a card attribute, or the ref if it is too big."""
    if not isinstance(ref, RemoteArtifact):
        return ref
    size = getattr(ref, "size_bytes", 0) or 0
    if size > MAX_CARD_ATTR_BYTES:
        sys.stdout.write(
            f"[remote_step] '{name}' is rendered by a @card but is "
            f"{size / 1024 / 1024:.0f} MB — left as a reference rather than "
            f"loaded into the driver. The card will show the reference; "
            f"render a smaller summary attribute instead.\n"
        )
        return ref
    try:
        value = ref.load()
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] could not load '{name}' for its @card: {exc}\n")
        return ref
    sys.stdout.write(f"[remote_step] loaded '{name}' for its @card\n")
    return value


# The card id @gpu_profile's mutator injects. Keying off this is the reliable
# way to notice the decorator: @gpu_profile is a StepMutator, so by the time
# step_init runs it has rewritten itself into a `card` plus a
# `user_step_decorator` wrapper, and nothing in the list is named
# "gpu_profile". The card, though, is an ordinary decorator and is there.
GPU_PROFILE_CARD_ID = "gpu_profile"


def _find_gpu_profile(decorators) -> dict | None:
    """A sibling @gpu_profile's settings, or None if the step has none.

    The decorator samples wherever it runs, which for a remote step is the
    driver — a pod with no GPU. The runner samples instead; this carries the
    request across.
    """
    for d in decorators:
        name = getattr(d, "name", "") or ""
        attrs = getattr(d, "attributes", {}) or {}
        # Written by hand as a classic decorator (older Metaflow), or found
        # via the card its mutator injects (current Metaflow).
        #
        # Only the classic shape carries `interval`. The mutator hands it to
        # the user_step_decorator wrapper and gives the card only
        # `refresh_interval = max(5, interval)`, which is not invertible — so
        # the card shape falls back to 1 s. That is the decorator's own default
        # and the finest setting, so no sample is lost; a larger `interval=`
        # just costs a little more memory than was asked for.
        if name == "gpu_profile" or (name == "card" and attrs.get("id") == GPU_PROFILE_CARD_ID):
            return {"interval": int(attrs.get("interval") or 1)}
    return None


def _find_env_vars(decorators) -> dict[str, str]:
    """`vars` from a sibling @environment.

    Those variables are set on the *driver* pod by Metaflow, and the runner is
    a different pod in a different cluster that inherits nothing — so without
    forwarding them the step body sees none of what @environment declared.
    """
    out: dict[str, str] = {}
    for d in decorators:
        if getattr(d, "name", "") != "environment":
            continue
        for key, val in ((getattr(d, "attributes", {}) or {}).get("vars") or {}).items():
            if val is None:
                continue
            out[str(key)] = str(val)
    return out


def _find_timeout_minutes(decorators) -> int | None:
    """A sibling @timeout as whole minutes, or None if the step has none.

    Metaflow applies @timeout to the driver task. Left alone, the driver would
    be killed on the user's deadline while the runner pod carried on running
    — and billing — against `job_timeout_minutes`, which knows nothing about
    the user's intent.
    """
    total = 0
    found = False
    for d in decorators:
        if getattr(d, "name", "") != "timeout":
            continue
        attrs = getattr(d, "attributes", {}) or {}
        seconds = int(attrs.get("seconds") or 0)
        minutes = int(attrs.get("minutes") or 0)
        hours = int(attrs.get("hours") or 0)
        if seconds or minutes or hours:
            found = True
            # Metaflow sums the three units, so they have to be summed here
            # too and only then rounded up to whole minutes -- the Job
            # deadline has no finer granularity.
            #
            # Adding a flat `1 if seconds else 0` instead treated seconds as
            # a rounding nudge on top of the coarser units. That is right for
            # @timeout(minutes=5, seconds=30) -> 6, but when seconds is the
            # only unit given it collapsed the whole request to one minute:
            # @timeout(seconds=1800) asked for 30 minutes and got a pod
            # killed 60 seconds in.
            total_seconds = hours * 3600 + minutes * 60 + seconds
            total = max(total, -(-total_seconds // 60))
    return total if found else None


def _replay_card_components(bucket: str, output_prefix: str, s3_client=None) -> None:
    """Append the components the step body built in the pod to the real card.

    `@card` renders on this driver task, so a `current.card.append(...)` in the
    runner had no card to reach — it raised there, or rendered nothing. The pod
    records the components; this puts them where `@card` will find them.

    Never allowed to fail the step: by this point the body has succeeded, and a
    card is a report.
    """
    import pickle

    from remote_step.runner_entry import CARD_COMPONENTS_FILENAME, _CardRecorder

    try:
        body = s3_client.get_object(Bucket=bucket, Key=f"{output_prefix}/{CARD_COMPONENTS_FILENAME}")["Body"].read()
        pending = pickle.loads(body)
    except Exception:  # noqa: BLE001
        return
    if not isinstance(pending, dict) or not pending:
        return

    try:
        from metaflow import current

        collector = current.card
    except Exception:  # noqa: BLE001
        # The step appended to a card but has no @card to render it. Say so —
        # the components are otherwise lost without explanation.
        total = sum(len(v) for v in pending.values())
        sys.stdout.write(
            f"[remote_step] the step built {total} card component(s) but this "
            f"step has no @card to render them. Add @card alongside "
            f"@remote_step.\n"
        )
        return

    # The gpu_profile card is a special case. @gpu_profile's wrapper still runs
    # on the driver — it is a user_step_decorator, absent from the list
    # step_init sees, so it cannot be dropped — and it fills that card at task
    # start from a machine with no GPU: "Drivers: unknown / unknown", "No GPU
    # devices found". Clearing it first means the card shows the readings taken
    # next to the GPU instead of the driver's blanks followed by ours.
    if GPU_PROFILE_CARD_ID in pending:
        try:
            collector[GPU_PROFILE_CARD_ID].clear()
        except Exception as exc:  # noqa: BLE001
            sys.stdout.write(f"[remote_step] could not clear the {GPU_PROFILE_CARD_ID} card: {exc}\n")

    applied = 0
    for card_id, blobs in pending.items():
        for blob in blobs:
            try:
                component = pickle.loads(blob)
            except Exception:  # noqa: BLE001
                continue
            if component is None:
                continue
            try:
                if card_id == _CardRecorder.DEFAULT_ID:
                    collector.append(component)
                else:
                    collector[card_id].append(component)
                applied += 1
            except Exception as exc:  # noqa: BLE001
                sys.stdout.write(f"[remote_step] could not append a card component to '{card_id}': {exc}\n")
    if applied:
        sys.stdout.write(f"[remote_step] replayed {applied} card component(s) from the step\n")
        try:
            collector.refresh(force=True)
        except Exception:  # noqa: BLE001
            pass


def _apply_run_tags(bucket: str, output_prefix: str, s3_client=None) -> None:
    """Replay the tag edits the step body asked for inside the pod.

    `current.run` in the runner is a recorder, not a real `Run` — the pod has
    no credentials for Outerbounds' metadata service, so the call there did
    nothing. The driver is authenticated, so it makes the write.

    Never allowed to fail the step: the body has already succeeded by this
    point, and a tag is metadata.
    """
    import json

    from remote_step.runner_entry import RUN_TAGS_FILENAME

    try:
        body = s3_client.get_object(Bucket=bucket, Key=f"{output_prefix}/{RUN_TAGS_FILENAME}")["Body"].read()
        pending = json.loads(body)
    except Exception:  # noqa: BLE001
        return
    added = pending.get("added") or []
    removed = pending.get("removed") or []
    if not added and not removed:
        return
    try:
        from metaflow import current

        run = current.run
        if added:
            run.add_tags(added)
        if removed:
            run.remove_tags(removed)
        sys.stdout.write(f"[remote_step] applied run tags from the step: +{added} -{removed}\n")
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] could not apply run tags {added or removed}: {exc}\n")


def _reraise_step_exception(bucket: str, output_prefix: str, step_name: str, s3_client=None):
    """Re-raise the step body's own exception, if the runner saved one.

    Returns normally when there is nothing to re-raise, so the caller falls
    through to its RunnerError. Only the user's exception is allowed to
    escape: a failure reading the record must not replace a step failure with
    an S3 error.
    """
    import pickle

    from remote_step.runner_entry import EXCEPTION_FILENAME

    try:
        body = s3_client.get_object(Bucket=bucket, Key=f"{output_prefix}/{EXCEPTION_FILENAME}")["Body"].read()
        record = pickle.loads(body)
    except Exception:  # noqa: BLE001
        return
    if not isinstance(record, dict):
        return

    remote_tb = record.get("traceback")
    if remote_tb:
        sys.stdout.write(f"[remote_step] {step_name} raised in the runner pod:\n{remote_tb}\n")
    exc = record.get("exception")
    if isinstance(exc, BaseException):
        raise exc
    # Unpicklable exception: the runner sent type and message only. Surface
    # those rather than silently degrading to "the job failed".
    type_name = record.get("type_name")
    if type_name:
        raise RunnerError(f"step '{step_name}' raised {type_name}: {record.get('message', '')}")


def _job_timeout_minutes(user_timeout: int | None, attr_timeout: int) -> int:
    """The runner Job's deadline.

    A user @timeout is the step's real intent, so it wins outright. It is
    never extended: Metaflow kills the *driver* at that same moment, and a Job
    deadline beyond it would leave the pod running — and billing — with nobody
    watching, which is the whole reason this exists.

    The two therefore expire together, so which side reports the timeout is a
    race: usually Metaflow's own driver timeout, sometimes the runner's
    `activeDeadlineSeconds`. Both stop the work, which is what matters.

    With no @timeout the decorator's own `job_timeout_minutes` stands.
    """
    if not user_timeout:
        return int(attr_timeout)
    return int(user_timeout)


def _join_branches(inputs) -> list[dict]:
    """One record per incoming branch of a join, for the spec.

    Metaflow hands a join step an `Inputs` of cloned flow objects, one per
    branch, each carrying that branch's artifacts. The runner cannot rebuild
    those — it has no datastore — so each branch's attributes are collected
    here and shipped. Attributes that are already `RemoteArtifact` refs stay
    refs, which is what stops a wide foreach join from pulling every branch's
    data through the driver.
    """
    if inputs is None:
        return []
    branches: list[dict] = []
    for i, branch in enumerate(inputs):
        step = getattr(branch, "_current_step", None) or f"branch_{i}"
        try:
            attrs = _collect_flow_attrs(branch)
        except Exception:  # noqa: BLE001
            attrs = {}
        branches.append({"step": step, "attrs": attrs})
    return branches


def _collect_flow_attrs(flow) -> dict:
    """Collect user-visible flow attributes to ship to the runner.

    Sources, in order of precedence (first hit wins for a given name):
      1. `flow._datastore` — Metaflow's prior-task artifact loader. This is
         the source for `self.<x>` from all upstream steps, materialised
         lazily via `flow.__getattr__`. We enumerate `_datastore._objects`.
      2. `flow.__dict__` — user's own assignments during the current run.
      3. Class-level `Parameter` / `property` descriptors — Metaflow rewrites
         `Parameter` as a `property` at runtime.

    Skips: callables, Metaflow-private attrs, step methods, non-pickleable.
    """
    import pickle

    out: dict[str, object] = {}

    def _try_add(name: str, val: object) -> None:
        # `callable(v)` is the usual filter for methods/functions that we
        # never want to serialise into the spec. RemoteArtifact wraps its
        # proxy dunders around a real object, so a wrapped ``list``/``dict``
        # etc. never claims callability — but we still want to ship refs
        # to callable Python objects untouched, so we let RemoteArtifact
        # slip past the callable guard regardless.
        if not isinstance(val, RemoteArtifact) and callable(val):
            return
        if not _pickleable(val):
            return
        if not _is_picklable_streaming(val):
            return
        out[name] = val

    # (1) prior-task artifacts via Metaflow's datastore
    ds = getattr(flow, "_datastore", None)
    if ds is not None:
        try:
            names = list(getattr(ds, "_objects", {}).keys())
        except Exception:  # noqa: BLE001
            names = []
        for name in names:
            if name.startswith("_") or name in _SKIP_ATTRS or name in out:
                continue
            try:
                val = getattr(flow, name)
            except Exception:  # noqa: BLE001
                continue
            _try_add(name, val)

    # (2) instance __dict__ — user-set attrs
    for name, val in vars(flow).items():
        if name.startswith("_") or name in _SKIP_ATTRS or name in out:
            continue
        _try_add(name, val)

    # (3) Parameters — declared as class-level Parameter, wrapped as property
    _METAFLOW_PROPERTY_SKIPS = {
        "script_name",
        "cmd",
        "index",
        "input",
        "foreach_stack",
        "merge_artifacts",
        "next",
    }
    for cls in type(flow).__mro__:
        for name, class_attr in vars(cls).items():
            if name.startswith("_") or name in out or name in _SKIP_ATTRS or name in _METAFLOW_PROPERTY_SKIPS:
                continue
            attr_type = type(class_attr).__name__
            if attr_type not in ("Parameter", "property"):
                continue
            try:
                val = getattr(flow, name)
            except Exception:  # noqa: BLE001
                continue
            _try_add(name, val)
    return out


def _flow_module_name(flow) -> str:
    """Return the importable module name for a flow class.

    When Metaflow runs `python flow.py`, the class lives in `__main__`. The
    runner container can't import `__main__` — instead we use the file's
    basename (without .py), which matches how Metaflow's code-package
    exposes the flow module in /workspace.
    """
    mod = type(flow).__module__
    if mod != "__main__":
        return mod
    import sys

    main_mod = sys.modules.get("__main__")
    if main_mod is not None and hasattr(main_mod, "__file__"):
        return os.path.splitext(os.path.basename(main_mod.__file__))[0]
    return mod


def _named_mfconfig() -> dict[str, str]:
    """Named subset of METAFLOW_* env vars to ship to the runner."""
    allowed = (
        "METAFLOW_SERVICE_URL",
        "METAFLOW_DATASTORE_SYSROOT_S3",
        "METAFLOW_DEFAULT_METADATA",
        "METAFLOW_DEFAULT_DATASTORE",
        "METAFLOW_DEFAULT_ENVIRONMENT",
        "METAFLOW_USER",
        "METAFLOW_CODE_URL",
        "METAFLOW_CODE_SHA",
        "OBP_AUTH_SERVER",
    )
    return {k: os.environ[k] for k in allowed if k in os.environ}
