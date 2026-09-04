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
from remote_step.config import RemoteStepConfig, load as load_config
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
DEFAULT_GITHUB_SECRET_SOURCE = "outerbounds.remote-step-github"
CACHED_ENV_FILENAME = ".remote_step_env.json"
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
        self._thread = threading.Thread(
            target=self._run, name="remote-step-mflog-pusher", daemon=True
        )
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
    """Detect if we're being invoked as part of an Argo deploy or Argo run.

    Signals, first-hit wins:
      1. `argo-workflows` in sys.argv (deploy time on user's machine)
      2. `METAFLOW_ARGO_WORKFLOWS` env var (Argo runtime)
      3. `ARGO_TEMPLATE` env var (inside an Argo pod)
    """
    if any("argo-workflows" in arg for arg in sys.argv):
        return True
    if os.environ.get("METAFLOW_ARGO_WORKFLOWS"):
        return True
    if os.environ.get("ARGO_TEMPLATE"):
        return True
    return False


def _is_k8s_task_runtime() -> bool:
    """Whether this process *is* the task, already executing inside a pod.

    Distinct from _is_argo_context(), which only fires at deploy time: of its
    three signals, `argo-workflows` in sys.argv is true while building the
    template and false inside the pod, and neither `METAFLOW_ARGO_WORKFLOWS`
    nor `ARGO_TEMPLATE` is ever set by Metaflow — so it reports False for a
    running task.

    METAFLOW_KUBERNETES_WORKLOAD is set on every Argo task pod
    (argo_workflows.py) and is the same flag KubernetesDecorator.task_pre_step
    itself gates on, so it is exactly the right signal for "the @kubernetes
    lifecycle hooks would do something useful here".
    """
    return bool(os.environ.get("METAFLOW_KUBERNETES_WORKLOAD"))


def _find_resources(decorators) -> tuple[int, int, int]:
    """Read cpu, memory, gpu from a sibling @resources decorator.

    Metaflow's @resources stores its kwargs on `decorator.attributes`.
    """
    for d in decorators:
        if getattr(d, "name", "") == "resources":
            attrs = getattr(d, "attributes", {}) or {}
            cpu = int(attrs.get("cpu") or 1)
            memory = int(attrs.get("memory") or 4000)
            gpu = int(attrs.get("gpu") or 0)
            return cpu, memory, gpu
    return 1, 4000, 0


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


def _drop_kubernetes(decorators) -> None:
    """Remove any sibling @kubernetes decorator (mutation in place)."""
    for d in list(decorators):
        if getattr(d, "name", "") == "kubernetes":
            decorators.remove(d)


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


def _inject_driver_kubernetes(decorators) -> None:
    """Inject a small @kubernetes decorator so the Argo pod is Small tier.

    Only applied when we detect an Argo context — locally, Metaflow can run
    the driver in-process at Local tier (0.1 OBC/min).

    _is_k8s_task_runtime() is checked as well, and it is not redundant. The
    injection has two jobs, and they happen in different processes:

      deploy time   sizes the Argo template's pod    (_is_argo_context)
      task  time    runs KubernetesDecorator's       (_is_k8s_task_runtime)
                    task_pre_step, which records
                    kubernetes-pod-name / -pod-id /
                    -node-ip as task metadata

    Only the first was firing. Outerbounds joins its per-task CPU/memory
    panel to cluster metrics through that pod metadata, so @remote_step
    driver tasks recorded none of it and showed no resource usage at all,
    while ordinary steps — which carry a real @kubernetes decorator — did.
    """
    if not (_is_argo_context() or _is_k8s_task_runtime()):
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
    attrs["cpu"] = DEFAULT_DRIVER_CPU
    attrs["memory"] = DEFAULT_DRIVER_MEMORY_MB
    if attrs.get("gpu_vendor") is None:
        attrs["gpu_vendor"] = "nvidia"
    if attrs.get("gpu") is None:
        attrs["gpu"] = 0
    if attrs.get("disk") is None:
        attrs["disk"] = 10240
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
    decorators.append(
        SecretsDecorator(attributes={"sources": [source_name]})
    )


class RemoteStepDecorator(StepDecorator):
    """`@remote_step` — offload one step's compute to Kubernetes.

    Kwargs:
        team: namespace to run in. Selects the team's Kueue ClusterQueue and
            therefore its quota. Required — there is no safe default, since
            picking the wrong one spends another team's capacity.
        cpu_arch: 'x86_64' (default) | 'arm64'
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

    def _flow_file_path(self) -> str | None:
        """Best-effort location of the flow's file for add_to_package."""
        import __main__

        return getattr(__main__, "__file__", None)

    name = "remote_step"
    defaults = {
        # Kubernetes namespace == team. No default: the namespace decides
        # whose Kueue quota is consumed, so guessing is worse than failing.
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
        # CPU architecture. "x86_64" (default) or "arm64" — arm64 lands on
        # the Graviton NodePool (c9g/m9g/r9g/x8g) and picks the arm64 variant
        # of the multi-arch runner image. Cheaper (~20%) and often faster for
        # ML CPU kernels. Cannot be combined with a GPU ask: the gpu NodePool
        # is x86 only.
        "cpu_arch": "x86_64",
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
        if step_name in ("start", "end"):
            raise SizingError(
                f"@remote_step on '{step_name}' — heavy compute must move to a "
                f"downstream step; start/end run in the Metaflow scheduler.",
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
        team = self.attributes.get("team")
        if not team:
            raise SizingError(
                f"@remote_step on '{step_name}' needs team=. It names the "
                f"Kubernetes namespace and therefore whose Kueue quota the "
                f"step spends, so there is no safe default.\n"
                f"  e.g. @remote_step(team=\"forecasting\")",
                step_name=step_name,
            )
        cpu, memory_mb, gpu = _find_resources(decorators)
        try:
            self._resources = resolve(
                cpu,
                memory_mb,
                gpu,
                cpu_arch=self.attributes["cpu_arch"],
                ephemeral_gb=self.attributes["ephemeral_gb"],
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

        _drop_kubernetes(decorators)
        _inject_driver_kubernetes(decorators)
        # Shrink @resources so Metaflow doesn't render a big pod for the driver.
        # We've already captured cpu/mem/gpu in self._resources for the pod.
        _shrink_resources(decorators)
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
        if _is_argo_context():
            gh_src = self.attributes.get("github_secret_source")
            if gh_src:
                _inject_secrets(decorators, gh_src)

        # sys.stdout.write, not logger(): Metaflow's logger stamps every
        # line with "YYYY-MM-DD HH:MM:SS.mmm ", which every other
        # [remote_step] line — all written from the driver body — does not
        # carry. Using it here made flow-init output look like a different
        # subsystem from the rest.
        sys.stdout.write(
            f"[remote_step] {step_name} -> {team} · "
            f"{format_resources(self._resources)}\n"
        )
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
        """
        cfg = self._config
        resources = self._resources
        env_spec = self._env_spec
        team = self.attributes["team"]
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
                sys.stdout.write(
                    f"[remote_step] captured inputs: {list(input_attrs.keys())}\n"
                )
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
                    _tags.extend(
                        t for t in (getattr(_current, "system_tags", None) or [])
                        if t not in _tags
                    )
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
                )
                spec_uri, spec = build_and_upload(
                    driver_ctx,
                    env_spec,
                    input_attrs,
                    cfg.payload_bucket,
                    s3_client=driver_s3,
                )
                sys.stdout.write(
                    f"[remote_step] submitted spec {spec_uri}\n"
                    f"[remote_step] {format_resources(resources)}\n"
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
                        sys.stdout.write(
                            f"[remote_step] forwarding {_k} to the runner "
                            f"(len={len(_v)})\n"
                        )
                        break
                # Outerbounds runtime context, so user code that talks to
                # Outerbounds integrations (Snowflake and friends) works from
                # inside the runner pod.
                for _k, _v in os.environ.items():
                    if _k.startswith(("METAFLOW_", "OBP_", "OUTERBOUNDS_")):
                        runner_env[_k] = _v

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
                    priority=self.attributes["priority"],
                    extra_env=runner_env,
                    timeout_minutes=self.attributes["job_timeout_minutes"],
                    client=api,
                )
                sys.stdout.write(
                    f"[remote_step] job {result.job_name} "
                    f"in {result.namespace} (queue {result.queue})\n"
                )
                outcome = poll_wait(
                    api,
                    result.namespace,
                    result.job_name,
                    pending_timeout_sec=pending_timeout,
                )
                if not outcome.succeeded:
                    detail = "\n  ".join(outcome.events) if outcome.events else ""
                    raise RunnerError(
                        f"step '{step_name}' failed: {outcome.reason}"
                        + (f" (exit {outcome.exit_code})" if outcome.exit_code is not None else "")
                        + (f"\n  {detail}" if detail else ""),
                        exit_code=outcome.exit_code,
                        job_name=outcome.job_name,
                    )
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
                for name, ref in outputs.items():
                    setattr(self_flow, name, ref)
                sys.stdout.write(
                    f"[remote_step] {step_name} finished, "
                    f"{len(outputs)} artifact(s) linked\n"
                )
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
                        case_map = {
                            case: getattr(self_flow, fn)
                            for case, fn in switch_cases.items()
                            if hasattr(self_flow, fn)
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
                        next_refs = [
                            getattr(self_flow, f)
                            for f in out_funcs
                            if hasattr(self_flow, f)
                        ]
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


_SKIP_ATTRS = frozenset({
    "next", "input", "index", "foreach_stack", "checkpoint",
    "_datastore", "_metadata", "_current_step", "_task", "_flow_state",
    "_graph", "_transition", "_flow_decorators", "_success", "_flow_state",
    "logger", "cards", "_cards", "_current",
})


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
        try:
            pickle.dumps(val, protocol=5)
        except Exception:  # noqa: BLE001
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
        "script_name", "cmd", "index", "input", "foreach_stack",
        "merge_artifacts", "next",
    }
    for cls in type(flow).__mro__:
        for name, class_attr in vars(cls).items():
            if (
                name.startswith("_")
                or name in out
                or name in _SKIP_ATTRS
                or name in _METAFLOW_PROPERTY_SKIPS
            ):
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
