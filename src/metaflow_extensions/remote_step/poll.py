"""Watch a step's Kubernetes Job and stream its logs.

Called by the driver after submit().

Logs come from `read_namespaced_pod_log(follow=True)` — a streaming read
against the kubelet, which holds the container's stdout directly, so there
is no ingest stage between the step writing a line and the driver printing
it. The runner also writes to CloudWatch for durable retention (30-day log
group); the driver does not read that copy.

While the step is waiting, the two possible causes are distinguished and
reported as they change:

  - `suspend: true` still set  -> Kueue has not admitted it; the team's
                                  ClusterQueue is at quota
  - admitted, pod Pending      -> Karpenter is launching a node
"""

from __future__ import annotations

from dataclasses import dataclass, field
import sys
import threading
import time
from typing import IO

from remote_step.errors import (
    KilledByUser,
    NodeLostError,
    PendingTimeoutError,
    RunnerError,
)


# How long to wait for the pod to start before giving up. Covers Kueue
# admission plus Karpenter provisioning; a cold GPU node with a large image
# is the slow case.
DEFAULT_PENDING_TIMEOUT_SEC = 20 * 60

# Watch calls are given a server-side timeout so a silent connection drop
# surfaces as a loop iteration rather than hanging forever.
WATCH_TIMEOUT_SEC = 60

# Cadence for the status line printed while waiting. Only emitted when the
# reason text changes, so a long quiet wait does not scroll.
STATUS_REPORT_INTERVAL_SEC = 15


@dataclass
class JobResult:
    """Terminal outcome of a step's Job."""

    job_name: str
    namespace: str
    succeeded: bool
    exit_code: int | None
    reason: str
    pod_name: str = ""
    node_name: str = ""
    instance_type: str = ""
    started_at: float | None = None
    ended_at: float | None = None
    # Populated when the pod was killed by the kubelet rather than exiting
    # on its own — OOMKilled being the one that matters in practice.
    termination_reason: str = ""
    events: list[str] = field(default_factory=list)


class _LogStreamer:
    """Streams a pod's stdout to `out` as it is produced.

    A single follow read covers the pod's whole life. The stream ends when
    the container exits, so the thread finishes on its own; `.stop()` exists
    for the Ctrl-C path.

    Restarts are handled by reconnecting: `follow=True` raises when the pod
    is not yet running, and the pod may not exist at all for the first few
    seconds after admission.
    """

    RECONNECT_BACKOFF_SEC = 1.0

    def __init__(self, core_v1, namespace: str, pod_name: str, out: IO) -> None:
        self._core = core_v1
        self._ns = namespace
        self._pod = pod_name
        self._out = out
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._resp = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="remote-step-logs", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        resp = self._resp
        if resp is not None:
            try:
                resp.close()
            except Exception:  # noqa: BLE001
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                # _preload_content=False returns the raw urllib3 response so
                # we can iterate it as it arrives. With the default (True)
                # the client buffers the entire log body and returns a str,
                # which would defeat the point.
                resp = self._core.read_namespaced_pod_log(
                    name=self._pod,
                    namespace=self._ns,
                    follow=True,
                    _preload_content=False,
                    timestamps=False,
                )
                self._resp = resp
                for chunk in resp.stream(amt=None, decode_content=True):
                    if self._stop.is_set():
                        return
                    if not chunk:
                        continue
                    text = (
                        chunk.decode("utf-8", "replace")
                        if isinstance(chunk, bytes)
                        else str(chunk)
                    )
                    self._out.write(text)
                    self._out.flush()
                # Clean end of stream: container exited.
                return
            except Exception:  # noqa: BLE001
                # Pod not running yet, or the connection dropped. Both are
                # normal; retry until the watcher tells us the Job is done.
                if self._stop.is_set():
                    return
                time.sleep(self.RECONNECT_BACKOFF_SEC)


def _pod_for_job(core_v1, namespace: str, job_name: str):
    """Return the Job's pod, or None if it does not exist yet.

    Selects on the controller-uid-free `job-name` label, which the Job
    controller sets on every pod it creates.
    """
    pods = core_v1.list_namespaced_pod(
        namespace=namespace,
        label_selector=f"job-name={job_name}",
    )
    if not pods.items:
        return None
    # backoffLimit is 0, so there is at most one pod. If a stale one exists
    # from a prior attempt, prefer the newest.
    return sorted(
        pods.items,
        key=lambda p: p.metadata.creation_timestamp or 0,
        reverse=True,
    )[0]


def _waiting_reason(job, pod, workload_admitted: bool | None) -> str:
    """Explain, in one line, why the step has not started."""
    if job is not None and job.spec.suspend:
        return (
            "queued — Kueue has not admitted this Workload yet "
            "(team ClusterQueue at quota)"
        )
    if pod is None:
        return "admitted — waiting for the Job controller to create the pod"
    phase = pod.status.phase
    if phase == "Pending":
        # An unschedulable pod is the interesting case: it means Karpenter is
        # either launching a node or cannot find capacity.
        conds = {c.type: c for c in (pod.status.conditions or [])}
        sched = conds.get("PodScheduled")
        if sched is not None and sched.status != "True":
            msg = (sched.message or sched.reason or "").strip()
            return f"waiting for a node — {msg}" if msg else "waiting for a node (Karpenter provisioning)"
        # Scheduled but not running: pulling the image.
        for cs in pod.status.container_statuses or []:
            w = cs.state.waiting if cs.state else None
            if w is not None:
                return f"starting — {w.reason or 'container initialising'}"
        return "scheduled — starting container"
    return f"pod {phase.lower()}"


def _container_exit(pod) -> tuple[int | None, str]:
    """Extract (exit_code, termination_reason) from the runner container."""
    for cs in pod.status.container_statuses or []:
        if cs.name != "runner":
            continue
        term = cs.state.terminated if cs.state else None
        if term is not None:
            return term.exit_code, (term.reason or "")
    return None, ""


def _recent_events(core_v1, namespace: str, name: str, limit: int = 8) -> list[str]:
    """Events referencing `name`, newest last. Used to explain a failure."""
    try:
        evs = core_v1.list_namespaced_event(
            namespace=namespace,
            field_selector=f"involvedObject.name={name}",
        )
    except Exception:  # noqa: BLE001
        return []
    rows = sorted(
        evs.items,
        key=lambda e: e.last_timestamp or e.event_time or 0,
    )
    return [
        f"{e.reason}: {(e.message or '').strip()}"
        for e in rows[-limit:]
        if e.reason
    ]


def wait(
    api_client,
    namespace: str,
    job_name: str,
    *,
    out: IO = sys.stdout,
    pending_timeout_sec: int = DEFAULT_PENDING_TIMEOUT_SEC,
    stream_logs: bool = True,
    refresher=None,
) -> JobResult:
    """Block until the Job finishes. Streams pod logs while it runs.

    Args:
        api_client: a `kubernetes` ApiClient (see eks_auth.api_client).
        refresher: optional token refresher; called each loop so a step
            outlasting the 15-minute EKS token keeps working.

    Raises:
        PendingTimeoutError: pod never started.
        KilledByUser: Ctrl-C. The Job is deleted on the way out so it does
            not keep holding Kueue quota.
    """
    from kubernetes import client as k8s_client

    batch_v1 = k8s_client.BatchV1Api(api_client)
    core_v1 = k8s_client.CoreV1Api(api_client)

    streamer: _LogStreamer | None = None
    pod_name = ""
    node_name = ""
    instance_type = ""
    started_wait = time.time()
    pod_started = False
    last_reason = ""
    last_report = 0.0

    try:
        while True:
            if refresher is not None:
                refresher.refresh_if_needed()

            job = batch_v1.read_namespaced_job(name=job_name, namespace=namespace)
            pod = _pod_for_job(core_v1, namespace, job_name)

            if pod is not None and not pod_name:
                pod_name = pod.metadata.name

            # Begin streaming as soon as the container can produce output.
            if (
                stream_logs
                and streamer is None
                and pod is not None
                and pod.status.phase in ("Running", "Succeeded", "Failed")
            ):
                pod_started = True
                node_name = pod.spec.node_name or ""
                if node_name:
                    try:
                        node = core_v1.read_node(name=node_name)
                        instance_type = (node.metadata.labels or {}).get(
                            "node.kubernetes.io/instance-type", ""
                        )
                    except Exception:  # noqa: BLE001
                        pass
                out.write(
                    f"[remote_step] pod {pod_name} running on {node_name}"
                    + (f" ({instance_type})" if instance_type else "")
                    + "\n"
                )
                out.flush()
                streamer = _LogStreamer(core_v1, namespace, pod_name, out)
                streamer.start()

            # Terminal?
            st = job.status
            if st.succeeded:
                exit_code, term_reason = (
                    _container_exit(pod) if pod is not None else (0, "")
                )
                if streamer is not None:
                    streamer.stop()
                return JobResult(
                    job_name=job_name,
                    namespace=namespace,
                    succeeded=True,
                    exit_code=exit_code if exit_code is not None else 0,
                    reason="Complete",
                    pod_name=pod_name,
                    node_name=node_name,
                    instance_type=instance_type,
                    ended_at=time.time(),
                    termination_reason=term_reason,
                )
            if st.failed:
                exit_code, term_reason = (
                    _container_exit(pod) if pod is not None else (None, "")
                )
                if streamer is not None:
                    streamer.stop()
                events = _recent_events(core_v1, namespace, pod_name or job_name)
                reason = ""
                for c in st.conditions or []:
                    if c.type == "Failed":
                        reason = c.reason or c.message or ""
                # A vanished node is retriable; a non-zero exit is the step's
                # own failure and is only retriable via @retry.
                if term_reason in ("NodeShutdown", "Evicted") or reason == "DeadlineExceeded":
                    raise NodeLostError(
                        f"step's node went away ({term_reason or reason}). "
                        f"Retriable — add @retry(times=1).",
                        job_name=job_name,
                        node_name=node_name,
                    )
                if term_reason == "OOMKilled":
                    raise RunnerError(
                        f"step was OOM-killed. It requested "
                        f"{job.spec.template.spec.containers[0].resources.limits.get('memory')} "
                        f"— raise @resources(memory=...).",
                        exit_code=exit_code,
                        job_name=job_name,
                        termination_reason=term_reason,
                    )
                return JobResult(
                    job_name=job_name,
                    namespace=namespace,
                    succeeded=False,
                    exit_code=exit_code,
                    reason=reason or term_reason or "Failed",
                    pod_name=pod_name,
                    node_name=node_name,
                    instance_type=instance_type,
                    ended_at=time.time(),
                    termination_reason=term_reason,
                    events=events,
                )

            # Still waiting. Report why, but only when it changes.
            if not pod_started:
                reason = _waiting_reason(job, pod, None)
                now = time.time()
                if reason != last_reason or (now - last_report) > 60:
                    elapsed = int(now - started_wait)
                    out.write(f"[remote_step] {reason} ({elapsed}s)\n")
                    out.flush()
                    last_reason = reason
                    last_report = now
                if now - started_wait > pending_timeout_sec:
                    events = _recent_events(core_v1, namespace, pod_name or job_name)
                    raise PendingTimeoutError(
                        f"pod did not start within {pending_timeout_sec}s. "
                        f"Last state: {reason}\n  "
                        + "\n  ".join(events),
                        job_name=job_name,
                        namespace=namespace,
                        waited_sec=int(now - started_wait),
                    )

            time.sleep(2 if not pod_started else 5)

    except KeyboardInterrupt:
        if streamer is not None:
            streamer.stop()
        # Delete the Job so it stops consuming the team's Kueue quota. Without
        # this a Ctrl-C'd step would keep its admission until the pod's own
        # termination, blocking other work in the same ClusterQueue.
        try:
            batch_v1.delete_namespaced_job(
                name=job_name,
                namespace=namespace,
                propagation_policy="Background",
            )
            out.write(f"[remote_step] deleted Job {job_name} after interrupt\n")
        except Exception:  # noqa: BLE001
            out.write(
                f"[remote_step] could not delete Job {job_name}; "
                f"clean up with `kubectl -n {namespace} delete job {job_name}`\n"
            )
        raise KilledByUser("interrupted by user", job_name=job_name) from None
