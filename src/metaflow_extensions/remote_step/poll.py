"""Watch a step's Kubernetes Job and stream its logs.

Called by the driver after submit().

Logs come from a follow read of the pod log — a streaming GET against the
kubelet, which holds the container's stdout directly, so there is no ingest
stage between the step writing a line and the driver printing it. The runner
also writes to CloudWatch for durable retention; the driver does not read
that copy.

While the step is waiting, the two possible causes are distinguished and
reported as they change:

  - `suspend: true` still set  -> Kueue has not admitted it; the team's
                                  ClusterQueue is at quota
  - admitted, pod Pending      -> Karpenter is launching a node
"""

from __future__ import annotations

from dataclasses import dataclass, field
import signal
import sys
import threading
import time
from typing import IO

from remote_step.errors import (
    KilledByUser,
    NodeLostError,
    PendingTimeoutError,
    RemoteStepError,
    RunnerError,
)
from remote_step.k8s import ApiError, NotFound

# How long to wait for the pod to start. Covers Kueue admission plus
# Karpenter provisioning; a cold GPU node with a large image is the slow case.
DEFAULT_PENDING_TIMEOUT_SEC = 20 * 60

# Consecutive API failures tolerated before giving up. A transient 5xx or a
# dropped connection must not fail a step whose pod is running fine, but an
# API server we genuinely cannot reach should not be retried forever.
MAX_CONSECUTIVE_API_ERRORS = 20

# Node loss is recorded on the POD, not the container: when a node vanishes
# there is usually no terminated container state at all, so checking only the
# container reason misreports it as an ordinary step failure.
NODE_LOSS_POD_REASONS = {"NodeLost", "Shutdown", "NodeShutdown", "Terminated"}
NODE_LOSS_TERM_REASONS = {"NodeShutdown", "Evicted", "ContainerStatusUnknown"}


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
    termination_reason: str = ""
    events: list[str] = field(default_factory=list)


class _LogStreamer:
    """Streams a pod's stdout to `out` as it is produced.

    Reconnects when the stream drops, which the API server does on an idle
    connection. Each reconnect asks only for what has happened since the last
    one — without that the server replays the log from the beginning, so a
    slow-logging step reprints its whole history on every reconnect and the
    driver's own stdout grows without bound.
    """

    RECONNECT_BACKOFF_SEC = 1.0

    def __init__(self, client, namespace: str, pod_name: str, out: IO) -> None:
        self._client = client
        self._ns = namespace
        self._pod = pod_name
        self._out = out
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_output_at = time.time()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="remote-step-logs", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the thread and wait briefly.

        The thread is a daemon and checks `_stop` between chunks, so a stream
        blocked on an idle socket is abandoned rather than joined — hence the
        short timeout and no attempt to close the response from here, which
        would race the reader.
        """
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)

    def _run(self) -> None:
        first = True
        while not self._stop.is_set():
            since = (
                None if first else max(1, int(time.time() - self._last_output_at) + 2)
            )
            try:
                for text in self._client.stream_pod_log(
                    self._ns, self._pod, since_seconds=since
                ):
                    if self._stop.is_set():
                        return
                    self._out.write(text)
                    self._out.flush()
                    self._last_output_at = time.time()
                return  # clean end of stream: the container exited
            except Exception:  # noqa: BLE001
                if self._stop.is_set():
                    return
                first = False
                time.sleep(self.RECONNECT_BACKOFF_SEC)


def _pod_for_job(client, namespace: str, job_name: str) -> dict | None:
    """Newest pod for the Job, or None if none exists yet."""
    pods = client.list_job_pods(namespace, job_name)
    if not pods:
        return None
    return sorted(
        pods,
        key=lambda p: (p.get("metadata", {}).get("creationTimestamp") or ""),
        reverse=True,
    )[0]


def _waiting_reason(job: dict, pod: dict | None) -> str:
    """Explain, in one line, why the step has not started."""
    if job.get("spec", {}).get("suspend"):
        return (
            "queued — Kueue has not admitted this Workload yet "
            "(team ClusterQueue at quota)"
        )
    if pod is None:
        return "admitted — waiting for the Job controller to create the pod"
    status = pod.get("status", {}) or {}
    phase = status.get("phase") or "Unknown"
    if phase == "Pending":
        for c in status.get("conditions") or []:
            if c.get("type") == "PodScheduled" and c.get("status") != "True":
                msg = (c.get("message") or c.get("reason") or "").strip()
                return (
                    f"waiting for a node — {msg}"
                    if msg
                    else "waiting for a node (Karpenter provisioning)"
                )
        for cs in status.get("containerStatuses") or []:
            w = (cs.get("state") or {}).get("waiting")
            if w:
                return f"starting — {w.get('reason') or 'container initialising'}"
        return "scheduled — starting container"
    return f"pod {str(phase).lower()}"


def _container_exit(pod: dict) -> tuple[int | None, str]:
    """(exit_code, termination_reason) for the runner container."""
    for cs in (pod.get("status", {}) or {}).get("containerStatuses") or []:
        if cs.get("name") != "runner":
            continue
        term = (cs.get("state") or {}).get("terminated")
        if term:
            return term.get("exitCode"), (term.get("reason") or "")
    return None, ""


def _recent_events(client, namespace: str, name: str, limit: int = 8) -> list[str]:
    """Events referencing `name`, newest last."""
    try:
        evs = client.list_events_for(namespace, name)
    except Exception:  # noqa: BLE001
        return []
    rows = sorted(
        evs, key=lambda e: (e.get("lastTimestamp") or e.get("eventTime") or "")
    )
    return [
        f"{e.get('reason')}: {(e.get('message') or '').strip()}"
        for e in rows[-limit:]
        if e.get("reason")
    ]


def _instance_type(client, node_name: str) -> str:
    if not node_name:
        return ""
    try:
        node = client.get_node(node_name)
    except Exception:  # noqa: BLE001
        return ""
    return (node.get("metadata", {}).get("labels") or {}).get(
        "node.kubernetes.io/instance-type", ""
    )


def wait(
    client,
    namespace: str,
    job_name: str,
    *,
    out: IO = sys.stdout,
    pending_timeout_sec: int = DEFAULT_PENDING_TIMEOUT_SEC,
    stream_logs: bool = True,
) -> JobResult:
    """Block until the Job finishes. Streams pod logs while it runs.

    On interruption the Job is deleted, because one left behind holds the
    team's Kueue admission until it finishes on its own. Both SIGINT and
    SIGTERM are handled: Argo cancels a workflow with SIGTERM, so catching
    only KeyboardInterrupt would leak quota on the one path that matters in
    production.
    """
    streamer: _LogStreamer | None = None
    pod_name = ""
    node_name = ""
    instance_type = ""
    started_wait = time.time()
    pod_started = False
    last_reason = ""
    last_report = 0.0
    api_errors = 0
    interrupted: list[str] = []

    def _on_signal(signum, _frame):
        interrupted.append(signal.Signals(signum).name)

    previous: dict = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[sig] = signal.getsignal(sig)
            signal.signal(sig, _on_signal)
        except (ValueError, OSError):
            # Not the main thread, or the platform disallows it. Falls back to
            # KeyboardInterrupt handling only.
            pass

    def _cleanup(why: str) -> None:
        if streamer is not None:
            streamer.stop()
        try:
            client.delete_job(namespace, job_name)
            out.write(f"[remote_step] deleted Job {job_name} after {why}\n")
        except Exception:  # noqa: BLE001
            out.write(
                f"[remote_step] could not delete Job {job_name}; clean up with "
                f"`kubectl -n {namespace} delete job {job_name}`\n"
            )
        out.flush()

    try:
        while True:
            if interrupted:
                _cleanup(interrupted[0])
                raise KilledByUser(
                    f"interrupted by {interrupted[0]}", job_name=job_name
                )

            try:
                job = client.get_job(namespace, job_name)
                pod = _pod_for_job(client, namespace, job_name)
                api_errors = 0
            except NotFound as exc:
                if streamer is not None:
                    streamer.stop()
                raise RemoteStepError(
                    f"Job {job_name} disappeared from namespace {namespace} "
                    f"while waiting — deleted externally, or evicted by Kueue "
                    f"without being requeued.",
                    job_name=job_name,
                    namespace=namespace,
                ) from exc
            except (ApiError, OSError) as exc:
                # Transient: a 5xx, a dropped connection, a token racing
                # renewal. The pod is very likely still running, so failing
                # the step here would abandon live work.
                api_errors += 1
                if api_errors >= MAX_CONSECUTIVE_API_ERRORS:
                    if streamer is not None:
                        streamer.stop()
                    raise RemoteStepError(
                        f"lost contact with the Kubernetes API after "
                        f"{api_errors} consecutive failures: {exc}",
                        job_name=job_name,
                        namespace=namespace,
                    ) from exc
                time.sleep(min(2 * api_errors, 15))
                continue

            if pod is not None and not pod_name:
                pod_name = pod.get("metadata", {}).get("name", "")
            pod_phase = (pod or {}).get("status", {}).get("phase")

            if (
                stream_logs
                and streamer is None
                and pod is not None
                and pod_phase in ("Running", "Succeeded", "Failed")
            ):
                node_name = (pod.get("spec") or {}).get("nodeName") or ""
                instance_type = _instance_type(client, node_name)
                out.write(
                    f"[remote_step] pod {pod_name} running on {node_name}"
                    + (f" ({instance_type})" if instance_type else "")
                    + "\n"
                )
                out.flush()
                streamer = _LogStreamer(client, namespace, pod_name, out)
                streamer.start()

            # Tracked independently of log streaming, so stream_logs=False
            # does not make the pending timeout fire on a healthy step.
            if pod_phase in ("Running", "Succeeded", "Failed"):
                pod_started = True

            status = job.get("status", {}) or {}

            if status.get("succeeded"):
                exit_code, term_reason = _container_exit(pod) if pod else (0, "")
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

            if status.get("failed"):
                exit_code, term_reason = _container_exit(pod) if pod else (None, "")
                pod_reason = (pod or {}).get("status", {}).get("reason") or ""
                if streamer is not None:
                    streamer.stop()
                events = _recent_events(client, namespace, pod_name or job_name)
                job_reason = ""
                for c in status.get("conditions") or []:
                    if c.get("type") == "Failed":
                        job_reason = c.get("reason") or c.get("message") or ""

                if (
                    pod_reason in NODE_LOSS_POD_REASONS
                    or term_reason in NODE_LOSS_TERM_REASONS
                    or job_reason == "DeadlineExceeded"
                ):
                    raise NodeLostError(
                        f"step's node went away "
                        f"({pod_reason or term_reason or job_reason}). "
                        f"Retriable — add @retry(times=1).",
                        exit_code,
                        job_name=job_name,
                        node_name=node_name,
                    )
                if term_reason == "OOMKilled":
                    raise RunnerError(
                        "step was OOM-killed — raise @resources(memory=...).",
                        exit_code,
                        job_name=job_name,
                        termination_reason=term_reason,
                    )
                return JobResult(
                    job_name=job_name,
                    namespace=namespace,
                    succeeded=False,
                    exit_code=exit_code,
                    reason=job_reason or pod_reason or term_reason or "Failed",
                    pod_name=pod_name,
                    node_name=node_name,
                    instance_type=instance_type,
                    ended_at=time.time(),
                    termination_reason=term_reason or pod_reason,
                    events=events,
                )

            if not pod_started:
                reason = _waiting_reason(job, pod)
                now = time.time()
                if reason != last_reason or (now - last_report) > 60:
                    out.write(f"[remote_step] {reason} ({int(now - started_wait)}s)\n")
                    out.flush()
                    last_reason = reason
                    last_report = now
                if now - started_wait > pending_timeout_sec:
                    events = _recent_events(client, namespace, pod_name or job_name)
                    _cleanup("pending timeout")
                    raise PendingTimeoutError(
                        f"pod did not start within {pending_timeout_sec}s. "
                        f"Last state: {reason}\n  " + "\n  ".join(events),
                        job_name=job_name,
                        namespace=namespace,
                        waited_sec=int(now - started_wait),
                    )

            time.sleep(2 if not pod_started else 5)

    except KeyboardInterrupt:
        _cleanup("interrupt")
        raise KilledByUser("interrupted by user", job_name=job_name) from None
    finally:
        for sig, handler in previous.items():
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass
