"""Block-and-poll loop for AWS Batch jobs.

Called by the driver body after SubmitJob. Polls DescribeJobs every ~15 s
for state, streams CloudWatch logs via `start_live_tail` (sub-second
latency once CW has ingested the event), and exits on a terminal state.
Network flakes are absorbed indefinitely.
"""

from __future__ import annotations

from dataclasses import dataclass
import sys
import threading
import time
from typing import IO

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from remote_step.config import RemoteStepConfig
from remote_step.errors import (
    KilledByUser,
    PendingTimeoutError,
    RunnerError,
    SpotInterruptionError,
)


TERMINAL_STATES = {"SUCCEEDED", "FAILED"}
PENDING_STATES = {"SUBMITTED", "PENDING", "RUNNABLE"}


@dataclass
class JobResult:
    """Terminal outcome of a Batch job."""

    job_id: str
    status: str
    exit_code: int | None
    status_reason: str
    cw_stream: str
    started_at: int | None
    ended_at: int | None


def _describe(batch, job_id: str) -> dict | None:
    resp = batch.describe_jobs(jobs=[job_id])
    jobs = resp.get("jobs", [])
    return jobs[0] if jobs else None


def _log_group_arn(region: str, account_id: str, log_group: str) -> str:
    """Build the ARN required by StartLiveTail's logGroupIdentifiers."""
    return f"arn:aws:logs:{region}:{account_id}:log-group:{log_group}"


class _LiveTail:
    """Streams CloudWatch log events to ``out`` in near-realtime.

    Runs `start_live_tail` in a background thread. CloudWatch pushes each
    ingested event as a `sessionUpdate` (sub-second delivery once CW has
    the event), so the only remaining lag is upstream: Fargate's awslogs
    driver batches ~5 s before flushing to CloudWatch — that's a floor
    we can't beat without swapping log drivers.

    The live-tail session is capped at 3 h by AWS; on expiry or transient
    error the loop reconnects. `.stop()` sets an event both the outer
    loop and the boto EventStream iterator check.
    """

    RECONNECT_BACKOFF_SEC = 2.0

    def __init__(
        self,
        logs_client,
        log_group_arn: str,
        stream_name: str,
        out: IO,
    ) -> None:
        self._logs = logs_client
        self._log_group_arn = log_group_arn
        self._stream_name = stream_name
        self._out = out
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_ts: int = 0  # ms — for the final get_log_events drain
        self._event_stream = None  # holds the in-flight EventStream so stop() can close it

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="remote-step-live-tail", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        stream = self._event_stream
        if stream is not None:
            try:
                stream.close()
            except Exception:  # noqa: BLE001
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)

    @property
    def last_event_ms(self) -> int:
        return self._last_ts

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                resp = self._logs.start_live_tail(
                    logGroupIdentifiers=[self._log_group_arn],
                    logStreamNames=[self._stream_name],
                )
            except (ClientError, BotoCoreError):
                # Stream may not exist yet — retry.
                if self._stop.wait(self.RECONNECT_BACKOFF_SEC):
                    return
                continue
            self._event_stream = resp["responseStream"]
            try:
                for event in self._event_stream:
                    if self._stop.is_set():
                        break
                    update = event.get("sessionUpdate")
                    if not update:
                        continue
                    for record in update.get("sessionResults", []) or []:
                        msg = record.get("message", "")
                        ts = record.get("timestamp", 0)
                        if ts:
                            self._last_ts = max(self._last_ts, ts)
                        self._out.write(msg + "\n")
                    self._out.flush()
            except (ClientError, BotoCoreError, Exception):  # noqa: BLE001
                # Session ended / expired / network glitch — reconnect.
                if self._stop.wait(self.RECONNECT_BACKOFF_SEC):
                    return
                continue
            finally:
                self._event_stream = None


def _drain_after(
    logs_client,
    log_group: str,
    stream_name: str,
    out: IO,
    start_time_ms: int,
) -> None:
    """Final get_log_events sweep to catch anything the live-tail dropped.

    Live-tail sessions can end abruptly (session cap, transient errors);
    on terminal state we do one paginated sweep from ``start_time_ms``
    forward to make sure nothing is missing before we return.
    """
    kwargs = {
        "logGroupName": log_group,
        "logStreamName": stream_name,
        "startFromHead": True,
    }
    if start_time_ms:
        kwargs["startTime"] = start_time_ms + 1
    cursor: str | None = None
    while True:
        if cursor:
            kwargs["nextToken"] = cursor
        try:
            resp = logs_client.get_log_events(**kwargs)
        except (ClientError, BotoCoreError):
            return
        events = resp.get("events", [])
        for event in events:
            out.write(event["message"] + "\n")
        out.flush()
        next_cursor = resp.get("nextForwardToken")
        if not events or next_cursor == cursor:
            return
        cursor = next_cursor


def wait(
    job_id: str,
    cfg: RemoteStepConfig,
    *,
    out: IO = sys.stdout,
    poll_interval: float = 15.0,
    pending_timeout: float = 60 * 60,
    batch_client=None,
    logs_client=None,
    sts_client=None,
) -> JobResult:
    """Block until the Batch job reaches a terminal state.

    Raises PendingTimeoutError if stuck in RUNNABLE longer than
    `pending_timeout`. Raises RunnerError / SpotInterruptionError on FAILED.
    Raises KilledByUser on KeyboardInterrupt (after terminating the job).
    """
    batch = batch_client or boto3.client("batch", region_name=cfg.region)
    logs = logs_client or boto3.client("logs", region_name=cfg.region)

    pending_since: float | None = time.time()
    log_stream: str | None = None
    live_tail: _LiveTail | None = None
    job: dict | None = None
    account_id: str | None = None
    try:
        while True:
            try:
                job = _describe(batch, job_id)
            except (ClientError, BotoCoreError) as exc:
                out.write(f"[remote_step] poll error, retrying: {exc}\n")
                out.flush()
                time.sleep(poll_interval)
                continue
            if not job:
                out.write(f"[remote_step] job {job_id} not found, retrying\n")
                out.flush()
                time.sleep(poll_interval)
                continue

            status = job["status"]
            container = job.get("container", {})
            new_stream = container.get("logStreamName") or log_stream

            if new_stream and new_stream != log_stream:
                log_stream = new_stream
                if account_id is None:
                    account_id = _account_id_from_job(job) or _sts_account(
                        sts_client, cfg.region
                    )
                if account_id and live_tail is None:
                    live_tail = _LiveTail(
                        logs,
                        _log_group_arn(cfg.region, account_id, cfg.log_group),
                        log_stream,
                        out,
                    )
                    live_tail.start()

            if status in PENDING_STATES:
                if pending_since is None:
                    pending_since = time.time()
                if time.time() - pending_since > pending_timeout:
                    _terminate(batch, job_id, "remote-step: pending timeout")
                    raise PendingTimeoutError(
                        f"job {job_id} stuck in {status} > {pending_timeout / 60:.0f} min. "
                        f"Batch has no capacity for the requested resources. "
                        f"Check compute environment or reduce @resources.",
                        job_id=job_id,
                        status=status,
                    )
            else:
                pending_since = None

            if status in TERMINAL_STATES:
                last_ts = live_tail.last_event_ms if live_tail else 0
                if live_tail is not None:
                    live_tail.stop()
                if log_stream:
                    _drain_after(logs, cfg.log_group, log_stream, out, last_ts)
                return _to_result(job, log_stream or "")

            time.sleep(poll_interval)

    except KeyboardInterrupt as exc:
        if live_tail is not None:
            live_tail.stop()
        out.write("\n[remote_step] Ctrl-C detected, terminating Batch job...\n")
        out.flush()
        _terminate(batch, job_id, "remote-step: user Ctrl-C")
        raise KilledByUser(f"job {job_id} terminated by user", job_id=job_id) from exc


def _account_id_from_job(job: dict) -> str | None:
    """Pull the account id from the job ARN (arn:aws:batch:region:acct:...)."""
    arn = job.get("jobArn", "")
    parts = arn.split(":")
    return parts[4] if len(parts) > 4 else None


def _sts_account(sts_client, region: str) -> str | None:
    try:
        sts = sts_client or boto3.client("sts", region_name=region)
        return sts.get_caller_identity().get("Account")
    except (ClientError, BotoCoreError):
        return None


def _terminate(batch, job_id: str, reason: str) -> None:
    try:
        batch.terminate_job(jobId=job_id, reason=reason)
    except (ClientError, BotoCoreError):
        pass


def _to_result(job: dict, log_stream: str) -> JobResult:
    container = job.get("container", {})
    reason = job.get("statusReason", "") or container.get("reason", "")
    result = JobResult(
        job_id=job["jobId"],
        status=job["status"],
        exit_code=container.get("exitCode"),
        status_reason=reason,
        cw_stream=log_stream,
        started_at=job.get("startedAt"),
        ended_at=job.get("stoppedAt"),
    )
    if result.status == "SUCCEEDED":
        return result
    # FAILED path: classify.
    if "Host EC2" in reason and "terminated" in reason:
        raise SpotInterruptionError(
            f"job {result.job_id} died from EC2 spot interruption. "
            f"Add @retry(times=N) to make remote_step steps self-heal.",
            exit_code=result.exit_code or 143,
            cw_stream=log_stream,
            job_id=result.job_id,
        )
    exit_code = result.exit_code
    raise RunnerError(
        _describe_failure(
            exit_code, reason, log_stream, _extract_region(job), _extract_log_group(job)
        ),
        exit_code=exit_code if exit_code is not None else -1,
        cw_stream=log_stream,
        job_id=result.job_id,
    )


def _extract_region(job: dict) -> str:
    arn = job.get("jobArn", "")
    parts = arn.split(":")
    return parts[3] if len(parts) > 3 else "us-west-2"


def _extract_log_group(job: dict) -> str:
    container = job.get("container") or {}
    log_cfg = container.get("logConfiguration") or {}
    opts = log_cfg.get("options") or {}
    return opts.get("awslogs-group") or "/aws/batch/remote-step-dev"


def _cw_console_url(region: str, log_group: str, log_stream: str) -> str:
    """Build a clickable CloudWatch console URL for the log stream.

    AWS's console uses double URL-encoding with `$` in place of `%` for
    fragment paths — so a literal `/` becomes `$252F` (i.e. `%25` for `%`
    then `2F` for `/`).
    """
    from urllib.parse import quote

    def _cw_encode(s: str) -> str:
        return quote(quote(s, safe=""), safe="").replace("%", "$")

    lg = _cw_encode(log_group)
    ls = _cw_encode(log_stream)
    return (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
        f"#logsV2:log-groups/log-group/{lg}/log-events/{ls}"
    )


def _describe_failure(
    exit_code: int | None,
    reason: str,
    cw_stream: str,
    region: str = "us-west-2",
    log_group: str = "/aws/batch/remote-step-dev",
) -> str:
    hint = {
        1: "user code raised. Check the CloudWatch stream for traceback.",
        137: "OOM (SIGKILL). Bump @resources.memory.",
        139: "segfault in a native library. Try pinning a lower version.",
        143: "SIGTERM (Batch cancelled the container).",
        3: "spec.json load failure inside runner — bug, please file an issue.",
        4: "env install failed. Check @pypi package versions.",
        5: "code-package fetch failed. mfconfig creds may be stale.",
        6: "user step body could not be invoked. Signature mismatch.",
    }.get(exit_code or 0, "runner exited non-zero.")
    url = _cw_console_url(region, log_group, cw_stream)
    return (
        f"Batch job FAILED (exit={exit_code}): {hint}\n"
        f"  reason: {reason}\n"
        f"  logs:   CloudWatch stream {cw_stream}\n"
        f"  open:   {url}"
    )
