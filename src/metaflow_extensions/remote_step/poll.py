"""Block-and-poll loop for AWS Batch jobs.

Called by the driver body after SubmitJob. Polls DescribeJobs every ~15 s,
tails CloudWatch once the container starts, exits on a terminal state.
Network flakes are absorbed indefinitely.
"""

from __future__ import annotations

from dataclasses import dataclass
import sys
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


def _stream_logs(logs_client, log_group: str, stream_name: str, out: IO, cursor: str | None):
    """Fetch new log events since cursor, print to out, return new cursor."""
    kwargs = {
        "logGroupName": log_group,
        "logStreamName": stream_name,
        "startFromHead": True,
    }
    if cursor:
        kwargs["nextToken"] = cursor
    try:
        resp = logs_client.get_log_events(**kwargs)
    except (ClientError, BotoCoreError):
        return cursor
    for event in resp.get("events", []):
        out.write(event["message"] + "\n")
    out.flush()
    next_cursor = resp.get("nextForwardToken")
    if next_cursor == cursor:
        return cursor
    return next_cursor


def wait(
    job_id: str,
    cfg: RemoteStepConfig,
    *,
    out: IO = sys.stdout,
    poll_interval: float = 15.0,
    pending_timeout: float = 60 * 60,
    batch_client=None,
    logs_client=None,
) -> JobResult:
    """Block until the Batch job reaches a terminal state.

    Raises PendingTimeoutError if stuck in RUNNABLE longer than
    `pending_timeout`. Raises RunnerError / SpotInterruptionError on FAILED.
    Raises KilledByUser on KeyboardInterrupt (after terminating the job).
    """
    batch = batch_client or boto3.client("batch", region_name=cfg.region)
    logs = logs_client or boto3.client("logs", region_name=cfg.region)

    cursor: str | None = None
    pending_since: float | None = time.time()
    log_stream: str | None = None
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
            log_stream = container.get("logStreamName") or log_stream

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

            if status == "RUNNING" and log_stream:
                cursor = _stream_logs(logs, cfg.log_group, log_stream, out, cursor)

            if status in TERMINAL_STATES:
                # Final log drain.
                if log_stream:
                    cursor = _stream_logs(logs, cfg.log_group, log_stream, out, cursor)
                return _to_result(job, log_stream or "")

            time.sleep(poll_interval)

    except KeyboardInterrupt as exc:
        out.write("\n[remote_step] Ctrl-C detected, terminating Batch job...\n")
        out.flush()
        _terminate(batch, job_id, "remote-step: user Ctrl-C")
        raise KilledByUser(f"job {job_id} terminated by user", job_id=job_id) from exc


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
    """Build a clickable CloudWatch console URL for the log stream."""
    from urllib.parse import quote

    # CloudWatch console double-encodes slashes as $252F.
    lg = quote(log_group, safe="").replace("%", "$")
    ls = quote(log_stream, safe="").replace("%", "$")
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
