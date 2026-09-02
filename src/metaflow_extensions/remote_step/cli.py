"""remote-step CLI — `doctor`, `ls`, `status`, `logs`, `kill`.

Every command takes only a job id (or nothing), so they work from any
directory. Auth resolution is boto3's default chain — profile via
`AWS_PROFILE` or `--profile`.
"""

from __future__ import annotations

import getpass
import os
import sys
import time
from datetime import datetime, timezone

import boto3
import click
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from remote_step.config import RemoteStepConfig, available_envs, load


def _load_cfg(env: str | None) -> RemoteStepConfig:
    try:
        return load(env)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"config error: {exc}", err=True)
        sys.exit(2)


def _session(profile: str | None, region: str) -> boto3.session.Session:
    return boto3.session.Session(profile_name=profile, region_name=region)


def _ok(label: str, detail: str) -> None:
    click.echo(click.style(f"ok    {label:<22}", fg="green") + detail)


def _fail(label: str, detail: str) -> None:
    click.echo(click.style(f"fail  {label:<22}", fg="red") + detail)


def _warn(label: str, detail: str) -> None:
    click.echo(click.style(f"warn  {label:<22}", fg="yellow") + detail)


@click.group()
@click.option("--profile", envvar="AWS_PROFILE", help="AWS profile name.")
@click.option("--env", envvar="REMOTE_STEP_ENV", help="remote-step environment.")
@click.pass_context
def cli(ctx: click.Context, profile: str | None, env: str | None) -> None:
    """remote-step control commands."""
    ctx.ensure_object(dict)
    ctx.obj["profile"] = profile
    ctx.obj["env"] = env


@cli.command()
@click.pass_context
def doctor(ctx: click.Context) -> None:
    """Verify infra + creds + image are usable."""
    cfg = _load_cfg(ctx.obj["env"])
    _ok("remote-step config", f"env {cfg.env_name} · region {cfg.region}")

    try:
        session = _session(ctx.obj["profile"], cfg.region)
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        _ok("aws credentials", identity["Arn"])
    except (NoCredentialsError, ClientError, BotoCoreError) as exc:
        _fail("aws credentials", str(exc))
        return

    batch = session.client("batch")
    for label, qname in (
        ("fargate queue", cfg.fargate_queue),
        ("ec2 cpu queue", cfg.ec2_cpu_queue),
        ("ec2 gpu queue", cfg.ec2_gpu_queue),
    ):
        try:
            resp = batch.describe_job_queues(jobQueues=[qname])
            queues = resp.get("jobQueues", [])
            if queues:
                _ok(label, f"{qname} · {queues[0]['state']}")
            else:
                _fail(label, f"{qname} not found")
        except (ClientError, BotoCoreError) as exc:
            _fail(label, str(exc))

    s3 = session.client("s3")
    try:
        s3.head_bucket(Bucket=cfg.payload_bucket)
        _ok("payload bucket", cfg.payload_bucket)
    except (ClientError, BotoCoreError) as exc:
        _fail("payload bucket", f"{cfg.payload_bucket}: {exc}")

    logs = session.client("logs")
    try:
        resp = logs.describe_log_groups(logGroupNamePrefix=cfg.log_group)
        if any(g["logGroupName"] == cfg.log_group for g in resp.get("logGroups", [])):
            _ok("log group", cfg.log_group)
        else:
            _fail("log group", f"{cfg.log_group} not found")
    except (ClientError, BotoCoreError) as exc:
        _fail("log group", str(exc))

    ecr = session.client("ecr")
    repo_uri = cfg.runner_image.rsplit(":", 1)[0]
    tag = cfg.runner_image.rsplit(":", 1)[1] if ":" in cfg.runner_image else "latest"
    repo_name = repo_uri.split("/")[-1]
    try:
        resp = ecr.describe_images(
            repositoryName=repo_name,
            imageIds=[{"imageTag": tag}],
        )
        pushed = resp["imageDetails"][0]["imagePushedAt"]
        _ok("runner image", f"{repo_name}:{tag} pushed {pushed:%Y-%m-%d %H:%M}")
    except (ClientError, BotoCoreError) as exc:
        _fail("runner image", f"{repo_name}:{tag}: {exc}")


@cli.command()
@click.option("--mine", is_flag=True, help="Only jobs submitted by this user.")
@click.option("--orphaned", is_flag=True, help="Jobs whose driver task is dead.")
@click.pass_context
def ls(ctx: click.Context, mine: bool, orphaned: bool) -> None:
    """List remote-step jobs across queues, newest first."""
    cfg = _load_cfg(ctx.obj["env"])
    session = _session(ctx.obj["profile"], cfg.region)
    batch = session.client("batch")
    rows: list[dict] = []
    for q in (cfg.fargate_queue, cfg.ec2_cpu_queue, cfg.ec2_gpu_queue):
        for state in ("RUNNING", "SUCCEEDED", "FAILED", "RUNNABLE", "PENDING", "STARTING"):
            try:
                resp = batch.list_jobs(jobQueue=q, jobStatus=state)
            except (ClientError, BotoCoreError):
                continue
            rows.extend(resp.get("jobSummaryList", []))
    if not rows:
        click.echo("(no jobs)")
        return
    rows.sort(key=lambda r: r.get("createdAt", 0), reverse=True)
    user = getpass.getuser()
    click.echo(f"{'JOB':<40} {'FLOW':<28} {'STATE':<10} {'STARTED':<20}")
    for r in rows:
        if mine and user not in r.get("jobName", ""):
            continue
        started = r.get("startedAt", 0)
        started_str = (
            datetime.fromtimestamp(started / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            if started
            else "-"
        )
        name = r.get("jobName", "")
        flow = name.split("-")[2] if name.count("-") >= 2 else name
        click.echo(
            f"{r['jobId']:<40} {flow:<28} {r['status']:<10} {started_str:<20}"
        )
    if orphaned:
        click.echo("(orphaned detection requires Metaflow client — not yet wired)")


@cli.command()
@click.argument("job_id")
@click.pass_context
def status(ctx: click.Context, job_id: str) -> None:
    """Show a job's state, driver context, and measured cost."""
    cfg = _load_cfg(ctx.obj["env"])
    session = _session(ctx.obj["profile"], cfg.region)
    batch = session.client("batch")
    resp = batch.describe_jobs(jobs=[job_id])
    jobs = resp.get("jobs", [])
    if not jobs:
        click.echo(f"job {job_id} not found", err=True)
        sys.exit(1)
    j = jobs[0]
    container = j.get("container", {}) or {}
    tags = j.get("tags", {}) or {}
    started = j.get("startedAt")
    stopped = j.get("stoppedAt")
    hourly = float(tags.get("remote_step:hourly_usd", 0))
    duration_h = 0.0
    cost = 0.0
    if started and stopped:
        duration_h = (stopped - started) / 1000 / 3600
        cost = duration_h * hourly

    click.echo(f"job        {j['jobId']}  {j['status']}")
    click.echo(f"flow       {tags.get('remote_step:flow', '?')}  "
               f"user {tags.get('remote_step:user', '?')}")
    click.echo(f"queue      {j.get('jobQueue', '?')}")
    stream = container.get("logStreamName") or "-"
    click.echo(f"logs       {cfg.log_group}:{stream}")
    if started:
        click.echo(f"started    {datetime.fromtimestamp(started / 1000, tz=timezone.utc)}")
    if stopped:
        click.echo(f"ended      {datetime.fromtimestamp(stopped / 1000, tz=timezone.utc)}")
    if hourly:
        click.echo(f"cost       ~${cost:.2f} measured over {duration_h:.2f}h "
                   f"(estimate ${hourly:.3f}/h)")
    reason = j.get("statusReason")
    if reason:
        click.echo(f"reason     {reason}")


@cli.command()
@click.argument("job_id")
@click.option("--follow", is_flag=True, help="Stream to completion.")
@click.pass_context
def logs(ctx: click.Context, job_id: str, follow: bool) -> None:
    """Print (or stream) CloudWatch logs for a job."""
    cfg = _load_cfg(ctx.obj["env"])
    session = _session(ctx.obj["profile"], cfg.region)
    batch = session.client("batch")
    logs_client = session.client("logs")

    resp = batch.describe_jobs(jobs=[job_id])
    jobs = resp.get("jobs", [])
    if not jobs:
        click.echo(f"job {job_id} not found", err=True)
        sys.exit(1)
    stream = (jobs[0].get("container") or {}).get("logStreamName")
    if not stream:
        click.echo("job has not produced a log stream yet", err=True)
        sys.exit(1)
    cursor: str | None = None
    exit_code = 0
    while True:
        kwargs = {
            "logGroupName": cfg.log_group,
            "logStreamName": stream,
            "startFromHead": True,
        }
        if cursor:
            kwargs["nextToken"] = cursor
        try:
            resp = logs_client.get_log_events(**kwargs)
        except (ClientError, BotoCoreError) as exc:
            click.echo(f"log fetch error: {exc}", err=True)
            time.sleep(5)
            continue
        for event in resp.get("events", []):
            click.echo(event["message"])
        cursor = resp.get("nextForwardToken")
        if not follow:
            break
        job = batch.describe_jobs(jobs=[job_id])["jobs"][0]
        if job["status"] in ("SUCCEEDED", "FAILED"):
            container = job.get("container") or {}
            exit_code = container.get("exitCode", 1) if job["status"] == "FAILED" else 0
            break
        time.sleep(3)
    sys.exit(exit_code)


@cli.command()
@click.argument("job_id")
@click.option("--reason", default="remote-step CLI", help="Termination reason string.")
@click.pass_context
def kill(ctx: click.Context, job_id: str, reason: str) -> None:
    """Terminate a running Batch job."""
    cfg = _load_cfg(ctx.obj["env"])
    session = _session(ctx.obj["profile"], cfg.region)
    batch = session.client("batch")
    batch.terminate_job(jobId=job_id, reason=reason)
    click.echo(f"terminated {job_id}")


@cli.command(name="envs")
def envs_cmd() -> None:
    """List available env config files."""
    for name in available_envs():
        click.echo(name)


if __name__ == "__main__":
    cli(obj={})
