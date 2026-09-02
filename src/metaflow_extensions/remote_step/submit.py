"""AWS Batch SubmitJob wrapper.

Registers a Batch job definition on the fly (name derived from placement +
image), then submits a job that points to the payload spec.json.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import time

import boto3
from botocore.exceptions import ClientError

from remote_step.config import RemoteStepConfig, queue_for
from remote_step.errors import SubmitError
from remote_step.sizing import ResolvedPlacement


THROTTLE_CODES = {"ThrottlingException", "TooManyRequestsException", "RequestLimitExceeded"}


@dataclass
class SubmitResult:
    """Return value of submit(): what got submitted and where."""

    job_id: str
    job_name: str
    queue: str
    job_definition_arn: str


def job_def_name(cfg: RemoteStepConfig, placement: ResolvedPlacement) -> str:
    """Deterministic job-def name so repeated submits reuse the same def."""
    digest = hashlib.sha1(
        f"{cfg.runner_image}|{placement.queue}|{placement.cpu}|"
        f"{placement.memory_mb}|{placement.gpus}|{placement.instance_type or ''}|"
        f"{cfg.log_group}|v2".encode()
    ).hexdigest()[:12]
    return f"remote-step-{cfg.env_name}-{placement.queue}-{digest}"


def ensure_job_definition(
    cfg: RemoteStepConfig,
    placement: ResolvedPlacement,
    batch_client=None,
) -> str:
    """Register a Batch job definition if it doesn't exist, return its ARN."""
    batch = batch_client or boto3.client("batch", region_name=cfg.region)
    name = job_def_name(cfg, placement)
    existing = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
    if existing.get("jobDefinitions"):
        return existing["jobDefinitions"][0]["jobDefinitionArn"]

    log_config = {
        "logDriver": "awslogs",
        "options": {
            "awslogs-group": cfg.log_group,
            "awslogs-region": cfg.region,
            "awslogs-stream-prefix": "remote-step",
            "awslogs-create-group": "true",
        },
    }

    if placement.queue == "fargate":
        platform_capabilities = ["FARGATE"]
        container_properties = {
            "image": cfg.runner_image,
            "command": ["/entrypoint.sh"],
            "jobRoleArn": cfg.job_role_arn,
            "executionRoleArn": cfg.job_execution_role_arn,
            "networkConfiguration": {"assignPublicIp": "ENABLED"},
            "fargatePlatformConfiguration": {"platformVersion": "1.4.0"},
            "resourceRequirements": [
                {"type": "VCPU", "value": str(placement.cpu)},
                {"type": "MEMORY", "value": str(placement.memory_mb)},
            ],
            "ephemeralStorage": {"sizeInGiB": 100},
            "logConfiguration": log_config,
        }
    else:
        platform_capabilities = ["EC2"]
        resource_requirements = [
            {"type": "VCPU", "value": str(placement.cpu)},
            {"type": "MEMORY", "value": str(placement.memory_mb)},
        ]
        if placement.gpus:
            resource_requirements.append(
                {"type": "GPU", "value": str(placement.gpus)}
            )
        container_properties = {
            "image": cfg.runner_image,
            "command": ["/entrypoint.sh"],
            "jobRoleArn": cfg.job_role_arn,
            "executionRoleArn": cfg.job_execution_role_arn,
            "resourceRequirements": resource_requirements,
            "logConfiguration": log_config,
        }

    resp = batch.register_job_definition(
        jobDefinitionName=name,
        type="container",
        platformCapabilities=platform_capabilities,
        containerProperties=container_properties,
        propagateTags=True,
    )
    return resp["jobDefinitionArn"]


def submit(
    cfg: RemoteStepConfig,
    placement: ResolvedPlacement,
    payload_uri: str,
    *,
    flow_name: str,
    run_id: str,
    step_name: str,
    user: str,
    batch_client=None,
    max_attempts: int = 5,
) -> SubmitResult:
    """Submit a Batch job. Returns SubmitResult with job_id."""
    batch = batch_client or boto3.client("batch", region_name=cfg.region)
    job_def_arn = ensure_job_definition(cfg, placement, batch_client=batch)
    queue = queue_for(cfg, placement.queue)

    tags = {
        "remote_step:flow": flow_name,
        "remote_step:run_id": run_id,
        "remote_step:step": step_name,
        "remote_step:user": user,
        "remote_step:env": cfg.env_name,
        "remote_step:hourly_usd": f"{placement.hourly_usd:.4f}",
    }
    env_overrides = [
        {"name": "REMOTE_STEP_SPEC_URI", "value": payload_uri},
        {"name": "REMOTE_STEP_ENV", "value": cfg.env_name},
        {"name": "REMOTE_STEP_LOG_GROUP", "value": cfg.log_group},
    ]
    # Forward git auth so uv can clone private git dependencies in the
    # Batch container. Metaflow's argo pod already has these set via the
    # user's @secrets integration or netrc.
    import os as _os
    import sys as _sys

    _found = False
    for _forward in ("GITHUB_TOKEN", "GIT_TOKEN", "GH_TOKEN"):
        _val = _os.environ.get(_forward)
        if _val:
            env_overrides.append({"name": _forward, "value": _val})
            _sys.stdout.write(
                f"[remote_step] forwarding {_forward} to Batch (len={len(_val)})\n"
            )
            _found = True
            break

    # Also forward the Outerbounds runtime context so user code that talks
    # to Outerbounds integrations (Snowflake, etc.) can reach them from
    # inside the Batch container.
    _obp_forward_prefixes = ("METAFLOW_", "OBP_", "OUTERBOUNDS_")
    for _key, _val in _os.environ.items():
        if any(_key.startswith(p) for p in _obp_forward_prefixes):
            env_overrides.append({"name": _key, "value": _val})

    if not _found:
        _relevant = [k for k in _os.environ if any(
            t in k.upper() for t in ("TOKEN", "GITHUB", "GIT", "AWS_")
        )]
        _sys.stdout.write(
            f"[remote_step] no git token env var found. Related keys: {_relevant}\n"
        )

    import re as _re
    raw_name = f"remote-step-{flow_name}-{run_id}-{step_name}"
    # AWS Batch job names accept [a-zA-Z0-9_-] only, up to 128 chars.
    job_name = _re.sub(r"[^A-Za-z0-9_-]", "-", raw_name)[:128]

    delay = 1.0
    for attempt in range(max_attempts):
        try:
            resp = batch.submit_job(
                jobName=job_name,
                jobQueue=queue,
                jobDefinition=job_def_arn,
                containerOverrides={"environment": env_overrides},
                tags=tags,
                propagateTags=True,
            )
            return SubmitResult(
                job_id=resp["jobId"],
                job_name=resp["jobName"],
                queue=queue,
                job_definition_arn=job_def_arn,
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in THROTTLE_CODES and attempt < max_attempts - 1:
                time.sleep(min(delay, 30))
                delay *= 2
                continue
            if code == "AccessDeniedException":
                raise SubmitError(
                    f"SubmitJob denied for queue {queue}. Check IAM policy "
                    f"and run `remote-step doctor`.",
                    queue=queue,
                    code=code,
                ) from exc
            raise SubmitError(
                f"SubmitJob failed ({code}): {exc}",
                queue=queue,
                code=code,
            ) from exc
    raise SubmitError(
        f"SubmitJob throttled after {max_attempts} attempts",
        queue=queue,
    )
