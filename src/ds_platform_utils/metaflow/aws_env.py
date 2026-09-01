r"""Where AWS jobs run, in one place.

Every flow that decorates a step needs the same four facts -- account, execution role, image, S3
prefix -- and repeating them per flow is how they drift. Each is overridable by environment
variable so a flow can be pointed at another account without editing it.

Defaults target the **sandbox** account (``847068433460``, us-west-2) while the requirements are
still being worked out. We hold AdministratorAccess there, so provisioning is self-service and
nothing waits on a platform request -- the point of running there is to keep moving.

The eventual home is **data-science-prod** (``209479263910``), where Outerbounds runs Metaflow
pods and a ``--with kubernetes`` run would submit jobs in the account it is already running in.
Moving is a change of ``REMOTE_STEP_AWS_ACCOUNT`` and profile: the sandbox resources deliberately
carry the same names and the same ``sagemaker-*`` bucket convention as the prod plan, so nothing
else has to change. ``EXTERNAL_COMPUTE.md`` has the roles prod still needs.

Build and push the image before the first run::

    aws ecr create-repository --repository-name remote-step-runtime --region us-west-2
    aws ecr get-login-password --region us-west-2 \\
      | docker login --username AWS --password-stdin 847068433460.dkr.ecr.us-west-2.amazonaws.com
    docker buildx build --platform linux/amd64 \\
      -t 847068433460.dkr.ecr.us-west-2.amazonaws.com/remote-step-runtime:py311 --push docker/
"""

from __future__ import annotations

import os

AWS_ACCOUNT = os.environ.get("REMOTE_STEP_AWS_ACCOUNT", "847068433460")
AWS_REGION = os.environ.get("REMOTE_STEP_AWS_REGION", "us-west-2")

# Which identity submits the job, and it differs by where the *task* runs.
#
# Locally, Metaflow injects Outerbounds' own task role for the S3 datastore -- it lives in a
# different account and cannot submit jobs here -- so naming a profile picks the identity
# deliberately instead of inheriting whatever the runtime happened to set.
#
# In a pod (`run --with kubernetes`) there is no profile at all. Use :func:`boto_session` rather
# than this value directly; it falls back to the ambient credential chain, which is the pod's
# IRSA role. Setting SAGEMAKER_AWS_PROFILE="" forces that fallback explicitly.
AWS_PROFILE = os.environ.get("REMOTE_STEP_AWS_PROFILE") or os.environ.get("SAGEMAKER_AWS_PROFILE", "sandbox") or None

# ---------------------------------------------------------------------------
# Shared by every backend
# ---------------------------------------------------------------------------

# The image is shared too. It carries a Python matching the submitting side and the packages the
# decorator needs on the far side -- neither of which is specific to SageMaker, so Batch and ECS
# run the same one.
IMAGE_URI = os.environ.get(
    "REMOTE_STEP_IMAGE_URI", f"{AWS_ACCOUNT}.dkr.ecr.{AWS_REGION}.amazonaws.com/remote-step-runtime:py311"
)

# Where payloads and results are staged. Named to match the `sagemaker-*` pattern that
# AmazonSageMakerFullAccess grants on, so a SageMaker execution role reads it without an extra
# policy; other backends need their job role granted explicitly. Create it with:
#   aws s3api create-bucket --bucket sagemaker-us-west-2-847068433460 --region us-west-2 \
#     --create-bucket-configuration LocationConstraint=us-west-2
S3_PREFIX = os.environ.get("REMOTE_STEP_S3_PREFIX", f"s3://sagemaker-{AWS_REGION}-{AWS_ACCOUNT}/remote-step")

# ---------------------------------------------------------------------------
# SageMaker
# ---------------------------------------------------------------------------

SAGEMAKER_ROLE_ARN = os.environ.get(
    "SAGEMAKER_ROLE_ARN", f"arn:aws:iam::{AWS_ACCOUNT}:role/RemoteStepSageMakerExecutionRole"
)

# True inside a Metaflow Kubernetes task, false on a laptop. Metaflow sets the first; the second
# is Kubernetes' own and is the backstop.
IN_KUBERNETES = bool(os.environ.get("METAFLOW_KUBERNETES_POD_NAME") or os.environ.get("KUBERNETES_SERVICE_HOST"))

# How a pod reaches this account. The pod runs as `obp-<id>-task` in Outerbounds' compute account
# (209479263910), whose policies cover its own buckets and nothing else -- a run from a pod fails
# on the first S3 write without this. That role may assume anything tagged
# `outerbounds.com/accessible-by-deployment=pattern`, which is the mechanism Outerbounds provides,
# so RemoteStepSubmitterRole carries that tag and trusts it.
#
# Deliberately not read from an env var alone: Metaflow does not forward arbitrary environment
# variables to Kubernetes tasks, so anything a pod needs has to be a default in code.
#
# Only used in a pod. Locally the SSO profile already has the permissions directly, and it is not
# in the role's trust policy, so assuming would fail.
# Keep the instance alive after a job so the next one skips provisioning and the image pull.
# Only effective on families with warm-pool quota -- see SAGEMAKER_INSTANCE_TYPES. Billed for the
# idle period, so this is a throughput/cost trade, not free.
KEEP_ALIVE_SECONDS = int(os.environ.get("REMOTE_STEP_KEEP_ALIVE_SECONDS", "0"))

SUBMITTER_ROLE_ARN = f"arn:aws:iam::{AWS_ACCOUNT}:role/RemoteStepSubmitterRole"
ASSUME_ROLE_ARN = os.environ.get("REMOTE_STEP_ASSUME_ROLE_ARN") or (SUBMITTER_ROLE_ARN if IN_KUBERNETES else None)


def boto_session(profile_name: str = None):
    """A boto3 session that works both on a laptop and inside a Metaflow pod.

    The same flow runs in two places with different credential arrangements, and a local env var
    does not help because Metaflow does not forward arbitrary environment variables to Kubernetes
    tasks -- which is exactly how a run fails with ``ProfileNotFound`` in the pod after working
    locally. So rather than require the caller to know where it is, this asks for the profile and
    falls back to the ambient chain when there is none.

    :param profile_name: Profile to prefer. Defaults to :data:`AWS_PROFILE`.
    :return: A boto3 Session.
    """
    import boto3
    from botocore.exceptions import ProfileNotFound

    wanted = AWS_PROFILE if profile_name is None else profile_name
    if not wanted:
        return boto3.Session()
    try:
        return boto3.Session(profile_name=wanted)
    except ProfileNotFound:
        # A pod has no profiles; its IRSA role is the ambient identity. Say so, because the
        # identity that submits the job is worth knowing when permissions fail.
        print(f"[aws_env] no '{wanted}' profile here; using ambient credentials (pod IRSA role)")
        return boto3.Session()


BATCH_JOB_QUEUE = os.environ.get("REMOTE_STEP_BATCH_QUEUE", "remote-step-spot")
BATCH_JOB_DEFINITION = os.environ.get("REMOTE_STEP_BATCH_JOB_DEFINITION", "remote-step")


def _shared_backend_settings() -> dict:
    """The arguments every backend takes: who submits, from where, with which image.

    Kept apart from the SageMaker-specific ones so a second backend is a new factory rather than
    another set of ``SAGEMAKER_*`` names bent to a different service.

    :return: Settings common to all backends.
    """
    return {
        "profile_name": AWS_PROFILE,
        "assume_role_arn": ASSUME_ROLE_ARN,
        "image_uri": IMAGE_URI,
        "s3_prefix": S3_PREFIX,
    }


def sagemaker_backend(**overrides):
    """Build a :class:`~ds_platform_utils.metaflow.compute_backends.SageMakerBackend` from the settings above.

    :param overrides: Any backend argument to override, e.g. ``job_kind`` or ``with_snowflake``.
    :return: A configured backend.
    """
    from .compute_backends import SageMakerBackend

    settings = {
        **_shared_backend_settings(),
        "role_arn": SAGEMAKER_ROLE_ARN,
        "job_kind": "training",
        "keep_alive_seconds": KEEP_ALIVE_SECONDS,
    }
    settings.update(overrides)
    return SageMakerBackend(**settings)


def batch_backend(**overrides):
    """Build a :class:`~ds_platform_utils.metaflow.compute_backends.BatchBackend` on Fargate Spot.

    The queue and job definition are created once per account -- see ``EXTERNAL_COMPUTE.md``. The
    job definition is reused for every step; sizing, command and environment are container
    overrides at submit time, so no revision accumulates.

    :param overrides: Any backend argument to override, e.g. ``with_snowflake``.
    :return: A configured backend.
    """
    from .compute_backends import BatchBackend

    # The image is dropped deliberately: Batch pins it in the job definition and refuses it as a
    # container override, so passing it here would be a setting that quietly does nothing.
    shared = {key: value for key, value in _shared_backend_settings().items() if key != "image_uri"}
    settings = {
        **shared,
        "job_queue": BATCH_JOB_QUEUE,
        "job_definition": BATCH_JOB_DEFINITION,
    }
    settings.update(overrides)
    return BatchBackend(**settings)
