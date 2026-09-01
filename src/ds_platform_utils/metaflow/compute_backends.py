"""The seam between "run this step body somewhere else" and "somewhere else".

Almost none of ``@remote_step`` cares which compute service is on the far side. Working out which
``self.X`` to ship, standing in for ``self``, encoding DataFrames, bundling files behind ``Path``
globals, guarding the Python version, streaming logs -- all of that is the same wherever the body
runs. Exactly one thing differs: how a payload is handed to a compute service and how its result
comes back.

This module is that one thing. A backend takes a :class:`RemoteCall` and returns a
:class:`JobHandle`; the decorator does everything else. Adding ECS, Batch or anything else means
implementing ``submit()``, not touching the decorator.

A handle exposes four members -- ``id``, ``status``, ``get_logs()``, ``result()`` -- which is all
:func:`ds_platform_utils.metaflow.remote_runtime.await_job` needs to turn a running job into live log output.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Protocol, runtime_checkable

from .snowflake_access import SNOWFLAKE_SECRET_ENV_VAR


@dataclass(frozen=True)
class Resources:
    """What a step asks for. Backends honour this as far as their service allows.

    SageMaker and ECS take an instance/task size per job, so they honour these directly. A
    backend that cannot size a job precisely is expected to round up and say so in ``describe``.
    The decorator does not care which -- it passes the request down and lets the backend explain
    itself.
    """

    cpu: int = 0
    memory: int = 0  # GB
    gpu: int = 0
    instances: int = 1


@dataclass(frozen=True)
class RemoteCall:
    """Everything a backend needs to run one step body, and nothing about how it runs it.

    Deliberately plain data: no sessions, no clients, no open connections. A backend can inspect
    it, log it, or serialise it without needing the decorator's context.
    """

    step_name: str
    fn_bytes: bytes
    """The step body, cloudpickled by value."""

    inputs: dict[str, Any]
    """Artifacts the body reads, already encoded (DataFrames as parquet)."""

    write_names: list[str]
    """Attributes to collect from the proxy afterwards."""

    step_names: list[str]
    """Flow step names, so ``self.<step>`` resolves in ``self.next(...)``."""

    python_version: tuple[int, int]
    """Checked before unpickling: the body is bytecode and does not cross minor versions."""

    code_dirs: list[tuple[str, str]] = field(default_factory=list)
    """``(directory, import_name)`` packages to make importable in the remote environment."""

    pip_requirements: list[str] = field(default_factory=list)
    """Packages to install before the body runs."""

    path_bundle: bytes = b""
    """Zipped files behind the body's ``Path`` globals."""

    path_map: dict[str, str] = field(default_factory=dict)
    """``{global_name: path_inside_archive}``."""

    resources: Resources = field(default_factory=Resources)


@runtime_checkable
class JobHandle(Protocol):
    """A submitted job, however the backend represents one."""

    @property
    def id(self) -> str:
        """Identifier to print and to look up server-side."""

    terminal_statuses: frozenset
    """States meaning the job has finished, in whatever words the service uses."""

    @property
    def status(self) -> str:
        """Current state, matched against :attr:`terminal_statuses`."""

    def get_logs(self) -> str:
        """All output so far. Polled while the job runs, so it must be safe to call repeatedly."""

    def result(self) -> bytes:
        """The pickled result, or raise whatever the body raised."""

    def cancel(self) -> None:
        """Stop the job and release anything staged for it.

        Called when the *waiting* side goes away -- an interrupt, or the pod being evicted. The
        job does not care that nobody is listening: it runs to completion or to its stopping
        condition, billing all the while, so somebody has to say stop.
        """


class ComputeBackend(Protocol):
    """Somewhere a step body can run."""

    name: str

    def describe(self, call: RemoteCall) -> str:
        """One line for the step log: where this is going and with what."""

    def submit(self, call: RemoteCall) -> JobHandle:
        """Start the job and return a handle. Must not block until completion."""


# SageMaker instance types we are prepared to select from, and what each provides. This is a
# per-job choice -- nothing has to be provisioned in advance, which is the main reason this
# approach is cheaper to operate than a standing pool.
# m7i/c7i rather than m5, and the reason is warm pools. Every account we checked carries a
# warm-pool quota of 0 for the m5 family and 30 for these -- so on m5 `keep_alive_seconds` is
# silently ineffective, which is the difference between a ~90s floor and a warm one. m7i is also a
# newer generation, so this costs nothing. Check `<type> for training warm pool usage` in Service
# Quotas before adding a family here.
SAGEMAKER_INSTANCE_TYPES: dict[str, dict[str, int]] = {
    "ml.m7i.large": {"cpu": 2, "memory": 8, "gpu": 0},
    "ml.m7i.xlarge": {"cpu": 4, "memory": 16, "gpu": 0},
    "ml.m7i.2xlarge": {"cpu": 8, "memory": 32, "gpu": 0},
    "ml.m7i.4xlarge": {"cpu": 16, "memory": 64, "gpu": 0},
    "ml.m7i.12xlarge": {"cpu": 48, "memory": 192, "gpu": 0},
    # No warm-pool quota on the GPU families, so a GPU step pays the cold floor every time.
    "ml.g4dn.xlarge": {"cpu": 4, "memory": 16, "gpu": 1},
    "ml.g5.2xlarge": {"cpu": 8, "memory": 32, "gpu": 1},
}

DEFAULT_SAGEMAKER_INSTANCE = "ml.m7i.large"

# Hard ceiling on a job's runtime, matching SageMaker's own default of one day.
#
# Deliberately generous. This decorator exists for the *heavy* steps -- a real training step can
# easily run over an hour, and a cap that kills legitimate work is worse than one that lets a
# stranded job run: the first breaks the flow every time, the second costs money occasionally.
#
# It is not the cost guard. Handles cancel themselves when the waiting step goes away, which covers
# interrupts and pod eviction; this only backstops SIGKILL, where nothing can run. Lower it per
# backend for a scheduled flow whose duration you know -- `max_runtime_seconds=` on the backend --
# and treat that as a safety net, not a timeout you expect to hit.
DEFAULT_MAX_RUNTIME_SECONDS = 24 * 60 * 60

# Secrets are named for the job with this prefix so the SageMaker execution role can read them
# under AmazonSageMakerFullAccess, which grants GetSecretValue on `AmazonSageMaker-*` only.
# Changing this means granting the execution role access to the new name explicitly.
SNOWFLAKE_SECRET_PREFIX = "AmazonSageMaker-remote-step-"


# What the *submitting* side needs, for any AWS backend. pandas and pyarrow because the decorator
# encodes DataFrames as parquet before handing them over; boto3 to submit the job. A flow whose
# body reads Snowflake adds ds-platform-utils and the connector itself -- see snowflake_access.
def _self_requirement() -> tuple:
    """This library, as a pip requirement pinned to the ref that is actually installed.

    ``@pypi_base`` builds an *isolated* environment holding only what it is told, so a flow using
    the decorator has to declare the library the decorator lives in -- otherwise the submitting
    step fails with ``ModuleNotFoundError: ds_platform_utils`` before it reaches any compute.

    The ref is read from the installed distribution rather than hard-coded, because a hard-coded
    ``@main`` would silently give a branch-testing flow a different version of the decorator on the
    submitting side than the one it is being tested with.

    :return: ``(name, version)`` in ``@pypi`` shape.
    """
    import json
    from importlib.metadata import distribution

    try:
        info = json.loads(distribution("ds-platform-utils").read_text("direct_url.json") or "{}")
        url, vcs = info["url"], info["vcs_info"]
        return (f"git+{url}", f"@{vcs.get('requested_revision') or vcs['commit_id']}")
    except Exception:
        # Installed from an index rather than git, or metadata missing. Fall back to the version.
        from importlib.metadata import version

        try:
            return ("ds-platform-utils", version("ds-platform-utils"))
        except Exception:
            return ("ds-platform-utils", "")


BACKEND_PACKAGES = {
    "boto3": "",
    "cloudpickle": "",
    "pandas": "2.2.3",
    "pyarrow": "",
    # The decorator itself. Resolved at import so a flow inherits the ref it installed.
    _self_requirement()[0]: _self_requirement()[1],
}

# Was SageMaker-specific before Batch existed, and flows still import it by that name.
SAGEMAKER_BACKEND_PACKAGES = BACKEND_PACKAGES

# What ``docker/Dockerfile`` bakes in. Steps inherit the flow's ``@pypi_base`` packages, and most
# of them are already here -- this is what keeps that inheritance cheap. Keep it in step with the
# Dockerfile: a name listed here but missing from the image becomes a ModuleNotFoundError in a
# container, and one missing here is only a slower job.
IMAGE_PACKAGES = frozenset(
    {
        "boto3",
        "cloudpickle",
        "ds-platform-utils",
        "numpy",
        "pandas",
        "pyarrow",
        "scikit-learn",
        "snowflake-connector-python",
        "xgboost",
    }
)


def requirement_name(requirement: str) -> str:
    """Distribution name a pip requirement refers to.

    Handles the three shapes that reach here: a bare name, a name with a specifier, and a VCS URL
    (``git+https://github.com/patterninc/ds-platform-utils.git@main`` -> ``ds-platform-utils``).

    :param requirement: A pip requirement string.
    :return: Normalised distribution name.
    """
    if "://" in requirement:
        tail = requirement.rstrip("/").split("/")[-1]
        name = re.split(r"[.@]", tail)[0]
    else:
        name = re.split(r"[<>=!~\[;\s]", requirement, maxsplit=1)[0]
    return name.strip().replace("_", "-").lower()


def filter_preinstalled(
    requirements: Iterable[str],
    available: frozenset[str] = frozenset(),
    never_install: frozenset[str] = frozenset(),
) -> list[str]:
    """Drop requirements a container should not spend time installing.

    Steps inherit the whole of the flow's ``@pypi_base``, which is what makes a body import the
    same names wherever it runs. This is the other half of that trade: without it every job pays
    to reinstall what the runtime already had.

    Two different reasons to skip, kept apart on purpose:

    - ``available`` -- the runtime ships it. Only *unversioned* requirements are dropped. A flow
      that pins a version means it, and letting pip confirm an already-satisfied pin costs about a
      second, which is cheaper than silently running against a different version than was asked
      for. VCS requirements are always dropped when available, and for SageMaker that is not an
      optimisation: the image purges git, so ``git+https://`` could not install at all.
    - ``never_install`` -- installing it would break something, pinned or not.

    :param requirements: pip requirement strings.
    :param available: Names the runtime already provides.
    :param never_install: Names to drop regardless of any version specifier.
    :return: What is left to install.
    """
    kept = []
    for requirement in requirements:
        name = requirement_name(requirement)
        if name in never_install:
            continue
        unversioned = "://" in requirement or name == requirement.strip().replace("_", "-").lower()
        if name in available and unversioned:
            continue
        kept.append(requirement)
    return kept


def resolve_instance_type(cpu: int = 0, memory: int = 0, gpu: int = 0) -> str:
    """Pick the smallest instance type that satisfies a request.

    Nothing has to be provisioned first -- the instance is chosen per job, so a step asking for
    more simply gets a bigger machine for the minutes it runs.

    :param cpu: Minimum CPU cores.
    :param memory: Minimum memory in GB.
    :param gpu: Minimum GPUs.
    :return: An instance type name.
    :raises RuntimeError: If nothing in the table is big enough.
    """
    if not (cpu or memory or gpu):
        return DEFAULT_SAGEMAKER_INSTANCE

    candidates = [
        (name, spec)
        for name, spec in SAGEMAKER_INSTANCE_TYPES.items()
        if spec["cpu"] >= cpu and spec["memory"] >= memory and spec["gpu"] >= gpu
    ]
    if not candidates:
        raise RuntimeError(
            f"No instance type in SAGEMAKER_INSTANCE_TYPES satisfies cpu={cpu}, memory={memory}GB, "
            f"gpu={gpu}. Add a bigger one -- AWS has plenty; the table is just what we allow."
        )
    name, _ = min(candidates, key=lambda item: (item[1]["gpu"], item[1]["cpu"], item[1]["memory"]))
    return name


def _tail_for_failure(logs: str, lines: int = 25) -> str:
    """The end of a container's log, which is where a Python traceback ends up.

    SageMaker's own ``FailureReason`` for a crashed body is ``"AlgorithmError: , exit code: 1"`` --
    true and useless. The actual cause is in the container's output, so a failure raised without it
    forces a reader to go hunting for the log that was streamed minutes earlier. In the Outerbounds
    UI, where the whole log arrives in one dump at the end, that is the line most likely to be read
    and the least likely to help.

    :param logs: Everything the container wrote.
    :param lines: How many trailing lines to keep.
    :return: The tail, or empty when there is nothing.
    """
    kept = [line for line in (logs or "").splitlines() if line.strip()][-lines:]
    return "\n".join(kept)


def _failure_message(job_name: str, reason: Optional[str], logs: str) -> str:
    """Build an exception message that leads with what actually went wrong.

    :param job_name: The failed job.
    :param reason: SageMaker's ``FailureReason``.
    :param logs: Container output, for the traceback.
    :return: A message worth reading.
    """
    tail = _tail_for_failure(logs)
    detail = f"\n--- container output (tail) ---\n{tail}" if tail else ""
    return f"{job_name} failed: {reason or 'no reason given'}{detail}"


# What a container is told to run. Deliberately not baked into the image: the entrypoint has to
# match the code that submitted the job, and an image is a separate artifact that goes stale
# independently. Shipping it per job is the same reasoning as shipping `code.zip` rather than
# baking `helpers` in.
#
# Only boto3 is assumed, which every candidate runtime already needs in order to reach S3.
CONTAINER_BOOTSTRAP = (
    "import boto3,os;"
    "u=os.environ['REMOTE_STEP_ENTRYPOINT_URI'];"
    "b,_,k=u.removeprefix('s3://').partition('/');"
    "boto3.client('s3').download_file(b,k,'/tmp/entrypoint.py');"
    "exec(open('/tmp/entrypoint.py').read(),{'__name__':'__main__'})"
)

CONTAINER_COMMAND = ["python3", "-u", "-c", CONTAINER_BOOTSTRAP]


def stage_call(s3_client: Any, job_prefix: str, call: RemoteCall, requirements: list[str]) -> dict:
    """Put everything a container needs into S3 and return the environment that points at it.

    This is the whole transport, and it is deliberately backend-agnostic. SageMaker offers S3
    input channels; Batch, ECS and EKS offer nothing at all. Building on the lowest common
    denominator -- three S3 URIs in environment variables -- means a new backend has to submit a
    job and nothing more, rather than reimplementing payload delivery.

    :param s3_client: A boto3 S3 client.
    :param job_prefix: ``s3://bucket/prefix`` unique to this job.
    :param call: The step body and everything it needs.
    :param requirements: pip requirements for the container, after filtering.
    :return: Environment variables naming the staged objects.
    """
    import io
    import pickle
    import zipfile

    bucket, _, key_prefix = job_prefix.removeprefix("s3://").partition("/")

    # The packages the body imports, plus this repo's helpers, as one archive.
    code_buffer = io.BytesIO()
    with zipfile.ZipFile(code_buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for directory, import_name in call.code_dirs:
            for path in sorted(Path(directory).rglob("*.py")):
                archive.write(path, f"{import_name}/{path.relative_to(directory)}")
    s3_client.put_object(Bucket=bucket, Key=f"{key_prefix}/input/code.zip", Body=code_buffer.getvalue())

    payload = {
        "fn_bytes": call.fn_bytes,
        "inputs": call.inputs,
        "write_names": call.write_names,
        "step_names": call.step_names,
        "path_bundle": call.path_bundle,
        "path_map": call.path_map,
        "python_version": list(call.python_version),
        "pip_requirements": requirements,
    }
    s3_client.put_object(Bucket=bucket, Key=f"{key_prefix}/input/payload.pkl", Body=pickle.dumps(payload))

    entrypoint = Path(__file__).parent / "container_entrypoint.py"
    s3_client.put_object(Bucket=bucket, Key=f"{key_prefix}/input/entrypoint.py", Body=entrypoint.read_bytes())

    return {
        "REMOTE_STEP_ENTRYPOINT_URI": f"{job_prefix}/input/entrypoint.py",
        "REMOTE_STEP_PAYLOAD_URI": f"{job_prefix}/input/payload.pkl",
        "REMOTE_STEP_CODE_URI": f"{job_prefix}/input/code.zip",
        "REMOTE_STEP_RESULT_URI": f"{job_prefix}/output/result.pkl",
        # Container stdout is a pipe, not a TTY, so Python block-buffers it and a body's print()
        # calls sit unseen until the process exits -- indistinguishable from "logs only appear at
        # the end". The command also passes -u; this covers anything the body subprocesses.
        "PYTHONUNBUFFERED": "1",
    }


# SageMaker's terminal states. Batch and ECS use different words; each handle declares its own
# rather than translating into a shared vocabulary, which is what lets a backend be added without
# touching the polling loop.
SAGEMAKER_TERMINAL = frozenset({"Completed", "Failed", "Stopped"})


class SageMakerTrainingHandle:
    """A running SageMaker Training job.

    Training rather than Processing for one reason: **warm pools**. A training job can set
    ``KeepAlivePeriodInSeconds`` and keep its instance alive for the next job, which turns a
    ~2 minute provision-and-pull into seconds. Processing jobs have no equivalent field.

    The result comes back through the same S3 URI every backend uses, not through SageMaker's
    ``model.tar.gz`` convention -- so there is no tarball to unpack and nothing about the return
    path that another backend would have to imitate.
    """

    terminal_statuses = SAGEMAKER_TERMINAL

    def __init__(self, job_name: str, output_uri: str, session: Any, secret_name: Optional[str] = None) -> None:
        self._job_name = job_name
        self._output_uri = output_uri
        self._session = session
        self._secret_name = secret_name
        self._sagemaker = session.client("sagemaker")
        self._logs = session.client("logs")

    @property
    def id(self) -> str:
        """The training job name."""
        return self._job_name

    def _discard_secret(self) -> None:
        """Delete the short-lived credential, whatever happened to the job.

        Without recovery: the token is minutes-lived anyway, and leaving it in the deletion window
        would defeat the point of making it ephemeral.
        """
        if not self._secret_name:
            return
        try:
            self._session.client("secretsmanager").delete_secret(
                SecretId=self._secret_name, ForceDeleteWithoutRecovery=True
            )
        except Exception as exc:  # never let cleanup mask the job's own outcome
            print(f"[sagemaker] could not delete {self._secret_name}: {exc}")

    @property
    def status(self) -> str:
        """SageMaker's own status, which is the vocabulary the log streamer terminates on."""
        return self._sagemaker.describe_training_job(TrainingJobName=self._job_name)["TrainingJobStatus"]

    def cancel(self) -> None:
        """Stop the job and delete its staged credentials. Safe to call on an already-stopped job."""
        try:
            self._sagemaker.stop_training_job(TrainingJobName=self._job_name)
            print(f"[sagemaker] stopped {self._job_name}")
        except Exception as exc:  # already terminal, or gone
            print(f"[sagemaker] could not stop {self._job_name}: {exc}")
        self._discard_secret()

    def get_logs(self) -> str:
        """Container output so far, from the training log group."""
        return _read_log_stream(self._logs, "/aws/sagemaker/TrainingJobs", self._job_name)

    def result(self) -> bytes:
        """Read the pickled result the container uploaded.

        :raises RuntimeError: If the job failed, with the container's own traceback.
        """
        try:
            described = self._sagemaker.describe_training_job(TrainingJobName=self._job_name)
            if described["TrainingJobStatus"] == "Failed":
                raise RuntimeError(_failure_message(self._job_name, described.get("FailureReason"), self.get_logs()))

            bucket, _, key = self._output_uri.removeprefix("s3://").partition("/")
            return self._session.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        finally:
            self._discard_secret()


def _read_log_stream(logs_client: Any, group: str, prefix: str) -> str:
    """Return everything written to a job's log stream so far.

    Empty until the stream exists -- a job that has not started writing has none, and the streamer
    already tolerates that.

    :param logs_client: A CloudWatch Logs client.
    :param group: Log group name.
    :param prefix: Log stream name prefix, which is the job name.
    :return: The stream's messages, newline-joined.
    """
    streams = logs_client.describe_log_streams(logGroupName=group, logStreamNamePrefix=prefix).get("logStreams", [])
    if not streams:
        return ""
    events = logs_client.get_log_events(
        logGroupName=group, logStreamName=streams[0]["logStreamName"], startFromHead=True
    )["events"]
    return "".join(f"{event['message']}\n" for event in events)


class SageMakerJobHandle:
    """A running SageMaker Processing job, shaped like the handle the decorator expects.

    ``get_logs`` reads CloudWatch rather than the job itself, because that is where a processing
    container's stdout goes. It returns everything so far each time, which is what the log streamer
    assumes.
    """

    terminal_statuses = SAGEMAKER_TERMINAL

    def __init__(self, job_name: str, result_uri: str, session: Any) -> None:
        self._job_name = job_name
        self._result_uri = result_uri
        self._session = session
        self._sagemaker = session.client("sagemaker")
        self._logs = session.client("logs")

    @property
    def id(self) -> str:
        """The processing job name, which is how you find it in the console."""
        return self._job_name

    @property
    def status(self) -> str:
        """SageMaker's own status, which is the vocabulary the log streamer terminates on."""
        return self._sagemaker.describe_processing_job(ProcessingJobName=self._job_name)["ProcessingJobStatus"]

    def cancel(self) -> None:
        """Stop the job. Safe to call on an already-stopped job."""
        try:
            self._sagemaker.stop_processing_job(ProcessingJobName=self._job_name)
            print(f"[sagemaker] stopped {self._job_name}")
        except Exception as exc:  # already terminal, or gone
            print(f"[sagemaker] could not stop {self._job_name}: {exc}")

    def get_logs(self) -> str:
        """Return the container's output so far, from CloudWatch.

        Empty until the log stream exists -- a job that has not started writing has no stream, and
        the streamer already tolerates a failed fetch.
        """
        return _read_log_stream(self._logs, "/aws/sagemaker/ProcessingJobs", self._job_name)

    def result(self) -> bytes:
        """Read the pickled result back from S3.

        :raises RuntimeError: If the job failed, with SageMaker's own failure reason.
        """
        described = self._sagemaker.describe_processing_job(ProcessingJobName=self._job_name)
        if described["ProcessingJobStatus"] == "Failed":
            raise RuntimeError(_failure_message(self._job_name, described.get("FailureReason"), self.get_logs()))

        bucket, _, key = self._result_uri.removeprefix("s3://").partition("/")
        return self._session.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()


class SageMakerBackend:
    """Runs the body as a SageMaker training or processing job.

    ``job_kind`` picks which. Training jobs are the default and the better one: they support warm
    pools, where processing jobs do not, and a warm pool is the only lever that meaningfully cuts
    the per-job startup cost. Processing jobs remain useful for work that fits their input/output
    conventions more naturally.

    What this buys over running the body on the Metaflow pod: the step's task stays small, so
    Outerbounds bills it at the small band while the work runs on an instance sized per job.

    What it costs: **data locality and startup**. Measured overhead is ~90-110s before the body
    starts, so a step must be long enough to amortise it. A body that reads Snowflake also needs
    credentials staged for it -- see :mod:`ds_platform_utils.metaflow.snowflake_access`, and prefer unloading to S3
    over holding a connection open for large reads.

    See ``container_entrypoint.py`` for what runs inside, and the module docstring of
    ``src/sagemaker_processing_flow.py`` for what has to exist in AWS first.
    """

    name = "sagemaker"

    def __init__(  # noqa: PLR0913
        self,
        role_arn: str,
        image_uri: str,
        s3_prefix: str,
        instance_type: Optional[str] = None,
        volume_size_gb: int = 30,
        max_runtime_seconds: int = DEFAULT_MAX_RUNTIME_SECONDS,
        job_kind: str = "training",
        with_snowflake: bool = False,
        keep_alive_seconds: int = 0,
        profile_name: Optional[str] = None,
        assume_role_arn: Optional[str] = None,
        image_packages: Optional[Iterable[str]] = None,
        boto_session: Any = None,
    ) -> None:
        self.role_arn = role_arn
        self.image_uri = image_uri
        self.s3_prefix = s3_prefix.rstrip("/")
        self.instance_type = instance_type
        self.volume_size_gb = volume_size_gb
        self.max_runtime_seconds = max_runtime_seconds
        self.job_kind = job_kind
        self.with_snowflake = with_snowflake
        self.keep_alive_seconds = keep_alive_seconds
        self.profile_name = profile_name
        self.assume_role_arn = assume_role_arn
        self.image_packages = frozenset(IMAGE_PACKAGES if image_packages is None else image_packages)
        self._boto_session = boto_session

    @property
    def session(self) -> Any:
        """A boto3 session, created lazily so importing this module needs no AWS credentials.

        ``profile_name`` matters more than it looks. A Metaflow step does not run with your shell's
        credentials -- Outerbounds injects its own task role for the S3 datastore, which lives in a
        different account and cannot write here. Naming a profile picks the identity deliberately
        instead of inheriting whatever the runtime happened to set.

        In production the equivalent is the pod's IRSA role holding cross-account permissions;
        this is the local stand-in for that.
        """
        if self._boto_session is None:
            import boto3
            from botocore.exceptions import ProfileNotFound

            try:
                base = boto3.Session(profile_name=self.profile_name)
            except ProfileNotFound:
                # A Metaflow pod has no AWS profiles -- its IRSA role is the ambient identity.
                # Failing here would mean a flow that works locally dies the moment it runs
                # `--with kubernetes`, which is the deployment that matters.
                print(f"[sagemaker] no '{self.profile_name}' profile here; using ambient credentials")
                base = boto3.Session()

            self._boto_session = self._assume(base) if self.assume_role_arn else base
        return self._boto_session

    def _assume(self, base: Any) -> Any:
        """Return a session for ``assume_role_arn``, refreshing its credentials as needed.

        This is what makes ``--with kubernetes`` work. The pod runs as an Outerbounds task role in
        Outerbounds' own account, which has no reason to hold permissions in ours; rather than ask
        a vendor to carry our policies, the pod assumes a role we own and everything downstream
        uses that identity.

        Credentials refresh automatically. A plain ``assume_role`` call returns credentials that
        expire in an hour, and this session is held for the whole life of a job -- a long fit would
        fail partway through polling, which is a miserable way to lose an hour of compute.

        :param base: The session whose identity does the assuming.
        :return: A session using the assumed role.
        """
        import boto3
        import botocore.session
        from botocore.credentials import DeferredRefreshableCredentials

        sts = base.client("sts")

        def fetch():
            response = sts.assume_role(RoleArn=self.assume_role_arn, RoleSessionName="remote-step")
            credentials = response["Credentials"]
            return {
                "access_key": credentials["AccessKeyId"],
                "secret_key": credentials["SecretAccessKey"],
                "token": credentials["SessionToken"],
                "expiry_time": credentials["Expiration"].isoformat(),
            }

        # A *fresh* botocore session, not `boto3.Session()._session`. boto3 registers its resource
        # injections on the session it builds, and handing that same session to a second
        # boto3.Session runs them again -- which fails with
        # 'Cannot inject class attribute "upload_file", attribute already exists in class dict.'
        botocore_session = botocore.session.get_session()
        botocore_session._credentials = DeferredRefreshableCredentials(fetch, "assume-role")
        print(f"[sagemaker] assuming {self.assume_role_arn}")
        return boto3.Session(botocore_session=botocore_session, region_name=base.region_name)

    def _instance_for(self, resources: Resources) -> str:
        """Chosen instance type, honouring an explicit override."""
        return self.instance_type or resolve_instance_type(
            cpu=resources.cpu, memory=resources.memory, gpu=resources.gpu
        )

    def describe(self, call: RemoteCall) -> str:
        """Report the job kind and instance shape, which here is a real per-job choice."""
        instance = self._instance_for(call.resources)
        count = f" x{call.resources.instances}" if call.resources.instances > 1 else ""
        warm = f", warm pool {self.keep_alive_seconds}s" if self.keep_alive_seconds else ""
        return f"sagemaker {self.job_kind} job on {instance}{count}{warm}"

    def submit(self, call: RemoteCall) -> JobHandle:
        """Stage the payload to S3 and start a processing job.

        The container entrypoint reads that payload and calls ``execute_remote_step`` -- the same
        function any backend runs, unchanged, because it takes plain arguments.

        :param call: The step body and everything it needs.
        :return: A handle wrapping the processing job.
        """
        import time

        # The body is cloudpickled, and cloudpickle's own format moves between versions -- a stock
        # image's older copy fails with "Can't get attribute '_function_setstate'". Pin the loader
        # to whatever did the dumping.
        from importlib.metadata import version as installed_version

        requirements = [
            *filter_preinstalled(
                [r for r in call.pip_requirements if not r.startswith("cloudpickle")],
                available=self.image_packages,
            ),
            f"cloudpickle=={installed_version('cloudpickle')}",
        ]

        # The random suffix is not decoration. A foreach submits its branches within the same
        # second, so a name built from the step and a whole-second timestamp collides -- observed
        # as ResourceExistsException from CreateSecret, killing the run after the branches had
        # already started. The name also keys the S3 prefix and the job itself, so it has to be
        # unique per submission rather than per step per second.
        unique = uuid.uuid4().hex[:8]
        job_name = f"{call.step_name.replace('_', '-')}-{int(time.time())}-{unique}"[:63]
        job_prefix = f"{self.s3_prefix}/{job_name}"

        environment = stage_call(self.session.client("s3"), job_prefix, call, requirements)
        environment["AWS_DEFAULT_REGION"] = self.session.region_name or "us-west-2"

        if self.job_kind == "training":
            return self._submit_training(job_name, job_prefix, call, environment)

        secret_name = self._stage_snowflake_credentials(job_name)
        if secret_name:
            environment[SNOWFLAKE_SECRET_ENV_VAR] = secret_name

        self.session.client("sagemaker").create_processing_job(
            ProcessingJobName=job_name,
            RoleArn=self.role_arn,
            AppSpecification={"ImageUri": self.image_uri, "ContainerEntrypoint": CONTAINER_COMMAND},
            Environment=environment,
            ProcessingResources={
                "ClusterConfig": {
                    "InstanceCount": call.resources.instances,
                    "InstanceType": self._instance_for(call.resources),
                    "VolumeSizeInGB": self.volume_size_gb,
                }
            },
            StoppingCondition={"MaxRuntimeInSeconds": self.max_runtime_seconds},
        )

        return SageMakerJobHandle(job_name, environment["REMOTE_STEP_RESULT_URI"], self.session, secret_name)

    def _stage_snowflake_credentials(self, job_name: str) -> Optional[str]:
        """Mint a short-lived Snowflake token and park it in Secrets Manager for one job.

        This piggybacks on the ``snowflake-default`` Outerbounds integration that flows already
        use, so no service user or key pair is involved -- the same security integration, user and
        role, just reached from a step that can call it. The token never touches the payload or S3;
        only the secret's name does, and the handle deletes it when the job ends.

        The ``AmazonSageMaker-`` prefix is load-bearing, not cosmetic. ``AmazonSageMakerFullAccess``
        grants ``GetSecretValue`` only on secrets matching that prefix (or tagged
        ``SageMaker=true``), so a differently-named secret leaves the container unable to read its
        own credentials -- with no IAM change needed as long as the name matches.

        :param job_name: Used to name the secret, so an orphan is traceable to its job.
        :return: The secret's name, or None when Snowflake access was not requested.
        """
        if not self.with_snowflake:
            return None

        import json

        from metaflow_extensions.outerbounds.plugins.snowflake.snowflake import get_oauth_connection_params

        params = get_oauth_connection_params(integration="snowflake-default")
        secret_name = f"{SNOWFLAKE_SECRET_PREFIX}{job_name}"
        self.session.client("secretsmanager").create_secret(
            Name=secret_name,
            SecretString=json.dumps(params),
            Description="Short-lived Snowflake OAuth token for one remote_step job. Safe to delete.",
        )
        print(f"[sagemaker] staged Snowflake credentials as {secret_name} (deleted when the job ends)")
        return secret_name

    def _submit_training(self, job_name: str, job_prefix: str, call: RemoteCall, environment: dict) -> JobHandle:
        """Start a training job instead of a processing job.

        Same payload, different API. The reason to prefer it is ``KeepAlivePeriodInSeconds``:
        with a warm pool the next job reuses this instance, skipping provisioning and image pull.

        :param job_name: Name for the job.
        :param job_prefix: S3 prefix holding the staged payload.
        :param call: The step body and everything it needs.
        :param environment: Variables from :func:`stage_call`, naming the staged objects.
        :return: A handle wrapping the training job.
        """
        secret_name = self._stage_snowflake_credentials(job_name)
        if secret_name:
            # Only the secret's *name* travels; the token itself never leaves Secrets Manager.
            environment[SNOWFLAKE_SECRET_ENV_VAR] = secret_name

        resource_config = {
            "InstanceCount": call.resources.instances,
            "InstanceType": self._instance_for(call.resources),
            "VolumeSizeInGB": self.volume_size_gb,
        }
        if self.keep_alive_seconds:
            resource_config["KeepAlivePeriodInSeconds"] = self.keep_alive_seconds

        # No InputDataConfig. The container fetches its own payload from S3, the same way it will
        # on Batch or ECS, so SageMaker's channels are one less thing that differs between
        # backends. OutputDataConfig is still required by the API, but nothing is written to it --
        # the result goes straight to REMOTE_STEP_RESULT_URI, which also means no model.tar.gz to
        # unpack on the way back.
        self.session.client("sagemaker").create_training_job(
            TrainingJobName=job_name,
            RoleArn=self.role_arn,
            AlgorithmSpecification={
                "TrainingImage": self.image_uri,
                "TrainingInputMode": "File",
                "ContainerEntrypoint": CONTAINER_COMMAND,
            },
            OutputDataConfig={"S3OutputPath": f"{job_prefix}/unused"},
            ResourceConfig=resource_config,
            StoppingCondition={"MaxRuntimeInSeconds": self.max_runtime_seconds},
            Environment=environment,
        )
        return SageMakerTrainingHandle(job_name, environment["REMOTE_STEP_RESULT_URI"], self.session, secret_name)


# ---------------------------------------------------------------------------
# AWS Batch
# ---------------------------------------------------------------------------

# Batch's own vocabulary. Nothing translates into SageMaker's -- see JobHandle.terminal_statuses.
BATCH_TERMINAL = frozenset({"SUCCEEDED", "FAILED"})

# Fargate accepts only certain vCPU/memory pairings, and rejects anything else at submit time
# rather than rounding. Memory is in MiB, and each vCPU allows a fixed range; these are the
# smallest valid pairing at or above each size we are prepared to select.
FARGATE_SIZES: list[tuple[float, int]] = [
    (0.25, 512),
    (0.5, 1024),
    (1, 2048),
    (2, 4096),
    (4, 8192),
    (8, 16384),
    (16, 32768),
]


def resolve_fargate_size(cpu: int = 0, memory: int = 0) -> tuple[str, str]:
    """Pick the smallest Fargate sizing that satisfies a request.

    :param cpu: Minimum vCPUs.
    :param memory: Minimum memory in GB.
    :return: ``(vcpu, mib)`` as the strings Batch wants.
    :raises RuntimeError: If the request exceeds what Fargate offers, which is a real ceiling --
        16 vCPU and 120 GB. Anything larger needs an EC2 compute environment.
    """
    wanted_mib = memory * 1024
    for vcpu, mib in FARGATE_SIZES:
        if vcpu >= cpu and mib >= wanted_mib:
            return str(vcpu).rstrip("0").rstrip(".") if vcpu >= 1 else str(vcpu), str(mib)
    raise RuntimeError(
        f"Fargate cannot provide cpu={cpu}, memory={memory}GB (its ceiling is 16 vCPU / 120 GB). "
        f"Use an EC2 compute environment for anything larger, or the SageMaker backend."
    )


class BatchJobHandle:
    """A submitted Batch job.

    Logs live in the ``awslogs`` stream Batch creates for the container, which is only named once
    the job reaches RUNNING -- before that ``get_logs`` has nothing to return, which the streamer
    already tolerates.
    """

    terminal_statuses = BATCH_TERMINAL

    def __init__(self, job_id: str, job_name: str, result_uri: str, session: Any, secret_name: Optional[str] = None):
        self._job_id = job_id
        self._job_name = job_name
        self._result_uri = result_uri
        self._session = session
        self._secret_name = secret_name
        self._batch = session.client("batch")
        self._logs = session.client("logs")
        self._log_stream: Optional[str] = None

    @property
    def id(self) -> str:
        """Batch's job id, which is what `aws batch describe-jobs` wants."""
        return f"{self._job_name} ({self._job_id})"

    def _describe(self) -> dict:
        """One job description, or an empty dict if Batch has forgotten it."""
        jobs = self._batch.describe_jobs(jobs=[self._job_id]).get("jobs", [])
        return jobs[0] if jobs else {}

    @property
    def status(self) -> str:
        """Batch's own status.

        Also caches the log stream name, which only appears once a container is attached -- doing
        it here means the streamer picks up logs the moment they exist without an extra call.
        """
        described = self._describe()
        if self._log_stream is None:
            self._log_stream = described.get("container", {}).get("logStreamName")
        return described.get("status", "SUBMITTED")

    def get_logs(self) -> str:
        """Container output so far. Empty until Batch has attached a container and it has written.

        A job that dies before its container starts -- a failed image pull, say -- has a stream
        *name* but no stream, and asking for it raises. That must not propagate: this is called
        from the failure path, so an exception here replaces the reason the job failed with a
        complaint about missing logs, which is the opposite of helpful.
        """
        if not self._log_stream:
            return ""
        try:
            events = self._logs.get_log_events(
                logGroupName="/aws/batch/job", logStreamName=self._log_stream, startFromHead=True
            )["events"]
        except Exception:
            return ""
        return "".join(f"{event['message']}\n" for event in events)

    def _discard_secret(self) -> None:
        """Delete the staged Snowflake credentials, if any were needed."""
        if not self._secret_name:
            return
        try:
            self._session.client("secretsmanager").delete_secret(
                SecretId=self._secret_name, ForceDeleteWithoutRecovery=True
            )
        except Exception as exc:  # never let cleanup mask the job's own outcome
            print(f"[batch] could not delete {self._secret_name}: {exc}")
        finally:
            self._secret_name = None

    def cancel(self) -> None:
        """Stop the job and release its credentials. Safe on an already-finished job."""
        try:
            self._batch.terminate_job(jobId=self._job_id, reason="remote_step: nothing is waiting")
            print(f"[batch] terminated {self._job_name}")
        except Exception as exc:
            print(f"[batch] could not terminate {self._job_name}: {exc}")
        self._discard_secret()

    def result(self) -> bytes:
        """Read the pickled result the container uploaded.

        :raises RuntimeError: If the job failed, with the container's own traceback.
        """
        try:
            described = self._describe()
            if described.get("status") == "FAILED":
                # Batch surfaces a spot reclaim here rather than as a distinct status, and that is
                # worth seeing plainly: it is the risk that decides whether Spot is usable.
                reason = described.get("statusReason") or described.get("container", {}).get("reason")
                raise RuntimeError(_failure_message(self._job_name, reason, self.get_logs()))

            bucket, _, key = self._result_uri.removeprefix("s3://").partition("/")
            return self._session.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        finally:
            self._discard_secret()


class BatchBackend:
    """Runs the body as an AWS Batch job on Fargate Spot.

    Why Batch alongside SageMaker: **Spot**. Batch runs interruptible capacity natively at roughly
    a third of on-demand, and there is no SageMaker service premium on the instance either. Against
    a measured floor of 85s cold / 46s warm on SageMaker, the interesting questions are whether
    Fargate starts faster and whether Spot's price makes the difference moot.

    What it gives up: no warm pools, so every job is a cold start; and Fargate's ceiling of 16 vCPU
    and 120 GB with no GPU. An EC2 compute environment lifts both, at the cost of an instance role
    and slower scale-up -- worth adding once there is a workload that needs it.

    The image is **not** a backend setting here, unlike SageMaker. Batch pins it in the job
    definition and ``containerOverrides`` accepts only sizing, command and environment, so changing
    the image means registering a new job definition revision. Taking an ``image_uri`` that could
    not be honoured would be worse than not offering one.

    Spot capacity can be reclaimed mid-job. The job then fails rather than resuming, and
    ``result()`` surfaces Batch's reason so that shows up as itself rather than as a mystery.
    """

    name = "batch"

    def __init__(  # noqa: PLR0913
        self,
        job_queue: str,
        job_definition: str,
        s3_prefix: str,
        max_runtime_seconds: int = DEFAULT_MAX_RUNTIME_SECONDS,
        with_snowflake: bool = False,
        profile_name: Optional[str] = None,
        assume_role_arn: Optional[str] = None,
        image_packages: Optional[Iterable[str]] = None,
        boto_session: Any = None,
    ) -> None:
        self.job_queue = job_queue
        self.job_definition = job_definition
        self.s3_prefix = s3_prefix.rstrip("/")
        self.max_runtime_seconds = max_runtime_seconds
        self.with_snowflake = with_snowflake
        self.profile_name = profile_name
        self.assume_role_arn = assume_role_arn
        self.image_packages = frozenset(IMAGE_PACKAGES if image_packages is None else image_packages)
        self._boto_session = boto_session

    # Session handling is identical to SageMaker's -- profile locally, ambient plus assume-role in
    # a pod -- so it is inherited rather than copied.
    session = SageMakerBackend.session
    _assume = SageMakerBackend._assume
    _stage_snowflake_credentials = SageMakerBackend._stage_snowflake_credentials

    def describe(self, call: RemoteCall) -> str:
        """Report the queue and the Fargate sizing the request resolved to."""
        vcpu, mib = resolve_fargate_size(cpu=call.resources.cpu, memory=call.resources.memory)
        return f"batch job on {self.job_queue} (fargate spot, {vcpu} vCPU / {int(mib) // 1024} GB)"

    def submit(self, call: RemoteCall) -> JobHandle:
        """Stage the payload to S3 and submit a Batch job.

        Everything that varies per step is a container override, so one job definition serves every
        step rather than accumulating a revision per submission.

        :param call: The step body and everything it needs.
        :return: A handle wrapping the Batch job.
        """
        import time
        from importlib.metadata import version as installed_version

        requirements = [
            *filter_preinstalled(
                [r for r in call.pip_requirements if not r.startswith("cloudpickle")],
                available=self.image_packages,
            ),
            f"cloudpickle=={installed_version('cloudpickle')}",
        ]

        unique = uuid.uuid4().hex[:8]
        job_name = f"{call.step_name.replace('_', '-')}-{int(time.time())}-{unique}"[:128]
        job_prefix = f"{self.s3_prefix}/{job_name}"

        environment = stage_call(self.session.client("s3"), job_prefix, call, requirements)
        environment["AWS_DEFAULT_REGION"] = self.session.region_name or "us-west-2"

        secret_name = self._stage_snowflake_credentials(job_name)
        if secret_name:
            environment[SNOWFLAKE_SECRET_ENV_VAR] = secret_name

        vcpu, mib = resolve_fargate_size(cpu=call.resources.cpu, memory=call.resources.memory)
        overrides: dict = {
            "command": CONTAINER_COMMAND,
            "environment": [{"name": key, "value": value} for key, value in environment.items()],
            "resourceRequirements": [
                {"type": "VCPU", "value": vcpu},
                {"type": "MEMORY", "value": mib},
            ],
        }
        submitted = self.session.client("batch").submit_job(
            jobName=job_name,
            jobQueue=self.job_queue,
            jobDefinition=self.job_definition,
            containerOverrides=overrides,
            timeout={"attemptDurationSeconds": self.max_runtime_seconds},
        )

        return BatchJobHandle(
            submitted["jobId"], job_name, environment["REMOTE_STEP_RESULT_URI"], self.session, secret_name
        )
