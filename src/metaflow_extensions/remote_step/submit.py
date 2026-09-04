"""Create the Kubernetes Job that runs one step.

The resource ask is stated, not solved: Karpenter reads the pod's requests
and picks the cheapest instance that satisfies them, and Kueue maps the
request onto a ResourceFlavor. So this module only has to validate the ask
and render it as a pod spec.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import re

from remote_step.config import RemoteStepConfig, check_team
from remote_step.errors import SizingError, SubmitError


# Kueue reads this label to decide which LocalQueue (and therefore which
# ClusterQueue and quota) a Job belongs to. Without it Kueue ignores the Job
# entirely and it runs unqueued, bypassing quota — so its absence is a
# correctness bug, not a missing nicety.
QUEUE_LABEL = "kueue.x-k8s.io/queue-name"
PRIORITY_LABEL = "kueue.x-k8s.io/priority-class"

# Set by karpenter-nodepools.yaml on every node it launches, and mirrored by
# the Kueue ResourceFlavors' nodeLabels.
ARCH_LABEL = "kubernetes.io/arch"

GPU_RESOURCE = "nvidia.com/gpu"

VALID_ARCHES = ("x86_64", "arm64")
# Kubernetes uses Go's GOARCH names, not uname's.
_ARCH_TO_K8S = {"x86_64": "amd64", "arm64": "arm64"}

VALID_PRIORITIES = ("low", "normal", "high")


@dataclass(frozen=True)
class StepResources:
    """The validated resource ask for one step.

    Carries no instance type or cost estimate: with Karpenter the instance is
    not chosen until the node is launched, so any submit-time figure would be
    a guess.
    """

    cpu: int
    memory_mb: int
    gpus: int
    cpu_arch: str
    # Local ephemeral storage for the pod's writable layer. Steps stream
    # artifacts through memory, but pip/uv unpacking wheels and any
    # tempfile use land here.
    ephemeral_gb: int = 40

    @property
    def k8s_arch(self) -> str:
        return _ARCH_TO_K8S[self.cpu_arch]


def resolve(
    cpu: int,
    memory_mb: int,
    gpu: int = 0,
    cpu_arch: str = "x86_64",
    ephemeral_gb: int = 40,
) -> StepResources:
    """Validate a resource ask and return it normalised.

    Kubernetes accepts arbitrary integer cpu/memory, so there is no rounding.
    The only rejections are asks no NodePool can satisfy.
    """
    if cpu < 1:
        raise SizingError(f"@resources(cpu={cpu}) — cpu must be >= 1", cpu=cpu)
    if memory_mb < 1:
        raise SizingError(
            f"@resources(memory={memory_mb}) — memory_mb must be >= 1",
            memory_mb=memory_mb,
        )
    if gpu < 0:
        raise SizingError(f"@resources(gpu={gpu}) — gpu must be >= 0", gpu=gpu)
    if cpu_arch not in VALID_ARCHES:
        raise SizingError(
            f"cpu_arch={cpu_arch!r} — must be one of {VALID_ARCHES}",
            cpu_arch=cpu_arch,
        )
    if gpu > 0 and cpu_arch == "arm64":
        # The gpu NodePool pins kubernetes.io/arch: amd64 (g6/g6e/p6-b200 are
        # all x86), so an arm64 GPU ask would be admitted by Kueue and then
        # never schedule. Refuse it at submit time where the message is
        # actionable, rather than letting it sit Pending until Kueue's
        # waitForPodsReady timeout evicts it.
        raise SizingError(
            f"cpu_arch='arm64' with gpu={gpu} — the GPU NodePool is x86 only "
            f"(g6/g6e/p6-b200). Drop cpu_arch to use x86_64, or drop the GPU.",
            cpu_arch=cpu_arch,
            gpu=gpu,
        )
    return StepResources(
        cpu=cpu,
        memory_mb=memory_mb,
        gpus=gpu,
        cpu_arch=cpu_arch,
        ephemeral_gb=ephemeral_gb,
    )


def format_resources(r: StepResources) -> str:
    """One-line summary for dry-run and submit output."""
    parts = [f"{r.cpu} vCPU", f"{r.memory_mb / 1024:.0f} GB"]
    if r.gpus:
        parts.append(f"{r.gpus}× GPU")
    arch = "arm64 (Graviton)" if r.cpu_arch == "arm64" else "x86_64"
    return f"{' / '.join(parts)} · {arch} · instance chosen by Karpenter"


@dataclass
class SubmitResult:
    """What got submitted and where."""

    job_name: str
    namespace: str
    queue: str
    uid: str
    labels: dict[str, str] = field(default_factory=dict)


def _dns1123(raw: str, limit: int = 63) -> str:
    """Coerce to a DNS-1123 label: lowercase alnum and '-', no edge '-'."""
    s = re.sub(r"[^a-z0-9-]", "-", raw.lower())
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:limit].strip("-")


def job_name(
    flow_name: str,
    run_id: str,
    step_name: str,
    attempt: int,
    task_id: str = "",
) -> str:
    """Deterministic, DNS-safe, per-run-unique Job name.

    The run id cannot simply be truncated. Argo run ids are
    `argo-<flowname>-<suffix>`, so the distinguishing part is at the END —
    truncating to a fixed prefix yields the same name for every run of a
    flow, and with ttlSecondsAfterFinished the previous run's Job is still
    present, so the next submit gets a 409 and a non-retriable SubmitError.

    So a short hash of the full (run_id, task_id) goes in instead. It is not
    human-readable, but the flow and step names are, and the full run id and
    task id are on the Job as annotations for anyone who needs them.
    """
    # Budget the 63-char label limit across the parts rather than truncating
    # the whole string, so the step name (the useful part when scanning
    # `kubectl get jobs`) is never the bit that gets cut.
    flow = _dns1123(flow_name, 18)
    step = _dns1123(step_name, 18)
    digest = hashlib.sha1(f"{run_id}|{task_id}".encode()).hexdigest()[:10]
    return _dns1123(f"rs-{flow}-{step}-{digest}-{attempt}", 63)


def build_manifest(
    cfg: RemoteStepConfig,
    resources: StepResources,
    payload_uri: str,
    *,
    flow_name: str,
    run_id: str,
    step_name: str,
    task_id: str,
    attempt: int,
    user: str,
    team: str,
    priority: str = "normal",
    extra_env: dict[str, str] | None = None,
    timeout_minutes: int = 240,
) -> dict:
    """Build the batch/v1 Job manifest for one step attempt."""
    if priority not in VALID_PRIORITIES:
        raise SubmitError(
            f"priority={priority!r} — must be one of {VALID_PRIORITIES}",
            priority=priority,
        )
    check_team(cfg, team)

    name = job_name(flow_name, run_id, step_name, attempt, task_id)

    # Labels are for selecting and accounting; they must be valid label
    # values. Anything that might not be (a flow name with a dot, a long
    # user id) goes in annotations instead.
    labels = {
        QUEUE_LABEL: cfg.local_queue,
        PRIORITY_LABEL: priority,
        "remote-step.pattern.com/flow": _dns1123(flow_name),
        "remote-step.pattern.com/step": _dns1123(step_name),
        "remote-step.pattern.com/run-id": _dns1123(str(run_id)),
        "remote-step.pattern.com/attempt": str(attempt),
    }
    annotations = {
        "remote-step.pattern.com/flow-name": flow_name,
        "remote-step.pattern.com/step-name": step_name,
        "remote-step.pattern.com/task-id": str(task_id),
        "remote-step.pattern.com/user": user,
        "remote-step.pattern.com/spec-uri": payload_uri,
    }

    env = [
        {"name": "REMOTE_STEP_SPEC_URI", "value": payload_uri},
        {"name": "REMOTE_STEP_LOG_GROUP", "value": cfg.log_group},
        # Set explicitly rather than relying on the Pod Identity webhook to
        # provide it. entrypoint.sh builds boto3 clients with
        # region_name=os.environ.get("AWS_REGION"), and if that is unset
        # boto3 falls through to IMDS — which Bottlerocket does not expose to
        # pods, so the step would fail on its first S3 call with a confusing
        # "no region" error rather than anything pointing here.
        {"name": "AWS_REGION", "value": cfg.region},
        {"name": "AWS_DEFAULT_REGION", "value": cfg.region},
        # Surfaced so the runner can label its own CloudWatch log stream and
        # so `nproc`-style code sees a sane value.
        {
            "name": "REMOTE_STEP_POD_NAME",
            "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}},
        },
        {
            "name": "REMOTE_STEP_NODE_NAME",
            "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}},
        },
    ]
    for k, v in (extra_env or {}).items():
        env.append({"name": k, "value": v})

    requests = {
        "cpu": str(resources.cpu),
        "memory": f"{resources.memory_mb}Mi",
        "ephemeral-storage": f"{resources.ephemeral_gb}Gi",
    }
    # Memory is limited to the request so a runaway step is OOM-killed with a
    # clear signal rather than evicting its neighbours. CPU is deliberately
    # NOT limited: throttling a step that briefly wants more cores makes it
    # slower for no benefit, and Kueue has already charged the request
    # against quota.
    limits = {
        "memory": f"{resources.memory_mb}Mi",
        "ephemeral-storage": f"{resources.ephemeral_gb}Gi",
    }
    node_selector = {ARCH_LABEL: resources.k8s_arch}
    if resources.gpus:
        # Extended resources must appear in limits; Kubernetes copies the
        # value to requests. This single entry is what steers Kueue to the
        # `gpu` ResourceFlavor and Karpenter to the gpu NodePool — and the
        # flavor's toleration for the NoSchedule taint is applied by Kueue,
        # so we do not set one here.
        limits[GPU_RESOURCE] = str(resources.gpus)

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": team,
            "labels": labels,
            "annotations": annotations,
        },
        "spec": {
            # Kueue's webhook flips this to false on admission. Submitting
            # suspended is what makes the Job queue rather than run
            # immediately.
            "suspend": True,
            # Retries are Metaflow's job (@retry), not the Job controller's.
            # A Job-level retry would re-run the step body without the driver
            # knowing, and would write to the same output prefix.
            "backoffLimit": 0,
            # Clean up finished Jobs so `kubectl get jobs` stays readable.
            # 24h is long enough to inspect a failure the next morning.
            "ttlSecondsAfterFinished": 24 * 3600,
            # A real ceiling on the step. Without this a hung step hangs the
            # driver — and therefore the Outerbounds pod — indefinitely,
            # because poll.wait has no deadline once the pod has started.
            # Kubernetes marks the Job failed with reason DeadlineExceeded,
            # which poll.py maps to the retriable NodeLostError path.
            "activeDeadlineSeconds": max(60, int(timeout_minutes) * 60),
            "template": {
                "metadata": {"labels": labels, "annotations": annotations},
                "spec": {
                    "restartPolicy": "Never",
                    "serviceAccountName": cfg.service_account,
                    "nodeSelector": node_selector,
                    # Steps are single-pod and stateless; on node loss
                    # Metaflow retries. Give the kubelet a little time to
                    # flush logs on eviction.
                    "terminationGracePeriodSeconds": 30,
                    "containers": [
                        {
                            "name": "runner",
                            "image": cfg.runner_image,
                            # No `command`. The image's ENTRYPOINT is
                            # ["/usr/bin/tini","--","/entrypoint.sh"], and a
                            # pod-spec `command` REPLACES the entrypoint
                            # rather than appending to it — which drops tini,
                            # makes bash (then python) PID 1, and PID 1
                            # ignores signals with a default disposition. The
                            # container would then never see SIGTERM on
                            # eviction and would always be SIGKILLed after
                            # terminationGracePeriodSeconds, so the grace
                            # period below would buy nothing.
                            "env": env,
                            "resources": {
                                "requests": requests,
                                "limits": limits,
                            },
                        }
                    ],
                },
            },
        },
    }


def submit(
    cfg: RemoteStepConfig,
    resources: StepResources,
    payload_uri: str,
    *,
    flow_name: str,
    run_id: str,
    step_name: str,
    task_id: str,
    attempt: int,
    user: str,
    team: str,
    priority: str = "normal",
    extra_env: dict[str, str] | None = None,
    timeout_minutes: int = 240,
    client=None,
) -> SubmitResult:
    """Create the Job. Returns SubmitResult."""
    from remote_step.k8s import Conflict, Forbidden

    manifest = build_manifest(
        cfg,
        resources,
        payload_uri,
        flow_name=flow_name,
        run_id=run_id,
        step_name=step_name,
        task_id=task_id,
        attempt=attempt,
        user=user,
        team=team,
        priority=priority,
        extra_env=extra_env,
        timeout_minutes=timeout_minutes,
    )
    name = manifest["metadata"]["name"]
    try:
        created = client.create_job(team, manifest)
    except Conflict as exc:
        raise SubmitError(
            f"Job {name!r} already exists in namespace {team!r}. A previous "
            f"attempt of this step is still present; wait for its TTL or "
            f"delete it with `kubectl -n {team} delete job {name}`.",
            job_name=name,
            namespace=team,
        ) from exc
    except Forbidden as exc:
        raise SubmitError(
            f"forbidden creating a Job in namespace {team!r}. The submitter "
            f"role's EKS access entry is scoped to team namespaces — check "
            f"{team!r} is in var.teams in infra/eks and that the access "
            f"policy covers it.",
            job_name=name,
            namespace=team,
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise SubmitError(
            f"could not create Job {name!r} in {team!r}: {exc}",
            job_name=name,
            namespace=team,
        ) from exc
    return SubmitResult(
        job_name=name,
        namespace=team,
        queue=cfg.local_queue,
        uid=(created.get("metadata") or {}).get("uid", ""),
        labels=manifest["metadata"]["labels"],
    )
