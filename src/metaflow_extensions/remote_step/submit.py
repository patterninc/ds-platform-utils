"""Create the Kubernetes Job that runs one step.

The resource ask is stated, not solved: Karpenter reads the pod's requests
and picks the cheapest instance that satisfies them, and Kueue maps the
request onto a ResourceFlavor. So this module only has to validate the ask
and render it as a pod spec.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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


def job_name(flow_name: str, run_id: str, step_name: str, attempt: int) -> str:
    """Deterministic, DNS-safe Job name.

    Includes attempt so a retry does not collide with the failed Job, which
    may still exist pending TTL cleanup.
    """
    # Budget the 63-char label limit across the parts rather than truncating
    # the whole string, so the step name (the useful part when scanning
    # `kubectl get jobs`) is never the bit that gets cut.
    flow = _dns1123(flow_name, 20)
    step = _dns1123(step_name, 20)
    run = _dns1123(str(run_id), 12)
    return _dns1123(f"rs-{flow}-{run}-{step}-{attempt}", 63)


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
) -> dict:
    """Build the batch/v1 Job manifest for one step attempt."""
    if priority not in VALID_PRIORITIES:
        raise SubmitError(
            f"priority={priority!r} — must be one of {VALID_PRIORITIES}",
            priority=priority,
        )
    check_team(cfg, team)

    name = job_name(flow_name, run_id, step_name, attempt)

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
                            "command": ["/entrypoint.sh"],
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
    api_client=None,
) -> SubmitResult:
    """Create the Job. Returns SubmitResult."""
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

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
    )
    batch = k8s_client.BatchV1Api(api_client)
    name = manifest["metadata"]["name"]
    try:
        created = batch.create_namespaced_job(namespace=team, body=manifest)
    except ApiException as exc:
        if exc.status == 409:
            raise SubmitError(
                f"Job {name!r} already exists in namespace {team!r}. A "
                f"previous attempt of this step is still present; wait for "
                f"its TTL or delete it with "
                f"`kubectl -n {team} delete job {name}`.",
                job_name=name,
                namespace=team,
            ) from exc
        if exc.status == 403:
            raise SubmitError(
                f"forbidden creating a Job in namespace {team!r}. The "
                f"submitter role's EKS access entry is scoped to team "
                f"namespaces — check {team!r} is in var.teams in infra/eks "
                f"and that the access policy covers it.",
                job_name=name,
                namespace=team,
            ) from exc
        raise SubmitError(
            f"could not create Job {name!r} in {team!r}: "
            f"{exc.status} {exc.reason}",
            job_name=name,
            namespace=team,
        ) from exc
    return SubmitResult(
        job_name=name,
        namespace=team,
        queue=cfg.local_queue,
        uid=created.metadata.uid,
        labels=manifest["metadata"]["labels"],
    )
