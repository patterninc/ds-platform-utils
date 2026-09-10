"""@kubernetes sizing beyond cpu/memory/gpu.

`disk` and `shared_memory` used to be dropped on the floor:

- `@kubernetes(disk=200000)` on a step unpacking a 120 GB dataset produced a
  pod with a 40 GiB ephemeral-storage *limit*, so the kubelet evicted it
  partway through -- and an eviction reads as node loss, so it looked like an
  infrastructure blip rather than a sizing mistake.
- `shared_memory` was silently unsupported, leaving /dev/shm at the container
  default of 64 MB, which is what makes a torch DataLoader with workers die on
  a bus error.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.errors import SizingError
from remote_step.plugins.remote_step_decorator import _find_resources
from remote_step.submit import (
    MAX_EPHEMERAL_GB_CPU,
    MAX_EPHEMERAL_GB_GPU,
    resolve,
)


class Deco:
    def __init__(self, name, **attrs):
        self.name = name
        self.attributes = dict(attrs)


# --------------------------------------------------------------- discovery


def test_disk_is_read_from_kubernetes():
    _, _, _, disk, _ = _find_resources([Deco("kubernetes", cpu=8, memory=32000, disk=200000)])
    assert disk == 200000


def test_shared_memory_is_read_from_kubernetes():
    _, _, _, _, shm = _find_resources([Deco("kubernetes", cpu=8, memory=32000, shared_memory=8192)])
    assert shm == 8192


def test_the_larger_of_two_declarations_wins():
    """Same reconciliation Metaflow does for cpu/memory."""
    decos = [Deco("resources", cpu=2, disk=50000), Deco("kubernetes", cpu=8, disk=120000)]
    cpu, _, _, disk, _ = _find_resources(decos)
    assert (cpu, disk) == (8, 120000)


def test_absent_sizing_reads_as_zero():
    cpu, mem, gpu, disk, shm = _find_resources([Deco("resources", cpu=4, memory=8000)])
    assert (cpu, mem, gpu, disk, shm) == (4, 8000, 0, 0, 0)


def test_string_values_are_accepted():
    """@kubernetes normalises its attributes to strings."""
    _, _, _, disk, shm = _find_resources([Deco("kubernetes", disk="200000", shared_memory="4096")])
    assert (disk, shm) == (200000, 4096)


# ------------------------------------------------------------- ephemeral_gb


def test_a_disk_ask_raises_the_pod_scratch_space():
    r = resolve(8, 32000, 0, cpu_arch="x86_64", ephemeral_gb=40)
    assert r.ephemeral_gb == 40
    bigger = resolve(8, 32000, 0, cpu_arch="x86_64", ephemeral_gb=118)
    assert bigger.ephemeral_gb == 118


def test_an_ask_beyond_the_node_volume_is_refused_not_left_pending():
    """Kueue would admit it and it would never schedule."""
    with pytest.raises(SizingError, match="exceeds what a CPU node can offer"):
        resolve(8, 32000, 0, cpu_arch="x86_64", ephemeral_gb=MAX_EPHEMERAL_GB_CPU + 1)


def test_the_gpu_pool_has_a_larger_ceiling():
    """500 GB data volume there, against 200 on CPU nodes."""
    r = resolve(8, 32000, 1, cpu_arch="x86_64", ephemeral_gb=MAX_EPHEMERAL_GB_CPU + 50)
    assert r.ephemeral_gb == MAX_EPHEMERAL_GB_CPU + 50
    with pytest.raises(SizingError, match="exceeds what a gpu node can offer"):
        resolve(8, 32000, 1, cpu_arch="x86_64", ephemeral_gb=MAX_EPHEMERAL_GB_GPU + 1)


def test_the_error_names_the_number_and_the_way_out():
    with pytest.raises(SizingError) as exc:
        resolve(8, 32000, 0, cpu_arch="x86_64", ephemeral_gb=500)
    msg = str(exc.value)
    assert "500" in msg and "170" in msg
    assert "karpenter-nodeclasses" in msg


def test_zero_ephemeral_is_refused():
    with pytest.raises(SizingError, match="must be >= 1"):
        resolve(4, 8000, 0, cpu_arch="x86_64", ephemeral_gb=0)


# -------------------------------------------------------------- /dev/shm


def test_shared_memory_reaches_the_resource_object():
    r = resolve(8, 32000, 0, cpu_arch="x86_64", shm_mb=8192)
    assert r.shm_mb == 8192


def test_shared_memory_larger_than_memory_is_refused():
    """/dev/shm is tmpfs and charged to the memory limit."""
    with pytest.raises(SizingError, match="exceeds memory"):
        resolve(8, 8000, 0, cpu_arch="x86_64", shm_mb=16000)


def test_no_shared_memory_leaves_the_container_default():
    assert resolve(8, 32000, 0, cpu_arch="x86_64").shm_mb == 0


def cfg():
    from remote_step.config import RemoteStepConfig

    return RemoteStepConfig(
        cluster_name="pattern-ml-platform",
        cluster_endpoint="https://example.eks.amazonaws.com",
        region="us-west-2",
        payload_bucket="pattern-ml-platform",
        runner_image="example.dkr.ecr.us-west-2.amazonaws.com/runner:latest",
        service_account="remote-step-runner",
        submitter_role_arn="arn:aws:iam::1:role/submitter",
        artifact_read_role_arn="arn:aws:iam::1:role/reader",
        local_queue="default",
        log_group="/pattern-ml-platform/steps",
        teams=("forecasting",),
    )


def manifest_for(**kw):
    from remote_step.submit import build_manifest

    return build_manifest(
        cfg(),
        resolve(8, 32000, 0, cpu_arch="x86_64", **kw),
        "s3://b/spec.json",
        flow_name="F",
        run_id="1",
        step_name="s",
        task_id="t",
        attempt=0,
        user="u",
        team="forecasting",
    )


def test_the_manifest_mounts_dev_shm_only_when_asked():
    spec = manifest_for(shm_mb=8192)["spec"]["template"]["spec"]
    assert spec["volumes"] == [
        {"name": "dev-shm", "emptyDir": {"medium": "Memory", "sizeLimit": "8192Mi"}}
    ]
    assert spec["containers"][0]["volumeMounts"] == [{"name": "dev-shm", "mountPath": "/dev/shm"}]


def test_without_shared_memory_no_volume_is_added():
    """Nothing extra in the pod spec for the overwhelming majority of steps."""
    spec = manifest_for()["spec"]["template"]["spec"]
    assert spec["volumes"] == []
    assert spec["containers"][0]["volumeMounts"] == []


def test_ephemeral_storage_lands_in_requests_and_limits():
    """Both, so a runaway step is evicted rather than filling the node."""
    res = manifest_for(ephemeral_gb=118)["spec"]["template"]["spec"]["containers"][0]["resources"]
    assert res["requests"]["ephemeral-storage"] == "118Gi"
    assert res["limits"]["ephemeral-storage"] == "118Gi"
