"""EC2 instance catalog for placement decisions.

Snapshotted for us-west-2. Ordered so `smallest_*_fit` returns the cheapest
viable instance by iterating in-order.
"""


from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class InstanceType:
    """A single EC2 instance type entry.

    `gpus` is 0 for CPU-only instances. `gpu_model` is a human string used
    only for messages (e.g. 'L4', 'H100').
    """

    name: str
    vcpu: int
    memory_gb: int
    gpus: int = 0
    gpu_model: str = ""


CPU_CATALOG: list[InstanceType] = [
    InstanceType("c8i.4xlarge", vcpu=16, memory_gb=32),
    InstanceType("m8i.4xlarge", vcpu=16, memory_gb=64),
    InstanceType("r8i.4xlarge", vcpu=16, memory_gb=128),
    InstanceType("c8i.8xlarge", vcpu=32, memory_gb=64),
    InstanceType("m8i.8xlarge", vcpu=32, memory_gb=128),
    InstanceType("r8i.8xlarge", vcpu=32, memory_gb=256),
    InstanceType("c8i.12xlarge", vcpu=48, memory_gb=96),
    InstanceType("m8i.12xlarge", vcpu=48, memory_gb=192),
    InstanceType("r8i.12xlarge", vcpu=48, memory_gb=384),
    InstanceType("c8i.24xlarge", vcpu=96, memory_gb=192),
    InstanceType("m8i.24xlarge", vcpu=96, memory_gb=384),
    InstanceType("r8i.24xlarge", vcpu=96, memory_gb=768),
]

GPU_CATALOG: list[InstanceType] = [
    InstanceType("g6.xlarge", vcpu=4, memory_gb=16, gpus=1, gpu_model="L4"),
    InstanceType("g6.2xlarge", vcpu=8, memory_gb=32, gpus=1, gpu_model="L4"),
    InstanceType("g6.4xlarge", vcpu=16, memory_gb=64, gpus=1, gpu_model="L4"),
    InstanceType("g6.12xlarge", vcpu=48, memory_gb=192, gpus=4, gpu_model="L4"),
    InstanceType("g6e.xlarge", vcpu=4, memory_gb=32, gpus=1, gpu_model="L40S"),
    InstanceType("g6e.12xlarge", vcpu=48, memory_gb=384, gpus=4, gpu_model="L40S"),
    InstanceType("p4d.24xlarge", vcpu=96, memory_gb=1152, gpus=8, gpu_model="A100-40G"),
    InstanceType("p5.48xlarge", vcpu=192, memory_gb=2048, gpus=8, gpu_model="H100-80G"),
]


def smallest_cpu_fit(cpu: int, memory_mb: int) -> InstanceType | None:
    """Return the smallest (first-listed) CPU instance that fits or None."""
    memory_gb = memory_mb / 1024.0
    fits = [i for i in CPU_CATALOG if i.vcpu >= cpu and i.memory_gb >= memory_gb]
    if not fits:
        return None
    return sorted(fits, key=lambda i: (i.vcpu, i.memory_gb))[0]


def smallest_gpu_fit(cpu: int, memory_mb: int, gpus: int) -> InstanceType | None:
    """Return the smallest GPU instance that fits (cpu, mem, gpu) or None."""
    memory_gb = memory_mb / 1024.0
    fits = [
        i
        for i in GPU_CATALOG
        if i.vcpu >= cpu and i.memory_gb >= memory_gb and i.gpus >= gpus
    ]
    if not fits:
        return None
    return sorted(fits, key=lambda i: (i.gpus, i.vcpu, i.memory_gb))[0]


def largest_cpu() -> InstanceType:
    """Largest CPU instance in catalog — used in refusal messages."""
    return max(CPU_CATALOG, key=lambda i: (i.vcpu, i.memory_gb))


def largest_gpu() -> InstanceType:
    """Largest GPU instance in catalog — used in refusal messages."""
    return max(GPU_CATALOG, key=lambda i: (i.gpus, i.vcpu, i.memory_gb))
