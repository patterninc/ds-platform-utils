"""Placement resolver.

Pure functions. Given (cpu, memory_mb, gpu, placement), decide:
  - which Batch queue (Fargate / EC2 CPU / EC2 GPU)
  - which (cpu, memory_mb) to actually request (rounded up for Fargate)
  - which EC2 instance type to force (if applicable)
  - estimated hourly USD

Never calls AWS. All catalogs are local snapshots.
"""


from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from remote_step.errors import SizingError
from remote_step.instances import (
    largest_cpu,
    largest_gpu,
    smallest_cpu_fit,
    smallest_gpu_fit,
)
from remote_step.rates import ec2_hourly_usd, fargate_hourly_usd


# Fargate valid (vCPU, memory_gb) combos on Batch/ECS with Linux platform 1.4.0+.
# For each vCPU tier: list of supported memory values in GB. Ordered ascending.
FARGATE_COMBOS: dict[float, list[int]] = {
    0.25: [1, 2],  # 512 MiB rounded to 1 GB (we don't ship the sub-GB tier)
    0.5: [1, 2, 3, 4],
    1: [2, 3, 4, 5, 6, 7, 8],
    2: list(range(4, 17)),  # 4..16 in 1 GB steps
    4: list(range(8, 31)),  # 8..30 in 1 GB steps
    8: list(range(16, 61, 4)),  # 16..60 in 4 GB steps
    16: list(range(32, 121, 8)),  # 32..120 in 8 GB steps
    32: [60, 120, 244],  # discrete only
}

Placement = Literal["auto", "fargate", "ec2"]
Queue = Literal["fargate", "ec2_cpu", "ec2_gpu"]


@dataclass(frozen=True)
class ResolvedPlacement:
    """The resolved placement decision passed to submit()."""

    queue: Queue
    cpu: float  # vCPU requested (Fargate: 0.25/0.5/1/2/4/8/16/32; EC2: instance vCPU)
    memory_mb: int
    gpus: int
    instance_type: str | None  # only set for EC2 queues
    hourly_usd: float
    rounded_from: tuple[int, int, int] | None  # (cpu, mem_mb, gpu) original ask


def _round_up_fargate(cpu: int, memory_mb: int) -> tuple[float, int] | None:
    """Round ask up to nearest valid Fargate combo.

    Returns (fargate_vcpu, fargate_memory_mb) or None if no combo fits under
    the 32 vCPU / 244 GB Fargate ceiling.
    """
    memory_gb = memory_mb / 1024.0
    for fargate_vcpu in sorted(FARGATE_COMBOS.keys()):
        if fargate_vcpu < cpu:
            continue
        for mem_gb_option in FARGATE_COMBOS[fargate_vcpu]:
            if mem_gb_option >= memory_gb:
                return fargate_vcpu, mem_gb_option * 1024
    return None


def resolve(
    cpu: int,
    memory_mb: int,
    gpu: int = 0,
    placement: Placement = "auto",
) -> ResolvedPlacement:
    """Resolve (cpu, memory_mb, gpu) to a concrete placement.

    Args:
        cpu: requested vCPU cores. Must be >= 1 unless gpu > 0.
        memory_mb: requested memory in MB (matches Metaflow's convention).
        gpu: requested GPU count (0 for CPU-only).
        placement: routing override. 'auto' picks best; 'fargate' refuses
            if the ask cannot fit Fargate; 'ec2' skips Fargate.

    Returns:
        ResolvedPlacement with queue, sizing, and estimated hourly USD.

    Raises:
        SizingError with named alternatives on refusal.
    """
    if cpu < 1:
        raise SizingError(
            f"@resources(cpu={cpu}) — cpu must be >= 1",
            cpu=cpu,
        )
    if memory_mb < 1:
        raise SizingError(
            f"@resources(memory={memory_mb}) — memory_mb must be >= 1",
            memory_mb=memory_mb,
        )
    if gpu < 0:
        raise SizingError(
            f"@resources(gpu={gpu}) — gpu must be >= 0",
            gpu=gpu,
        )
    if placement not in ("auto", "fargate", "ec2"):
        raise SizingError(
            f"placement={placement!r} — must be one of 'auto', 'fargate', 'ec2'",
            placement=placement,
        )

    ask = (cpu, memory_mb, gpu)

    if gpu > 0:
        fit = smallest_gpu_fit(cpu, memory_mb, gpu)
        if fit is None:
            biggest = largest_gpu()
            raise SizingError(
                f"ask (cpu={cpu}, mem={memory_mb} MB, gpu={gpu}) — no GPU "
                f"instance in catalog fits. Largest: {biggest.name} "
                f"({biggest.vcpu} vCPU / {biggest.memory_gb} GB / "
                f"{biggest.gpus}× {biggest.gpu_model})",
                cpu=cpu,
                memory_mb=memory_mb,
                gpu=gpu,
            )
        rounded = ask if (fit.vcpu == cpu and fit.memory_gb * 1024 == memory_mb) else ask
        return ResolvedPlacement(
            queue="ec2_gpu",
            cpu=fit.vcpu,
            memory_mb=fit.memory_gb * 1024,
            gpus=fit.gpus,
            instance_type=fit.name,
            hourly_usd=ec2_hourly_usd(fit.name),
            rounded_from=rounded,
        )

    # No GPU: try Fargate first (unless forced to EC2)
    if placement != "ec2":
        rounded = _round_up_fargate(cpu, memory_mb)
        if rounded is not None:
            f_cpu, f_mem_mb = rounded
            return ResolvedPlacement(
                queue="fargate",
                cpu=f_cpu,
                memory_mb=f_mem_mb,
                gpus=0,
                instance_type=None,
                hourly_usd=fargate_hourly_usd(f_cpu, f_mem_mb),
                rounded_from=ask,
            )
        if placement == "fargate":
            raise SizingError(
                f"placement='fargate' but ask (cpu={cpu}, mem={memory_mb} MB) "
                f"exceeds Fargate max (32 vCPU / 244 GB). Drop placement kwarg "
                f"to auto-route to EC2, or reduce @resources.",
                cpu=cpu,
                memory_mb=memory_mb,
            )

    # Fall through to EC2 CPU
    fit = smallest_cpu_fit(cpu, memory_mb)
    if fit is None:
        biggest = largest_cpu()
        raise SizingError(
            f"ask (cpu={cpu}, mem={memory_mb} MB) exceeds every EC2 instance "
            f"in catalog. Largest: {biggest.name} ({biggest.vcpu} vCPU / "
            f"{biggest.memory_gb} GB). Reduce @resources or extend instances.py.",
            cpu=cpu,
            memory_mb=memory_mb,
        )
    return ResolvedPlacement(
        queue="ec2_cpu",
        cpu=fit.vcpu,
        memory_mb=fit.memory_gb * 1024,
        gpus=0,
        instance_type=fit.name,
        hourly_usd=ec2_hourly_usd(fit.name),
        rounded_from=ask,
    )


def format_placement(p: ResolvedPlacement) -> str:
    """Human-readable one-line summary — used in dry-run and submit output."""
    if p.queue == "fargate":
        base = f"Fargate {p.cpu} vCPU / {p.memory_mb // 1024} GB"
    else:
        base = f"EC2 {p.instance_type} ({p.cpu} vCPU / {p.memory_mb // 1024} GB"
        if p.gpus:
            base += f" / {p.gpus} GPU"
        base += ")"
    line = f"{base} · ~${p.hourly_usd:.3f}/h"
    if p.rounded_from and (
        p.rounded_from[0] != p.cpu or p.rounded_from[1] != p.memory_mb
    ):
        rf = p.rounded_from
        line += f" · rounded up from ({rf[0]} vCPU, {rf[1]} MB)"
    return line
