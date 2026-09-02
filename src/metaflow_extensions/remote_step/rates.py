"""Cost estimates for placement decisions.

Snapshotted rates for us-west-2. Regenerate periodically via
`python -m remote_step.gen_rates --region us-west-2`. Drift is expected and
affects estimates, not correctness.
"""


from __future__ import annotations

FARGATE_VCPU_USD = 0.04048
FARGATE_GB_USD = 0.004445

EC2_HOURLY_USD = {
    # CPU instances
    "c8i.4xlarge": 0.72,
    "c8i.8xlarge": 1.44,
    "c8i.12xlarge": 2.16,
    "c8i.24xlarge": 4.32,
    "m8i.4xlarge": 0.81,
    "m8i.8xlarge": 1.61,
    "m8i.12xlarge": 2.42,
    "m8i.24xlarge": 4.84,
    "r8i.4xlarge": 1.06,
    "r8i.8xlarge": 2.12,
    "r8i.12xlarge": 3.18,
    "r8i.24xlarge": 6.36,
    # GPU instances
    "g6.xlarge": 0.805,
    "g6.2xlarge": 0.978,
    "g6.4xlarge": 1.323,
    "g6.12xlarge": 4.602,
    "g6e.xlarge": 1.861,
    "g6e.12xlarge": 10.494,
    "p4d.24xlarge": 32.77,
    "p5.48xlarge": 98.32,
}


def fargate_hourly_usd(cpu: float, memory_mb: int) -> float:
    """Estimate Fargate hourly cost for a (cpu, memory_mb) configuration."""
    gb = memory_mb / 1024.0
    return round(cpu * FARGATE_VCPU_USD + gb * FARGATE_GB_USD, 4)


def ec2_hourly_usd(instance_type: str) -> float:
    """Return the on-demand hourly rate for an EC2 instance type."""
    if instance_type not in EC2_HOURLY_USD:
        raise KeyError(f"no rate snapshot for instance type '{instance_type}'")
    return EC2_HOURLY_USD[instance_type]
