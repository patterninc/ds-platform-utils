"""Asks that must be refused, not left Pending.

Every other flow here proves something works. These prove the decorator says
no at submit time, where the message is actionable, instead of letting Kueue
admit a pod that can never schedule and sit there until waitForPodsReady
evicts it with nothing anywhere explaining why.

Run with `--refusal <name>`; each raises during step_init, so no pod is ever
created and this costs nothing to run.
"""

import sys

from metaflow import FlowSpec, Parameter, remote_step, resources, step

from _check import check


def expect_refusal(label, build):
    """Call `build` and require a SizingError naming the problem."""
    from remote_step.errors import SizingError

    try:
        build()
    except SizingError as exc:
        print(f"[check] PASS  {label}: refused -> {exc}", flush=True)
        return str(exc)
    except Exception as exc:  # noqa: BLE001
        raise AssertionError(f"{label}: wrong error type {type(exc).__name__}: {exc}") from exc
    raise AssertionError(f"{label}: accepted an ask that cannot schedule")


class Fx24SizingRefusals(FlowSpec):
    @step
    def start(self):
        # resolve() is the gate every ask passes through, so it is checked
        # directly -- a flow cannot declare four contradictory asks at once.
        from remote_step.submit import MAX_EPHEMERAL_GB_CPU, MAX_EPHEMERAL_GB_GPU, resolve

        msg = expect_refusal(
            "ephemeral_gb beyond a CPU node's volume",
            lambda: resolve(8, 32000, 0, cpu_arch="x86_64", ephemeral_gb=MAX_EPHEMERAL_GB_CPU + 1),
        )
        check("the message names the ceiling", str(MAX_EPHEMERAL_GB_CPU) in msg, True)
        check("and points at the fix", "karpenter-nodeclasses" in msg, True)

        expect_refusal(
            "ephemeral_gb beyond a GPU node's store",
            lambda: resolve(8, 32000, 1, cpu_arch="x86_64", ephemeral_gb=MAX_EPHEMERAL_GB_GPU + 1),
        )
        expect_refusal(
            "arm64 with a GPU",
            lambda: resolve(8, 32000, 1, cpu_arch="arm64"),
        )
        expect_refusal(
            "shared_memory larger than memory",
            lambda: resolve(8, 8000, 0, cpu_arch="x86_64", shm_mb=16000),
        )
        expect_refusal("cpu below one", lambda: resolve(0, 8000, 0, cpu_arch="x86_64"))
        expect_refusal("negative gpu", lambda: resolve(1, 8000, -1, cpu_arch="x86_64"))
        expect_refusal("unknown arch", lambda: resolve(1, 8000, 0, cpu_arch="riscv"))

        # A sane large ask must still be accepted -- the guards must not be
        # so eager that real work is blocked.
        ok = resolve(96, 700000, 0, cpu_arch="arm64", ephemeral_gb=160)
        check("a 96 vCPU ask is accepted", ok.cpu, 96)
        check("a 160 GB scratch ask is accepted", ok.ephemeral_gb, 160)
        gpu_ok = resolve(48, 380000, 8, cpu_arch="x86_64", ephemeral_gb=400)
        check("an 8-GPU ask is accepted", gpu_ok.gpus, 8)

        self.next(self.end)

    @step
    def end(self):
        print("[fx24] OK")


if __name__ == "__main__":
    Fx24SizingRefusals()
