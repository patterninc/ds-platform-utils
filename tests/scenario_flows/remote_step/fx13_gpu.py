"""A GPU step: x86 fallback, the device is visible, @gpu_profile samples it.

Two fixes ride on this. cpu_arch defaults to arm64 but the GPU NodePool is
amd64 only, so a GPU ask has to fall back rather than sit Pending. And the
GPU sampler discarded every reading for a body over 300s, so any real
training run reported "peak 0% util" -- this body deliberately runs past
that boundary.
"""

import platform
import subprocess
import time

from metaflow import FlowSpec, current, gpu_profile, remote_step, resources, step

from _check import check

BODY_SECONDS = 320  # past the sampler's 300s process duration


class Fx13Gpu(FlowSpec):
    @step
    def start(self):
        self.next(self.train)

    @gpu_profile(interval=1)
    @remote_step
    @resources(cpu=4, memory=16000, gpu=1)
    @step
    def train(self):
        check("fell back to x86 for the GPU", platform.machine(), "x86_64")
        smi = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,driver_version,memory.total",
             "--format=csv,noheader"],
            capture_output=True, text=True,
        )
        print(f"[fx13] nvidia-smi rc={smi.returncode} out={smi.stdout.strip()}", flush=True)
        check("nvidia-smi works in the pod", smi.returncode, 0)
        check("a device is listed", smi.stdout.strip(), predicate=lambda s: len(s) > 0)

        # Keep the GPU busy so utilisation is non-zero, and run past 300s so
        # the sampler's process-rollover path is exercised.
        started = time.time()
        burned = self.burn_gpu(BODY_SECONDS)
        check("body ran past the sampler boundary", time.time() - started,
              predicate=lambda e: e > 300)

        self.device = smi.stdout.strip()
        self.gpu_seconds = burned
        self.next(self.end)

    def burn_gpu(self, seconds):
        """Load the GPU if torch is present, otherwise just wait it out."""
        started = time.time()
        try:
            import torch

            if torch.cuda.is_available():
                a = torch.randn(4096, 4096, device="cuda")
                while time.time() - started < seconds:
                    a = (a @ a).clamp_(-1, 1)
                torch.cuda.synchronize()
                print("[fx13] burned with torch matmuls", flush=True)
                return time.time() - started
        except Exception as exc:  # noqa: BLE001
            print(f"[fx13] torch unavailable ({exc}); idling instead", flush=True)
        while time.time() - started < seconds:
            time.sleep(10)
        return time.time() - started

    @step
    def end(self):
        check("device reported back", self.device, predicate=lambda d: bool(d))
        prof = getattr(self, "remote_gpu_profile", None)
        print(f"[fx13] remote_gpu_profile present: {prof is not None}", flush=True)
        if prof is not None:
            readings = prof.get("readings") or {}
            counts = {g: len(v.get("gpu_utilization", [])) for g, v in readings.items()}
            print(f"[fx13] samples per device: {counts}", flush=True)
            # The regression: a >300s body used to collapse to a single sample.
            check("more than one sample survived", max(counts.values(), default=0),
                  predicate=lambda n: n > 1)
        print("[fx13] OK")


if __name__ == "__main__":
    Fx13Gpu()
