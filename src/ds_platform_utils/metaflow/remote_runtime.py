"""Backend-agnostic pieces of running a step somewhere other than the Metaflow pod.

Nothing here knows which compute service is in use. It is the vocabulary the decorator and the
backends share: version pins both sides must agree on, evidence of where code ran, and the polling
loop that turns a job into live log output.

:mod:`ds_platform_utils.metaflow.compute_backends` supplies the backends themselves, and
:mod:`ds_platform_utils.metaflow.snowflake_access` covers the separate question of how a container reaches Snowflake
*data*.

Nothing here contacts AWS at import time, so flow modules stay importable without credentials --
``show``, linting and tests all still work.
"""

# Imported by every step, including ones whose environment may default to an older Python than the
# project's 3.11. Deferring annotation evaluation keeps `str | Path` style hints from being
# executed at import time, where they would raise TypeError on Python < 3.10.
from __future__ import annotations

import os
import platform
import socket
import sys
import time
from pathlib import Path
from typing import Optional

# Pass to every @pypi step. Without an explicit pin the remote environment can be built on an older
# Python than this project targets, which is how a 3.9 image ends up running 3.11 code. It must
# also match the container image: the step body travels as pickled bytecode, which is not portable
# across minor versions, and the decorator refuses a mismatch rather than failing obscurely.
PYTHON_VERSION = "3.11"

# Pin pandas rather than floating it. Unpinned, different steps resolve different majors depending
# on what else is in their environment, and a DataFrame pickled by pandas 3.x cannot be unpickled
# by 2.x.
PANDAS_VERSION = "2.2.3"


def runtime_fingerprint(label: str) -> dict:
    """Describe the machine this is executing on, so we can prove where work actually ran.

    Call it in a Metaflow step and again inside a job: a differing hostname is the evidence that
    the payload left the EKS node. It needs no assumption about how any particular service labels
    its containers, which is why it is a hostname comparison rather than an environment check.

    Only variable *names* are recorded, never values -- these dicts end up in logs and artifacts.

    :param label: Where this was called from, e.g. ``"metaflow step"``.
    :return: Hostname, platform, Python version, CPU count and container markers.
    """
    container_markers = sorted(key for key in os.environ if key.startswith(("SM_", "AWS_", "ECS_", "TRAINING_")))
    return {
        "label": label,
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "cpu_count": os.cpu_count(),
        "cpu_limit": container_cpu_limit(),
        "container_markers": container_markers,
    }


def container_cpu_limit() -> Optional[float]:
    """CPUs this container is actually limited to, as opposed to what the machine has.

    ``os.cpu_count()`` reports the *host's* cores, so a pod requesting 2 CPUs on a 32-core node
    reports 32. That is not a rounding error when the number is being used to reason about cost --
    it reads as a large, expensive task when the pod is small and cheap. Reading the cgroup quota
    gives what was actually requested.

    :return: CPU limit as a float, or None when unlimited or unreadable (a laptop, typically).
    """
    # cgroup v2: "<quota> <period>", or "max <period>" when unrestricted.
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        return None if quota == "max" else round(int(quota) / int(period), 2)
    except (OSError, ValueError):
        pass

    # cgroup v1
    try:
        quota = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text())
        period = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text())
        return None if quota <= 0 else round(quota / period, 2)
    except (OSError, ValueError):
        return None


# Whether a job has finished is the backend's own question -- SageMaker says Completed/Failed/
# Stopped, Batch says SUCCEEDED/FAILED, ECS says STOPPED. A single vocabulary here would force
# every backend to translate into it, so handles answer `done` themselves and `status` stays a
# display string in whatever words the service uses.

# How often to ask for new container output. Neither SageMaker nor ECS offers a tail or follow on
# the job itself, so this is a poll: short enough to feel live in the Outerbounds UI, long enough
# not to hammer CloudWatch.
LOG_POLL_SECONDS = 5.0


def stream_job_logs(job, poll_seconds: float = LOG_POLL_SECONDS) -> str:
    """Print the container's output into this step's logs *while the job runs*.

    Polls ``get_logs()`` and prints whatever is new since the last look. Without it the whole log
    arrives in one dump after the job ends, which is useless for watching a long fit.

    Log fetches are allowed to fail -- a container that has not started yet has nothing to return,
    and a transient CloudWatch failure should not take down a job that is otherwise fine.

    :param job: A handle satisfying :class:`~ds_platform_utils.metaflow.compute_backends.JobHandle`.
    :param poll_seconds: Seconds between polls.
    :return: The terminal status the job reached.
    """
    printed = 0
    while True:
        status = str(job.status)
        try:
            logs = job.get_logs() or ""
        except Exception:
            logs = ""

        # A shorter log than last time means the source rotated or reset; start again rather than
        # slicing into the middle of it.
        if len(logs) < printed:
            printed = 0
        if len(logs) > printed:
            print(logs[printed:], end="", flush=True)
            printed = len(logs)

        # Asked against the status already in hand rather than a second `done` call: one describe
        # per poll, and the backend keeps its own words.
        if status in job.terminal_statuses:
            return status
        time.sleep(poll_seconds)


def await_job(job, label: str = "job", poll_seconds: float = LOG_POLL_SECONDS):
    """Block on a job, streaming its container output as it runs, and return its result.

    :param job: A handle satisfying :class:`~ds_platform_utils.metaflow.compute_backends.JobHandle`.
    :param label: Name used in log lines.
    :param poll_seconds: Seconds between log polls.
    :return: Whatever the decorated function returned.
    """
    print(f"[remote_runtime] {label}: job {job.id} accepted; streaming container output", flush=True)
    status = stream_job_logs(job, poll_seconds=poll_seconds)
    print(f"[remote_runtime] {label}: job {job.id} {status}", flush=True)

    # Raises the payload's exception on failure -- the logs above already explain why.
    return job.result()
