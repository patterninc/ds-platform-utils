"""S3 key layout for the payload bucket — the single owner of this schema.

Every key under the payload bucket is built here, so the shape is defined
once rather than assembled from f-strings at each call site.

Layout::

    s3://<payload_bucket>/
      <submitter>/<run_id>/
        code/<uuid8>/code.tgz
        specs/<task_id>/<attempt>/spec.json
        inputs/<task_id>/<attempt>/<name>.pkl
        outputs/<task_id>/<attempt>/<name>.pkl
        outputs/<task_id>/<attempt>/output-manifest.json

Everything a run produces sits under one `<submitter>/<run_id>/` prefix, so
a run can be inspected, copied or lifecycle-expired with a single prefix
operation.

`<submitter>` names the control plane that submitted the work, not the
compute that ran it. Today that is always Outerbounds. It exists so a second
submitter — a local `--with kubernetes` run, a CI job, another orchestrator —
lands in its own subtree instead of interleaving run ids with Outerbounds',
whose run ids are only unique within Outerbounds.

`<task_id>/<attempt>` is retained under specs/inputs/outputs so a retry
writes to a fresh attempt rather than overwriting the previous one; the
failed attempt's blobs stay readable for a post-mortem.
"""

from __future__ import annotations

MANIFEST_FILENAME = "output-manifest.json"

# Default submitter segment. Overridable so a non-Outerbounds submitter does
# not collide on run ids.
DEFAULT_SUBMITTER = "outerbounds"


def run_prefix(run_id: str, submitter: str = DEFAULT_SUBMITTER) -> str:
    """Root prefix for everything belonging to one run."""
    return f"{submitter}/{run_id}"


def code_key(
    run_id: str,
    unique: str,
    submitter: str = DEFAULT_SUBMITTER,
) -> str:
    """Key for a code tarball.

    `unique` disambiguates repeated submits within a run — the driver builds
    a fresh tarball per step, and two steps of the same run must not clobber
    each other.
    """
    return f"{run_prefix(run_id, submitter)}/code/{unique}/code.tgz"


def spec_key(
    run_id: str,
    task_id: str,
    attempt: int,
    submitter: str = DEFAULT_SUBMITTER,
) -> str:
    """Key for a task attempt's spec.json."""
    return f"{run_prefix(run_id, submitter)}/specs/{task_id}/{attempt}/spec.json"


def inputs_prefix(
    run_id: str,
    task_id: str,
    attempt: int,
    submitter: str = DEFAULT_SUBMITTER,
) -> str:
    """Prefix for oversized inputs the driver uploads instead of inlining."""
    return f"{run_prefix(run_id, submitter)}/inputs/{task_id}/{attempt}"


def output_prefix(
    run_id: str,
    task_id: str,
    attempt: int,
    submitter: str = DEFAULT_SUBMITTER,
) -> str:
    """Prefix for a task attempt's output blobs and its manifest."""
    return f"{run_prefix(run_id, submitter)}/outputs/{task_id}/{attempt}"


def manifest_key(
    run_id: str,
    task_id: str,
    attempt: int,
    submitter: str = DEFAULT_SUBMITTER,
) -> str:
    """Key for a task attempt's output-manifest.json."""
    return f"{output_prefix(run_id, task_id, attempt, submitter)}/{MANIFEST_FILENAME}"
