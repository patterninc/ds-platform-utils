"""S3 key layout for the payload bucket — the single owner of this schema.

Every key under the payload bucket is built here, so the shape is defined
once rather than assembled from f-strings at each call site.

Layout::

    s3://<payload_bucket>/
      <submitter>/<perimeter>/<flow_name>/<run_id>/
        code/<uuid8>/code.tgz
        specs/<task_id>/<attempt>/spec.json
        inputs/<task_id>/<attempt>/<name>.pkl
        outputs/<task_id>/<attempt>/<name>.pkl
        outputs/<task_id>/<attempt>/output-manifest.json

The prefix reads left to right from broadest to narrowest, so every level is
independently useful with a single `aws s3 ls` or lifecycle rule: everything
one submitter produced, everything in a perimeter, every run of one flow, or
one run.

`<submitter>` names the control plane that submitted the work, not the
compute that ran it. Today that is always Outerbounds. It exists so a second
submitter — a local run, a CI job, another orchestrator — lands in its own
subtree instead of interleaving run ids with Outerbounds'.

`<perimeter>` is the Outerbounds perimeter. Run ids are only unique within a
perimeter, so without this a `default` and a `prod` run could in principle
collide on the same prefix.

`<task_id>/<attempt>` is retained under specs/inputs/outputs so a retry
writes to a fresh attempt rather than overwriting the previous one; the
failed attempt's blobs stay readable for a post-mortem.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

MANIFEST_FILENAME = "output-manifest.json"

# Default submitter segment. Overridable so a non-Outerbounds submitter does
# not collide on run ids.
DEFAULT_SUBMITTER = "outerbounds"

# Used when the perimeter cannot be determined. Matches Outerbounds' own
# default perimeter name, so the common case lands where you would expect
# even if detection fails.
DEFAULT_PERIMETER = "default"

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def slug(raw: str, fallback: str = "unknown") -> str:
    """Make `raw` safe for one S3 key segment.

    S3 tolerates almost anything, but a key segment containing '/' would
    silently create a level of hierarchy, and spaces or '#' make the keys
    awkward to handle with the CLI. Collapse anything outside a conservative
    set to '-'.
    """
    s = _UNSAFE.sub("-", (raw or "").strip()).strip("-.")
    return s or fallback


def resolve_perimeter(default: str = DEFAULT_PERIMETER) -> str:
    """Best-effort Outerbounds perimeter name.

    Checked in order, because which of these exists depends on where the
    driver is running:

      1. OBP_PERIMETER / OB_CURRENT_PERIMETER — set explicitly, or by
         Outerbounds on the pod.
      2. The perimeter embedded in OBP_METAFLOW_CONFIG_URL, which looks like
         `.../v1/perimeters/<name>/metaflowconfigs/`. This is the one most
         likely to be present on an Argo pod, since the driver forwards
         OBP_* variables.
      3. ~/.metaflowconfig/ob_config.json, which is where the `outerbounds`
         CLI records it locally.

    Falls back rather than raising: an unknown perimeter produces a slightly
    less tidy prefix, which is not worth failing a run over.
    """
    for key in ("OBP_PERIMETER", "OB_CURRENT_PERIMETER"):
        val = os.environ.get(key)
        if val:
            return slug(val, default)

    for key in ("OBP_METAFLOW_CONFIG_URL", "OB_CURRENT_PERIMETER_MF_CONFIG_URL"):
        url = os.environ.get(key, "")
        m = re.search(r"/perimeters/([^/]+)", url)
        if m:
            return slug(m.group(1), default)

    try:
        cfg = Path.home() / ".metaflowconfig" / "ob_config.json"
        if cfg.exists():
            body = json.loads(cfg.read_text())
            val = body.get("OB_CURRENT_PERIMETER")
            if val:
                return slug(val, default)
            url = body.get("OB_CURRENT_PERIMETER_MF_CONFIG_URL", "")
            m = re.search(r"/perimeters/([^/]+)", url)
            if m:
                return slug(m.group(1), default)
    except Exception:  # noqa: BLE001
        pass

    return default


def run_prefix(
    perimeter: str,
    flow_name: str,
    run_id: str,
    submitter: str = DEFAULT_SUBMITTER,
) -> str:
    """Root prefix for everything belonging to one run."""
    return (
        f"{slug(submitter, DEFAULT_SUBMITTER)}"
        f"/{slug(perimeter, DEFAULT_PERIMETER)}"
        f"/{slug(flow_name, 'unknown-flow')}"
        f"/{slug(str(run_id), 'unknown-run')}"
    )


def code_key(
    perimeter: str,
    flow_name: str,
    run_id: str,
    unique: str,
    submitter: str = DEFAULT_SUBMITTER,
) -> str:
    """Key for a code tarball.

    `unique` disambiguates repeated submits within a run — the driver builds
    a fresh tarball per step, and two steps of the same run must not clobber
    each other.
    """
    root = run_prefix(perimeter, flow_name, run_id, submitter)
    return f"{root}/code/{slug(unique)}/code.tgz"


def spec_key(
    perimeter: str,
    flow_name: str,
    run_id: str,
    task_id: str,
    attempt: int,
    submitter: str = DEFAULT_SUBMITTER,
) -> str:
    """Key for a task attempt's spec.json."""
    root = run_prefix(perimeter, flow_name, run_id, submitter)
    return f"{root}/specs/{slug(str(task_id))}/{int(attempt)}/spec.json"


def inputs_prefix(
    perimeter: str,
    flow_name: str,
    run_id: str,
    task_id: str,
    attempt: int,
    submitter: str = DEFAULT_SUBMITTER,
) -> str:
    """Prefix for oversized inputs the driver uploads instead of inlining."""
    root = run_prefix(perimeter, flow_name, run_id, submitter)
    return f"{root}/inputs/{slug(str(task_id))}/{int(attempt)}"


def output_prefix(
    perimeter: str,
    flow_name: str,
    run_id: str,
    task_id: str,
    attempt: int,
    submitter: str = DEFAULT_SUBMITTER,
) -> str:
    """Prefix for a task attempt's output blobs and its manifest."""
    root = run_prefix(perimeter, flow_name, run_id, submitter)
    return f"{root}/outputs/{slug(str(task_id))}/{int(attempt)}"


def manifest_key(output_prefix_: str) -> str:
    """Key for the manifest, derived from an already-computed output prefix.

    Takes the prefix rather than the identifiers so the runner does not have
    to reconstruct it. The driver puts `output_prefix` in spec.json, and the
    runner writes its manifest relative to that — which means a change to the
    layout cannot desynchronise the two sides.
    """
    return f"{output_prefix_.rstrip('/')}/{MANIFEST_FILENAME}"
