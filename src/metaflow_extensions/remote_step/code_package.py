"""Package the user's project code and upload to our payload S3 bucket.

We tar the code and re-upload to our bucket so the runner pod's IAM
role can fetch it. If we forwarded Metaflow's own code-package URL (which
lives in the Outerbounds account), the runner's role would need
cross-account read on the Outerbounds datastore — a change we don't own.

Strategy:
  1. If `METAFLOW_EXTRACTED_ROOT` is set (Argo pod), tar from
     `<root>/.mf_code` — Metaflow's already-extracted code lives there.
  2. Otherwise, tar cwd:
     - `git ls-files -co --exclude-standard` — tracked + untracked, minus gitignored
     - fall back to walk of cwd if not a git repo
     - skip files over 10 MB
     - refuse if total > 50 MB
"""

from __future__ import annotations

import hashlib
import io
import os
import subprocess
import tarfile
import uuid
from typing import Iterable

import boto3

from remote_step import keys


MAX_FILE_BYTES = 10 * 1024 * 1024
MAX_TOTAL_BYTES = 50 * 1024 * 1024


def _git_files(root: str) -> Iterable[str] | None:
    """Try `git ls-files -co --exclude-standard`. Return None if not a git repo."""
    try:
        proc = subprocess.run(
            ["git", "ls-files", "-co", "--exclude-standard"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return [line for line in proc.stdout.splitlines() if line.strip()]


EXCLUDE_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    "node_modules",
    # Skip Metaflow's own bundled code — the runner container has it via pip.
    "metaflow",
    "metaflow_extensions",
    ".mf_meta",
    ".metaflow",
    "micromamba",
    "linux-64",
    "linux-aarch64",
    "osx-64",
    "osx-arm64",
}

# Only ship a small set of source/data suffixes. Anything else is either
# reproducible from pip / conda inside the runner or too big to justify.
INCLUDE_SUFFIXES = {
    ".py",
    ".sql",
    ".yaml",
    ".yml",
    ".json",
    ".toml",
    ".txt",
    ".md",
    ".lock",
    ".cfg",
    ".ini",
}


def _walk_files(root: str) -> list[str]:
    """Fallback file discovery when not a git repo."""
    out: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for f in filenames:
            rel = os.path.relpath(os.path.join(dirpath, f), root)
            out.append(rel)
    return out


def build_tarball(root: str) -> bytes:
    """Tar the project (respecting git ignore rules), return bytes.

    Skips files over MAX_FILE_BYTES. Refuses if total > MAX_TOTAL_BYTES.
    """
    files = _git_files(root) or _walk_files(root)
    buf = io.BytesIO()
    total = 0
    skipped: list[tuple[str, int]] = []
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for rel in files:
            parts = rel.split(os.sep)
            if any(p in EXCLUDE_DIRS for p in parts):
                continue
            _, ext = os.path.splitext(rel)
            if ext and ext.lower() not in INCLUDE_SUFFIXES:
                continue
            full = os.path.join(root, rel)
            if not os.path.isfile(full):
                continue
            size = os.path.getsize(full)
            if size > MAX_FILE_BYTES:
                skipped.append((rel, size))
                continue
            tar.add(full, arcname=rel)
            total += size
            if total > MAX_TOTAL_BYTES:
                raise RuntimeError(
                    f"code package exceeds {MAX_TOTAL_BYTES // 1024 // 1024} MB "
                    f"(so far: {total // 1024 // 1024} MB). Trim unused files."
                )
    blob = buf.getvalue()
    return blob


def resolve_code_package(
    payload_bucket: str,
    run_id: str,
    s3_client=None,
    root: str | None = None,
) -> tuple[str, str]:
    """Return (code_url, code_sha) for a tarball uploaded to our S3 bucket.

    If `METAFLOW_EXTRACTED_ROOT` is set (we're in an Argo pod), tar from
    `<root>/.mf_code`. Otherwise tar `root` (default cwd).
    """
    if root is None:
        # Argo pod: tar from the parent so we pick up whatever layout Metaflow
        # laid down (flow file placed by argo bootstrap, `.mf_code/*` for user
        # code shipped with the run, etc.). Metaflow's own vendored copies
        # of `metaflow/` and `metaflow_extensions/` are excluded by
        # EXCLUDE_DIRS — the runner container already has them via pip.
        mf_root = os.environ.get("METAFLOW_EXTRACTED_ROOT")
        if mf_root and os.path.isdir(mf_root):
            root = mf_root
        else:
            root = os.getcwd()
    blob = build_tarball(root)
    sha = hashlib.sha256(blob).hexdigest()
    key = keys.code_key(run_id, uuid.uuid4().hex[:8])
    s3 = s3_client or boto3.client("s3")
    s3.put_object(Bucket=payload_bucket, Key=key, Body=blob)
    return f"s3://{payload_bucket}/{key}", sha
