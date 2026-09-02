"""output-manifest.json — the runner's declaration of what it produced.

Written by the runner after user step body completes. Read by the driver
to build the RemoteArtifact refs assigned to `self`.

Schema is deliberately small and forward-compatible: unknown top-level keys
are preserved on read.
"""

from __future__ import annotations

from dataclasses import asdict
import json

import boto3

from remote_step.artifact import RemoteArtifact
from remote_step.errors import ManifestMissingError

MANIFEST_KEY = "output-manifest.json"


def output_prefix(run_id: str, task_id: str, attempt: int) -> str:
    """Canonical prefix for a task attempt's output blobs."""
    return f"outputs/{run_id}/{task_id}/{attempt}"


def manifest_key(run_id: str, task_id: str, attempt: int) -> str:
    """Full S3 key for the manifest file."""
    return f"{output_prefix(run_id, task_id, attempt)}/{MANIFEST_KEY}"


def write(
    bucket: str,
    run_id: str,
    task_id: str,
    attempt: int,
    outputs: dict[str, RemoteArtifact],
    s3_client=None,
) -> None:
    """Write output-manifest.json to S3 with the runner's outputs."""
    s3 = s3_client or boto3.client("s3")
    body = {
        "version": 1,
        "run_id": run_id,
        "task_id": task_id,
        "attempt": attempt,
        "outputs": {
            name: {
                "s3_uri": ref.s3_uri,
                "size_bytes": ref.size_bytes,
                "kind": ref.kind,
                "sha256": ref.sha256,
                "pickle_protocol": ref.pickle_protocol,
            }
            for name, ref in outputs.items()
        },
    }
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key(run_id, task_id, attempt),
        Body=json.dumps(body).encode("utf-8"),
        ContentType="application/json",
    )


def read(
    bucket: str,
    run_id: str,
    task_id: str,
    attempt: int,
    s3_client=None,
) -> dict[str, RemoteArtifact]:
    """Read the manifest and return the outputs dict."""
    s3 = s3_client or boto3.client("s3")
    key = manifest_key(run_id, task_id, attempt)
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        blob = resp["Body"].read()
    except s3.exceptions.NoSuchKey as exc:
        raise ManifestMissingError(
            f"expected manifest not found at s3://{bucket}/{key}. "
            f"Batch job may have exited before writing outputs. "
            f"Retry via @retry(times=1).",
            s3_uri=f"s3://{bucket}/{key}",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        # moto sometimes raises ClientError instead of NoSuchKey.
        if "NoSuchKey" in str(type(exc).__name__) or "NoSuchKey" in str(exc):
            raise ManifestMissingError(
                f"expected manifest not found at s3://{bucket}/{key}",
                s3_uri=f"s3://{bucket}/{key}",
            ) from exc
        raise
    body = json.loads(blob)
    if body.get("version") != 1:
        raise ManifestMissingError(
            f"manifest version {body.get('version')} unsupported (expected 1)"
        )
    outputs: dict[str, RemoteArtifact] = {}
    for name, ref in body["outputs"].items():
        outputs[name] = RemoteArtifact(
            s3_uri=ref["s3_uri"],
            size_bytes=ref["size_bytes"],
            kind=ref["kind"],
            sha256=ref["sha256"],
            pickle_protocol=ref.get("pickle_protocol", 5),
        )
    return outputs


def as_dict(art: RemoteArtifact) -> dict:
    """Serialise a RemoteArtifact to the manifest schema form."""
    return {k: v for k, v in asdict(art).items() if not k.startswith("_")}
