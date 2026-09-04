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

from remote_step.keys import (  # noqa: E402
    MANIFEST_FILENAME as MANIFEST_KEY,
    manifest_key,
)


def write(
    bucket: str,
    output_prefix: str,
    run_id: str,
    task_id: str,
    attempt: int,
    outputs: dict[str, RemoteArtifact],
    s3_client=None,
) -> None:
    """Write output-manifest.json under `output_prefix`.

    The prefix is passed in rather than rebuilt from the identifiers: the
    runner gets it from spec.json, which is the same value the driver used
    when it uploaded the inputs. That way the key layout is decided in one
    place and the two sides cannot disagree about it.
    """
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
                "read_role_arn": ref.read_role_arn,
            }
            for name, ref in outputs.items()
        },
    }
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key(output_prefix),
        Body=json.dumps(body).encode("utf-8"),
        ContentType="application/json",
    )


def read(
    bucket: str,
    output_prefix: str,
    s3_client=None,
) -> dict[str, RemoteArtifact]:
    """Read the manifest under `output_prefix` and return the outputs dict."""
    s3 = s3_client or boto3.client("s3")
    key = manifest_key(output_prefix)
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        blob = resp["Body"].read()
    except s3.exceptions.NoSuchKey as exc:
        raise ManifestMissingError(
            f"expected manifest not found at s3://{bucket}/{key}. "
            f"The pod may have exited before writing outputs. "
            f"Retry via @retry(times=1).",
            s3_uri=f"s3://{bucket}/{key}",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        # moto sometimes raises ClientError instead of NoSuchKey.
        #
        # AccessDenied is treated the same way on purpose: S3 answers a
        # missing key with 403 rather than 404 when the caller lacks
        # s3:ListBucket, so a genuinely absent manifest surfaces as
        # AccessDenied and would otherwise re-raise as a bare ClientError,
        # losing the retriable classification and the actionable message.
        text = f"{type(exc).__name__} {exc}"
        if "NoSuchKey" in text or "AccessDenied" in text or "404" in text:
            raise ManifestMissingError(
                f"expected manifest not found (or unreadable) at "
                f"s3://{bucket}/{key}: {type(exc).__name__}. "
                f"The pod may have exited before writing outputs. "
                f"Retry via @retry(times=1).",
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
            read_role_arn=ref.get("read_role_arn", ""),
        )
    return outputs


def as_dict(art: RemoteArtifact) -> dict:
    """Serialise a RemoteArtifact to the manifest schema form."""
    return {k: v for k, v in asdict(art).items() if not k.startswith("_")}
