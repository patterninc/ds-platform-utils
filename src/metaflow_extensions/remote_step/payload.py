"""Build and upload the spec.json payload the runner reads.

spec.json shape:

    {
        "version": 1,
        "flow_module": "src.weekly_flow",
        "flow_class": "WeeklyForecastFlow",
        "step_name": "do_forecast",
        "flow_name": "WeeklyForecastFlow",
        "run_id": "224221",
        "task_id": "abc123",
        "attempt": 0,
        "code_package_url": "s3://pattern-ml-platform/outerbounds/default/WeeklyForecastFlow/224221/code/9f3ab21c/code.tgz",
        "code_package_sha": "...",
        "datastore_root": "s3://outerbounds-datastore/",
        "env": {
            "python": "3.10.15",
            "packages": {"numpy": "2.2.6", ...}
        },
        "inputs": {
            "df_train": {"kind": "RemoteArtifact", "s3_uri": "...", ...},
            "config": {"kind": "inline", "blob_b64": "..."}
        },
        "output_bucket": "pattern-ml-platform",
        "output_prefix": "outerbounds/default/WeeklyForecastFlow/224221/outputs/abc123/0",
        "mfconfig": {"METAFLOW_SERVICE_URL": "...", ...}
    }
"""


from __future__ import annotations

from __future__ import annotations

import base64
from dataclasses import dataclass
import json
import pickle
from typing import Any

import boto3

from remote_step import keys
from remote_step.artifact import RemoteArtifact
from remote_step.errors import RemoteStepError

MAX_INLINE_INPUT_BYTES = 100 * 1024 * 1024  # 100 MB total
INLINE_ATTR_LIMIT_BYTES = 4 * 1024 * 1024  # any single attr over 4 MB is uploaded as a RemoteArtifact ref


@dataclass
class DriverContext:
    """What the driver task knows about itself at submit time."""

    flow_module: str
    flow_class: str
    step_name: str
    flow_name: str
    run_id: str
    task_id: str
    attempt: int
    code_package_url: str
    code_package_sha: str
    datastore_root: str
    mfconfig: dict[str, str]
    tags: list[str] = None  # type: ignore[assignment]
    artifact_read_role_arn: str = ""
    # Outerbounds perimeter. Part of the S3 prefix because run ids are only
    # unique within a perimeter.
    perimeter: str = keys.DEFAULT_PERIMETER


def build_spec(
    ctx: DriverContext,
    env_spec: dict,
    inputs: dict[str, Any],
    output_bucket: str,
    s3_client=None,
) -> dict:
    """Build the spec.json dict.

    Attrs already stored as `RemoteArtifact` pass through as tiny refs.
    Anything bigger than `INLINE_ATTR_LIMIT_BYTES` when pickled is uploaded
    to the payload bucket and referenced as a `RemoteArtifact`, so the
    inline portion of the spec stays tiny even for huge upstream inputs.
    """
    from remote_step.artifact import write_artifact_from_buf
    import io as _io

    prefix = keys.output_prefix(
        ctx.perimeter, ctx.flow_name, ctx.run_id, ctx.task_id, ctx.attempt
    )
    in_prefix = keys.inputs_prefix(
        ctx.perimeter, ctx.flow_name, ctx.run_id, ctx.task_id, ctx.attempt
    )
    serialised: dict[str, dict] = {}
    inline_total = 0
    for name, val in inputs.items():
        if isinstance(val, RemoteArtifact):
            serialised[name] = {
                "kind": "RemoteArtifact",
                "s3_uri": val.s3_uri,
                "size_bytes": val.size_bytes,
                "type_kind": val.kind,
                "sha256": val.sha256,
                "pickle_protocol": val.pickle_protocol,
            }
            continue
        # Pickle into a BytesIO so we can reuse the *same* buffer for
        # both the size-check and the upload — the driver pod is Small
        # tier and can't afford the 2-3x peak RAM that a
        # ``pickle.dumps -> io.BytesIO(bytes)`` round-trip would need.
        _buf = _io.BytesIO()
        pickle.dump(val, _buf, protocol=5)
        size = _buf.tell()
        if size > INLINE_ATTR_LIMIT_BYTES:
            key = f"{in_prefix}/{name}.pkl"
            ref = write_artifact_from_buf(
                obj_kind=type(val).__module__ + "." + type(val).__qualname__,
                buf=_buf,
                size=size,
                bucket=output_bucket,
                key=key,
                s3_client=s3_client,
                pickle_protocol=5,
                read_role_arn=ctx.artifact_read_role_arn,
            )
            serialised[name] = {
                "kind": "RemoteArtifact",
                "s3_uri": ref.s3_uri,
                "size_bytes": ref.size_bytes,
                "type_kind": ref.kind,
                "sha256": ref.sha256,
                "pickle_protocol": ref.pickle_protocol,
            }
            continue
        inline_total += size
        if inline_total > MAX_INLINE_INPUT_BYTES:
            raise RemoteStepError(
                f"inline inputs exceed {MAX_INLINE_INPUT_BYTES // 1024 // 1024} "
                f"MB (culprit near '{name}'). Produce large upstream artifacts "
                f"with @remote_step so they travel as RemoteArtifact refs.",
                culprit=name,
                size_bytes=inline_total,
            )
        # Small attr (< 4 MB): fine to materialise the pickle as bytes
        # for base64 encoding. The buffer is tiny by construction.
        serialised[name] = {
            "kind": "inline",
            "type_kind": type(val).__module__ + "." + type(val).__qualname__,
            "blob_b64": base64.b64encode(_buf.getvalue()).decode("ascii"),
        }
    return {
        "version": 1,
        "flow_module": ctx.flow_module,
        "flow_class": ctx.flow_class,
        "step_name": ctx.step_name,
        "flow_name": ctx.flow_name,
        "run_id": ctx.run_id,
        "task_id": ctx.task_id,
        "attempt": ctx.attempt,
        "code_package_url": ctx.code_package_url,
        "code_package_sha": ctx.code_package_sha,
        "datastore_root": ctx.datastore_root,
        "env": env_spec,
        "inputs": serialised,
        "output_bucket": output_bucket,
        "output_prefix": prefix,
        "perimeter": ctx.perimeter,
        "mfconfig": ctx.mfconfig,
        "tags": list(ctx.tags or []),
        "artifact_read_role_arn": ctx.artifact_read_role_arn,
    }





def upload_spec(bucket: str, spec: dict, s3_client=None) -> str:
    """Upload spec.json to S3, return full s3:// URI."""
    s3 = s3_client or boto3.client("s3")
    key = keys.spec_key(
        spec.get("perimeter", keys.DEFAULT_PERIMETER),
        spec["flow_name"],
        spec["run_id"],
        spec["task_id"],
        spec["attempt"],
    )
    body = json.dumps(spec).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"


def build_and_upload(
    ctx: DriverContext,
    env_spec: dict,
    inputs: dict[str, Any],
    output_bucket: str,
    s3_client=None,
) -> tuple[str, dict]:
    """Convenience: build spec + upload. Returns (s3_uri, spec_dict)."""
    spec = build_spec(ctx, env_spec, inputs, output_bucket, s3_client=s3_client)
    uri = upload_spec(output_bucket, spec, s3_client=s3_client)
    return uri, spec
