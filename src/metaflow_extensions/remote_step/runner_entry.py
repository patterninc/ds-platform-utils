"""Runs inside the AWS Batch container.

Life of a run:
  1. Read spec.json (URI passed via env or argv).
  2. Materialise inputs (RemoteArtifact refs stay lazy; inline blobs unpickle).
  3. Import the user's flow module from /workspace.
  4. Build a fake_self with the input attrs.
  5. Execute the user's step body against fake_self.
  6. Snapshot new/modified attrs.
  7. Pickle each output to S3, write output-manifest.json.

Emits stage markers to stderr so the driver-side poller can attribute
failures. Exit codes:
    0 success
    3 spec/payload load failure
    4 env issue detected inside runner
    5 code-package fetch failure (usually caught by entrypoint.sh)
    6 could not call user step body (import/signature)
    1 user code raised
"""


from __future__ import annotations

from __future__ import annotations

import base64
import concurrent.futures
import hashlib
import importlib
import io
import json
import os
import pickle
import sys
import threading
import time
import traceback
from typing import Any
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import TransferConfig

from remote_step.artifact import RemoteArtifact


# Multipart upload thresholds (bytes) for the runner's output persistence.
# - _S3_MULTIPART_THRESHOLD: switch from put_object to upload_fileobj here.
# - _S3_MULTIPART_CHUNK_SIZE: part size handed to the TransferManager. Larger
#   parts mean fewer round trips at the cost of more RAM per concurrent
#   upload. 32 MB is a good balance for 10-100 GB outputs.
# - _S3_LARGE_BLOB_THRESHOLD: above this we bump per-upload concurrency
#   from the boto default (10 threads) so a single huge pickle saturates
#   the Fargate task's egress bandwidth.
# - _S3_MAX_CONCURRENCY_BIG / _S3_MAX_CONCURRENCY_SMALL: worker counts
#   inside boto3.s3.transfer.TransferManager per upload.
_S3_MULTIPART_THRESHOLD = 100 * 1024 * 1024        # 100 MB
_S3_MULTIPART_CHUNK_SIZE = 32 * 1024 * 1024        # 32 MB
_S3_LARGE_BLOB_THRESHOLD = 2 * 1024 * 1024 * 1024  # 2 GB
_S3_MAX_CONCURRENCY_SMALL = 10
_S3_MAX_CONCURRENCY_BIG = 32

# Number of output attrs we upload in parallel across the outputs loop.
# Each worker gets its own dedicated boto S3 client (boto clients are not
# thread-safe for concurrent multipart uploads on the same instance).
_OUTPUTS_PARALLELISM = 4


def _transfer_config_for(size: int) -> TransferConfig:
    """Return a TransferConfig tuned to the payload's size."""
    concurrency = (
        _S3_MAX_CONCURRENCY_BIG
        if size >= _S3_LARGE_BLOB_THRESHOLD
        else _S3_MAX_CONCURRENCY_SMALL
    )
    return TransferConfig(
        multipart_threshold=_S3_MULTIPART_THRESHOLD,
        multipart_chunksize=_S3_MULTIPART_CHUNK_SIZE,
        max_concurrency=concurrency,
        use_threads=True,
    )


def _stage(name: str, ok: bool = True, t0: float | None = None) -> None:
    """Print a stage marker to stderr."""
    if t0 is not None:
        dur = f"{time.time() - t0:.1f}s"
    else:
        dur = ""
    status = "OK " if ok else "ERR"
    sys.stdout.write(f"[remote_step] STAGE={name} {status} {dur}\n")
    sys.stdout.flush()


def _read_spec(spec_uri: str, s3_client) -> dict:
    parsed = urlparse(spec_uri)
    if parsed.scheme != "s3":
        with open(spec_uri) as f:
            return json.load(f)
    obj = s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    return json.loads(obj["Body"].read())


def _hydrate_input(name: str, ref: dict, s3_client) -> Any:
    """Rebuild an input value from the spec entry.

    The Batch container has plenty of memory, so we materialise
    RemoteArtifact refs into the original Python objects here — the user's
    step body then sees native `pd.DataFrame`, `int`, etc. exactly as
    prior steps produced them. Driver on Argo pod never loads them.
    """
    kind = ref.get("kind")
    if kind == "RemoteArtifact":
        artifact = RemoteArtifact(
            s3_uri=ref["s3_uri"],
            size_bytes=ref["size_bytes"],
            kind=ref["type_kind"],
            sha256=ref["sha256"],
            pickle_protocol=ref.get("pickle_protocol", 5),
        )
        return artifact.load(s3_client=s3_client)
    if kind == "inline":
        return pickle.loads(base64.b64decode(ref["blob_b64"]))
    raise ValueError(f"unknown input kind for {name!r}: {kind}")


class _FakeSelf:
    """Object presented to the user step body in place of Metaflow's `self`.

    Absorbs Metaflow-flow-specific calls (`self.next(...)`, `self.input`,
    references to sibling step methods like `self.scale`) so unmodified
    step bodies run cleanly. Only attribute writes matter — those become
    RemoteArtifact outputs.
    """

    def next(self, *args, **kwargs):
        """No-op stand-in for Metaflow's `self.next(...)` — routing runs on driver."""
        return None

    @property
    def input(self):
        """Absent inputs stand-in — foreach flows are v2."""
        return None

    def __getattr__(self, name):
        """Missing attrs resolve to a no-op callable — usually a step-method reference.

        Only triggered when normal attribute lookup fails (i.e. never set on
        the instance). Prevents `self.next(self.scale)` and similar from
        crashing when the user's step body references sibling steps.
        """
        if name.startswith("__"):
            raise AttributeError(name)

        def _placeholder(*args, **kwargs):
            return None

        _placeholder.__name__ = name
        return _placeholder


def _put_pickle(obj: Any, bucket: str, key: str, s3_client) -> tuple[int, str]:
    """Pickle obj, upload to S3, return (size_bytes, sha256_hex).

    S3's ``PutObject`` caps at 5 GB per request, so anything above
    ``_S3_MULTIPART_THRESHOLD`` goes through ``upload_fileobj`` — boto3's
    TransferManager, which transparently splits into multipart chunks and
    parallelises the upload with a per-blob concurrency tuned to size
    (small: 10 workers, ≥2 GB: 32 workers) to saturate the Fargate
    task's egress bandwidth.
    """
    buf = io.BytesIO()
    pickle.dump(obj, buf, protocol=5)
    blob = buf.getvalue()
    size = len(blob)
    sha = hashlib.sha256(blob).hexdigest()
    if size <= _S3_MULTIPART_THRESHOLD:
        s3_client.put_object(Bucket=bucket, Key=key, Body=blob)
    else:
        cfg = _transfer_config_for(size)
        s3_client.upload_fileobj(io.BytesIO(blob), Bucket=bucket, Key=key, Config=cfg)
    return size, sha


def main(spec_uri: str | None = None) -> int:
    """Runner entry point. Returns POSIX-style exit code."""
    # Line-buffer so mflog / Outerbounds UI sees each line as it's written.
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except Exception:  # noqa: BLE001
        pass
    spec_uri = spec_uri or os.environ.get("REMOTE_STEP_SPEC_URI")
    if not spec_uri:
        sys.stdout.write("[remote_step] REMOTE_STEP_SPEC_URI unset\n")
        return 3

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))

    # 1. Load spec.
    t0 = time.time()
    try:
        spec = _read_spec(spec_uri, s3)
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] STAGE=load_spec ERR {exc}\n")
        traceback.print_exc()
        return 3
    _stage("load_spec", t0=t0)

    # 2. Hydrate inputs onto fake_self.
    t0 = time.time()
    fake = _FakeSelf()
    try:
        for name, ref in spec.get("inputs", {}).items():
            setattr(fake, name, _hydrate_input(name, ref, s3))
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] STAGE=hydrate_inputs ERR {exc}\n")
        traceback.print_exc()
        return 3
    _stage("hydrate_inputs", t0=t0)
    inputs_snapshot = set(vars(fake).keys())

    # Patch metaflow.current with the flow's context so user code that
    # reads `current.tags`, `current.run_id`, etc. works inside Batch.
    try:
        from metaflow import current as _current
        _current._flow_name = spec.get("flow_name")
        _current._run_id = spec.get("run_id")
        _current._step_name = spec.get("step_name")
        _current._task_id = spec.get("task_id")
        _current._retry_count = spec.get("attempt", 0)
        _all_tags = tuple(spec.get("tags") or [])
        _current._tags = _all_tags
        _current._system_tags = tuple(
            t for t in _all_tags if t.startswith(("user:", "runtime:", "python_version:", "metaflow_version:", "project:", "project_branch:"))
        )
        _current._is_running = True
    except Exception:  # noqa: BLE001
        pass

    # 3. Import user step. Find the flow module file anywhere under /workspace.
    t0 = time.time()
    try:
        sys.path.insert(0, "/workspace")
        flow_module_name = spec["flow_module"]
        flow_module = None
        try:
            flow_module = importlib.import_module(flow_module_name)
        except (ImportError, ModuleNotFoundError):
            for dirpath, _dirs, files in os.walk("/workspace"):
                if f"{flow_module_name}.py" in files:
                    if dirpath not in sys.path:
                        sys.path.insert(0, dirpath)
                    flow_module = importlib.import_module(flow_module_name)
                    break
        if flow_module is None:
            raise ImportError(f"could not locate module {flow_module_name} in /workspace")
        flow_cls = getattr(flow_module, spec["flow_class"])
        step_fn = getattr(flow_cls, spec["step_name"])
        original = getattr(step_fn, "__wrapped__", step_fn)
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] STAGE=import_step ERR {exc}\n")
        traceback.print_exc()
        return 6
    _stage("import_step", t0=t0)

    # 4. Execute user body.
    _stage("user_step_start")
    t0 = time.time()
    try:
        original(fake)
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] STAGE=user_step_end ERR {exc}\n")
        traceback.print_exc()
        return 1
    _stage("user_step_end", t0=t0)

    # 5. Snapshot new/modified attrs.
    new_attrs = {
        k: v
        for k, v in vars(fake).items()
        if not k.startswith("_") and (k not in inputs_snapshot or v is not None)
    }
    # Drop attrs that started as inputs and were not reassigned.
    input_ids: dict[str, int] = {
        k: id(v) for k, v in vars(fake).items() if k in inputs_snapshot
    }
    outputs = {
        k: v
        for k, v in new_attrs.items()
        if k not in input_ids or id(v) != input_ids[k]
    }

    # 6. Persist outputs. Parallelise across attrs so a step with many
    # multi-GB DataFrames doesn't pay the per-upload wall-clock N times
    # over. Each worker gets its own boto3 S3 client — the botocore
    # client's connection pool is thread-safe for calls, but sharing one
    # client across concurrent multipart uploads has been flaky in
    # practice, so we spend the ~200 KB per extra client to be safe.
    t0 = time.time()
    bucket = spec["output_bucket"]
    prefix = spec["output_prefix"]
    # Every ref we write out inherits the cross-account read role from the
    # spec so downstream non-@remote_step consumers on the Outerbounds pod
    # can lazy-load without our bucket having to be readable from OB's
    # account directly.
    read_role_arn = spec.get("artifact_read_role_arn", "") or ""
    manifest_outputs: dict[str, RemoteArtifact] = {}
    manifest_lock = threading.Lock()
    _local = threading.local()

    def _worker_s3():
        client = getattr(_local, "s3", None)
        if client is None:
            client = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))
            _local.s3 = client
        return client

    def _upload_one(item: tuple[str, Any]) -> None:
        name, val = item
        key = f"{prefix}/{name}.pkl"
        size, sha = _put_pickle(val, bucket, key, _worker_s3())
        ref = RemoteArtifact(
            s3_uri=f"s3://{bucket}/{key}",
            size_bytes=size,
            kind=type(val).__module__ + "." + type(val).__qualname__,
            sha256=sha,
            pickle_protocol=5,
            read_role_arn=read_role_arn,
        )
        with manifest_lock:
            manifest_outputs[name] = ref

    workers = min(_OUTPUTS_PARALLELISM, max(1, len(outputs)))
    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="remote-step-upload"
        ) as pool:
            for fut in concurrent.futures.as_completed(
                [pool.submit(_upload_one, item) for item in outputs.items()]
            ):
                # Re-raise the first worker exception; the executor will
                # cancel remaining futures on ThreadPoolExecutor exit.
                fut.result()
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] STAGE=persist_outputs ERR {exc}\n")
        traceback.print_exc()
        return 1
    _stage("persist_outputs", t0=t0)

    # 7. Write manifest.
    t0 = time.time()
    try:
        from remote_step.manifest import write as write_manifest

        write_manifest(
            bucket,
            spec["run_id"],
            spec["task_id"],
            spec["attempt"],
            manifest_outputs,
            s3_client=s3,
        )
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] STAGE=write_manifest ERR {exc}\n")
        traceback.print_exc()
        return 1
    _stage("write_manifest", t0=t0)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else None))
