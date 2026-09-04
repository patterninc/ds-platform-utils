"""Runs inside the runner pod.

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
from botocore.config import Config as BotocoreConfig

from remote_step.artifact import RemoteArtifact, _upload_buf


def _make_s3_client() -> Any:
    """Create an S3 client with a connection pool sized for our workload.

    The default boto pool of 10 chokes TransferManager runs that fan
    out to 32 threads for ≥2 GB blobs. Adaptive retries help absorb
    the occasional S3 throttling on very-parallel multipart uploads.
    """
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION"),
        config=BotocoreConfig(
            max_pool_connections=64,
            retries={"max_attempts": 8, "mode": "adaptive"},
        ),
    )


# Number of output attrs we upload in parallel across the outputs loop.
# Each worker gets its own dedicated boto S3 client (boto clients are
# safe to call from multiple threads, but sharing one across concurrent
# TransferManager runs has been flaky under load). The multipart /
# concurrency thresholds themselves live in ``artifact.py`` and are
# reused via the shared ``_upload_buf`` helper.
_OUTPUTS_PARALLELISM = 4


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

    The runner pod has plenty of memory, so we materialise
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

    Memory-hot path — we're routinely serialising DataFrames in the 1-30 GB
    range. Keep peak RAM to a single copy of the pickled bytes by:
      1. Pickling into a BytesIO (allocation 1).
      2. Streaming that BytesIO through sha256 in 4 MB chunks
         (no bytes copy).
      3. Handing the same BytesIO to boto3 for upload — put_object below
         100 MB, TransferManager upload_fileobj above (size-tuned
         multipart concurrency to saturate node egress).

    Previous version did ``blob = buf.getvalue()`` + ``io.BytesIO(blob)``,
    pushing peak RAM to 3× the pickle size and OOM-killing the pod on
    multi-GB outputs.
    """
    buf = io.BytesIO()
    pickle.dump(obj, buf, protocol=5)
    size = buf.tell()

    buf.seek(0)
    h = hashlib.sha256()
    for chunk in iter(lambda: buf.read(4 * 1024 * 1024), b""):
        h.update(chunk)
    sha = h.hexdigest()

    buf.seek(0)
    # Delegate to the shared uploader — same multipart threshold, size-tuned
    # concurrency, and progress-logging callback. Key name doubles as the
    # progress label so a multi-attr step logs one interleaved stream of
    # "download build_df_core_daily/df_core_daily.pkl: 512.3 / 4,096.0 MB (12.5%)".
    _upload_buf(s3_client, bucket, key, buf, size, label=f"upload {key.rsplit('/', 1)[-1]}")
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

    s3 = _make_s3_client()

    # 1. Load spec.
    t0 = time.time()
    try:
        spec = _read_spec(spec_uri, s3)
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] STAGE=load_spec ERR {exc}\n")
        traceback.print_exc()
        return 3
    _stage("load_spec", t0=t0)

    # 2. Hydrate inputs onto fake_self. Parallel across attrs so a step
    # with a handful of multi-GB DataFrames doesn't serialise the
    # downloads; each worker gets its own thread-local boto client for
    # the same reason as the outputs loop.
    t0 = time.time()
    fake = _FakeSelf()
    inputs_dict = spec.get("inputs", {}) or {}
    _hydrate_local = threading.local()

    def _hydrate_worker_s3():
        client = getattr(_hydrate_local, "s3", None)
        if client is None:
            client = _make_s3_client()
            _hydrate_local.s3 = client
        return client

    def _hydrate_one(item: tuple[str, dict]) -> tuple[str, Any]:
        name, ref = item
        return name, _hydrate_input(name, ref, _hydrate_worker_s3())

    try:
        if inputs_dict:
            workers = min(_OUTPUTS_PARALLELISM, len(inputs_dict))
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=workers, thread_name_prefix="remote-step-hydrate"
            ) as pool:
                for fut in concurrent.futures.as_completed(
                    [pool.submit(_hydrate_one, item) for item in inputs_dict.items()]
                ):
                    name, val = fut.result()
                    setattr(fake, name, val)
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] STAGE=hydrate_inputs ERR {exc}\n")
        traceback.print_exc()
        return 3
    _stage("hydrate_inputs", t0=t0)
    inputs_snapshot = set(vars(fake).keys())

    # Patch metaflow.current with the flow's context so user code that
    # reads `current.tags`, `current.run_id`, etc. works inside the pod.
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
            client = _make_s3_client()
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
