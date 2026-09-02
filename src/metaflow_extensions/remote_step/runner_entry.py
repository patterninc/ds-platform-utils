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
import hashlib
import importlib
import io
import json
import os
import pickle
import sys
import time
import traceback
from typing import Any
from urllib.parse import urlparse

import boto3

from remote_step.artifact import RemoteArtifact


def _stage(name: str, ok: bool = True, t0: float | None = None) -> None:
    """Print a stage marker to stderr."""
    if t0 is not None:
        dur = f"{time.time() - t0:.1f}s"
    else:
        dur = ""
    status = "OK " if ok else "ERR"
    sys.stderr.write(f"[remote_step] STAGE={name} {status} {dur}\n")
    sys.stderr.flush()


def _read_spec(spec_uri: str, s3_client) -> dict:
    parsed = urlparse(spec_uri)
    if parsed.scheme != "s3":
        with open(spec_uri) as f:
            return json.load(f)
    obj = s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    return json.loads(obj["Body"].read())


def _hydrate_input(name: str, ref: dict, s3_client) -> Any:
    """Rebuild an input value from the spec entry."""
    kind = ref.get("kind")
    if kind == "RemoteArtifact":
        return RemoteArtifact(
            s3_uri=ref["s3_uri"],
            size_bytes=ref["size_bytes"],
            kind=ref["type_kind"],
            sha256=ref["sha256"],
            pickle_protocol=ref.get("pickle_protocol", 5),
        )
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
    """Pickle obj, upload to S3, return (size_bytes, sha256_hex)."""
    buf = io.BytesIO()
    pickle.dump(obj, buf, protocol=5)
    blob = buf.getvalue()
    sha = hashlib.sha256(blob).hexdigest()
    s3_client.put_object(Bucket=bucket, Key=key, Body=blob)
    return len(blob), sha


def main(spec_uri: str | None = None) -> int:
    """Runner entry point. Returns POSIX-style exit code."""
    spec_uri = spec_uri or os.environ.get("REMOTE_STEP_SPEC_URI")
    if not spec_uri:
        sys.stderr.write("[remote_step] REMOTE_STEP_SPEC_URI unset\n")
        return 3

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))

    # 1. Load spec.
    t0 = time.time()
    try:
        spec = _read_spec(spec_uri, s3)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"[remote_step] STAGE=load_spec ERR {exc}\n")
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
        sys.stderr.write(f"[remote_step] STAGE=hydrate_inputs ERR {exc}\n")
        traceback.print_exc()
        return 3
    _stage("hydrate_inputs", t0=t0)
    inputs_snapshot = set(vars(fake).keys())

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
        sys.stderr.write(f"[remote_step] STAGE=import_step ERR {exc}\n")
        traceback.print_exc()
        return 6
    _stage("import_step", t0=t0)

    # 4. Execute user body.
    _stage("user_step_start")
    t0 = time.time()
    try:
        original(fake)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"[remote_step] STAGE=user_step_end ERR {exc}\n")
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

    # 6. Persist outputs.
    t0 = time.time()
    bucket = spec["output_bucket"]
    prefix = spec["output_prefix"]
    manifest_outputs: dict[str, RemoteArtifact] = {}
    try:
        for name, val in outputs.items():
            key = f"{prefix}/{name}.pkl"
            size, sha = _put_pickle(val, bucket, key, s3)
            manifest_outputs[name] = RemoteArtifact(
                s3_uri=f"s3://{bucket}/{key}",
                size_bytes=size,
                kind=type(val).__module__ + "." + type(val).__qualname__,
                sha256=sha,
                pickle_protocol=5,
            )
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"[remote_step] STAGE=persist_outputs ERR {exc}\n")
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
        sys.stderr.write(f"[remote_step] STAGE=write_manifest ERR {exc}\n")
        traceback.print_exc()
        return 1
    _stage("write_manifest", t0=t0)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else None))
