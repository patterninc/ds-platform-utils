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
from remote_step.errors import RemoteStepError


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


EXCEPTION_FILENAME = "exception.pkl"
RUN_TAGS_FILENAME = "run_tags.json"


class _ModelStandIn:
    """Stands in for `current.model` in the pod, with `loaded` populated.

    `@model(load=[...])` downloads in `task_pre_step`, which for a remote step
    runs on the *driver* — so the files landed on a Small-tier pod that never
    touches them, and `current.model.loaded[...]` did not exist here at all.

    The download happens here instead, which puts the model next to the GPU
    and keeps a multi-GB file off the driver. It works because the model
    *reference* is an ordinary flow artifact: `@model` resolves it through
    `getattr(flow, name)`, and the stand-in `self` already carries it.
    """

    def __init__(self, loaded=None):
        self.loaded = loaded

    def save(self, *args, **kwargs):
        raise RemoteStepError(
            "current.model.save() is not supported inside @remote_step yet — "
            "only @model(load=...). Save the model as an ordinary artifact "
            "(self.<name> = ...), or drop @remote_step from this step."
        )

    def load(self, reference, path=None):
        """Fetch a model by reference, as `current.model.load` does."""
        import tempfile

        from metaflow_extensions.obcheckpoint.plugins.machine_learning_utilities.modeling_utils.core import (  # noqa: E501
            _load_model,
        )

        backend = _model_storage_backend()
        if backend is None:
            raise RemoteStepError("could not reach the model store from the runner pod")
        dest = path or tempfile.mkdtemp(prefix="remote_step_model_")
        os.makedirs(dest, exist_ok=True)
        key = reference.get("key") if isinstance(reference, dict) else reference
        _load_model(backend, model_key=key, path=dest)
        return dest


def _model_storage_backend():
    """The artifact store the model lives in, built from the forwarded config.

    `datastore_context.get()` falls back to a default built from the
    `METAFLOW_*` datastore settings, which the driver forwards into this pod —
    and reading Outerbounds' datastore from here is known to work.
    """
    try:
        from metaflow_extensions.obcheckpoint.plugins.machine_learning_utilities.datastore.context import (  # noqa: E501
            datastore_context,
        )

        return datastore_context.get()
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] model store unavailable: {exc}\n")
        return None


def _load_models(spec: dict, fake) -> _ModelStandIn | None:
    """Populate `current.model.loaded` for the step, or None if unused."""
    request = spec.get("model_loads") or {}
    refs = request.get("load")
    if not refs:
        return None

    backend = _model_storage_backend()
    if backend is None:
        return None
    try:
        from metaflow_extensions.obcheckpoint.plugins.machine_learning_utilities.modeling_utils.core import (  # noqa: E501
            LoadedModels,
        )
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] @model unavailable in the runner: {exc}\n")
        return None

    # Tuples do not survive JSON, so they arrive as [name, path] pairs.
    artifact_references = [tuple(r) if isinstance(r, list) else r for r in refs]
    try:
        loaded = LoadedModels(
            storage_backend=backend,
            flow=fake,
            artifact_references=artifact_references,
            temp_dir_root=request.get("temp_dir_root"),
        )
    except Exception as exc:  # noqa: BLE001
        # Loud: the body is about to read a path that will not be there.
        raise RemoteStepError(f"@model(load=...) failed in the runner pod: {exc}") from exc

    names = [str(n) for n in artifact_references]
    sys.stdout.write(f"[remote_step] @model loaded {names} in the runner pod\n")
    return _ModelStandIn(loaded=loaded)


CARD_COMPONENTS_FILENAME = "card_components.pkl"


class _CardRecorder:
    """Stands in for `current.card` in the pod so card content survives.

    `@card` runs on the driver task, so `current.card` does not exist in the
    runner at all — `current.card.append(...)` raised AttributeError there, and
    a step that guarded the call rendered an empty card either way.

    This records what the body appends and the driver replays it into the real
    card. Components are pickled: all of Metaflow's own components take that
    except `Artifact`, which holds a module reference, and for those a Markdown
    note is recorded instead so the card says what could not cross rather than
    dropping it in silence.

    Mirrors `CardComponentCollector`: `append`, `extend`, `clear`, `refresh`,
    and `card[id]` for a specific card.
    """

    DEFAULT_ID = "_default"

    def __init__(self, card_id: str | None = None, sink: dict | None = None):
        self._card_id = card_id or self.DEFAULT_ID
        # Shared across every per-id view so one save collects them all.
        self._sink: dict[str, list[bytes]] = sink if sink is not None else {}
        self._warned_refresh = False

    def __getitem__(self, card_id: str) -> _CardRecorder:
        return _CardRecorder(card_id=card_id, sink=self._sink)

    def __setitem__(self, card_id: str, components) -> None:
        self._sink[card_id] = []
        self[card_id].extend(components)

    def append(self, component, id=None) -> None:  # noqa: A002 - matches Metaflow
        bucket = self._sink.setdefault(self._card_id, [])
        try:
            bucket.append(pickle.dumps(component, protocol=5))
        except Exception:  # noqa: BLE001
            bucket.append(self._unpicklable_note(component))

    def extend(self, components) -> None:
        for component in components or []:
            self.append(component)

    def clear(self) -> None:
        self._sink[self._card_id] = []

    def refresh(self, data=None, force=False) -> None:
        """No-op. A live refresh cannot reach the driver's card mid-step."""
        if not self._warned_refresh:
            self._warned_refresh = True
            sys.stdout.write(
                "[remote_step] card.refresh() does nothing in a remote step — "
                "the card is rendered on the driver once the step finishes.\n"
            )

    def get(self, card_id=None):
        return self._sink.get(card_id or self._card_id, [])

    @property
    def components(self):
        return self._sink.get(self._card_id, [])

    @staticmethod
    def _unpicklable_note(component) -> bytes:
        """A Markdown standing in for a component that will not pickle."""
        name = type(component).__name__
        try:
            from metaflow.cards import Markdown

            return pickle.dumps(
                Markdown(
                    f"_`{name}` could not be carried out of the remote step "
                    f"(not picklable). Assign the value as an artifact and "
                    f"render it in a non-remote step._"
                ),
                protocol=5,
            )
        except Exception:  # noqa: BLE001
            return pickle.dumps(None, protocol=5)

    def pending(self) -> dict[str, list[bytes]]:
        return {cid: comps for cid, comps in self._sink.items() if comps}


def _save_card_components(recorder: _CardRecorder, spec: dict, s3_client=None) -> None:
    """Persist recorded card components for the driver to replay."""
    pending = recorder.pending()
    if not pending:
        return
    prefix = spec.get("output_prefix")
    bucket = spec.get("output_bucket")
    if not prefix or not bucket:
        return
    try:
        client = s3_client or _make_s3_client()
        client.put_object(
            Bucket=bucket,
            Key=f"{prefix}/{CARD_COMPONENTS_FILENAME}",
            Body=pickle.dumps(pending, protocol=5),
        )
        counts = {cid: len(c) for cid, c in pending.items()}
        sys.stdout.write(f"[remote_step] recorded card components for the driver: {counts}\n")
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] could not save card components: {exc}\n")


class _GpuSampler:
    """Samples GPU utilisation in the runner pod for the duration of the body.

    `@gpu_profile` runs on the driver, which has no GPU, so it sampled nothing
    for a `@remote_step`. The GPU is here, so the sampling has to be here too.

    Reuses Outerbounds' own `GPUMonitor` — an `nvidia-smi -l` subprocess plus a
    reader thread — so the readings have the same shape their card expects.

    This is the data half only. `@gpu_profile` renders through
    `current.card["gpu_profile"]`, and a card written in this pod does not
    reach the driver's card (gap 6), so the readings are exposed as an
    artifact and a log summary instead.
    """

    ARTIFACT_NAME = "gpu_profile_data"

    def __init__(self, interval: int = 1):
        self._interval = interval
        self._monitor = None
        self.info: dict = {}

    def start(self) -> None:
        try:
            from metaflow_extensions.outerbounds.profilers.gpu import (
                GPUMonitor,
                GPUProfiler,
            )
        except Exception as exc:  # noqa: BLE001
            sys.stdout.write(f"[remote_step] gpu_profile: profiler unavailable ({exc}); not sampling\n")
            return
        try:
            self.info = GPUProfiler.read_gpu_info() or {}
        except Exception:  # noqa: BLE001
            self.info = {}
        devices = self.info.get("devices") or []
        if not devices:
            sys.stdout.write("[remote_step] gpu_profile: no GPU devices visible; not sampling\n")
            return
        try:
            self._monitor = GPUMonitor(interval=self._interval)
            self._monitor.create_new_monitor()
            sys.stdout.write(
                f"[remote_step] gpu_profile: sampling {len(devices)} device(s) "
                f"every {self._interval}s — driver "
                f"{self.info.get('driver_version', 'unknown')}, CUDA "
                f"{self.info.get('cuda_version', 'unknown')}\n"
            )
        except Exception as exc:  # noqa: BLE001
            self._monitor = None
            sys.stdout.write(f"[remote_step] gpu_profile: could not start: {exc}\n")

    def finish(self) -> dict | None:
        """Stop sampling and return the readings, or None if none were taken."""
        if self._monitor is None:
            return None
        try:
            readings = self._monitor.read()
        except Exception as exc:  # noqa: BLE001
            sys.stdout.write(f"[remote_step] gpu_profile: read failed: {exc}\n")
            readings = None
        try:
            self._monitor.cleanup()
        except Exception:  # noqa: BLE001
            pass
        if not readings:
            return None
        self._log_summary(readings)
        return {"info": self.info, "readings": readings}

    @staticmethod
    def _log_summary(readings: dict) -> None:
        """Peak utilisation per device, so the log alone answers 'was the GPU used'."""
        for device, series in (readings or {}).items():
            try:
                utils = [float(s) for s in (series.get("gpu_utilization") or [])]
                mem = [float(s) for s in (series.get("memory_used") or [])]
            except Exception:  # noqa: BLE001
                continue
            if not utils and not mem:
                continue
            sys.stdout.write(
                f"[remote_step] gpu_profile {device}: "
                f"peak {max(utils or [0]):.0f}% util, "
                f"peak {max(mem or [0]):.0f} MB memory, "
                f"{len(utils)} samples\n"
            )


class _RunRecorder:
    """Stands in for `current.run` so tag edits survive out of the pod.

    A real `Run` needs a Metaflow client, and the runner has no credentials on
    Outerbounds' metadata service — so `current.run.add_tags([...])` had
    nothing to call and quietly did nothing. Recording the calls and letting
    the driver replay them keeps the write on the side that is authenticated
    for it.

    Only tag edits are recorded. Reads (`run.data`, `run.tags`) would need the
    client that is missing, so they still raise rather than return a lie.
    """

    def __init__(self):
        self.added: list[str] = []
        self.removed: list[str] = []

    @staticmethod
    def _as_list(tags) -> list[str]:
        if isinstance(tags, str):
            return [tags]
        return [str(t) for t in (tags or [])]

    def add_tag(self, tag):
        self.added.extend(self._as_list(tag))

    def add_tags(self, tags):
        self.added.extend(self._as_list(tags))

    def remove_tag(self, tag):
        self.removed.extend(self._as_list(tag))

    def remove_tags(self, tags):
        self.removed.extend(self._as_list(tags))

    def replace_tag(self, old, new):
        self.remove_tag(old)
        self.add_tag(new)

    def replace_tags(self, old, new):
        self.remove_tags(old)
        self.add_tags(new)

    def pending(self) -> dict[str, list[str]]:
        # De-duplicated, order preserved, so replaying is idempotent.
        return {
            "added": list(dict.fromkeys(self.added)),
            "removed": list(dict.fromkeys(self.removed)),
        }


def _save_run_tags(recorder: _RunRecorder, spec: dict, s3_client=None) -> None:
    """Persist recorded tag edits for the driver to apply."""
    pending = recorder.pending()
    if not pending["added"] and not pending["removed"]:
        return
    prefix = spec.get("output_prefix")
    bucket = spec.get("output_bucket")
    if not prefix or not bucket:
        return
    try:
        client = s3_client or _make_s3_client()
        client.put_object(
            Bucket=bucket,
            Key=f"{prefix}/{RUN_TAGS_FILENAME}",
            Body=json.dumps(pending).encode(),
        )
        sys.stdout.write(f"[remote_step] recorded run tag edits for the driver: {pending}\n")
    except Exception:  # noqa: BLE001
        return


def _save_exception(exc: BaseException, spec: dict, s3_client=None) -> None:
    """Store the user's exception so the driver can re-raise this exact error.

    `@catch(var="e")` runs on the driver, where the only failure visible is
    the RunnerError the poller raises — so `e` was always a RunnerError and
    the user's own exception was reduced to log text. Persisting it here lets
    the driver re-raise the original.

    Best-effort by design: an exception holding a socket, a file handle or a
    thread does not pickle, and losing the detail is far better than turning a
    step failure into a crash in the failure handler. The traceback cannot be
    pickled either, so it is formatted and carried alongside.
    """
    prefix = spec.get("output_prefix")
    bucket = spec.get("output_bucket")
    if not prefix or not bucket:
        return
    try:
        payload = pickle.dumps({"exception": exc, "traceback": traceback.format_exc()}, protocol=5)
    except Exception:  # noqa: BLE001
        # Unpicklable exception — try again with just the text, so the driver
        # can at least reproduce the type and message.
        try:
            payload = pickle.dumps(
                {
                    "exception": None,
                    "type_name": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                },
                protocol=5,
            )
        except Exception:  # noqa: BLE001
            return
    try:
        client = s3_client or _make_s3_client()
        client.put_object(Bucket=bucket, Key=f"{prefix}/{EXCEPTION_FILENAME}", Body=payload)
    except Exception:  # noqa: BLE001
        return


def _hydrate_foreach_input(spec: dict, s3_client=None) -> Any:
    """The task's `self.input`, or None when the step is not in a foreach.

    `has_foreach_input` distinguishes "not a foreach" from "a foreach whose
    value happens to be None", which matters because the second is legitimate.
    """
    if not spec.get("has_foreach_input"):
        return None
    ref = spec.get("foreach_input")
    if not ref:
        return None
    return _hydrate_input("_foreach_input", ref, s3_client or _make_s3_client())


def _patch_project_context(spec: dict) -> None:
    """Replay @project's additions to `metaflow.current` inside the pod.

    `project_name`, `branch_name`, `is_production` and friends are not
    built-in properties of `current` — @project injects them with
    `_update_env`, which never runs here because the runner is not executing
    a Metaflow task. So we inject the same keys from the values the driver
    read off its own `current`.

    `current.is_production` is the one that matters most: user code branches
    on it to choose a schema, and an absent attribute reads as falsy, so a
    production run would quietly write to staging tables.
    """
    project = spec.get("project") or {}
    if not project:
        return
    try:
        from metaflow import current as _current

        _current._update_env(dict(project))
    except Exception:  # noqa: BLE001
        pass


class _FakeBranch:
    """One incoming branch of a join, as `inputs.<step>` / `inputs[i]`.

    Attributes hydrate on first access and are cached. Lazy on purpose: a join
    over a 100-way foreach would otherwise download every branch's artifacts
    to answer `inputs[0].x`, and the step may only want one of them.
    """

    def __init__(self, step: str, entries: dict[str, dict], s3_client=None):
        self._current_step = step
        self._entries = entries
        self._s3_client = s3_client
        self._cache: dict[str, Any] = {}

    def _shas(self) -> dict[str, str]:
        """Content hash per attribute, for merge_artifacts conflict checks."""
        return {name: (e or {}).get("sha256") or "" for name, e in self._entries.items()}

    def __getattr__(self, name: str) -> Any:
        # __getattr__ only fires when normal lookup fails, so the instance
        # attributes set in __init__ never reach here.
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._cache:
            return self._cache[name]
        if name not in self._entries:
            raise AttributeError(f"step '{self._current_step}' produced no attribute '{name}'")
        val = _hydrate_input(name, self._entries[name], self._s3_client or _make_s3_client())
        self._cache[name] = val
        return val

    def __repr__(self) -> str:
        return f"<branch {self._current_step}: {sorted(self._entries)}>"


class _FakeInputs:
    """Metaflow's `Inputs` shape: iterable, indexable, and keyed by step name.

    Mirrors `metaflow.datastore.inputs.Inputs` so the three documented access
    patterns all work — `inputs.step_a.x`, `inputs[0].x`, and
    `(inp.x for inp in inputs)`.
    """

    def __init__(self, branches: list[_FakeBranch]):
        self.flows = list(branches)
        for branch in self.flows:
            setattr(self, branch._current_step, branch)

    def __getitem__(self, idx):
        return self.flows[idx]

    def __iter__(self):
        return iter(self.flows)

    def __len__(self) -> int:
        return len(self.flows)


def _build_join_inputs(spec: dict, s3_client=None) -> _FakeInputs | None:
    """The `inputs` argument for a join step's body, or None if not a join."""
    if not spec.get("is_join"):
        return None
    branches = [
        _FakeBranch(
            step=(b.get("step") or f"branch_{i}"),
            entries=(b.get("attrs") or {}),
            s3_client=s3_client,
        )
        for i, b in enumerate(spec.get("join_branches") or [])
    ]
    return _FakeInputs(branches)


class _FakeSelf:
    """Object presented to the user step body in place of Metaflow's `self`.

    Absorbs Metaflow-flow-specific calls (`self.next(...)`, `self.input`,
    references to sibling step methods like `self.scale`) so unmodified
    step bodies run cleanly. Only attribute writes matter — those become
    RemoteArtifact outputs.
    """

    def __init__(self, foreach_input=None):
        # Underscored so the outputs snapshot skips it: this is context handed
        # in, not something the step produced.
        self._foreach_input = foreach_input

    def next(self, *args, **kwargs):
        """No-op stand-in for Metaflow's `self.next(...)` — routing runs on driver."""
        return

    @property
    def input(self):
        """The task's foreach value, as Metaflow's `self.input` would give it.

        Read straight off the driver's own `self.input` and shipped in the
        spec. Hardcoding None here made every foreach child see None, so
        `self.worker = self.input` and `self.a, self.b = self.input` either
        stored nothing or raised.
        """
        return self._foreach_input

    def merge_artifacts(self, inputs, exclude=None, include=None):
        """Copy artifacts common to the incoming branches onto `self`.

        Mirrors `FlowSpec.merge_artifacts`: an attribute already set on `self`
        wins and is skipped; `include` and `exclude` are mutually exclusive;
        an attribute arriving with different content from two branches is an
        unresolved conflict and raises, unless it was named in `include`.

        Conflicts are decided on the content hashes carried in the spec, so
        nothing is downloaded to compare — and the value assigned is whatever
        the branch entry holds, which for a RemoteArtifact stays a lazy ref.

        Without this, `__getattr__` below answered `.merge_artifacts` with a
        no-op placeholder, so every upstream artifact a join meant to keep was
        silently dropped.
        """
        include = list(include or [])
        exclude = list(exclude or [])
        if include and exclude:
            raise RemoteStepError("`exclude` and `include` are mutually exclusive in merge_artifacts")
        if inputs is None:
            raise RemoteStepError("merge_artifacts needs the join step's `inputs`; it can only be called in a join")

        # `vars(self)`, not `hasattr(self, ...)`: __getattr__ below answers
        # every non-dunder name with a placeholder, so hasattr is always True
        # and would make this skip every attribute and merge nothing.
        already_set = vars(self)

        to_merge: dict[str, tuple[_FakeBranch, str]] = {}
        unresolved: list[str] = []
        for branch in inputs:
            shas = branch._shas()
            for name in branch._entries:
                if name.startswith("_") or name in already_set:
                    continue
                if include:
                    if name not in include:
                        continue
                elif name in exclude:
                    continue
                sha = shas.get(name, "")
                previous = to_merge.setdefault(name, (branch, sha))
                if previous[1] != sha and name not in unresolved:
                    unresolved.append(name)

        if unresolved:
            # `include` deliberately does not resolve a conflict, matching
            # Metaflow: it narrows what is considered, and a named attribute
            # that still disagrees across branches is an error. The only ways
            # out are to decide the value yourself or to drop it.
            raise RemoteStepError(
                f"merge_artifacts: unresolved conflicts for "
                f"{', '.join(sorted(unresolved))} — the branches disagree. "
                f"Assign the attribute on self before merging to pick a value, "
                f"or exclude=[...] to drop it.",
                unresolved=sorted(unresolved),
            )

        missing = [name for name in include if name not in to_merge and name not in already_set]
        if missing:
            raise RemoteStepError(
                f"merge_artifacts: include names {', '.join(missing)}, which no incoming branch produced.",
                missing=missing,
            )

        for name, (branch, _sha) in to_merge.items():
            setattr(self, name, getattr(branch, name))

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
    fake = _FakeSelf(foreach_input=_hydrate_foreach_input(spec))
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
    # Identity of every input BEFORE the step body runs.
    #
    # This has to be captured here, not after. The rule for "is this an
    # output" is "the attribute is new, or it points at a different object
    # than the input did". Reading the ids after the body has run makes that
    # comparison vacuous: for `self.df = transform(self.df)` the recorded id
    # is already the id of the NEW object, so the attribute compares equal to
    # itself, is classified as an unmodified input, and is never uploaded.
    # The driver then leaves self.df on the stale upstream ref and every
    # downstream step silently consumes un-transformed data.
    input_ids_before: dict[str, int] = {k: id(v) for k, v in vars(fake).items()}

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
            t
            for t in _all_tags
            if t.startswith(
                ("user:", "runtime:", "python_version:", "metaflow_version:", "project:", "project_branch:")
            )
        )
        _current._is_running = True
    except Exception:  # noqa: BLE001
        pass
    _patch_project_context(spec)
    # `current.run` needs a Metaflow client the runner has no credentials for,
    # so hand the step a recorder and let the driver replay the tag edits.
    run_recorder = _RunRecorder()
    # `current.card` does not exist in this process either — @card runs on the
    # driver — so record what the body appends and let the driver replay it.
    card_recorder = _CardRecorder()
    try:
        from metaflow import current as _current

        _current._run = run_recorder
        type(_current).run = property(fget=lambda _self: run_recorder)
        _current._card = card_recorder
        type(_current).card = property(fget=lambda _self: card_recorder)
    except Exception:  # noqa: BLE001
        pass

    # @model(load=...) downloads in task_pre_step, which for a remote step runs
    # on the driver — the wrong machine. Do it here, where the step body is.
    model_standin = _load_models(spec, fake)
    if model_standin is not None:
        try:
            from metaflow import current as _current

            _current._model = model_standin
            type(_current).model = property(fget=lambda _self: model_standin)
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
    #
    # A join step's body is `def join(self, inputs)`, so it needs the second
    # positional argument — calling it with one raised TypeError before this.
    _stage("user_step_start")
    # Sampling has to bracket the body: the driver's @gpu_profile has no GPU
    # to look at, so nothing was ever measured for a remote step.
    gpu_sampler = None
    if spec.get("gpu_profile"):
        gpu_sampler = _GpuSampler(interval=int(spec.get("gpu_profile_interval") or 1))
        gpu_sampler.start()
    t0 = time.time()
    try:
        join_inputs = _build_join_inputs(spec)
        if join_inputs is not None:
            original(fake, join_inputs)
        else:
            original(fake)
    except Exception as exc:  # noqa: BLE001
        sys.stdout.write(f"[remote_step] STAGE=user_step_end ERR {exc}\n")
        traceback.print_exc()
        _save_exception(exc, spec)
        _save_card_components(card_recorder, spec)
        if gpu_sampler is not None:
            gpu_sampler.finish()
        return 1
    _stage("user_step_end", t0=t0)
    _save_run_tags(run_recorder, spec)
    _save_card_components(card_recorder, spec)
    if gpu_sampler is not None:
        gpu_readings = gpu_sampler.finish()
        if gpu_readings is not None:
            # Named the way @gpu_profile names its own artifact, so user code
            # that already reads it keeps working.
            setattr(fake, _GpuSampler.ARTIFACT_NAME, gpu_readings)

    # 5. Snapshot new/modified attrs.
    new_attrs = {
        k: v for k, v in vars(fake).items() if not k.startswith("_") and (k not in inputs_snapshot or v is not None)
    }
    # Drop attrs that started as inputs and were not reassigned, comparing
    # against the pre-execution identities captured above.
    outputs = {k: v for k, v in new_attrs.items() if k not in input_ids_before or id(v) != input_ids_before[k]}

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
            for fut in concurrent.futures.as_completed([pool.submit(_upload_one, item) for item in outputs.items()]):
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
            # Straight from the spec: the driver decided the layout, so the
            # runner does not reconstruct it.
            prefix,
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
