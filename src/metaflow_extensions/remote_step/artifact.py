"""RemoteArtifact — a tiny reference to a pickled Python object in S3.

The driver task stores these on `self.<attr>` instead of the raw object,
keeping the Metaflow driver's memory footprint constant regardless of the
step's output size. Consumers call `.load()` to materialise the object.

Instances are pickle-clean so Metaflow's own artifact persistence works
without special handling.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import io
import pickle
from typing import Any

import boto3

from remote_step.errors import ArtifactLoadError


@dataclass
class RemoteArtifact:
    """A reference to a pickled object stored in S3.

    Attributes:
        s3_uri: Full s3:// URI to the pickle blob.
        size_bytes: Length of the blob for user-facing sizing.
        kind: 'module.QualName' of the pickled object's type.
        sha256: Hex digest of the blob, verified on load.
        pickle_protocol: Protocol used when writing (5 by default).
    """

    s3_uri: str
    size_bytes: int
    kind: str
    sha256: str
    pickle_protocol: int = 5

    _cached: Any = field(default=None, repr=False, compare=False)
    _loaded: bool = field(default=False, repr=False, compare=False)

    def __repr__(self) -> str:
        gb = self.size_bytes / 1024 / 1024 / 1024
        if gb >= 1:
            size = f"{gb:.2f} GB"
        else:
            size = f"{self.size_bytes / 1024 / 1024:.2f} MB"
        return f"RemoteArtifact(kind={self.kind}, size={size}, uri={self.s3_uri})"

    def __getstate__(self) -> dict:
        # Never pickle the cached materialised object.
        return {
            "s3_uri": self.s3_uri,
            "size_bytes": self.size_bytes,
            "kind": self.kind,
            "sha256": self.sha256,
            "pickle_protocol": self.pickle_protocol,
        }

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._cached = None
        self._loaded = False

    def load(self, s3_client=None) -> Any:
        """Download the blob, verify sha256, unpickle, cache in-instance."""
        if self._loaded:
            return self._cached
        s3 = s3_client or boto3.client("s3")
        bucket, key = _parse_s3_uri(self.s3_uri)
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            blob = resp["Body"].read()
        except Exception as exc:  # noqa: BLE001
            raise ArtifactLoadError(
                f"failed to fetch {self.s3_uri}: {exc}",
                s3_uri=self.s3_uri,
            ) from exc
        got = hashlib.sha256(blob).hexdigest()
        if got != self.sha256:
            raise ArtifactLoadError(
                f"sha256 mismatch for {self.s3_uri}: "
                f"expected {self.sha256}, got {got}",
                s3_uri=self.s3_uri,
                expected=self.sha256,
                got=got,
            )
        try:
            obj = pickle.loads(blob)
        except Exception as exc:  # noqa: BLE001
            raise ArtifactLoadError(
                f"unpickle failed for {self.s3_uri}: {exc}",
                s3_uri=self.s3_uri,
            ) from exc
        self._cached = obj
        self._loaded = True
        return obj

    # ------------------------------------------------------------------
    # Transparent-proxy dunders.
    #
    # A `@remote_step` writes its outputs as RemoteArtifact refs so the
    # driver's memory footprint stays flat. Downstream *non-remote* steps
    # still expect to interact with the underlying object (`df[col]`,
    # `df.merge(...)`, `list(items)`), so we lazy-load the blob the first
    # time the user reaches through the ref and delegate every proxy-y
    # operation to the materialised object.
    #
    # `@remote_step` downstream consumers keep zero-copy semantics —
    # `payload.build_spec` re-uses the same S3 ref without touching this
    # code path.
    # ------------------------------------------------------------------

    _PROXY_SKIP = frozenset(
        {
            "s3_uri",
            "size_bytes",
            "kind",
            "sha256",
            "pickle_protocol",
            "_cached",
            "_loaded",
            "load",
        }
    )

    def _proxy_target(self) -> Any:
        return self.load()

    def __getattr__(self, name: str) -> Any:
        # __getattr__ only fires when normal lookup fails, so our own
        # fields never route here. Guard against dunders / private names
        # so pickle/copy/introspection don't accidentally materialise us.
        if name.startswith("_") or name in RemoteArtifact._PROXY_SKIP:
            raise AttributeError(name)
        return getattr(self._proxy_target(), name)

    def __getitem__(self, key: Any) -> Any:
        return self._proxy_target()[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._proxy_target()[key] = value

    def __contains__(self, item: Any) -> bool:
        return item in self._proxy_target()

    def __iter__(self):
        return iter(self._proxy_target())

    def __len__(self) -> int:
        return len(self._proxy_target())

    def __bool__(self) -> bool:
        return bool(self._proxy_target())

    # No __call__ on purpose. Adding one makes `callable(remote_artifact)`
    # return True, which trips heuristics elsewhere that skip callables
    # when serialising (Metaflow's own artifact filters and our own
    # driver-side attribute capture in `_collect_flow_attrs`). Users who
    # need to call a wrapped callable can `ref.load()(...)` explicitly.


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split 's3://bucket/key/parts' into ('bucket', 'key/parts')."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// URI: {uri!r}")
    rest = uri[5:]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"malformed s3:// URI: {uri!r}")
    return bucket, key


def write_artifact(
    obj: Any,
    bucket: str,
    key: str,
    s3_client=None,
    pickle_protocol: int = 5,
) -> RemoteArtifact:
    """Pickle `obj`, upload to s3://bucket/key, return a RemoteArtifact."""
    buf = io.BytesIO()
    pickle.dump(obj, buf, protocol=pickle_protocol)
    blob = buf.getvalue()
    sha = hashlib.sha256(blob).hexdigest()
    s3 = s3_client or boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=blob)
    kind = type(obj).__module__ + "." + type(obj).__qualname__
    return RemoteArtifact(
        s3_uri=f"s3://{bucket}/{key}",
        size_bytes=len(blob),
        kind=kind,
        sha256=sha,
        pickle_protocol=pickle_protocol,
    )
