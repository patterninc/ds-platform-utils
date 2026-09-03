"""RemoteArtifact — a tiny reference to a pickled Python object in S3.

The driver task stores these on `self.<attr>` instead of the raw object,
keeping the Metaflow driver's memory footprint constant regardless of the
step's output size. Consumers call `.load()` (or reach through the
proxy dunders) to materialise the object.

Cross-account read (Outerbounds pod → our payload bucket) happens via
`read_role_arn`: when set, `.load()` calls `sts:AssumeRole` on that ARN
and uses the returned temp credentials to fetch the blob. The role is
created by our terraform (`ob_artifact_reader`) with a trust policy that
allows Outerbounds' pod task role to assume it — so downstream non-
@remote_step pods can lazy-load refs without our bucket having to be
readable from Outerbounds' account directly.

Instances are pickle-clean so Metaflow's own artifact persistence works
without special handling; `read_role_arn` is preserved across the pickle
roundtrip so the downstream pod that pulls the ref out of the Metaflow
datastore can still hop into our account to fetch the payload.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import io
import os
import pickle
import time
from typing import Any

import threading

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import BotoCoreError, ClientError

from remote_step.errors import ArtifactLoadError


# Cache assumed-role sessions across many RemoteArtifact.load() calls so a
# non-@remote_step step that reaches through several refs pays for one
# AssumeRole per role ARN (not one per artifact). Keyed by role ARN.
# Value: (client, expiry_epoch_seconds).
_ASSUMED_S3_CLIENTS: dict[str, tuple[Any, float]] = {}
_ASSUMED_S3_LOCK = threading.Lock()
_ASSUME_ROLE_TTL_SEC = 45 * 60  # AssumeRole hands out 1 h creds; refresh at 45 min.

# Connection pool size on the assumed client. Downloads of ≥2 GB blobs
# spin up 32 TransferManager threads, each of which wants its own HTTPS
# connection to S3. The boto default of 10 would starve them. Also
# helps when several concurrent RemoteArtifact.load() calls on the same
# pod fall back to AssumeRole simultaneously.
_ASSUMED_MAX_POOL_CONNECTIONS = 64


def _assumed_s3_client(role_arn: str, region: str | None = None) -> Any:
    """Return a boto3 S3 client whose creds come from sts:AssumeRole.

    Cached per role ARN; refreshed a few minutes before the STS creds
    expire. The read/refresh is guarded by a lock so concurrent
    threads don't race to issue duplicate ``sts:AssumeRole`` calls
    when the cache is cold or expiring.
    """
    now = time.time()
    with _ASSUMED_S3_LOCK:
        cached = _ASSUMED_S3_CLIENTS.get(role_arn)
        if cached and cached[1] > now + 60:
            return cached[0]
        sts = boto3.client("sts", region_name=region)
        resp = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="remote-step-artifact-read",
        )
        creds = resp["Credentials"]
        client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            config=BotocoreConfig(
                max_pool_connections=_ASSUMED_MAX_POOL_CONNECTIONS,
                retries={"max_attempts": 8, "mode": "adaptive"},
            ),
        )
        _ASSUMED_S3_CLIENTS[role_arn] = (
            client,
            now + _ASSUME_ROLE_TTL_SEC,
        )
        return client


@dataclass
class RemoteArtifact:
    """A reference to a pickled object stored in S3.

    Attributes:
        s3_uri: Full s3:// URI to the pickle blob.
        size_bytes: Length of the blob for user-facing sizing.
        kind: 'module.QualName' of the pickled object's type.
        sha256: Hex digest of the blob, verified on load.
        pickle_protocol: Protocol used when writing (5 by default).
        read_role_arn: Optional IAM role ARN to assume before fetching
            the blob. Empty string means "use ambient credentials".
    """

    s3_uri: str
    size_bytes: int
    kind: str
    sha256: str
    pickle_protocol: int = 5
    read_role_arn: str = ""

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
            "read_role_arn": self.read_role_arn,
        }

    def __setstate__(self, state: dict) -> None:
        # Backfill missing keys so old pickled refs still deserialise.
        state.setdefault("read_role_arn", "")
        self.__dict__.update(state)
        self._cached = None
        self._loaded = False

    def load(self, s3_client=None) -> Any:
        """Download the blob, verify sha256, unpickle, cache in-instance.

        Memory-hot path — a downstream non-@remote_step consumer that
        touches a multi-GB DataFrame ref will materialise the whole
        pickle *and* the unpickled Python object at the same time. Keep
        that ceiling to ~2× the pickle size by:
          1. Downloading straight into a ``BytesIO`` (no ``read()``
             bytes copy).
          2. Stream-hashing that buffer through sha256 (no bytes copy).
          3. Rewinding and calling ``pickle.load(buf)`` — pickle reads
             from the buffer incrementally, so we don't need a
             ``bytes`` blob at all before unpickling.

        If ``read_role_arn`` is set, first tries a direct fetch with the
        caller-provided client (or the ambient boto3 default). Falls
        back to assuming ``read_role_arn`` and retrying with the
        returned temporary credentials. That fallback is what makes
        non-@remote_step consumers work across the Outerbounds →
        our-account boundary.
        """
        if self._loaded:
            return self._cached
        bucket, key = _parse_s3_uri(self.s3_uri)
        region = os.environ.get("AWS_REGION") or None
        buf = self._fetch(bucket, key, s3_client, region)

        got = _sha256_of_buf(buf, buf.getbuffer().nbytes)
        if got != self.sha256:
            raise ArtifactLoadError(
                f"sha256 mismatch for {self.s3_uri}: "
                f"expected {self.sha256}, got {got}",
                s3_uri=self.s3_uri,
                expected=self.sha256,
                got=got,
            )
        try:
            obj = pickle.load(buf)
        except Exception as exc:  # noqa: BLE001
            raise ArtifactLoadError(
                f"unpickle failed for {self.s3_uri}: {exc}",
                s3_uri=self.s3_uri,
            ) from exc
        self._cached = obj
        self._loaded = True
        return obj

    def _fetch(self, bucket: str, key: str, s3_client, region: str | None) -> io.BytesIO:
        """S3 GetObject with an AssumeRole fallback. Returns a rewound BytesIO."""
        direct = s3_client or boto3.client("s3", region_name=region)
        try:
            return _download_to_buf(direct, bucket, key, self.size_bytes)
        except (ClientError, BotoCoreError) as direct_exc:
            if not self.read_role_arn:
                raise ArtifactLoadError(
                    f"failed to fetch {self.s3_uri}: {direct_exc}",
                    s3_uri=self.s3_uri,
                ) from direct_exc
            try:
                assumed = _assumed_s3_client(self.read_role_arn, region=region)
                return _download_to_buf(assumed, bucket, key, self.size_bytes)
            except (ClientError, BotoCoreError) as assumed_exc:
                raise ArtifactLoadError(
                    f"failed to fetch {self.s3_uri} even after assuming "
                    f"{self.read_role_arn}: {assumed_exc} "
                    f"(direct attempt: {direct_exc})",
                    s3_uri=self.s3_uri,
                ) from assumed_exc

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
            "read_role_arn",
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


_S3_MULTIPART_THRESHOLD = 100 * 1024 * 1024        # 100 MB
_S3_MULTIPART_CHUNK_SIZE = 32 * 1024 * 1024        # 32 MB
_S3_LARGE_BLOB_THRESHOLD = 2 * 1024 * 1024 * 1024  # 2 GB
_S3_MAX_CONCURRENCY_SMALL = 10
_S3_MAX_CONCURRENCY_BIG = 32


def _transfer_config_for(size: int) -> TransferConfig:
    """TransferManager config tuned to the payload size.

    Small blobs use the boto default 10-thread concurrency; anything at
    or above 2 GB gets 32 threads so a single huge input saturates the
    Fargate task's egress bandwidth on the driver side too.
    """
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


def _download_to_buf(
    s3_client, bucket: str, key: str, size_hint: int
) -> io.BytesIO:
    """Fetch s3://bucket/key into a fresh ``BytesIO`` rewound to 0.

    Above ``_S3_MULTIPART_THRESHOLD`` we use ``download_fileobj`` — the
    TransferManager issues ranged GETs in parallel with the same
    size-tuned concurrency as the upload path (32-way for ≥2 GB blobs).
    Below the threshold we stream ``get_object.Body`` into the buffer
    in 4 MB chunks, which keeps peak memory to a single copy of the
    payload instead of the two ``get_object().read()`` produces
    (``StreamingBody`` internal buffer + result bytes).

    Returning the buffer (instead of ``bytes``) lets callers
    stream-hash and stream-unpickle without materialising a second
    full copy in memory.
    """
    buf = io.BytesIO()
    if size_hint and size_hint > _S3_MULTIPART_THRESHOLD:
        s3_client.download_fileobj(
            Bucket=bucket,
            Key=key,
            Fileobj=buf,
            Config=_transfer_config_for(size_hint),
        )
    else:
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
        try:
            while True:
                chunk = body.read(4 * 1024 * 1024)
                if not chunk:
                    break
                buf.write(chunk)
        finally:
            body.close()
    buf.seek(0)
    return buf


def _sha256_of_buf(buf: io.BytesIO, size: int) -> str:
    """Stream ``buf`` through sha256 without copying its contents.

    ``buf.seek(0)`` is called on entry so callers can hand us a buffer
    the pickle was just written to. We leave ``buf`` rewound to 0 on
    exit so it's ready for the S3 client to read.
    """
    buf.seek(0)
    h = hashlib.sha256()
    for chunk in iter(lambda: buf.read(4 * 1024 * 1024), b""):
        h.update(chunk)
    buf.seek(0)
    return h.hexdigest()


def _upload_buf(s3, bucket: str, key: str, buf: io.BytesIO, size: int) -> None:
    """Send ``buf`` (rewound to 0) to S3.

    Reuses the same in-memory buffer for both the size-check and the
    upload — avoids the extra bytes copy an ``io.BytesIO(blob)`` wrap
    used to introduce on the multipart path.
    """
    if size <= _S3_MULTIPART_THRESHOLD:
        s3.put_object(Bucket=bucket, Key=key, Body=buf)
    else:
        cfg = _transfer_config_for(size)
        s3.upload_fileobj(buf, Bucket=bucket, Key=key, Config=cfg)


def write_artifact(
    obj: Any,
    bucket: str,
    key: str,
    s3_client=None,
    pickle_protocol: int = 5,
    read_role_arn: str = "",
) -> RemoteArtifact:
    """Pickle `obj`, upload to s3://bucket/key, return a RemoteArtifact.

    Peak RAM = 1× the pickled size (a single ``BytesIO``). We pickle into
    the buffer, stream-hash it into sha256 in 4 MB chunks, then hand the
    same buffer to boto3 for the upload. No intermediate ``bytes`` blob.
    """
    buf = io.BytesIO()
    pickle.dump(obj, buf, protocol=pickle_protocol)
    size = buf.tell()
    sha = _sha256_of_buf(buf, size)
    s3 = s3_client or boto3.client("s3")
    _upload_buf(s3, bucket, key, buf, size)
    return RemoteArtifact(
        s3_uri=f"s3://{bucket}/{key}",
        size_bytes=size,
        kind=type(obj).__module__ + "." + type(obj).__qualname__,
        sha256=sha,
        pickle_protocol=pickle_protocol,
        read_role_arn=read_role_arn,
    )


def write_artifact_from_buf(
    obj_kind: str,
    buf: io.BytesIO,
    size: int,
    bucket: str,
    key: str,
    s3_client=None,
    pickle_protocol: int = 5,
    read_role_arn: str = "",
) -> RemoteArtifact:
    """Upload an already-pickled ``BytesIO`` and return a RemoteArtifact.

    For callers that pickled once for a size check and want to reuse the
    same buffer for the upload (see ``payload.build_spec``). ``buf``
    must already contain the pickled bytes; we rewind it before hashing
    and before handing to boto so callers don't need to seek themselves.
    """
    sha = _sha256_of_buf(buf, size)
    s3 = s3_client or boto3.client("s3")
    _upload_buf(s3, bucket, key, buf, size)
    return RemoteArtifact(
        s3_uri=f"s3://{bucket}/{key}",
        size_bytes=size,
        kind=obj_kind,
        sha256=sha,
        pickle_protocol=pickle_protocol,
        read_role_arn=read_role_arn,
    )
