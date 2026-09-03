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

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError

from remote_step.errors import ArtifactLoadError


# Cache assumed-role sessions across many RemoteArtifact.load() calls so a
# non-@remote_step step that reaches through several refs pays for one
# AssumeRole per role ARN (not one per artifact). Keyed by role ARN.
# Value: (client, expiry_epoch_seconds).
_ASSUMED_S3_CLIENTS: dict[str, tuple[Any, float]] = {}
_ASSUME_ROLE_TTL_SEC = 45 * 60  # AssumeRole hands out 1 h creds; refresh at 45 min.


def _assumed_s3_client(role_arn: str, region: str | None = None) -> Any:
    """Return a boto3 S3 client whose creds come from sts:AssumeRole.

    Cached per role ARN; refreshed a few minutes before the STS creds
    expire. Falls back to the caller-provided s3 client if AssumeRole
    itself fails — the caller then sees the underlying S3 error, which
    is more actionable than an opaque "assume role failed".
    """
    now = time.time()
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

        If ``read_role_arn`` is set, first tries a direct fetch with the
        caller-provided client (or the ambient boto3 default) — this is
        the fast path for pods that already have direct S3 access, e.g.
        the Batch runner or the driver on the Outerbounds pod when its
        @secrets creds allow it. Falls back to assuming ``read_role_arn``
        and retrying with the returned temporary credentials. That
        fallback is what makes non-@remote_step consumers work across
        the Outerbounds → our-account boundary.
        """
        if self._loaded:
            return self._cached
        bucket, key = _parse_s3_uri(self.s3_uri)
        region = os.environ.get("AWS_REGION") or None
        blob = self._fetch(bucket, key, s3_client, region)
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

    def _fetch(self, bucket: str, key: str, s3_client, region: str | None) -> bytes:
        """Do the actual S3 GetObject, with an AssumeRole fallback.

        ``get_object().read()`` streams the whole body from a single
        connection — fine for small refs but leaves multi-gigabyte
        downloads badly under-utilised on a Fargate task. For blobs
        above ``_S3_MULTIPART_THRESHOLD`` we ask ``download_fileobj`` to
        use the size-tuned TransferManager (parallel ranged GETs).
        """
        direct = s3_client or boto3.client("s3", region_name=region)
        try:
            return _download(direct, bucket, key, self.size_bytes)
        except (ClientError, BotoCoreError) as direct_exc:
            if not self.read_role_arn:
                raise ArtifactLoadError(
                    f"failed to fetch {self.s3_uri}: {direct_exc}",
                    s3_uri=self.s3_uri,
                ) from direct_exc
            try:
                assumed = _assumed_s3_client(self.read_role_arn, region=region)
                return _download(assumed, bucket, key, self.size_bytes)
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


def _download(s3_client, bucket: str, key: str, size_hint: int) -> bytes:
    """Read s3://bucket/key into memory, parallelising above the multipart threshold.

    ``size_hint`` comes from the RemoteArtifact ref (which was written
    alongside the sha256), so the caller already knows the payload size
    and doesn't need a HEAD round trip. Small blobs stay on the
    single-connection ``get_object`` fast path; big ones use
    ``download_fileobj`` + the size-aware TransferConfig so a 10 GB
    pickle streams in with ~32 parallel ranged GETs.
    """
    if size_hint and size_hint > _S3_MULTIPART_THRESHOLD:
        buf = io.BytesIO()
        s3_client.download_fileobj(
            Bucket=bucket,
            Key=key,
            Fileobj=buf,
            Config=_transfer_config_for(size_hint),
        )
        return buf.getvalue()
    return s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()


def write_artifact(
    obj: Any,
    bucket: str,
    key: str,
    s3_client=None,
    pickle_protocol: int = 5,
    read_role_arn: str = "",
) -> RemoteArtifact:
    """Pickle `obj`, upload to s3://bucket/key, return a RemoteArtifact.

    Anything larger than ``_S3_MULTIPART_THRESHOLD`` is uploaded via
    ``upload_fileobj`` (boto3 TransferManager, transparent multipart)
    since S3's ``PutObject`` caps at 5 GB per request. Concurrency is
    scaled by payload size so a single multi-GB input can push through
    the pipe as fast as the network allows.
    """
    buf = io.BytesIO()
    pickle.dump(obj, buf, protocol=pickle_protocol)
    blob = buf.getvalue()
    size = len(blob)
    sha = hashlib.sha256(blob).hexdigest()
    s3 = s3_client or boto3.client("s3")
    if size <= _S3_MULTIPART_THRESHOLD:
        s3.put_object(Bucket=bucket, Key=key, Body=blob)
    else:
        cfg = _transfer_config_for(size)
        s3.upload_fileobj(io.BytesIO(blob), Bucket=bucket, Key=key, Config=cfg)
    kind = type(obj).__module__ + "." + type(obj).__qualname__
    return RemoteArtifact(
        s3_uri=f"s3://{bucket}/{key}",
        size_bytes=size,
        kind=kind,
        sha256=sha,
        pickle_protocol=pickle_protocol,
        read_role_arn=read_role_arn,
    )
