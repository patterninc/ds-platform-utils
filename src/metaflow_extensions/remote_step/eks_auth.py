"""Authenticate to the EKS API from the Outerbounds driver pod.

Three hops, because the driver and the cluster live in different AWS
accounts:

    Outerbounds pod task role          (their account)
      -> sts:AssumeRole                 pattern-ml-platform-ob-submitter
      -> EKS bearer token               presigned sts:GetCallerIdentity
      -> Kubernetes API                 scoped by an EKS access entry

The second hop is the non-obvious one. EKS does not issue tokens; a token
*is* a presigned `sts:GetCallerIdentity` URL, base64url-encoded with a
`k8s-aws-v1.` prefix. The API server's webhook resolves it back to an IAM
identity and matches that against the cluster's access entries. This is
exactly what `aws eks get-token` produces — reimplemented here because the
driver has botocore but no AWS CLI.

Nothing is cached across processes: a token is valid for 15 minutes, and a
driver task is short-lived.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
import threading
import time

import boto3
from botocore.signers import RequestSigner

from remote_step.errors import RemoteStepError


# EKS requires this exact prefix and rejects anything else.
TOKEN_PREFIX = "k8s-aws-v1."
# The signed URL's own expiry. EKS caps the usable token life at 15 minutes
# regardless, so this only has to outlive the request itself.
URL_EXPIRY_SECONDS = 60
# Refresh a little before the 15-minute cap so a long-running watch does not
# fail mid-stream.
TOKEN_TTL_SECONDS = 13 * 60


class EksAuthError(RemoteStepError):
    """Raised when the driver cannot obtain cluster credentials."""


@dataclass
class ClusterAccess:
    """Everything needed to talk to the cluster API."""

    endpoint: str
    ca_data: bytes  # PEM, decoded from the base64 EKS returns
    token: str
    expires_at: float
    session: boto3.Session

    def expired(self) -> bool:
        return time.time() >= self.expires_at


def assume_submitter(
    role_arn: str,
    region: str,
    session_name: str = "remote-step-driver",
) -> boto3.Session:
    """Assume the submitter role and return a session using it.

    The Outerbounds pod's own role cannot reach our cluster; only the
    submitter role has an access entry. A plain (non-refreshable) session is
    fine because the credentials outlive any single step submission.
    """
    sts = boto3.client("sts", region_name=region)
    try:
        resp = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name[:64],
            # 1 hour: long enough for a step's whole submit+poll cycle, short
            # enough that a leaked credential is not durable.
            DurationSeconds=3600,
        )
    except Exception as exc:  # noqa: BLE001
        raise EksAuthError(
            f"could not assume {role_arn}.\n"
            f"  The Outerbounds pod task role must be trusted by that role's "
            f"assume-role policy. Check var.outerbounds_task_role_arn in "
            f"infra/eks matches the role this pod actually runs as — "
            f"`aws sts get-caller-identity` inside the pod shows it.",
            role_arn=role_arn,
        ) from exc
    c = resp["Credentials"]
    return boto3.Session(
        aws_access_key_id=c["AccessKeyId"],
        aws_secret_access_key=c["SecretAccessKey"],
        aws_session_token=c["SessionToken"],
        region_name=region,
    )


def bearer_token(session: boto3.Session, cluster_name: str, region: str) -> str:
    """Build an EKS bearer token from `session`'s credentials.

    Reimplements `aws eks get-token`: presign an STS GetCallerIdentity GET
    with the cluster name carried in the `x-k8s-aws-id` header, then
    base64url-encode the URL. The header is included in the signature, which
    is what binds the token to one cluster — the same signed URL cannot be
    replayed against a different one.
    """
    client = session.client("sts", region_name=region)
    signer = RequestSigner(
        client.meta.service_model.service_id,
        region,
        "sts",
        "v4",
        session.get_credentials(),
        session.events,
    )
    signed_url = signer.generate_presigned_url(
        {
            "method": "GET",
            "url": f"https://sts.{region}.amazonaws.com/"
            f"?Action=GetCallerIdentity&Version=2011-06-15",
            "body": {},
            "headers": {"x-k8s-aws-id": cluster_name},
            "context": {},
        },
        region_name=region,
        expires_in=URL_EXPIRY_SECONDS,
        operation_name="",
    )
    # base64url without padding — EKS rejects '=' padding.
    encoded = base64.urlsafe_b64encode(signed_url.encode()).decode().rstrip("=")
    return TOKEN_PREFIX + encoded


def describe_cluster(session: boto3.Session, cluster_name: str, region: str) -> tuple[str, bytes]:
    """Return (endpoint, CA PEM) for the cluster."""
    eks = session.client("eks", region_name=region)
    try:
        c = eks.describe_cluster(name=cluster_name)["cluster"]
    except Exception as exc:  # noqa: BLE001
        raise EksAuthError(
            f"eks:DescribeCluster failed for {cluster_name!r}. The submitter "
            f"role needs eks:DescribeCluster on this cluster.",
            cluster_name=cluster_name,
        ) from exc
    return c["endpoint"], base64.b64decode(c["certificateAuthority"]["data"])


def _iam_role_arn(sts_arn: str) -> str:
    """Normalise an assumed-role ARN to the underlying IAM role ARN.

    STS reports `arn:aws:sts::<acct>:assumed-role/<RoleName>/<session>`, but
    access entries are keyed on `arn:aws:iam::<acct>:role/<RoleName>`. For an
    SSO role the real path is `/aws-reserved/sso.amazonaws.com/...`, which we
    cannot reconstruct from the STS ARN — hence the prefix match in
    `_has_access_entry` rather than an exact lookup.
    """
    parts = sts_arn.split(":")
    if len(parts) < 6 or not parts[5].startswith("assumed-role/"):
        return sts_arn
    role_name = parts[5].split("/")[1]
    return f"arn:aws:iam::{parts[4]}:role/{role_name}"


def _has_access_entry(session: boto3.Session, cluster_name: str, region: str) -> bool:
    """Whether `session`'s identity has an EKS access entry on the cluster."""
    try:
        who = session.client("sts", region_name=region).get_caller_identity()["Arn"]
        eks = session.client("eks", region_name=region)
        entries = eks.list_access_entries(clusterName=cluster_name)["accessEntries"]
    except Exception:  # noqa: BLE001
        # No permission to look, or no cluster access at all — either way the
        # hop is the right next move.
        return False
    role_name = _iam_role_arn(who).rsplit("/", 1)[-1]
    # Match on the role name, not the full ARN: an SSO role's access entry
    # carries the /aws-reserved/sso.amazonaws.com/ path that the STS
    # assumed-role ARN does not expose.
    return any(e.rsplit("/", 1)[-1] == role_name for e in entries)


def acquire(
    *,
    cluster_name: str,
    region: str,
    submitter_role_arn: str,
    endpoint_hint: str = "",
    session_name: str = "remote-step-driver",
) -> ClusterAccess:
    """Do all three hops and return usable cluster credentials.

    `endpoint_hint` comes from the shipped config and is used as-is when
    present; the CA bundle still requires DescribeCluster, so this saves
    nothing today and is kept only so a future config carrying the CA can
    skip the call entirely.

    The role hop is conditional. It exists because the Outerbounds pod's own
    task role has no access entry on this cluster. An operator running a flow
    from their laptop is a different case: their SSO role already has one, so
    assuming the submitter role would be both unnecessary and impossible —
    only the Outerbounds task roles are trusted to assume it.

    So the ambient identity is checked for an access entry first, and the hop
    happens only when it has none. Note the check is specifically for an
    access entry rather than for `eks:DescribeCluster`: a role can easily
    hold the IAM permission through a broad policy while having no
    Kubernetes identity at all, and that combination would otherwise skip
    the hop and then fail with a 403 from the API server.
    """
    session = boto3.Session(region_name=region)
    if not _has_access_entry(session, cluster_name, region):
        session = assume_submitter(submitter_role_arn, region, session_name)
    endpoint, ca = describe_cluster(session, cluster_name, region)
    if endpoint_hint and endpoint_hint != endpoint:
        # Not fatal — the live value wins — but it means the shipped config
        # is stale, which usually means the cluster was rebuilt.
        import sys

        sys.stdout.write(
            f"[remote_step] cluster endpoint in config ({endpoint_hint}) "
            f"differs from live ({endpoint}); using live. Regenerate the env "
            f"config from terraform.\n"
        )
    return ClusterAccess(
        endpoint=endpoint,
        ca_data=ca,
        token=bearer_token(session, cluster_name, region),
        expires_at=time.time() + TOKEN_TTL_SECONDS,
        session=session,
    )


class _TokenRefresher:
    """Keeps a `kubernetes` client's bearer token fresh.

    A step can run for hours; the token dies after 15 minutes. The
    kubernetes client reads `configuration.api_key` on every request, so
    rewriting that dict in place is enough — no need to rebuild the client.
    """

    def __init__(self, access: ClusterAccess, cluster_name: str, region: str, configuration):
        self._access = access
        self._cluster = cluster_name
        self._region = region
        self._cfg = configuration
        self._lock = threading.Lock()

    def refresh_if_needed(self) -> None:
        if not self._access.expired():
            return
        with self._lock:
            if not self._access.expired():
                return
            self._access.token = bearer_token(
                self._access.session, self._cluster, self._region
            )
            self._access.expires_at = time.time() + TOKEN_TTL_SECONDS
            self._cfg.api_key["BearerToken"] = "Bearer " + self._access.token


def api_client(
    access: ClusterAccess,
    cluster_name: str,
    region: str,
) -> tuple[object, _TokenRefresher]:
    """Build a `kubernetes` ApiClient for the cluster.

    Returns (api_client, refresher). Call `refresher.refresh_if_needed()`
    before any long-lived call — the token is only good for ~15 minutes and
    a step body can outlast that.
    """
    import tempfile

    from kubernetes import client as k8s_client

    cfg = k8s_client.Configuration()
    cfg.host = access.endpoint
    # The kubernetes client wants a CA file path, not bytes. Written with
    # delete=False and deliberately not cleaned up: the client reads it lazily
    # on every connection, so removing it would break later requests. The
    # driver pod is ephemeral, so a temp file per task is not a leak that
    # matters.
    ca_file = tempfile.NamedTemporaryFile(  # noqa: SIM115
        prefix="eks-ca-", suffix=".pem", delete=False
    )
    ca_file.write(access.ca_data)
    ca_file.flush()
    ca_file.close()
    cfg.ssl_ca_cert = ca_file.name
    cfg.verify_ssl = True
    # Key must be "BearerToken", not "authorization". Configuration.
    # auth_settings() gates on `if 'BearerToken' in self.api_key` and maps it
    # to the `authorization` header itself; keying on "authorization" makes
    # auth_settings() return {} and the client sends no Authorization header
    # at all, which the API server answers with a bare 401 that looks like a
    # bad token rather than a missing one.
    cfg.api_key = {"BearerToken": "Bearer " + access.token}

    client = k8s_client.ApiClient(configuration=cfg)
    return client, _TokenRefresher(access, cluster_name, region, cfg)
