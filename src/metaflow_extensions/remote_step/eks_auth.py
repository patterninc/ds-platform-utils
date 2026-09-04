"""Authenticate to the EKS API from the Outerbounds driver pod.

Three hops, because the driver and the cluster sit behind different
identities:

    Outerbounds pod task role          (OIDC federated)
      -> sts:AssumeRole                 pattern-ml-platform-ob-submitter
      -> EKS bearer token               presigned sts:GetCallerIdentity
      -> Kubernetes API                 scoped by an EKS access entry

The second hop is the non-obvious one. EKS does not issue tokens; a token
*is* a presigned `sts:GetCallerIdentity` URL, base64url-encoded with a
`k8s-aws-v1.` prefix. The API server's webhook resolves it back to an IAM
identity and matches that against the cluster's access entries. This is
exactly what `aws eks get-token` produces — reimplemented here because the
driver has botocore but no AWS CLI.

Two expiries have to be handled, and they are different lengths:

  - the assumed role's credentials, max 1 hour for a role-chained session
  - the EKS bearer token, capped at 15 minutes

A step can outlive both. The credentials are therefore wrapped in
botocore's RefreshableCredentials so boto3 re-assumes the role on its own,
and the token is regenerated on demand from that live session.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
import threading
import time

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session as _get_botocore_session
from botocore.signers import RequestSigner

from remote_step.errors import RemoteStepError


# EKS requires this exact prefix and rejects anything else.
TOKEN_PREFIX = "k8s-aws-v1."
# The signed URL's own expiry. EKS caps the usable token life at 15 minutes
# regardless, so this only has to outlive the request itself.
URL_EXPIRY_SECONDS = 60
# Regenerate the token before the 15-minute cap so a long watch never
# presents an expired one.
TOKEN_TTL_SECONDS = 10 * 60
# Role-chained sessions are capped at 1 hour by AWS regardless of what we
# ask for, so this is the ceiling rather than a choice.
ASSUME_DURATION_SECONDS = 3600


class EksAuthError(RemoteStepError):
    """Raised when the driver cannot obtain cluster credentials."""


def _refreshable_session(
    role_arn: str,
    region: str,
    session_name: str,
) -> boto3.Session:
    """Session whose credentials re-assume `role_arn` when they expire.

    A plain `assume_role` + `boto3.Session(aws_access_key_id=...)` hands out
    credentials that die after an hour with no way to renew, which fails any
    step that runs longer than that — and fails it *after* the pod has done
    all the work, when the driver goes to read the manifest.

    RefreshableCredentials calls the refresh function again shortly before
    expiry, so a step of any length keeps working.
    """
    base_sts = boto3.client("sts", region_name=region)

    def _refresh() -> dict:
        try:
            resp = base_sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName=session_name[:64],
                DurationSeconds=ASSUME_DURATION_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001
            raise EksAuthError(
                f"could not assume {role_arn}.\n"
                f"  The Outerbounds pod task role must be trusted by that "
                f"role, and the trust policy must allow sts:AssumeRole, "
                f"sts:TagSession AND sts:SetSourceIdentity — Outerbounds "
                f"federates with a source identity set, and without that "
                f"third action the hop fails even though AssumeRole is "
                f"allowed.\n"
                f"  Check var.outerbounds_task_role_arns in infra/eks covers "
                f"the role this pod runs as; `aws sts get-caller-identity` "
                f"inside the pod shows it.",
                role_arn=role_arn,
            ) from exc
        c = resp["Credentials"]
        return {
            "access_key": c["AccessKeyId"],
            "secret_key": c["SecretAccessKey"],
            "token": c["SessionToken"],
            # botocore parses this back; isoformat is what it expects.
            "expiry_time": c["Expiration"].isoformat(),
        }

    creds = RefreshableCredentials.create_from_metadata(
        metadata=_refresh(),
        refresh_using=_refresh,
        method="sts-assume-role",
    )
    botocore_session = _get_botocore_session()
    botocore_session._credentials = creds  # noqa: SLF001
    botocore_session.set_config_variable("region", region)
    return boto3.Session(botocore_session=botocore_session)


def bearer_token(session: boto3.Session, cluster_name: str, region: str) -> str:
    """Build an EKS bearer token from `session`'s current credentials.

    Reimplements `aws eks get-token`: presign an STS GetCallerIdentity GET
    with the cluster name carried in the `x-k8s-aws-id` header, then
    base64url-encode the URL. The header is included in the signature, which
    is what binds the token to one cluster — the same signed URL cannot be
    replayed against a different one.

    `session.get_credentials()` returns the refreshable object, so this picks
    up renewed credentials without being handed a new session.
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


def describe_cluster(
    session: boto3.Session, cluster_name: str, region: str
) -> tuple[str, bytes]:
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
    """Normalise an assumed-role ARN to the underlying IAM role ARN."""
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


@dataclass
class ClusterAccess:
    """A live, self-renewing handle on one cluster."""

    endpoint: str
    ca_data: bytes  # PEM, decoded from the base64 EKS returns
    session: boto3.Session
    cluster_name: str
    region: str

    _token: str = ""
    _token_expires: float = 0.0

    def __post_init__(self) -> None:
        self._lock = threading.Lock()

    def token(self) -> str:
        """Current bearer token, regenerated when it is close to expiry.

        Passed as a callable into the API client so every request picks up a
        fresh token without the caller tracking time. Cheap: presigning is a
        local signature computation, no network call.
        """
        with self._lock:
            if not self._token or time.time() >= self._token_expires:
                self._token = bearer_token(
                    self.session, self.cluster_name, self.region
                )
                self._token_expires = time.time() + TOKEN_TTL_SECONDS
            return self._token


def acquire(
    *,
    cluster_name: str,
    region: str,
    submitter_role_arn: str,
    endpoint_hint: str = "",
    session_name: str = "remote-step-driver",
) -> ClusterAccess:
    """Obtain renewable cluster credentials.

    The role hop is conditional. It exists because the Outerbounds pod's own
    task role has no access entry on this cluster. An operator running a flow
    from their laptop is a different case: their SSO role already has one, so
    assuming the submitter role would be both unnecessary and impossible —
    only the Outerbounds task roles are trusted to assume it.

    The check is specifically for an access entry rather than for
    `eks:DescribeCluster`: a role can hold the IAM permission through a broad
    policy while having no Kubernetes identity at all, and that combination
    would otherwise skip the hop and then fail with a 403 from the API
    server.
    """
    ambient = boto3.Session(region_name=region)
    if _has_access_entry(ambient, cluster_name, region):
        session = ambient
    else:
        session = _refreshable_session(submitter_role_arn, region, session_name)

    endpoint, ca = describe_cluster(session, cluster_name, region)
    if endpoint_hint and endpoint_hint != endpoint:
        import sys

        sys.stdout.write(
            f"[remote_step] cluster endpoint in config ({endpoint_hint}) "
            f"differs from live ({endpoint}); using live. Regenerate the env "
            f"config from terraform.\n"
        )
    return ClusterAccess(
        endpoint=endpoint,
        ca_data=ca,
        session=session,
        cluster_name=cluster_name,
        region=region,
    )


def api_client(access: ClusterAccess):
    """Build a Kubernetes REST client for the cluster.

    `access.token` is passed as a callable, not a string, so the client
    re-reads it per request. Combined with the refreshable STS credentials
    behind it, a step of any length keeps working: the token regenerates
    every 10 minutes and the underlying role is re-assumed before its hour
    expires.
    """
    from remote_step.k8s import K8sClient

    return K8sClient(
        endpoint=access.endpoint,
        ca_data=access.ca_data,
        token_provider=access.token,
    )
