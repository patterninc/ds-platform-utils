"""AWS IAM Identity Center (SSO) login in pure Python — no AWS CLI required.

The driver needs an AWS identity with an EKS access entry. On a laptop that is
an SSO role, and its token expires: the bearer token is regenerated every ten
minutes and the driver reads the output manifest at the very end, so a step
that outlives the session does all its work and then fails on the last read.
Requiring `aws sso login` first means requiring the CLI to be installed, which
is a dependency this package otherwise does not have.

This module does what the CLI's `sso login` does, using boto3's own sso-oidc
client:

    RegisterClient            -> a client id/secret for this machine
    StartDeviceAuthorization  -> a URL and user code
    (browser)                 -> the human approves
    CreateToken               -> an SSO access token
    GetRoleCredentials        -> temporary AWS credentials for the role

It deliberately does NOT depend on botocore's token-cache file format for
anything load-bearing. Credentials are obtained by calling GetRoleCredentials
ourselves and handed to boto3 directly, so a change to that format cannot
break us. The cache is read opportunistically to reuse a token the CLI already
obtained, and written back best-effort so the CLI benefits in turn — both
wrapped so a format change degrades to "log in again" rather than an error.

Non-interactive callers never see a browser. Inside a pod there is no SSO at
all: the task role supplies credentials through OIDC, `_ambient_ok()` succeeds
and none of this runs.
"""

from __future__ import annotations

import configparser
import hashlib
import json
import os
import sys
import time
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import ClientError
from botocore.session import get_session as _get_botocore_session

from remote_step.errors import RemoteStepError

# Treat a token expiring within this window as already expired, so a step does
# not start against credentials about to die.
EXPIRY_MARGIN_SEC = 5 * 60
# Fallback poll interval if StartDeviceAuthorization does not supply one.
DEFAULT_POLL_INTERVAL_SEC = 5
# Registration scope required to call GetRoleCredentials.
DEFAULT_SCOPES = ("sso:account:access",)
CLIENT_NAME = "ds-platform-utils-remote-step"


class SsoAuthError(RemoteStepError):
    """Raised when SSO credentials cannot be obtained."""


@dataclass(frozen=True)
class SsoProfile:
    """The SSO settings of one named profile in ~/.aws/config."""

    profile: str
    start_url: str
    sso_region: str
    account_id: str
    role_name: str
    # Present for the `sso_session` config style, absent for the older style
    # where the SSO settings live directly on the profile. It decides the
    # token cache filename, which botocore keys on whichever of the two
    # identifies the session.
    session_name: str | None
    scopes: tuple[str, ...] = DEFAULT_SCOPES

    @property
    def cache_key(self) -> str:
        raw = self.session_name or self.start_url
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _aws_config_path() -> Path:
    return Path(
        os.environ.get("AWS_CONFIG_FILE") or (Path.home() / ".aws" / "config")
    )


def _cache_dir() -> Path:
    return Path.home() / ".aws" / "sso" / "cache"


def read_profile(profile: str | None = None) -> SsoProfile | None:
    """Parse the SSO settings of `profile`, or None if it is not an SSO profile.

    Handles both config styles: `sso_session = <name>` pointing at an
    `[sso-session <name>]` block, and the older form with sso_start_url and
    sso_region written directly on the profile.
    """
    profile = profile or os.environ.get("AWS_PROFILE") or "default"
    path = _aws_config_path()
    if not path.is_file():
        return None

    parser = configparser.ConfigParser()
    try:
        parser.read(path)
    except configparser.Error:
        return None

    section = f"profile {profile}" if profile != "default" else "default"
    if not parser.has_section(section):
        return None
    prof = parser[section]

    account_id = prof.get("sso_account_id")
    role_name = prof.get("sso_role_name")
    if not (account_id and role_name):
        return None  # not an SSO profile

    session_name = prof.get("sso_session")
    scopes: tuple[str, ...] = DEFAULT_SCOPES
    if session_name:
        sso_section = f"sso-session {session_name}"
        if not parser.has_section(sso_section):
            return None
        sess = parser[sso_section]
        start_url = sess.get("sso_start_url", "")
        sso_region = sess.get("sso_region", "")
        raw_scopes = sess.get("sso_registration_scopes")
        if raw_scopes:
            scopes = tuple(s.strip() for s in raw_scopes.split(",") if s.strip())
    else:
        start_url = prof.get("sso_start_url", "")
        sso_region = prof.get("sso_region", "")

    if not (start_url and sso_region):
        return None

    return SsoProfile(
        profile=profile,
        start_url=start_url,
        sso_region=sso_region,
        account_id=account_id,
        role_name=role_name,
        session_name=session_name,
        scopes=scopes,
    )


def _parse_expiry(raw) -> datetime | None:
    """Parse the several shapes AWS uses for an expiry timestamp."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, (int, float)):  # milliseconds since epoch
        return datetime.fromtimestamp(raw / 1000.0, tz=timezone.utc)
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def _seconds_left(raw) -> float:
    exp = _parse_expiry(raw)
    if exp is None:
        return -1.0
    return (exp - datetime.now(timezone.utc)).total_seconds()


def read_cached_token(prof: SsoProfile) -> dict | None:
    """The cached token blob for this profile, or None if absent/unreadable.

    Returned whether or not it has expired — the caller decides, because an
    expired token may still carry a usable refresh token.
    """
    path = _cache_dir() / f"{prof.cache_key}.json"
    try:
        if not path.is_file():
            return None
        body = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return body if isinstance(body, dict) and body.get("accessToken") else None


def write_cached_token(prof: SsoProfile, blob: dict) -> None:
    """Best-effort write so the AWS CLI can reuse what we obtained."""
    try:
        d = _cache_dir()
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{prof.cache_key}.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(blob))
        os.chmod(tmp, 0o600)
        tmp.replace(path)
    except (OSError, ValueError):
        pass


def _register_client(oidc, prof: SsoProfile, cached: dict | None) -> dict:
    """Reuse the cached client registration if valid, else register a new one."""
    if cached and _seconds_left(cached.get("registrationExpiresAt")) > 0:
        cid, secret = cached.get("clientId"), cached.get("clientSecret")
        if cid and secret:
            return {
                "clientId": cid,
                "clientSecret": secret,
                "registrationExpiresAt": cached.get("registrationExpiresAt"),
            }
    resp = oidc.register_client(
        clientName=CLIENT_NAME,
        clientType="public",
        scopes=list(prof.scopes),
    )
    return {
        "clientId": resp["clientId"],
        "clientSecret": resp["clientSecret"],
        "registrationExpiresAt": _parse_expiry(
            resp.get("clientSecretExpiresAt")
        ),
    }


def _try_refresh(oidc, prof: SsoProfile, cached: dict | None) -> dict | None:
    """Silently exchange a refresh token for a new access token.

    This is the path that avoids a browser entirely, and the one that runs
    when a token has aged out mid-session. Returns None if there is nothing
    to refresh with or the grant is rejected.
    """
    if not cached:
        return None
    refresh = cached.get("refreshToken")
    cid, secret = cached.get("clientId"), cached.get("clientSecret")
    if not (refresh and cid and secret):
        return None
    try:
        resp = oidc.create_token(
            clientId=cid,
            clientSecret=secret,
            grantType="refresh_token",
            refreshToken=refresh,
        )
    except ClientError:
        return None  # expired or revoked; caller falls back to device login
    return _token_blob(prof, resp, cid, secret, cached.get("registrationExpiresAt"))


def _token_blob(
    prof: SsoProfile, resp: dict, client_id: str, client_secret: str, reg_expiry
) -> dict:
    """Assemble a cache-shaped blob from a CreateToken response."""
    expires_at = datetime.now(timezone.utc).timestamp() + int(
        resp.get("expiresIn") or 0
    )
    blob = {
        "startUrl": prof.start_url,
        "region": prof.sso_region,
        "accessToken": resp["accessToken"],
        "expiresAt": datetime.fromtimestamp(expires_at, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "clientId": client_id,
        "clientSecret": client_secret,
    }
    if resp.get("refreshToken"):
        blob["refreshToken"] = resp["refreshToken"]
    reg = _parse_expiry(reg_expiry)
    if reg:
        blob["registrationExpiresAt"] = reg.isoformat().replace("+00:00", "Z")
    return blob


def _device_login(oidc, prof: SsoProfile, reg: dict) -> dict:
    """Run the device authorization grant, opening a browser for approval."""
    auth = oidc.start_device_authorization(
        clientId=reg["clientId"],
        clientSecret=reg["clientSecret"],
        startUrl=prof.start_url,
    )
    url = auth.get("verificationUriComplete") or auth["verificationUri"]
    interval = int(auth.get("interval") or DEFAULT_POLL_INTERVAL_SEC)
    deadline = time.time() + int(auth.get("expiresIn") or 600)

    # Print before opening: on a headless machine the browser call is a no-op
    # and the URL is the only way through.
    sys.stdout.write(
        f"\n[remote_step] AWS SSO login required for profile "
        f"{prof.profile!r}.\n"
        f"  Approve in your browser: {url}\n"
        f"  Verification code: {auth.get('userCode', '(embedded in the URL)')}\n"
    )
    sys.stdout.flush()
    try:
        webbrowser.open(url)
    except Exception:  # noqa: BLE001 - headless, or no handler registered
        pass

    while time.time() < deadline:
        try:
            resp = oidc.create_token(
                clientId=reg["clientId"],
                clientSecret=reg["clientSecret"],
                grantType="urn:ietf:params:oauth:grant-type:device_code",
                deviceCode=auth["deviceCode"],
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "AuthorizationPendingException":
                time.sleep(interval)
                continue
            if code == "SlowDownException":
                # The service is asking us to back off; it is not an error.
                interval += 5
                time.sleep(interval)
                continue
            if code == "ExpiredTokenException":
                raise SsoAuthError(
                    "the SSO login request expired before it was approved. "
                    "Run the flow again."
                ) from exc
            raise SsoAuthError(f"SSO login failed: {exc}") from exc
        sys.stdout.write("[remote_step] SSO login complete.\n")
        sys.stdout.flush()
        return _token_blob(
            prof,
            resp,
            reg["clientId"],
            reg["clientSecret"],
            reg.get("registrationExpiresAt"),
        )

    raise SsoAuthError(
        "timed out waiting for SSO approval in the browser. Run the flow again."
    )


def access_token(prof: SsoProfile, *, interactive: bool = True) -> dict:
    """Return a valid token blob for `prof`, logging in if necessary.

    Order matters: reuse, then silent refresh, then browser. Only the last
    needs a human, and it is skipped entirely when `interactive` is False.
    """
    cached = read_cached_token(prof)
    if cached and _seconds_left(cached.get("expiresAt")) > EXPIRY_MARGIN_SEC:
        return cached

    oidc = boto3.client("sso-oidc", region_name=prof.sso_region)

    refreshed = _try_refresh(oidc, prof, cached)
    if refreshed:
        write_cached_token(prof, refreshed)
        return refreshed

    if not interactive:
        left = _seconds_left(cached.get("expiresAt")) if cached else -1
        state = (
            f"expired {abs(left) / 60:.0f} min ago"
            if cached
            else "no cached session"
        )
        raise SsoAuthError(
            f"AWS SSO session for profile {prof.profile!r} needs a browser "
            f"login ({state}), but this process is not interactive.\n"
            f"  Run any command from a terminal to log in, or set AWS "
            f"credentials in the environment.",
            profile=prof.profile,
        )

    reg = _register_client(oidc, prof, cached)
    blob = _device_login(oidc, prof, reg)
    write_cached_token(prof, blob)
    return blob


def session(profile: str | None = None, *, interactive: bool = True) -> boto3.Session:
    """A boto3 Session for `profile`'s SSO role, logging in if needed.

    Credentials come from GetRoleCredentials and are wrapped in
    RefreshableCredentials, so they renew on their own for as long as the SSO
    token lives — and the refresh path retries a silent token refresh first,
    so an aging session recovers without a browser. It never opens a browser
    from the refresh callback: that runs on a background thread mid-step,
    where a blocking prompt nobody is watching would hang the run.
    """
    prof = read_profile(profile)
    if prof is None:
        raise SsoAuthError(
            f"profile {profile or os.environ.get('AWS_PROFILE') or 'default'!r} "
            f"is not configured for AWS SSO (no sso_account_id/sso_role_name "
            f"in ~/.aws/config)."
        )

    token = access_token(prof, interactive=interactive)

    def _fetch() -> dict:
        nonlocal token
        sso = boto3.client("sso", region_name=prof.sso_region)
        try:
            resp = sso.get_role_credentials(
                roleName=prof.role_name,
                accountId=prof.account_id,
                accessToken=token["accessToken"],
            )
        except ClientError:
            # Token died since we last used it. Try a silent refresh once;
            # deliberately no browser here.
            token = access_token(prof, interactive=False)
            resp = sso.get_role_credentials(
                roleName=prof.role_name,
                accountId=prof.account_id,
                accessToken=token["accessToken"],
            )
        c = resp["roleCredentials"]
        return {
            "access_key": c["accessKeyId"],
            "secret_key": c["secretAccessKey"],
            "token": c["sessionToken"],
            "expiry_time": datetime.fromtimestamp(
                c["expiration"] / 1000.0, tz=timezone.utc
            ).isoformat(),
        }

    creds = RefreshableCredentials.create_from_metadata(
        metadata=_fetch(), refresh_using=_fetch, method="sso"
    )
    botocore_session = _get_botocore_session()
    botocore_session._credentials = creds  # noqa: SLF001
    return boto3.Session(botocore_session=botocore_session)
