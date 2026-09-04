"""Minimal Kubernetes REST client, built on urllib3.

Deliberately not the official `kubernetes` package, for one concrete reason:
the driver runs inside the flow's own Metaflow pypi environment, not inside
our container image, so anything it imports has to be a declared dependency
of every flow that uses @remote_step. The official client pulls pyyaml,
requests, google-auth, oauthlib, websocket-client and certifi — and ML flows
routinely pin pyyaml and requests themselves, so that set is a standing
version-conflict risk over an implementation detail of one decorator.

urllib3 is a hard requirement of botocore, so it is already present wherever
boto3 is, which is everywhere Metaflow's S3 datastore works. Depending on it
costs nothing.

Only the operations @remote_step performs are implemented:

    create_job          POST   /apis/batch/v1/namespaces/{ns}/jobs
    get_job             GET    /apis/batch/v1/namespaces/{ns}/jobs/{name}
    delete_job          DELETE /apis/batch/v1/namespaces/{ns}/jobs/{name}
    list_job_pods       GET    /api/v1/namespaces/{ns}/pods?labelSelector=
    stream_pod_log      GET    /api/v1/namespaces/{ns}/pods/{pod}/log?follow=1
    read_pod_log        GET    /api/v1/namespaces/{ns}/pods/{pod}/log
    list_events_for     GET    /api/v1/namespaces/{ns}/events?fieldSelector=
    get_node            GET    /api/v1/nodes/{name}
    server_version      GET    /version

Responses are plain dicts, so callers index them rather than walking the
attribute objects the official client returns.
"""

from __future__ import annotations

import json
import tempfile
from typing import Iterator
from urllib.parse import urlencode

import urllib3

from remote_step.errors import RemoteStepError


class ApiError(RemoteStepError):
    """Non-2xx response from the API server."""

    def __init__(self, message: str, status: int = 0, **ctx):
        super().__init__(message, status=status, **ctx)
        self.status = status


class NotFound(ApiError):
    """404 — the object does not exist (or no longer does)."""


class Conflict(ApiError):
    """409 — the object already exists."""


class Forbidden(ApiError):
    """403 — authenticated but not authorised."""


def _raise_for_status(method: str, path: str, status: int, body: bytes) -> None:
    detail = ""
    try:
        detail = (body or b"").decode("utf-8", "replace")[:400]
    except Exception:  # noqa: BLE001
        pass
    msg = f"{method} {path} -> {status}: {detail}"
    if status == 404:
        raise NotFound(msg, status=status)
    if status == 409:
        raise Conflict(msg, status=status)
    if status == 403:
        raise Forbidden(msg, status=status)
    raise ApiError(msg, status=status)


class K8sClient:
    """Thin authenticated wrapper over one cluster's API endpoint.

    The bearer token is read through `token_provider` on every request
    rather than captured once: an EKS token lives ~15 minutes and a step can
    run for hours, so the provider regenerates as needed.
    """

    def __init__(
        self,
        endpoint: str,
        ca_data: bytes,
        token_provider,
        read_timeout_sec: float = 60.0,
    ) -> None:
        self._base = endpoint.rstrip("/")
        self._token_provider = token_provider
        # urllib3 wants a CA file path. delete=False and never cleaned up on
        # purpose: the pool reads it lazily per connection, so removing it
        # would break later requests. The driver pod is ephemeral.
        ca = tempfile.NamedTemporaryFile(  # noqa: SIM115
            prefix="eks-ca-", suffix=".pem", delete=False
        )
        ca.write(ca_data)
        ca.flush()
        ca.close()
        self._pool = urllib3.PoolManager(
            cert_reqs="CERT_REQUIRED",
            ca_certs=ca.name,
            timeout=urllib3.Timeout(connect=10.0, read=read_timeout_sec),
            retries=urllib3.Retry(
                total=3,
                backoff_factor=0.5,
                # POST is excluded: retrying a Job create could produce a
                # second Job. The caller handles 409 instead.
                allowed_methods=frozenset(["GET", "DELETE"]),
                status_forcelist=[429, 500, 502, 503, 504],
            ),
        )

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token_provider()}",
            "Accept": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: dict | None = None,
        params: dict[str, str] | None = None,
        preload: bool = True,
        read_timeout: float | None = None,
    ):
        url = self._base + path
        if params:
            url += "?" + urlencode(params)
        headers = self._headers()
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        kw = {}
        if read_timeout is not None:
            kw["timeout"] = urllib3.Timeout(connect=10.0, read=read_timeout)
        resp = self._pool.request(
            method, url, body=data, headers=headers, preload_content=preload, **kw
        )
        if resp.status >= 400:
            _raise_for_status(method, path, resp.status, resp.data if preload else b"")
        return resp

    def _json(self, method: str, path: str, **kw) -> dict:
        resp = self._request(method, path, **kw)
        raw = resp.data or b"{}"
        return json.loads(raw.decode("utf-8", "replace") or "{}")

    # ------------------------------------------------------------------- calls

    def server_version(self) -> dict:
        return self._json("GET", "/version")

    def create_job(self, namespace: str, manifest: dict) -> dict:
        return self._json(
            "POST", f"/apis/batch/v1/namespaces/{namespace}/jobs", body=manifest
        )

    def get_job(self, namespace: str, name: str) -> dict:
        return self._json("GET", f"/apis/batch/v1/namespaces/{namespace}/jobs/{name}")

    def delete_job(self, namespace: str, name: str) -> dict:
        # Background propagation so the call returns without waiting for the
        # pod to be reaped; we only care that Kueue quota is released.
        return self._json(
            "DELETE",
            f"/apis/batch/v1/namespaces/{namespace}/jobs/{name}",
            body={
                "apiVersion": "meta/v1",
                "kind": "DeleteOptions",
                "propagationPolicy": "Background",
            },
        )

    def list_job_pods(self, namespace: str, job_name: str) -> list[dict]:
        out = self._json(
            "GET",
            f"/api/v1/namespaces/{namespace}/pods",
            params={"labelSelector": f"job-name={job_name}"},
        )
        return out.get("items", []) or []

    def get_node(self, name: str) -> dict:
        return self._json("GET", f"/api/v1/nodes/{name}")

    def list_events_for(self, namespace: str, object_name: str) -> list[dict]:
        out = self._json(
            "GET",
            f"/api/v1/namespaces/{namespace}/events",
            params={"fieldSelector": f"involvedObject.name={object_name}"},
        )
        return out.get("items", []) or []

    def stream_pod_log(
        self,
        namespace: str,
        pod: str,
        container: str | None = None,
        since_seconds: int | None = None,
    ) -> Iterator[str]:
        """Yield the pod's stdout as it is produced.

        `preload_content=False` is what makes this a stream rather than a
        buffered read of the whole log.

        `since_seconds` matters on reconnect: without it the API server
        replays the log from the beginning, so a step that logs slowly and
        drops an idle connection would print its whole history again on
        every reconnect.

        No read timeout: a follow stream is legitimately idle between the
        step's own log lines, and a timeout would tear it down mid-step.
        """
        params = {"follow": "true", "timestamps": "false"}
        if container:
            params["container"] = container
        if since_seconds is not None:
            params["sinceSeconds"] = str(max(1, int(since_seconds)))
        resp = self._request(
            "GET",
            f"/api/v1/namespaces/{namespace}/pods/{pod}/log",
            params=params,
            preload=False,
            read_timeout=None,
        )
        try:
            for chunk in resp.stream(amt=None, decode_content=True):
                if not chunk:
                    continue
                yield (
                    chunk.decode("utf-8", "replace")
                    if isinstance(chunk, bytes)
                    else str(chunk)
                )
        finally:
            try:
                resp.release_conn()
            except Exception:  # noqa: BLE001
                pass

    def read_pod_log(
        self, namespace: str, pod: str, container: str | None = None
    ) -> str:
        """Whole log, non-streaming."""
        params = {"follow": "false", "timestamps": "false"}
        if container:
            params["container"] = container
        resp = self._request(
            "GET",
            f"/api/v1/namespaces/{namespace}/pods/{pod}/log",
            params=params,
        )
        return (resp.data or b"").decode("utf-8", "replace")
