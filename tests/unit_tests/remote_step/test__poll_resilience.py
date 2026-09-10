"""What the poll loop does when the Kubernetes API misbehaves.

This is the most cost-sensitive path in the decorator. A remote step can hold
96 vCPU or a GPU, so a driver that returns to Metaflow while its pod carries
on running bills for up to `job_timeout_minutes` (4 hours by default) with
nobody watching. Two ways that happened:

- k8s.py talks to the API through a urllib3 PoolManager, and none of urllib3's
  errors derive from OSError, so the loop's transient handler never saw them.
- the only cleanup was on KeyboardInterrupt; the `finally` restores signal
  handlers and nothing else.
"""

import io
import time

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest
import urllib3

from remote_step.errors import RemoteStepError
from remote_step.k8s import ApiError
from remote_step.poll import wait


def job(succeeded=True, active=0):
    """A terminal Job object, shaped like the API returns it."""
    status = {"succeeded": 1} if succeeded else {"failed": 1}
    status["active"] = active
    return {"metadata": {"name": "j"}, "status": status}


def pod(phase="Succeeded", exit_code=0):
    return {
        "metadata": {"name": "j-abc", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "spec": {"nodeName": "node-1"},
        "status": {
            "phase": phase,
            "containerStatuses": [{"state": {"terminated": {"exitCode": exit_code}}}],
        },
    }


class FakeClient:
    """Feeds a scripted sequence of get_job outcomes.

    An entry that is an exception class or instance is raised; anything else
    is returned.
    """

    def __init__(self, script):
        self.script = list(script)
        self.deleted = []
        self.calls = 0

    def get_job(self, ns, name):
        self.calls += 1
        item = self.script.pop(0) if self.script else job()
        if isinstance(item, BaseException) or (isinstance(item, type) and issubclass(item, BaseException)):
            raise item
        return item

    def list_job_pods(self, ns, name):
        return [pod()]

    def list_events_for(self, ns, name):
        return []

    def get_node(self, name):
        return {"metadata": {"labels": {"node.kubernetes.io/instance-type": "m9g.2xlarge"}}}

    def stream_pod_log(self, ns, name, since_seconds=None):
        return iter(())

    def delete_job(self, ns, name):
        self.deleted.append((ns, name))


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """The backoff sleeps for real seconds; 40 failures is minutes of them."""
    monkeypatch.setattr(time, "sleep", lambda *_a, **_k: None)


def run(client):
    return wait(client, "forecasting", "j", out=io.StringIO(), stream_logs=False)


URLLIB3_ERRORS = [
    urllib3.exceptions.ProtocolError("connection reset"),
    urllib3.exceptions.MaxRetryError(pool=None, url="/apis", reason=None),
    urllib3.exceptions.ReadTimeoutError(pool=None, url="/apis", message="timed out"),
    urllib3.exceptions.NewConnectionError(None, "dns failure"),
]


@pytest.mark.parametrize("err", URLLIB3_ERRORS, ids=lambda e: type(e).__name__)
def test_a_urllib3_blip_is_transient_not_fatal(err):
    """The pod is very likely still running; failing here would abandon it."""
    client = FakeClient([err, err, job()])
    result = run(client)
    assert result.succeeded
    assert client.deleted == [], "a transient blip must not delete a healthy Job"


def test_no_urllib3_error_derives_from_oserror():
    """Why the original `except (ApiError, OSError)` could never catch them."""
    for err in URLLIB3_ERRORS:
        assert not isinstance(err, OSError), type(err).__name__


def test_an_api_error_is_still_transient():
    client = FakeClient([ApiError("503", status=503), job()])
    assert run(client).succeeded


def test_sustained_api_failure_gives_up_and_deletes_the_job():
    """Contact is lost, so the pod must not be left running."""
    client = FakeClient([urllib3.exceptions.ProtocolError("reset")] * 40)
    with pytest.raises(RemoteStepError, match="lost contact"):
        run(client)
    assert client.deleted == [("forecasting", "j")]


def test_an_unexpected_driver_error_deletes_the_job():
    """The case that returned control to Metaflow with the pod still billing."""

    class Boom(Exception):
        pass

    client = FakeClient([Boom("something nobody predicted")])
    with pytest.raises(Boom):
        run(client)
    assert client.deleted == [("forecasting", "j")], "pod left running on an unexpected failure"


def test_a_clean_success_does_not_delete_the_job():
    """TTL owns the Job on the happy path; deleting here would drop its logs."""
    client = FakeClient([job()])
    assert run(client).succeeded
    assert client.deleted == []
