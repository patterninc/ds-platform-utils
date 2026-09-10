"""Carrying the step body's own exception back to the driver.

`@catch(var="e")` runs on the driver, where the only visible failure is the
RunnerError the poller raises -- so `e` was a RunnerError and the user's own
exception type was lost. The runner persists the exception; the driver
re-raises it.

An exception holding a lock, a cursor or a socket does not pickle, and that
degradation path had two defects: it raised TypeError from inside the failure
handler, and it reported the wrong traceback.
"""

import pickle
import threading

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.errors import RunnerError
from remote_step.plugins.remote_step_decorator import _reraise_step_exception
from remote_step.runner_entry import _save_exception


class Unpicklable(RuntimeError):
    """What a DB cursor, a thread or a live socket does to an exception."""

    def __init__(self, msg):
        super().__init__(msg)
        self.lock = threading.Lock()


class Picklable(ValueError):
    pass


class FakeS3:
    def __init__(self):
        self.body = None

    def put_object(self, **kw):
        self.body = kw["Body"]
        return {}

    def get_object(self, **kw):
        if self.body is None:
            raise KeyError("no such key")
        body = self.body

        class B:
            def read(self_inner):
                return body

        return {"Body": B()}


def save(exc_factory):
    """Raise from a named function so the traceback has something to name."""
    s3 = FakeS3()

    def failing_user_step():
        raise exc_factory()

    try:
        failing_user_step()
    except BaseException as e:  # noqa: BLE001
        _save_exception(e, {"output_prefix": "p", "output_bucket": "b"}, s3_client=s3)
    return s3


# --------------------------------------------------------- picklable exception


def test_a_picklable_exception_is_re_raised_as_itself():
    s3 = save(lambda: Picklable("bad input"))
    with pytest.raises(Picklable, match="bad input"):
        _reraise_step_exception("b", "p", "train", s3_client=s3, exit_code=1)


def test_the_record_carries_the_users_traceback():
    s3 = save(lambda: Picklable("bad input"))
    tb = pickle.loads(s3.body)["traceback"]
    assert "failing_user_step" in tb
    assert "Picklable: bad input" in tb


# ------------------------------------------------------- unpicklable exception


def test_an_unpicklable_exception_degrades_to_type_and_message():
    """It must not raise TypeError from inside the failure handler.

    RunnerError requires exit_code; constructing it with only a message turned
    this branch into `TypeError: RunnerError.__init__() missing 1 required
    positional argument: 'exit_code'`, so the real error was lost entirely.
    """
    s3 = save(lambda: Unpicklable("db cursor exploded"))
    with pytest.raises(RunnerError) as exc:
        _reraise_step_exception("b", "p", "train", s3_client=s3, exit_code=1)
    assert "Unpicklable" in str(exc.value)
    assert "db cursor exploded" in str(exc.value)


def test_the_degraded_error_is_retriable_and_carries_an_exit_code():
    """@retry has to be able to act on it like any other runner failure."""
    s3 = save(lambda: Unpicklable("boom"))
    with pytest.raises(RunnerError) as exc:
        _reraise_step_exception("b", "p", "train", s3_client=s3, exit_code=7)
    assert exc.value.retriable is True
    assert exc.value.exit_code == 7


def test_the_exit_code_defaults_when_the_caller_has_none():
    s3 = save(lambda: Unpicklable("boom"))
    with pytest.raises(RunnerError) as exc:
        _reraise_step_exception("b", "p", "train", s3_client=s3)
    assert exc.value.exit_code == 1


def test_the_traceback_is_the_users_not_our_pickling_failure():
    """The driver prints this string as "raised in the runner pod".

    format_exc() called inside the except branch formats *our* pickling
    TypeError, so a user whose exception held a lock saw a traceback about
    _thread.lock inside remote_step and none of their own stack.
    """
    s3 = save(lambda: Unpicklable("db cursor exploded"))
    record = pickle.loads(s3.body)
    tb = record["traceback"]
    assert record["exception"] is None, "this is the degraded record"
    assert "failing_user_step" in tb, tb
    assert "Unpicklable: db cursor exploded" in tb, tb
    assert "cannot pickle" not in tb, tb
    assert "_save_exception" not in tb, tb


# ------------------------------------------------------------------ robustness


def test_nothing_saved_means_the_caller_falls_through():
    """Returns normally so the caller raises its own RunnerError."""
    _reraise_step_exception("b", "p", "train", s3_client=FakeS3(), exit_code=1)


def test_a_spec_without_an_output_location_saves_nothing():
    s3 = FakeS3()
    try:
        raise Picklable("x")
    except Picklable as e:
        _save_exception(e, {}, s3_client=s3)
    assert s3.body is None


def test_a_read_failure_does_not_replace_the_step_failure():
    """An S3 error here must not become the reported cause."""

    class Broken(FakeS3):
        def get_object(self, **kw):
            raise RuntimeError("s3 unavailable")

    _reraise_step_exception("b", "p", "train", s3_client=Broken(), exit_code=1)
