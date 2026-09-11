"""Forcing driver log uploads without burning the driver to do it.

Metaflow's own sidecar backs off to ~30s for long tasks, so the Outerbounds UI
lagged the stream badly; this pushes uploads on a tight cadence instead. But
`metaflow.mflog.save_logs` spawns a fresh interpreter, imports metaflow, and
reads and re-uploads the *entire* capture file -- so on a step that logs once
and then computes for four hours, a 3s cadence meant ~4,800 interpreter starts
and 4,800 uploads of identical bytes, on a driver with two cores.
"""

import os
import sys

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest


class _Completed:
    """Stand-in for subprocess.CompletedProcess."""

    returncode = 0

from remote_step.plugins import remote_step_decorator as rsd
from remote_step.plugins.remote_step_decorator import (
    MFLOG_FORCE_UPLOAD_INTERVAL_SEC,
    MFLOG_MAX_INTERVAL_SEC,
    MFLOG_TIGHT_CADENCE_BYTES,
    _MflogPusher,
)


@pytest.fixture
def logs(tmp_path, monkeypatch):
    """Point the pusher at real files it can stat."""
    out = tmp_path / "stdout"
    err = tmp_path / "stderr"
    out.write_text("")
    err.write_text("")
    monkeypatch.setenv("MFLOG_STDOUT", str(out))
    monkeypatch.setenv("MFLOG_STDERR", str(err))
    return out, err


# ------------------------------------------------------------------- cadence


def test_a_small_log_keeps_the_tight_cadence():
    p = _MflogPusher()
    assert p._interval_for(0) == MFLOG_FORCE_UPLOAD_INTERVAL_SEC
    assert p._interval_for(MFLOG_TIGHT_CADENCE_BYTES) == MFLOG_FORCE_UPLOAD_INTERVAL_SEC


def test_the_cadence_stretches_as_the_log_grows():
    """save_logs re-sends the whole file, so bytes shipped grow quadratically."""
    p = _MflogPusher()
    assert p._interval_for(8 * 1024 * 1024) > MFLOG_FORCE_UPLOAD_INTERVAL_SEC


def test_the_cadence_is_capped():
    p = _MflogPusher()
    assert p._interval_for(10 * 1024**3) == MFLOG_MAX_INTERVAL_SEC


def test_the_cap_matches_metaflows_own_slow_end():
    assert MFLOG_MAX_INTERVAL_SEC == 30.0


# --------------------------------------------------------------- size probing


def test_the_size_is_the_sum_of_both_streams(logs):
    out, err = logs
    out.write_text("a" * 100)
    err.write_text("b" * 50)
    assert _MflogPusher._log_size() == 150


def test_a_missing_file_does_not_raise(logs, monkeypatch):
    monkeypatch.setenv("MFLOG_STDOUT", "/nonexistent/path")
    assert _MflogPusher._log_size() >= 0


def test_no_env_means_zero(monkeypatch):
    monkeypatch.delenv("MFLOG_STDOUT", raising=False)
    monkeypatch.delenv("MFLOG_STDERR", raising=False)
    assert _MflogPusher._log_size() == 0


# ----------------------------------------------------------------- the loop


def spawn_recording_pusher(monkeypatch, interval=0.01):
    """A pusher whose uploads are counted instead of run."""
    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)

        class R:
            returncode = 0

        return R()

    import remote_step.plugins.remote_step_decorator as mod

    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    p = _MflogPusher(interval=interval)
    return p, calls


def test_an_idle_step_stops_uploading(logs, monkeypatch):
    """The case that cost the most: one log line, then hours of compute."""
    out, _ = logs
    out.write_text("the only line this step ever prints\n")
    p, calls = spawn_recording_pusher(monkeypatch)

    p.start()
    # Let several cycles elapse with the file unchanged.
    import time

    time.sleep(0.3)
    p.stop()

    # One upload for the initial content, plus at most the final flush.
    assert len(calls) <= 2, f"{len(calls)} uploads for an unchanging log"


def test_new_output_triggers_an_upload(logs, monkeypatch):
    out, _ = logs
    p, calls = spawn_recording_pusher(monkeypatch)
    p.start()
    import time

    time.sleep(1.1)  # the pusher delays its first cycle by 1.0s on purpose
    for i in range(3):
        out.write_text("line %d\n" % i * (i + 1))
        time.sleep(0.08)
    p.stop()
    assert len(calls) >= 2, "growth must still be uploaded promptly"


def test_the_pusher_does_nothing_without_the_mflog_env(monkeypatch):
    """Locally the vars are absent and this must be inert."""
    monkeypatch.delenv("MFLOG_STDOUT", raising=False)
    p, calls = spawn_recording_pusher(monkeypatch)
    p.start()
    p.stop()
    assert calls == []


def test_stop_flushes_whatever_arrived_last(logs, monkeypatch):
    """stop() runs in a finally, so the failure path needs this too."""
    out, _ = logs
    p, calls = spawn_recording_pusher(monkeypatch, interval=5.0)
    p.start()
    import time

    time.sleep(1.2)  # past the initial delay, before the first long wait ends
    before = len(calls)
    out.write_text("a line written just before the step ended\n")
    p.stop()
    assert len(calls) > before, "the last output must not be lost"


# ------------------------------------------------- which interpreter to spawn
#
# The pusher exists to keep the UI within ~3s of the driver, and it spawns
# `metaflow.mflog.save_logs` to do it. It used to spawn a bare "python", which
# is not guaranteed to exist anywhere -- a modern macOS ships only `python3`,
# and so do plenty of slim Linux images. The resulting FileNotFoundError is an
# OSError, so the handler swallowed it, every forced upload silently never
# happened, and the UI fell back to Metaflow's own sidecar. Its sigmoid tops
# out near 30s, so the symptom was "logs refresh every 30 seconds" with
# nothing in any log to say why.


def test_the_pusher_spawns_the_running_interpreter(monkeypatch):
    """Not a bare "python" -- that need not exist on PATH at all."""
    seen = []
    monkeypatch.setattr(
        rsd.subprocess, "run", lambda cmd, **kw: seen.append(cmd) or _Completed()
    )

    assert rsd._MflogPusher()._save_logs() is True

    assert len(seen) == 1
    assert seen[0][0] == sys.executable, "must spawn the interpreter actually running"
    assert seen[0][1:] == ["-m", "metaflow.mflog.save_logs"]


def test_a_missing_interpreter_is_reported_once(monkeypatch, capsys):
    """Silence here is what let a 10x UI lag survive unnoticed."""
    def boom(cmd, **kw):
        raise FileNotFoundError(2, "No such file or directory", cmd[0])

    monkeypatch.setattr(rsd.subprocess, "run", boom)
    pusher = rsd._MflogPusher()

    assert pusher._save_logs() is False
    first = capsys.readouterr().err
    assert "live log push unavailable" in first
    assert "FileNotFoundError" in first

    # ...and not again on every cycle for the life of the step.
    assert pusher._save_logs() is False
    assert capsys.readouterr().err == ""


def test_a_failed_push_does_not_advance_the_size_watermark(monkeypatch):
    """Otherwise a recovered interpreter would skip the bytes it missed.

    `last_size` is only updated when the upload actually ran, so output
    written while pushing was broken is still sent once it works again.
    """
    monkeypatch.setattr(
        rsd.subprocess, "run", lambda cmd, **kw: (_ for _ in ()).throw(OSError("nope"))
    )
    assert rsd._MflogPusher()._save_logs() is False


def test_an_empty_sys_executable_still_spawns_something(monkeypatch):
    """Embedded interpreters can leave sys.executable blank."""
    seen = []
    monkeypatch.setattr(rsd.sys, "executable", "")
    monkeypatch.setattr(
        rsd.subprocess, "run", lambda cmd, **kw: seen.append(cmd) or _Completed()
    )

    assert rsd._MflogPusher()._save_logs() is True
    assert seen[0][0] == "python", "fall back rather than spawn an empty string"


def test_a_disabled_pusher_says_so(monkeypatch, capsys):
    """Which of the two cadences is in effect must be visible somewhere.

    Without MFLOG_STDOUT there is no file to upload, so the forced push
    cannot run and the UI falls back to Metaflow's own sidecar -- roughly a
    10x difference in staleness, previously with nothing anywhere to say
    which one you were getting.
    """
    monkeypatch.delenv("MFLOG_STDOUT", raising=False)
    pusher = rsd._MflogPusher()

    pusher.start()

    assert pusher._thread is None, "no thread when there is nothing to upload"
    assert "live log push disabled" in capsys.readouterr().err
