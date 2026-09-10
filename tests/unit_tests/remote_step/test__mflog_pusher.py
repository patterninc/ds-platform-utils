"""Forcing driver log uploads without burning the driver to do it.

Metaflow's own sidecar backs off to ~30s for long tasks, so the Outerbounds UI
lagged the stream badly; this pushes uploads on a tight cadence instead. But
`metaflow.mflog.save_logs` spawns a fresh interpreter, imports metaflow, and
reads and re-uploads the *entire* capture file -- so on a step that logs once
and then computes for four hours, a 3s cadence meant ~4,800 interpreter starts
and 4,800 uploads of identical bytes, on a driver with two cores.
"""

import os

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

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
