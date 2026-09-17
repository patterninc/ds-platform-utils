"""Forcing driver log uploads on a flat, unconditional cadence.

Metaflow's own sidecar backs off as a task ages (~0.3s early, ~30s past about
20 minutes), and a @remote_step driver is long-lived by construction -- it
stays alive for the whole offloaded step. So on exactly the runs people watch,
the sidecar has given up and this class is what keeps the Outerbounds UI close
to the stream.

Two cleverer schemes were tried here and both cost freshness:

  - backing the interval off with log size (4 MB threshold, 30s cap). The
    driver's stdout carries the entire streamed runner-pod log, so a chatty
    step crossed 4 MB in its first minute and degraded to 6s, then 12s, then
    the full 30s -- the sidecar cadence this class exists to beat.
  - skipping the upload when the capture file had not grown. That makes
    freshness depend on os.path.getsize() reflecting every append within a 3s
    sample, and the reported symptom was a UI refreshing about every 20s where
    the old unconditional loop had been instant.

Uploading unconditionally is not free -- save_logs re-reads and re-uploads the
whole file, and each call spawns an interpreter (~1.4s measured). It is
self-limiting where it counts, though: subprocess.run blocks, so a large file
cannot be shipped more often than it takes to ship, and a 28 MB log settles at
its own ~16s cadence regardless of the interval.
"""

import sys
import time

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.plugins import remote_step_decorator as rsd
from remote_step.plugins.remote_step_decorator import (
    MFLOG_FORCE_UPLOAD_INTERVAL_SEC,
    MFLOG_IDLE_HEARTBEAT_SEC,
    _MflogPusher,
)


class _Completed:
    """Stand-in for subprocess.CompletedProcess."""

    returncode = 0


@pytest.fixture
def logs(tmp_path, monkeypatch):
    """Point the pusher at real files, as a Metaflow task pod would."""
    out = tmp_path / "stdout"
    err = tmp_path / "stderr"
    out.write_text("")
    err.write_text("")
    monkeypatch.setenv("MFLOG_STDOUT", str(out))
    monkeypatch.setenv("MFLOG_STDERR", str(err))
    return out, err


def spawn_recording_pusher(monkeypatch, interval=0.05):
    """A pusher whose uploads are counted instead of run."""
    calls = []
    monkeypatch.setattr(
        rsd.subprocess, "run", lambda cmd, **kw: calls.append(cmd) or _Completed()
    )
    return _MflogPusher(interval=interval), calls


# ------------------------------------------------------------------- cadence


def test_the_cadence_is_flat():
    """No size-dependent interval at all -- that is the whole point.

    A size-scaled interval is what produced 6s at 8 MB, 12s at 16 MB and 30s
    at 40 MB, on logs a chatty step reaches in its first minute.
    """
    assert MFLOG_FORCE_UPLOAD_INTERVAL_SEC == 3.0
    assert not hasattr(_MflogPusher, "_interval_for"), "no size-scaled interval"


def test_the_cadence_beats_metaflows_own_slow_end():
    """Below the sidecar's ~30s floor, or this class buys nothing."""
    from metaflow.mflog import update_delay

    assert MFLOG_FORCE_UPLOAD_INTERVAL_SEC < update_delay(3600)


# --------------------------------------------------------------- size probing


def test_the_size_is_the_sum_of_both_streams(logs):
    out, err = logs
    out.write_text("a" * 10)
    err.write_text("b" * 7)
    assert _MflogPusher._log_size() == 17


def test_a_missing_file_does_not_raise(logs, monkeypatch):
    """A background thread must not die because a stream is not created yet."""
    monkeypatch.setenv("MFLOG_STDOUT", "/nonexistent/path")
    assert _MflogPusher._log_size() >= 0


def test_no_env_means_zero(monkeypatch):
    monkeypatch.delenv("MFLOG_STDOUT", raising=False)
    monkeypatch.delenv("MFLOG_STDERR", raising=False)
    assert _MflogPusher._log_size() == 0


# ----------------------------------------------------------------- the loop


def test_an_idle_step_stops_uploading(logs, monkeypatch):
    """The case that cost the most: one log line, then hours of compute.

    Each upload spawns an interpreter (~1.4s measured) and re-ships the whole
    file, so an unconditional 3s loop burns ~1200 of them an hour on a driver
    that is idle by definition -- it is waiting out the runner pod. A flat
    size means there is nothing new to show, so skipping costs no freshness.
    """
    out, _ = logs
    out.write_text("the only line this step ever prints\n")
    p, calls = spawn_recording_pusher(monkeypatch)

    p.start()
    time.sleep(1.4)  # 1.0s of that is the pusher's deliberate initial delay
    p.stop()

    # One upload for the initial content, plus the unconditional final flush.
    assert len(calls) <= 2, f"{len(calls)} uploads for an unchanging log"


def test_the_heartbeat_bounds_how_stale_a_flat_size_can_get(monkeypatch):
    """The floor under skip-if-unchanged.

    If the size can ever stall while output is genuinely pending -- a buffered
    writer, a step pausing mid-line -- nothing else would break it out.
    """
    assert 0 < MFLOG_IDLE_HEARTBEAT_SEC <= 30.0


def test_the_final_flush_is_not_gated_on_the_size_probe(logs, monkeypatch):
    """The one upload that must not be skipped.

    Gating it would put the lines explaining a failure behind that probe being
    right, so it runs unconditionally even when the size looks unchanged.
    """
    out, _ = logs
    out.write_text("x\n")
    p, calls = spawn_recording_pusher(monkeypatch, interval=5.0)
    monkeypatch.setattr(p, "_log_size", lambda: 999)  # frozen: never "grows"

    p.start()
    time.sleep(1.2)
    before = len(calls)
    p.stop()

    assert len(calls) > before, "final flush must run regardless of the probe"


def test_new_output_is_uploaded_promptly(logs, monkeypatch):
    out, _ = logs
    p, calls = spawn_recording_pusher(monkeypatch)
    p.start()
    time.sleep(1.1)  # the pusher delays its first cycle by 1.0s on purpose
    for i in range(3):
        out.write_text("line %d\n" % i * (i + 1))
        time.sleep(0.08)
    p.stop()
    assert len(calls) >= 2


def test_the_pusher_does_nothing_without_the_mflog_env(monkeypatch):
    """Locally the vars are absent and this must be inert."""
    monkeypatch.delenv("MFLOG_STDOUT", raising=False)
    p, calls = spawn_recording_pusher(monkeypatch)
    p.start()
    p.stop()
    assert calls == []


def test_stop_flushes_whatever_arrived_last(logs, monkeypatch):
    """stop() runs in a finally, so the failure path needs this too.

    The wait uses `break`, not `return` -- returning would skip the final
    flush and lose every line written since the last cycle, which on a failing
    step is the part that explains the failure.
    """
    out, _ = logs
    p, calls = spawn_recording_pusher(monkeypatch, interval=5.0)
    p.start()
    time.sleep(1.2)  # past the initial delay, before the first long wait ends
    before = len(calls)
    out.write_text("a line written just before the step ended\n")
    p.stop()
    assert len(calls) > before, "the last output must not be lost"


# ------------------------------------------------- which interpreter to spawn
#
# It used to spawn a bare "python", which need not exist: a modern macOS ships
# only `python3`, and so do plenty of slim Linux images. The resulting
# FileNotFoundError is an OSError, so the handler swallowed it, every forced
# upload silently never happened, and the UI fell back to the sidecar -- with
# nothing anywhere to say why. (On the Argo driver image both names resolve to
# the same binary, so this only ever bit local runs.)


def test_the_pusher_spawns_the_running_interpreter(monkeypatch):
    seen = []
    monkeypatch.setattr(rsd.subprocess, "run", lambda cmd, **kw: seen.append(cmd) or _Completed())

    assert _MflogPusher()._save_logs() is True

    assert len(seen) == 1
    assert seen[0][0] == sys.executable
    assert seen[0][1:] == ["-m", "metaflow.mflog.save_logs"]


def test_a_missing_interpreter_is_reported_once(monkeypatch, capsys):
    """Silence here is what let a 10x UI lag survive unnoticed."""

    def boom(cmd, **kw):
        raise FileNotFoundError(2, "No such file or directory", cmd[0])

    monkeypatch.setattr(rsd.subprocess, "run", boom)
    pusher = _MflogPusher()

    assert pusher._save_logs() is False
    first = capsys.readouterr().err
    assert "live log push unavailable" in first
    assert "FileNotFoundError" in first

    assert pusher._save_logs() is False
    assert capsys.readouterr().err == "", "reported once per step, not per cycle"


def test_an_empty_sys_executable_still_spawns_something(monkeypatch):
    """Embedded interpreters can leave sys.executable blank."""
    seen = []
    monkeypatch.setattr(rsd.sys, "executable", "")
    monkeypatch.setattr(rsd.subprocess, "run", lambda cmd, **kw: seen.append(cmd) or _Completed())

    assert _MflogPusher()._save_logs() is True
    assert seen[0][0] == "python", "fall back rather than spawn an empty string"


def test_a_disabled_pusher_says_so(monkeypatch, capsys):
    """Which cadence is in effect must be visible somewhere."""
    monkeypatch.delenv("MFLOG_STDOUT", raising=False)
    pusher = _MflogPusher()

    pusher.start()

    assert pusher._thread is None, "no thread when there is nothing to upload"
    assert "live log push disabled" in capsys.readouterr().err


def test_the_upload_timeout_exceeds_a_realistic_upload(monkeypatch):
    """The timeout must not be shorter than the thing it times.

    It was 15s. save_logs re-reads and re-uploads the whole capture file, and a
    28 MB log measures ~16s -- so on exactly the chatty steps someone watches,
    every push was killed mid-upload, returned False, and the next cycle began
    another doomed attempt. The push did not slow down, it stopped working, and
    the UI fell back to Metaflow's own sidecar with only a single warning line
    to say so.
    """
    from remote_step.plugins.remote_step_decorator import MFLOG_UPLOAD_TIMEOUT_SEC

    assert MFLOG_UPLOAD_TIMEOUT_SEC >= 60, "must clear a multi-MB whole-file upload"


def test_the_upload_timeout_is_what_gets_passed(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        rsd.subprocess, "run", lambda cmd, **kw: seen.update(kw) or _Completed()
    )

    _MflogPusher()._save_logs()

    assert seen["timeout"] == rsd.MFLOG_UPLOAD_TIMEOUT_SEC


def test_a_timed_out_upload_is_reported_and_does_not_advance_the_watermark(monkeypatch, capsys):
    """What the operator actually saw in a fanout task's stderr."""
    def slow(cmd, **kw):
        raise rsd.subprocess.TimeoutExpired(cmd, kw.get("timeout", 0))

    monkeypatch.setattr(rsd.subprocess, "run", slow)
    pusher = _MflogPusher()

    assert pusher._save_logs() is False, "a timeout must not count as a successful upload"
    assert "live log push unavailable" in capsys.readouterr().err
