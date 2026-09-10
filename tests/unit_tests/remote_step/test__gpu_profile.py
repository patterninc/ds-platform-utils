"""GPU sampling for a `@remote_step` (gap 14, data half).

`@gpu_profile` samples on the driver, which has no GPU, so a remote step was
measured as an idle machine. The GPU is in the runner pod, so the sampling
moves there. Only the data half: the decorator renders through
`current.card["gpu_profile"]`, and a card written in the pod does not reach the
driver's card (gap 6).
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest
from remote_step.plugins.remote_step_decorator import _find_gpu_profile
from remote_step.runner_entry import _GpuSampler


class Deco:
    def __init__(self, name, **attributes):
        self.name = name
        self.attributes = attributes


# ------------------------------------------------------- driver-side detection


def test_a_step_without_gpu_profile():
    assert _find_gpu_profile([Deco("resources", gpu=1)]) is None
    assert _find_gpu_profile([]) is None


def test_gpu_profile_is_detected_with_its_default_interval():
    assert _find_gpu_profile([Deco("gpu_profile")]) == {"interval": 1}


def test_gpu_profile_is_detected_through_the_card_its_mutator_injects():
    """The realistic case. @gpu_profile is a StepMutator: by step_init it has
    rewritten itself into a card plus a user_step_decorator, and nothing in the
    list is named "gpu_profile" any more.
    """
    decos = [Deco("card", type="blank", id="gpu_profile", refresh_interval=5)]
    assert _find_gpu_profile(decos) == {"interval": 1}


def test_an_unrelated_card_is_not_mistaken_for_gpu_profile():
    assert _find_gpu_profile([Deco("card", type="html", id="dqv_report")]) is None
    assert _find_gpu_profile([Deco("card", type="blank")]) is None


def test_an_explicit_interval_is_carried():
    assert _find_gpu_profile([Deco("gpu_profile", interval=5)]) == {"interval": 5}


def test_a_none_interval_falls_back_to_one():
    """`@gpu_profile()` with no argument leaves the attribute unset."""
    assert _find_gpu_profile([Deco("gpu_profile", interval=None)]) == {"interval": 1}


# ----------------------------------------------------------- pod-side sampling


def test_a_sampler_that_never_started_returns_nothing():
    """finish() must be safe even when start() bailed — no GPU, no profiler."""
    assert _GpuSampler().finish() is None


def test_no_visible_devices_does_not_start_sampling(monkeypatch, capsys):
    """A GPU-less pod must log and carry on, not fail the step."""
    import metaflow_extensions.outerbounds.profilers.gpu as gpu_mod

    monkeypatch.setattr(gpu_mod.GPUProfiler, "read_gpu_info", staticmethod(lambda: {"devices": []}))
    sampler = _GpuSampler()
    sampler.start()

    assert sampler.finish() is None
    assert "no GPU devices visible" in capsys.readouterr().out


def test_a_profiler_that_cannot_be_imported_is_survivable(monkeypatch, capsys):
    """The runner image may not carry the Outerbounds profiler."""
    import builtins

    real_import = builtins.__import__

    def refuse(name, *args, **kwargs):
        if "profilers.gpu" in name:
            raise ImportError("no profiler here")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", refuse)
    sampler = _GpuSampler()
    sampler.start()
    monkeypatch.setattr(builtins, "__import__", real_import)

    assert sampler.finish() is None
    assert "profiler unavailable" in capsys.readouterr().out


def test_readings_are_returned_with_the_device_info(monkeypatch):
    import metaflow_extensions.outerbounds.profilers.gpu as gpu_mod

    info = {"devices": [{"device_id": "0"}], "driver_version": "580", "cuda_version": "13.0"}
    readings = {"0": {"gpu_utilization": ["10", "90"], "memory_used": ["100", "2048"]}}

    class FakeMonitor:
        def __init__(self, interval=1):
            self.interval = interval

        def create_new_monitor(self):
            pass

        def read(self):
            return readings

        def cleanup(self):
            pass

    monkeypatch.setattr(gpu_mod.GPUProfiler, "read_gpu_info", staticmethod(lambda: info))
    monkeypatch.setattr(gpu_mod, "GPUMonitor", FakeMonitor)

    sampler = _GpuSampler(interval=2)
    sampler.start()
    out = sampler.finish()

    assert out == {"info": info, "readings": readings}


def test_peak_utilisation_is_logged(capsys):
    """The log alone should answer whether the GPU was actually used."""
    _GpuSampler._log_summary({"gpu-0": {"gpu_utilization": ["10", "97", "40"], "memory_used": ["100", "8192"]}})
    out = capsys.readouterr().out
    assert "peak 97% util" in out
    assert "peak 8192 MB memory" in out
    assert "3 samples" in out


@pytest.mark.parametrize(
    "series",
    [
        {},
        {"gpu_utilization": [], "memory_used": []},
        {"gpu_utilization": ["not-a-number"]},
    ],
)
def test_summary_survives_unusable_readings(capsys, series):
    """nvidia-smi emits 'N/A' for a device that does not report utilisation."""
    _GpuSampler._log_summary({"gpu-0": series})


def test_the_artifact_name_cannot_be_clobbered_by_the_driver():
    """@gpu_profile is a StepMutator, so its wrapper still runs on the driver
    and writes `gpu_profile_data` at task_finished — after our outputs are
    applied. A distinct name is the only way ours survives.
    """
    assert _GpuSampler.ARTIFACT_NAME == "remote_gpu_profile"


def test_the_decorator_is_dropped_from_the_driver():
    """The driver has no GPU, and its empty reading overwrites the pod's.

    @gpu_profile writes `gpu_profile_data` at task_finished, which runs after
    the runner's outputs are applied — so leaving it on the driver replaced a
    real sampling with `nvidia-smi not found`.
    """
    from remote_step.plugins.remote_step_decorator import _drop_gpu_profile

    decorators = [Deco("gpu_profile", interval=1), Deco("resources", gpu=1)]
    removed = _drop_gpu_profile(decorators)

    assert [d.name for d in decorators] == ["resources"]
    assert removed[0]["interval"] == 1


def test_dropping_when_there_is_no_gpu_profile_changes_nothing():
    from remote_step.plugins.remote_step_decorator import _drop_gpu_profile

    decorators = [Deco("resources", gpu=1)]
    assert _drop_gpu_profile(decorators) == []
    assert len(decorators) == 1


def test_a_classic_decorator_still_carries_its_interval():
    """The hand-written shape is the only one that has `interval`."""
    assert _find_gpu_profile([Deco("gpu_profile", interval=5)]) == {"interval": 5}


def test_the_mutator_shape_samples_at_one_second():
    """`interval=` is given to the wrapper, not the card, so it cannot be
    recovered from the card. 1 s is the finest setting, so no sample is lost.
    """
    decos = [Deco("card", type="blank", id="gpu_profile", refresh_interval=5)]
    assert _find_gpu_profile(decos) == {"interval": 1}


def test_the_reader_is_pumped_before_reading(monkeypatch):
    """create_new_monitor only spawns `nvidia-smi -l`, which appends to a CSV.

    Nothing parses that file until _update_readings() runs, so without the
    pump read() returns {} and no artifact is ever produced — which is exactly
    how the first live GPU run linked only one artifact.
    """
    import metaflow_extensions.outerbounds.profilers.gpu as gpu_mod

    calls = []

    class FakeMonitor:
        def __init__(self, interval=1):
            pass

        def create_new_monitor(self):
            pass

        def _update_readings(self):
            calls.append("update")

        def read(self):
            calls.append("read")
            return {"0": {"gpu_utilization": ["50"], "memory_used": ["100"]}}

        def cleanup(self):
            pass

    monkeypatch.setattr(
        gpu_mod.GPUProfiler,
        "read_gpu_info",
        staticmethod(lambda: {"devices": [{"device_id": "0"}]}),
    )
    monkeypatch.setattr(gpu_mod, "GPUMonitor", FakeMonitor)

    sampler = _GpuSampler()
    sampler.start()
    out = sampler.finish()

    assert calls == ["update", "read"], calls
    assert out["readings"]


def test_the_card_summary_is_recorded_before_the_cards_are_saved(monkeypatch):
    """finish() appends to the gpu_profile card, so it must run before the save.

    Saving first left that summary unrecorded, and the card then showed only
    what the driver's own wrapper had written — all unknowns.
    """
    import metaflow_extensions.outerbounds.profilers.gpu as gpu_mod

    from remote_step.runner_entry import _CardRecorder

    class FakeMonitor:
        def __init__(self, interval=1):
            pass

        def create_new_monitor(self):
            pass

        def _update_readings(self):
            pass

        def read(self):
            return {"0": {"gpu_utilization": ["42"], "memory_used": ["512"]}}

        def cleanup(self):
            pass

    monkeypatch.setattr(
        gpu_mod.GPUProfiler,
        "read_gpu_info",
        staticmethod(lambda: {"devices": [{"device_id": "0"}], "driver_version": "580"}),
    )
    monkeypatch.setattr(gpu_mod, "GPUMonitor", FakeMonitor)

    recorder = _CardRecorder()
    monkeypatch.setattr("metaflow.current.card", recorder, raising=False)

    sampler = _GpuSampler()
    sampler.start()
    sampler.finish()

    # finish() must already have populated the recorder — a save at this point
    # would carry the summary.
    assert _GpuSampler.CARD_ID in recorder.pending()


class TestLongBodyKeepsItsReadings:
    """A body longer than the monitor's nominal duration must keep its samples.

    `create_new_monitor()` spawns `nvidia-smi -l` and nothing pumps the reader
    while the body runs, so `_current_readings` is still empty when the step
    ends. Past that duration (300s by default) `_update_readings()` folds that
    empty dict into `_past_readings`, discards the CSV holding every real
    sample, spawns a fresh nvidia-smi and reads *that* -- so every training
    step long enough to be worth profiling reported one sample and the card
    said "peak 0% util".
    """

    SAMPLES = [
        "00000000:00:1E.0, 2026/09/10 12:00:0%d.000, %d, %d, 23028" % (i, 40 + i, 1000 * i)
        for i in range(6)
    ]

    def monitor_with_csv(self, tmp_path, monkeypatch, ended):
        """A real GPUMonitor whose CSV is already populated."""
        from metaflow_extensions.outerbounds.profilers.gpu import GPUMonitor

        m = GPUMonitor.__new__(GPUMonitor)
        m._interval = 1
        m._duration = 300
        m._finished = False
        m._max_samples = None
        # Class-level mutables on GPUMonitor are shared between instances, so
        # reset them per test rather than inheriting another test's data.
        m._current_readings = {}
        m._past_readings = {}

        csv = tmp_path / "gpu.csv"
        csv.write_text("\n".join(self.SAMPLES) + "\n")
        monkeypatch.setattr(type(m), "_current_file", property(lambda _s: str(csv)))
        monkeypatch.setattr(m, "current_process_has_ended", lambda: ended)
        monkeypatch.setattr(m, "current_process_is_running", lambda: not ended)

        # If the reset path is taken it must not really spawn nvidia-smi; make
        # the replacement CSV hold a single sample, as it would in practice.
        def fake_new_monitor():
            fresh = tmp_path / "fresh.csv"
            fresh.write_text(self.SAMPLES[0] + "\n")
            monkeypatch.setattr(type(m), "_current_file", property(lambda _s: str(fresh)))

        monkeypatch.setattr(m, "clear_current_monitor", lambda: None)
        monkeypatch.setattr(m, "create_new_monitor", fake_new_monitor)
        monkeypatch.setattr(m, "cleanup", lambda: None)
        return m

    def sampler_for(self, monitor):
        from remote_step.runner_entry import _GpuSampler

        s = _GpuSampler(interval=1)
        s._monitor = monitor
        s.info = {"driver_version": "580.65", "cuda_version": "12.8", "devices": [{"name": "L40S"}]}
        return s

    def count(self, result):
        readings = result["readings"]
        gpu = next(iter(readings.values()))
        return len(gpu["gpu_utilization"])

    def test_a_long_body_keeps_every_sample(self, tmp_path, monkeypatch):
        """The regression: 6 samples must not collapse to 1."""
        m = self.monitor_with_csv(tmp_path, monkeypatch, ended=True)
        result = self.sampler_for(m).finish()
        assert result is not None
        assert self.count(result) == len(self.SAMPLES)

    def test_a_short_body_still_works(self, tmp_path, monkeypatch):
        """The case that always worked, kept working."""
        m = self.monitor_with_csv(tmp_path, monkeypatch, ended=False)
        result = self.sampler_for(m).finish()
        assert self.count(result) == len(self.SAMPLES)

    def test_the_peak_reflects_the_whole_body(self, tmp_path, monkeypatch):
        """Utilisation rises through the samples; the summary must see the top."""
        m = self.monitor_with_csv(tmp_path, monkeypatch, ended=True)
        result = self.sampler_for(m).finish()
        gpu = next(iter(result["readings"].values()))
        assert max(int(x) for x in gpu["gpu_utilization"]) == 45

    def test_an_empty_csv_falls_back_rather_than_returning_nothing(self, tmp_path, monkeypatch):
        """A body shorter than one sampling interval."""
        from metaflow_extensions.outerbounds.profilers.gpu import GPUMonitor

        m = GPUMonitor.__new__(GPUMonitor)
        m._interval, m._duration, m._finished, m._max_samples = 1, 300, False, None
        m._current_readings, m._past_readings = {}, {}
        empty = tmp_path / "empty.csv"
        empty.write_text("")
        monkeypatch.setattr(type(m), "_current_file", property(lambda _s: str(empty)))
        monkeypatch.setattr(m, "_update_readings", lambda: None)
        monkeypatch.setattr(m, "read", lambda: {})
        monkeypatch.setattr(m, "cleanup", lambda: None)
        assert self.sampler_for(m).finish() is None
