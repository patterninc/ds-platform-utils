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


def test_the_artifact_keeps_the_name_gpu_profile_uses():
    """User code already reads `gpu_profile_data`, so keep the name."""
    assert _GpuSampler.ARTIFACT_NAME == "gpu_profile_data"


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
