"""The one line reporting what a step used against what it asked for.

This is the only place the two meet. Outerbounds shows the *driver's* panel —
2 vCPU — so a step asking for 20 and using 3 looks perfectly sized there.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
import pytest

from remote_step import runner_entry as re_mod


@pytest.fixture
def cgroup(tmp_path, monkeypatch):
    """Point the reader at a fake cgroup tree."""
    monkeypatch.setattr(re_mod, "CGROUP_ROOT", str(tmp_path))

    def write(name, content):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    return write


def test_nothing_is_logged_when_the_host_exposes_nothing(cgroup, capsys):
    """A host without these files must not fail a step that already succeeded."""
    re_mod._log_resource_usage({"step_name": "train", "requested": {"cpu": 4}}, 10.0)
    assert capsys.readouterr().out == ""


def test_cgroup_v2_usage_against_the_request(cgroup, capsys):
    cgroup("cpu.stat", "usage_usec 20000000\nuser_usec 1\n")  # 20 CPU-seconds
    cgroup("memory.peak", str(2 * 1024**3))  # 2 GiB

    re_mod._log_resource_usage(
        {"step_name": "train", "requested": {"cpu": 4, "memory_mb": 8192}},
        wall_seconds=10.0,
    )
    out = capsys.readouterr().out
    # 20 CPU-seconds over 10s wall = 2.0 vCPU of the 4 asked for
    assert "2.0 of 4 vCPU (50%)" in out
    assert "peak memory 2.0 of 8.0 GB (25%)" in out
    assert "train used" in out


def test_cgroup_v1_is_read_too(cgroup, capsys):
    """An older host exposes cpuacct/memory under v1 names."""
    cgroup("cpuacct/cpuacct.usage", str(5 * 10**9))  # 5 CPU-seconds
    cgroup("memory/memory.max_usage_in_bytes", str(1024**3))

    re_mod._log_resource_usage({"step_name": "s", "requested": {"cpu": 1, "memory_mb": 1024}}, 5.0)
    out = capsys.readouterr().out
    assert "1.0 of 1 vCPU" in out
    assert "peak memory 1.0 of 1.0 GB" in out


def test_final_usage_is_labelled_when_no_peak_counter_exists(cgroup, capsys):
    """`memory.current` is a floor, not a peak — say which one it is."""
    cgroup("memory.current", str(1024**3))
    re_mod._log_resource_usage({"step_name": "s", "requested": {"memory_mb": 2048}}, 1.0)
    out = capsys.readouterr().out
    assert "final memory 1.0 of 2.0 GB" in out
    assert "peak memory" not in out


def test_usage_is_reported_without_a_request(cgroup, capsys):
    """No percentage to give, but the absolute numbers are still useful."""
    cgroup("cpu.stat", "usage_usec 1000000\n")
    cgroup("memory.peak", str(1024**3))

    re_mod._log_resource_usage({"step_name": "s", "requested": {}}, 1.0)
    out = capsys.readouterr().out
    assert "1.0 vCPU" in out
    assert "peak memory 1.0 GB" in out
    assert "%" not in out


def test_a_zero_wall_time_does_not_divide_by_zero(cgroup, capsys):
    cgroup("cpu.stat", "usage_usec 1000000\n")
    cgroup("memory.peak", str(1024**3))

    re_mod._log_resource_usage({"step_name": "s", "requested": {"cpu": 2}}, 0.0)
    out = capsys.readouterr().out
    assert "vCPU" not in out  # cannot compute a rate
    assert "peak memory" in out


def test_a_malformed_cgroup_file_is_ignored(cgroup, capsys):
    cgroup("memory.peak", "not-a-number")
    cgroup("cpu.stat", "garbage\n")
    re_mod._log_resource_usage({"step_name": "s", "requested": {"cpu": 1}}, 1.0)
    assert capsys.readouterr().out == ""


def test_an_oversized_ask_is_visible(cgroup, capsys):
    """The case worth catching: 20 vCPU asked, 3 used."""
    cgroup("cpu.stat", "usage_usec 300000000\n")  # 300 CPU-seconds
    cgroup("memory.peak", str(4 * 1024**3))

    re_mod._log_resource_usage(
        {"step_name": "do_forecast", "requested": {"cpu": 20, "memory_mb": 42000}},
        wall_seconds=100.0,
    )
    out = capsys.readouterr().out
    assert "3.0 of 20 vCPU (15%)" in out
    assert "(10%)" in out  # 4 GB of ~41 GB
