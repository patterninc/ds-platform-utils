"""The one line reporting what a step used against what it asked for.

This is the only place the two meet. Outerbounds shows the *driver's* panel —
2 vCPU — so a step asking for 20 and using 3 looks perfectly sized there.

CPU is a *difference* between two readings. The cgroup counter covers the
container's whole lifetime, which includes the entrypoint's `uv pip install`;
dividing that total by the body's wall time once reported "507549.9 of 2 vCPU
(25377494%)" for a body that ran in milliseconds.
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


@pytest.fixture
def many_cores(monkeypatch):
    """A node big enough that the plausibility guard is not what is under test."""
    monkeypatch.setattr(re_mod.os, "cpu_count", lambda: 64)


def test_nothing_is_logged_when_the_host_exposes_nothing(cgroup, capsys):
    """A host without these files must not fail a step that already succeeded."""
    re_mod._log_resource_usage({"step_name": "train", "requested": {"cpu": 4}}, 10.0, 0.0)
    assert capsys.readouterr().out == ""


def test_cgroup_v2_usage_against_the_request(cgroup, capsys, many_cores):
    cgroup("cpu.stat", "usage_usec 20000000\nuser_usec 1\n")  # 20 CPU-seconds
    cgroup("memory.peak", str(2 * 1024**3))  # 2 GiB

    re_mod._log_resource_usage(
        {"step_name": "train", "requested": {"cpu": 4, "memory_mb": 8192}},
        wall_seconds=10.0,
        cpu_seconds_at_start=0.0,
    )
    out = capsys.readouterr().out
    # 20 CPU-seconds over 10s wall = 2.0 vCPU of the 4 asked for
    assert "2.0 of 4 vCPU (50%)" in out
    assert "peak memory 2.0 of 8.0 GB (25%)" in out
    assert "train used" in out


def test_only_the_cpu_burned_by_the_body_is_counted(cgroup, capsys, many_cores):
    """The bug that reported 25377494%.

    The counter already holds the install phase. Here 500 CPU-seconds were
    spent before the body and 20 inside it, over 10s of wall — 2 vCPU, not 52.
    """
    cgroup("cpu.stat", "usage_usec 520000000\n")
    cgroup("memory.peak", str(1024**3))

    re_mod._log_resource_usage(
        {"step_name": "work", "requested": {"cpu": 2, "memory_mb": 8192}},
        wall_seconds=10.0,
        cpu_seconds_at_start=500.0,
    )
    out = capsys.readouterr().out
    assert "2.0 of 2 vCPU (100%)" in out


def test_a_short_body_after_a_long_install_is_not_absurd(cgroup, capsys, many_cores):
    """The exact shape of the reported bug: milliseconds of body, minutes of install."""
    cgroup("cpu.stat", "usage_usec 253774940000\n")  # ~253k CPU-seconds accumulated
    cgroup("memory.peak", str(int(0.9 * 1024**3)))

    re_mod._log_resource_usage(
        {"step_name": "work", "requested": {"cpu": 2, "memory_mb": 8192}},
        wall_seconds=0.01,
        cpu_seconds_at_start=253774.94,
    )
    out = capsys.readouterr().out
    assert "0.0 of 2 vCPU (0%)" in out
    assert "25377494%" not in out


def test_cpu_is_withheld_when_there_is_no_baseline(cgroup, capsys, many_cores):
    """Without a baseline the reading still holds the install phase.

    No number is better than one known to be inflated.
    """
    cgroup("cpu.stat", "usage_usec 520000000\n")
    cgroup("memory.peak", str(1024**3))

    re_mod._log_resource_usage({"step_name": "s", "requested": {"cpu": 2, "memory_mb": 1024}}, 10.0)
    out = capsys.readouterr().out
    assert "vCPU" not in out
    assert "peak memory 1.0 of 1.0 GB" in out


def test_an_impossible_figure_is_suppressed(cgroup, capsys, monkeypatch):
    """More cores than the node has means the counter was not container-scoped."""
    monkeypatch.setattr(re_mod.os, "cpu_count", lambda: 2)
    cgroup("cpu.stat", "usage_usec 100000000\n")  # 100 CPU-seconds
    cgroup("memory.peak", str(1024**3))

    re_mod._log_resource_usage(
        {"step_name": "s", "requested": {"cpu": 2, "memory_mb": 1024}},
        wall_seconds=1.0,  # would be 100 vCPU on a 2-core node
        cpu_seconds_at_start=0.0,
    )
    out = capsys.readouterr().out
    assert "vCPU" not in out
    assert "peak memory" in out  # the rest of the line still lands


def test_using_every_core_of_the_node_is_still_reported(cgroup, capsys, monkeypatch):
    """The guard must not swallow a legitimately saturated step."""
    monkeypatch.setattr(re_mod.os, "cpu_count", lambda: 8)
    cgroup("cpu.stat", "usage_usec 8000000\n")  # 8 CPU-seconds over 1s = 8 vCPU

    re_mod._log_resource_usage({"step_name": "s", "requested": {"cpu": 8}}, 1.0, 0.0)
    assert "8.0 of 8 vCPU (100%)" in capsys.readouterr().out


def test_a_counter_that_goes_backwards_is_clamped(cgroup, capsys, many_cores):
    """Should not happen, but a negative vCPU figure would be nonsense."""
    cgroup("cpu.stat", "usage_usec 1000000\n")
    cgroup("memory.peak", str(1024**3))

    re_mod._log_resource_usage({"step_name": "s", "requested": {"cpu": 2, "memory_mb": 1024}}, 1.0, 500.0)
    out = capsys.readouterr().out
    assert "0.0 of 2 vCPU (0%)" in out
    assert "-" not in out.split("used")[1]


def test_cgroup_v1_is_read_too(cgroup, capsys, many_cores):
    """An older host exposes cpuacct/memory under v1 names."""
    cgroup("cpuacct/cpuacct.usage", str(5 * 10**9))  # 5 CPU-seconds
    cgroup("memory/memory.max_usage_in_bytes", str(1024**3))

    re_mod._log_resource_usage({"step_name": "s", "requested": {"cpu": 1, "memory_mb": 1024}}, 5.0, 0.0)
    out = capsys.readouterr().out
    assert "1.0 of 1 vCPU" in out
    assert "peak memory 1.0 of 1.0 GB" in out


def test_final_usage_is_labelled_when_no_peak_counter_exists(cgroup, capsys):
    """`memory.current` is a floor, not a peak — say which one it is."""
    cgroup("memory.current", str(1024**3))
    re_mod._log_resource_usage({"step_name": "s", "requested": {"memory_mb": 2048}}, 1.0, 0.0)
    out = capsys.readouterr().out
    assert "final memory 1.0 of 2.0 GB" in out
    assert "peak memory" not in out


def test_usage_is_reported_without_a_request(cgroup, capsys, many_cores):
    """No percentage to give, but the absolute numbers are still useful."""
    cgroup("cpu.stat", "usage_usec 1000000\n")
    cgroup("memory.peak", str(1024**3))

    re_mod._log_resource_usage({"step_name": "s", "requested": {}}, 1.0, 0.0)
    out = capsys.readouterr().out
    assert "1.0 vCPU" in out
    assert "peak memory 1.0 GB" in out
    assert "%" not in out


def test_a_zero_wall_time_does_not_divide_by_zero(cgroup, capsys):
    cgroup("cpu.stat", "usage_usec 1000000\n")
    cgroup("memory.peak", str(1024**3))

    re_mod._log_resource_usage({"step_name": "s", "requested": {"cpu": 2}}, 0.0, 0.0)
    out = capsys.readouterr().out
    assert "vCPU" not in out  # cannot compute a rate
    assert "peak memory" in out


def test_a_malformed_cgroup_file_is_ignored(cgroup, capsys):
    cgroup("memory.peak", "not-a-number")
    cgroup("cpu.stat", "garbage\n")
    re_mod._log_resource_usage({"step_name": "s", "requested": {"cpu": 1}}, 1.0, 0.0)
    assert capsys.readouterr().out == ""


def test_an_oversized_ask_is_visible(cgroup, capsys, many_cores):
    """The case worth catching: 20 vCPU asked, 3 used."""
    cgroup("cpu.stat", "usage_usec 300000000\n")  # 300 CPU-seconds
    cgroup("memory.peak", str(4 * 1024**3))

    re_mod._log_resource_usage(
        {"step_name": "do_forecast", "requested": {"cpu": 20, "memory_mb": 42000}},
        wall_seconds=100.0,
        cpu_seconds_at_start=0.0,
    )
    out = capsys.readouterr().out
    assert "3.0 of 20 vCPU (15%)" in out
    assert "(10%)" in out  # 4 GB of ~41 GB
