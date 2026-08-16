import sys
import textwrap
from pathlib import Path

import pytest
from metaflow import FlowSpec, step

from ds_platform_utils.metaflow import uv_pypi, uv_pypi_base

# the decorators above are the public API; these back them and are tested through the module
from ds_platform_utils.metaflow.pypi_packages import _get_packages_from_uv_lock, _get_pypi_kwargs

# only read for `requires-python`; dependencies are taken from the lock, never from here
PYPROJECT = textwrap.dedent("""
    [project]
    name = "my-flows"
    dependencies = ["pandas", "polars", "ds-platform-utils"]
""")

UV_LOCK = textwrap.dedent("""
    version = 1

    [[package]]
    name = "my-flows"
    version = "0.1.0"
    source = { virtual = "." }
    dependencies = [
        { name = "pandas" },
        { name = "polars" },
        { name = "ds-platform-utils" },
        # a universal resolution: one entry per marker region, each naming its own version
        { name = "numpy", version = "1.26.4", marker = "python_full_version < '3.11'" },
        { name = "numpy", version = "2.3.0", marker = "python_full_version >= '3.11'" },
        { name = "pyobjc-core", marker = "sys_platform == 'darwin'" },
    ]

    [package.dev-dependencies]
    dev = [{ name = "pytest" }]

    [[package]]
    name = "pandas"
    version = "2.3.2"
    source = { registry = "https://pypi.org/simple" }

    [[package]]
    name = "polars"
    version = "1.36.1"
    source = { registry = "https://pypi.org/simple" }
    resolution-markers = ["python_full_version >= '3.12'"]

    [[package]]
    name = "polars"
    version = "1.30.0"
    source = { registry = "https://pypi.org/simple" }
    resolution-markers = ["python_full_version < '3.12'"]

    [[package]]
    name = "ds-platform-utils"
    version = "0.5.1"
    source = { git = "https://github.com/patterninc/ds-platform-utils.git?rev=main#06ead9f018928951" }

    [[package]]
    name = "pytest"
    version = "8.4.1"
    source = { registry = "https://pypi.org/simple" }

    [[package]]
    name = "numpy"
    version = "1.26.4"
    source = { registry = "https://pypi.org/simple" }
    resolution-markers = ["python_full_version < '3.11'"]

    [[package]]
    name = "numpy"
    version = "2.3.0"
    source = { registry = "https://pypi.org/simple" }
    resolution-markers = ["python_full_version >= '3.11'"]

    [[package]]
    name = "pyobjc-core"
    version = "10.3.1"
    source = { registry = "https://pypi.org/simple" }
""")


@pytest.fixture
def project_root(tmp_path: Path) -> Path:
    (tmp_path / "pyproject.toml").write_text(PYPROJECT)
    (tmp_path / "uv.lock").write_text(UV_LOCK)
    return tmp_path


def test_uv_lock_pins_resolved_versions(project_root: Path):
    packages = _get_packages_from_uv_lock(project_root=project_root)
    assert packages["pandas"] == "2.3.2"


def test_uv_lock_pins_git_dep_to_resolved_commit(project_root: Path):
    packages = _get_packages_from_uv_lock(project_root=project_root)
    # the SHA from the URL fragment, not the requested "main" -- that is what makes it repeatable
    assert packages["ds-platform-utils"] == (
        "@ git+https://github.com/patterninc/ds-platform-utils.git@06ead9f018928951"
    )


def test_uv_lock_resolves_split_dep_against_the_python_version(project_root: Path):
    # numpy is locked twice; the root entries carry the marker that decides which one applies
    assert _get_packages_from_uv_lock(project_root=project_root, python="3.10")["numpy"] == "1.26.4"
    assert _get_packages_from_uv_lock(project_root=project_root, python="3.11")["numpy"] == "2.3.0"
    # a full three-part version has to compare the same way a bare "3.11" does
    assert _get_packages_from_uv_lock(project_root=project_root, python="3.12.7")["numpy"] == "2.3.0"


def test_uv_lock_drops_dep_gated_to_another_platform(project_root: Path):
    # pyobjc-core is darwin-only, and @pypi has nowhere to put the marker, so a Linux bake
    # must not be told to install it
    assert "pyobjc-core" not in _get_packages_from_uv_lock(project_root=project_root, python="3.11")
    darwin = _get_packages_from_uv_lock(project_root=project_root, python="3.11", sys_platform="darwin")
    assert darwin["pyobjc-core"] == "10.3.1"


def test_uv_lock_leaves_indistinguishable_multi_version_dep_unpinned(project_root: Path):
    packages = _get_packages_from_uv_lock(project_root=project_root)
    # polars is locked at two versions but its root entry carries no marker or version, so
    # there is nothing to resolve against -- hand it to @pypi rather than guess
    assert packages["polars"] == ""


def test_uv_lock_excludes_groups_unless_asked(project_root: Path):
    assert "pytest" not in _get_packages_from_uv_lock(project_root=project_root)
    assert _get_packages_from_uv_lock(dependency_groups="dev", project_root=project_root)["pytest"] == "8.4.1"


def test_uv_lock_rejects_unrecorded_group(project_root: Path):
    with pytest.raises(ValueError, match="is not recorded in"):
        _get_packages_from_uv_lock(dependency_groups=["nope"], project_root=project_root)


def test_returns_empty_map_when_files_are_missing(tmp_path: Path):
    # stands in for a remote task, whose code package holds only .py files
    assert _get_packages_from_uv_lock(project_root=tmp_path) == {}


def test_pypi_base_kwargs_carries_python_and_packages(project_root: Path):
    (project_root / ".python-version").write_text("3.11\n")
    kwargs = _get_pypi_kwargs(project_root=project_root)
    assert kwargs == {"python": "3.11", "packages": _get_packages_from_uv_lock(project_root=project_root)}


def test_pypi_base_kwargs_prefers_python_version_pin(project_root: Path):
    # a pin wins over the ">=3.9" floor the lock was resolved against
    (project_root / ".python-version").write_text("# set by uv python pin\ncpython@3.12\n")
    assert _get_pypi_kwargs(project_root=project_root)["python"] == "3.12"


def test_pypi_base_kwargs_falls_back_to_requires_python_floor(project_root: Path):
    lock = (project_root / "uv.lock").read_text()
    (project_root / "uv.lock").write_text(lock.replace("version = 1", 'version = 1\nrequires-python = ">=3.11,<3.13"'))
    # no .python-version, so the floor of the declared range is the most concrete thing left
    assert _get_pypi_kwargs(project_root=project_root)["python"] == "3.11"


def test_pypi_base_kwargs_falls_back_to_pyproject_requires_python(project_root: Path):
    pyproject = (project_root / "pyproject.toml").read_text()
    (project_root / "pyproject.toml").write_text(
        pyproject.replace("[project]", '[project]\nrequires-python = ">=3.10"')
    )
    # the lock declares no range, so pyproject.toml is the next most concrete source
    assert _get_pypi_kwargs(project_root=project_root)["python"] == "3.10"


def test_pypi_base_kwargs_falls_back_to_running_interpreter(project_root: Path):
    running = f"{sys.version_info.major}.{sys.version_info.minor}"
    assert _get_pypi_kwargs(project_root=project_root)["python"] == running


def test_pypi_base_kwargs_honours_explicit_python(project_root: Path):
    (project_root / ".python-version").write_text("3.9\n")
    assert _get_pypi_kwargs(python="3.13", project_root=project_root)["python"] == "3.13"


def test_pypi_base_kwargs_passes_groups_through(project_root: Path):
    assert _get_pypi_kwargs(dependency_groups=["dev"], project_root=project_root)["packages"]["pytest"] == "8.4.1"


def _build_flow():
    """Return an undecorated FlowSpec, so a test can apply the decorator itself.

    The decorators take `project_root`, which only exists once a fixture has run -- too late
    for an `@uv_pypi_base` written above a module-level class.
    """

    class MyFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    return MyFlow


@pytest.fixture
def pypi_base_spy(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Capture the arguments handed to Metaflow's `@pypi_base` instead of applying it.

    The contract under test is "call Metaflow's decorator with this environment", so asserting
    on the call keeps these tests off Metaflow's internals -- where the decorator is recorded
    has already moved once between versions.
    """
    recorded: dict = {}

    def spy(**kwargs):
        recorded.update(kwargs)
        return lambda target: target

    monkeypatch.setattr("metaflow.pypi_base", spy)
    return recorded


def test_uv_pypi_base_applies_derived_environment(project_root: Path, pypi_base_spy: dict):
    (project_root / ".python-version").write_text("3.11\n")
    uv_pypi_base(project_root=project_root)(_build_flow())
    assert pypi_base_spy["python"] == "3.11"
    assert pypi_base_spy["packages"] == _get_packages_from_uv_lock(project_root=project_root, python="3.11")


def test_uv_pypi_base_works_bare(project_root: Path, pypi_base_spy: dict, monkeypatch: pytest.MonkeyPatch):
    # the bare form has nowhere to pass project_root, so it walks up from the launch directory
    monkeypatch.chdir(project_root)
    uv_pypi_base(_build_flow())
    assert pypi_base_spy["packages"]["pandas"] == "2.3.2"


def test_uv_pypi_base_passes_groups_and_python_through(project_root: Path, pypi_base_spy: dict):
    uv_pypi_base(dependency_groups=["dev"], python="3.12", project_root=project_root)(_build_flow())
    assert pypi_base_spy["python"] == "3.12"
    assert pypi_base_spy["packages"]["pytest"] == "8.4.1"


def test_uv_pypi_base_omits_disabled_unless_asked(project_root: Path, pypi_base_spy: dict):
    # leaving the key out is what lets a step-level @uv_pypi inherit the flow's setting
    uv_pypi_base(project_root=project_root)(_build_flow())
    assert "disabled" not in pypi_base_spy


def test_uv_pypi_base_forwards_disabled(project_root: Path, pypi_base_spy: dict):
    uv_pypi_base(disabled=True, project_root=project_root)(_build_flow())
    assert pypi_base_spy["disabled"] is True


def test_uv_pypi_base_prints_the_resolved_environment(
    project_root: Path, pypi_base_spy: dict, capsys: pytest.CaptureFixture
):
    (project_root / ".python-version").write_text("3.11\n")
    uv_pypi_base(project_root=project_root)(_build_flow())
    out = capsys.readouterr().out

    header, *rows = out.rstrip("\n").splitlines()
    assert header == "@uv_pypi_base on MyFlow: python 3.11, 4 package(s) from uv.lock"

    names = [row.split()[0] for row in rows]
    assert names == sorted(names), "listed by name so two runs compare by eye"
    # the darwin-gated dependency is resolved away, not reported
    assert "pyobjc-core" not in names

    versions = dict(row.split(maxsplit=1) for row in rows)
    assert versions["pandas"].strip() == "2.3.2"
    # a deliberate "let @pypi resolve it" has to read as such rather than as a blank column
    assert versions["polars"].strip() == "(unpinned)"
    assert versions["ds-platform-utils"].strip().startswith("@ git+https://")

    # every version starts at the same column, padded to the longest name
    assert len({len(row) - len(row.split(maxsplit=1)[1]) for row in rows}) == 1


def test_uv_pypi_stays_quiet_on_a_step(project_root: Path, capsys: pytest.CaptureFixture):
    # Outerbounds already prints a package list per image it bakes, so a step-scoped decorator
    # reporting as well is pure duplication
    def train(self):
        pass

    uv_pypi(project_root=project_root)(step(train))
    assert capsys.readouterr().out == ""


def test_uv_pypi_base_stays_quiet_inside_a_task_subprocess(
    project_root: Path, pypi_base_spy: dict, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
):
    # metaflow launches one `step` subprocess per task, each re-importing the flow module; only
    # the client invocation should report, or one summary becomes one block per step
    monkeypatch.setattr("sys.argv", ["my_flow.py", "--quiet", "step", "start", "--run-id", "1"])
    uv_pypi_base(project_root=project_root)(_build_flow())
    assert capsys.readouterr().out == ""


def test_uv_pypi_base_can_be_silenced(project_root: Path, pypi_base_spy: dict, capsys: pytest.CaptureFixture):
    uv_pypi_base(log=False, project_root=project_root)(_build_flow())
    assert capsys.readouterr().out == ""


def test_uv_pypi_can_be_asked_to_report(project_root: Path, capsys: pytest.CaptureFixture):
    def train(self):
        pass

    uv_pypi(log=True, project_root=project_root)(step(train))
    assert "@uv_pypi on train:" in capsys.readouterr().out


def test_log_true_does_not_reintroduce_per_task_output(
    project_root: Path, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
):
    # log= picks which decorators report; it does not override the per-task suppression, or
    # opting a step in would print once per task again. The env var is the escape hatch.
    monkeypatch.setattr("sys.argv", ["my_flow.py", "step", "train"])

    def train(self):
        pass

    uv_pypi(log=True, project_root=project_root)(step(train))
    assert capsys.readouterr().out == ""


def test_pypi_log_env_var_forces_and_silences(
    project_root: Path, pypi_base_spy: dict, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr("sys.argv", ["my_flow.py", "step", "start"])
    monkeypatch.setenv("DS_PLATFORM_UTILS_PYPI_LOG", "1")
    uv_pypi_base(project_root=project_root)(_build_flow())
    assert "@uv_pypi_base" in capsys.readouterr().out, "1 overrides the per-task suppression"

    monkeypatch.setattr("sys.argv", ["my_flow.py", "run"])
    monkeypatch.setenv("DS_PLATFORM_UTILS_PYPI_LOG", "0")
    uv_pypi_base(project_root=project_root)(_build_flow())
    assert capsys.readouterr().out == "", "0 silences it even on the client"


def test_uv_pypi_base_says_nothing_when_no_lock_is_found(tmp_path: Path, capsys: pytest.CaptureFixture):
    # a remote task re-imports the flow module inside an already-baked image, so there is no
    # lockfile and nothing worth reporting -- printing there would just be noise per task
    uv_pypi_base(project_root=tmp_path)(_build_flow())
    assert capsys.readouterr().out == ""


def test_uv_pypi_base_flags_a_disabled_environment(
    project_root: Path, pypi_base_spy: dict, capsys: pytest.CaptureFixture
):
    uv_pypi_base(disabled=True, project_root=project_root)(_build_flow())
    assert "[environment disabled]" in capsys.readouterr().out


def test_uv_pypi_base_registers_with_metaflow(project_root: Path):
    # the one test that exercises the real decorator end to end
    from metaflow.flowspec import FlowStateItems

    decorated = uv_pypi_base(project_root=project_root)(_build_flow())
    recorded = decorated._flow_state[FlowStateItems.FLOW_DECORATORS]
    assert "pypi_base" in recorded
    assert recorded["pypi_base"][0].attributes["packages"] == _get_packages_from_uv_lock(project_root=project_root)


def test_uv_pypi_base_rejects_non_flow():
    with pytest.raises(Exception, match="can be applied only to FlowSpecs"):
        uv_pypi_base(object)


def test_uv_pypi_decorates_a_step(project_root: Path):
    def train(self):
        pass

    decorated = uv_pypi(project_root=project_root)(step(train))
    attributes = decorated.decorators[0].attributes
    assert attributes["packages"] == _get_packages_from_uv_lock(project_root=project_root)
    assert decorated.decorators[0].name == "pypi"


def test_finds_project_files_by_walking_up_from_cwd(project_root: Path, monkeypatch: pytest.MonkeyPatch):
    nested = project_root / "flows" / "nested"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    assert _get_packages_from_uv_lock()["pandas"] == "2.3.2"
