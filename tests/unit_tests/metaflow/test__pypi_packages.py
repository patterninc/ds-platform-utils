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


def test_uv_lock_leaves_multi_version_dep_unpinned(project_root: Path):
    packages = _get_packages_from_uv_lock(project_root=project_root)
    # locked at 1.36.1 and 1.30.0 behind different resolution markers, so pinning either
    # would break a bake on the other python version
    assert packages["polars"] == ""


def test_uv_lock_excludes_groups_unless_asked(project_root: Path):
    assert "pytest" not in _get_packages_from_uv_lock(project_root=project_root)
    assert _get_packages_from_uv_lock(groups="dev", project_root=project_root)["pytest"] == "8.4.1"


def test_uv_lock_rejects_unrecorded_group(project_root: Path):
    with pytest.raises(ValueError, match="is not recorded in"):
        _get_packages_from_uv_lock(groups=["nope"], project_root=project_root)


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
    assert _get_pypi_kwargs(groups=["dev"], project_root=project_root)["packages"]["pytest"] == "8.4.1"


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


def _pypi_attributes(flow):
    """Pull the attributes Metaflow recorded for whichever pypi decorator was applied."""
    return list(flow._flow_decorators.values())[0][0].attributes


def test_uv_pypi_base_applies_derived_environment(project_root: Path):
    (project_root / ".python-version").write_text("3.11\n")
    attributes = _pypi_attributes(uv_pypi_base(project_root=project_root)(_build_flow()))
    assert attributes["python"] == "3.11"
    assert attributes["packages"] == _get_packages_from_uv_lock(project_root=project_root)


def test_uv_pypi_base_works_bare(project_root: Path, monkeypatch: pytest.MonkeyPatch):
    # the bare form has nowhere to pass project_root, so it walks up from the launch directory
    monkeypatch.chdir(project_root)
    assert _pypi_attributes(uv_pypi_base(_build_flow()))["packages"]["pandas"] == "2.3.2"


def test_uv_pypi_base_registers_as_pypi_base(project_root: Path):
    decorated = uv_pypi_base(project_root=project_root)(_build_flow())
    assert "pypi_base" in decorated._flow_decorators


def test_uv_pypi_base_passes_groups_and_python_through(project_root: Path):
    attributes = _pypi_attributes(
        uv_pypi_base(groups=["dev"], python="3.12", project_root=project_root)(_build_flow())
    )
    assert attributes["python"] == "3.12"
    assert attributes["packages"]["pytest"] == "8.4.1"


def test_uv_pypi_base_forwards_disabled(project_root: Path):
    attributes = _pypi_attributes(uv_pypi_base(disabled=True, project_root=project_root)(_build_flow()))
    assert attributes["disabled"] is True


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
