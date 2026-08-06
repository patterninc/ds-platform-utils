import textwrap
from pathlib import Path

import pytest

from ds_platform_utils.metaflow import get_packages_from_pyproject, get_packages_from_uv_lock

PYPROJECT = textwrap.dedent("""
    [project]
    name = "my-flows"
    dependencies = [
        "pandas",
        "pydantic>=2",
        "sqlparse==0.5.3",
        "ds-platform-utils",
    ]

    [dependency-groups]
    dev = ["pytest>=8", {include-group = "lint"}]
    lint = ["ruff>=0.11"]

    [tool.uv.sources]
    ds-platform-utils = { git = "https://github.com/patterninc/ds-platform-utils.git", rev = "main" }
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


def test_pyproject_passes_declared_constraints_through(project_root: Path):
    packages = get_packages_from_pyproject(project_root=project_root)
    # an unconstrained dep is emitted unpinned, and an "==" pin is handed over bare because
    # metaflow prepends "==" itself
    assert packages["pandas"] == ""
    assert packages["pydantic"] == ">=2"
    assert packages["sqlparse"] == "0.5.3"


def test_pyproject_renders_uv_source_as_direct_reference(project_root: Path):
    packages = get_packages_from_pyproject(project_root=project_root)
    assert packages["ds-platform-utils"] == "@ git+https://github.com/patterninc/ds-platform-utils.git@main"


def test_pyproject_follows_include_group(project_root: Path):
    packages = get_packages_from_pyproject(groups=["dev"], project_root=project_root)
    assert packages["pytest"] == ">=8"
    # pulled in via {include-group = "lint"}
    assert packages["ruff"] == ">=0.11"


def test_pyproject_rejects_undeclared_group(project_root: Path):
    with pytest.raises(ValueError, match="is not declared in"):
        get_packages_from_pyproject(groups=["nope"], project_root=project_root)


def test_uv_lock_pins_resolved_versions(project_root: Path):
    packages = get_packages_from_uv_lock(project_root=project_root)
    assert packages["pandas"] == "2.3.2"


def test_uv_lock_pins_git_dep_to_resolved_commit(project_root: Path):
    packages = get_packages_from_uv_lock(project_root=project_root)
    # the SHA from the URL fragment, not the requested "main" -- that is what makes it repeatable
    assert packages["ds-platform-utils"] == (
        "@ git+https://github.com/patterninc/ds-platform-utils.git@06ead9f018928951"
    )


def test_uv_lock_leaves_multi_version_dep_unpinned(project_root: Path):
    packages = get_packages_from_uv_lock(project_root=project_root)
    # locked at 1.36.1 and 1.30.0 behind different resolution markers, so pinning either
    # would break a bake on the other python version
    assert packages["polars"] == ""


def test_uv_lock_excludes_groups_unless_asked(project_root: Path):
    assert "pytest" not in get_packages_from_uv_lock(project_root=project_root)
    assert get_packages_from_uv_lock(groups="dev", project_root=project_root)["pytest"] == "8.4.1"


def test_uv_lock_rejects_unrecorded_group(project_root: Path):
    with pytest.raises(ValueError, match="is not recorded in"):
        get_packages_from_uv_lock(groups=["nope"], project_root=project_root)


def test_returns_empty_map_when_files_are_missing(tmp_path: Path):
    # stands in for a remote task, whose code package holds only .py files
    assert get_packages_from_pyproject(project_root=tmp_path) == {}
    assert get_packages_from_uv_lock(project_root=tmp_path) == {}


def test_finds_project_files_by_walking_up_from_cwd(project_root: Path, monkeypatch: pytest.MonkeyPatch):
    nested = project_root / "flows" / "nested"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    assert get_packages_from_uv_lock()["pandas"] == "2.3.2"
    assert get_packages_from_pyproject()["pydantic"] == ">=2"
