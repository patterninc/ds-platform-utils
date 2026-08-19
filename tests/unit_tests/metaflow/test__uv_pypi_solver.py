import os
import tempfile
import textwrap

import pytest
from metaflow.plugins.pypi import pip as pip_module
from metaflow.plugins.pypi.pip import PipException
from metaflow.plugins.pypi.utils import pip_tags

# the module is reached through `enable_uv_pypi_solver`; its internals are tested directly
from ds_platform_utils.metaflow import enable_uv_pypi_solver, uv_pypi_solver
from ds_platform_utils.metaflow.uv_pypi_solver import (
    UVPip,
    _compile_args,
    _marker_environment,
    _packages_from_pylock,
    _requirement_lines,
    _run_uv,
    _uv_bin,
    _uv_python_platform,
)

PYTHON = "3.10.15"
PLATFORM = "linux-64"
TAGS = pip_tags(PYTHON, PLATFORM)
ENVIRONMENT = _marker_environment(PYTHON, PLATFORM)

#: uv's answer for a package that ships more than one wheel this environment could install.
PYLOCK_TWO_WHEELS = textwrap.dedent("""
    lock-version = "1.0"
    created-by = "uv"
    requires-python = ">=3.10"

    [[packages]]
    name = "charset-normalizer"
    version = "3.5.1"
    wheels = [
        { url = "https://files.pythonhosted.org/packages/9f/2f/charset_normalizer-3.5.1-cp37-abi3-manylinux1_x86_64.manylinux_2_28_x86_64.whl", hashes = { sha256 = "a6dac1" } },
        { url = "https://files.pythonhosted.org/packages/32/cd/charset_normalizer-3.5.1-cp310-cp310-manylinux2014_x86_64.manylinux_2_17_x86_64.whl", hashes = { sha256 = "96eefc" } },
    ]
""")


def test__requirement_lines_renders_every_version_form():
    packages = {
        "pandas": "2.2.2",
        "polars": ">=1.30",
        "pyarrow": "",
        "ds-platform-utils": "@ git+https://github.com/patterninc/ds-platform-utils.git@06ead9f",
    }
    assert _requirement_lines(packages) == [
        "pandas==2.2.2",
        "polars>=1.30",
        "pyarrow",
        "ds-platform-utils@ git+https://github.com/patterninc/ds-platform-utils.git@06ead9f",
    ]


@pytest.mark.parametrize(
    ("platform", "expected"),
    [
        ("osx-arm64", "aarch64-apple-darwin"),
        ("osx-64", "x86_64-apple-darwin"),
        # the newest glibc uv offers a target for that metaflow would also accept a wheel for
        ("linux-64", "x86_64-manylinux_2_38"),
        ("linux-aarch64", "aarch64-manylinux_2_38"),
    ],
)
def test__uv_python_platform_maps_the_platforms_metaflow_builds_for(platform, expected):
    assert _uv_python_platform(platform, PYTHON) == expected


def test__uv_python_platform_gives_up_on_a_platform_uv_cannot_target():
    # the caller reads None as "resolve this one with pip instead"
    assert _uv_python_platform("win-64", PYTHON) is None


def test__compile_args_pins_the_target_and_forwards_the_indices():
    args = _compile_args(
        ["pandas==2.2.2"],
        "x86_64-manylinux_2_38",
        PYTHON,
        ("https://index.example/simple", ["https://extra.example/simple"]),
    )
    assert args[args.index("--python-platform") + 1] == "x86_64-manylinux_2_38"
    assert args[args.index("--python-version") + 1] == PYTHON
    assert args[args.index("--index-url") + 1] == "https://index.example/simple"
    assert args[args.index("--extra-index-url") + 1] == "https://extra.example/simple"
    # @pypi installs wheels, so a package that only ships a source distribution is an error
    assert "--only-binary=:all:" in args


def test__compile_args_lets_a_direct_reference_be_built():
    args = _compile_args(
        ["requests@ git+https://github.com/psf/requests@v2.32.3"], "x86_64-manylinux_2_38", PYTHON, (None, [])
    )
    assert "--only-binary=:all:" not in args
    assert "--index-url" not in args


def test__packages_from_pylock_picks_the_wheel_the_environment_prefers():
    (package,) = _packages_from_pylock(PYLOCK_TWO_WHEELS, TAGS, ENVIRONMENT)
    # cp310-cp310 outranks the abi3 wheel built for an older interpreter
    assert package["url"].endswith(
        "charset_normalizer-3.5.1-cp310-cp310-manylinux2014_x86_64.manylinux_2_17_x86_64.whl"
    )
    assert package["require_build"] is False


def test__packages_from_pylock_reconstructs_a_git_reference():
    pylock = textwrap.dedent("""
        lock-version = "1.0"

        [[packages]]
        name = "requests"
        version = "2.32.3"
        vcs = { type = "git", url = "https://github.com/psf/requests", requested-revision = "v2.32.3", commit-id = "0e322af8" }
    """)
    assert _packages_from_pylock(pylock, TAGS, ENVIRONMENT) == [
        # pinned to the commit, not the branch, so the build is repeatable
        {"url": "git+https://github.com/psf/requests@0e322af8", "require_build": True, "hash": "0e322af8"}
    ]


def test__packages_from_pylock_marks_a_source_distribution_for_building():
    pylock = textwrap.dedent("""
        lock-version = "1.0"

        [[packages]]
        name = "some-package"
        version = "1.0.0"
        sdist = { url = "https://files.pythonhosted.org/packages/ab/cd/some_package-1.0.0.tar.gz", hashes = { sha256 = "deadbeef" } }
    """)
    assert _packages_from_pylock(pylock, TAGS, ENVIRONMENT) == [
        {
            "url": "https://files.pythonhosted.org/packages/ab/cd/some_package-1.0.0.tar.gz",
            "require_build": True,
            "hash": "deadbeef",
        }
    ]


def test__packages_from_pylock_drops_what_another_python_version_resolved_to():
    # uv answers for every version at or above the one it was asked about, so an entry can
    # belong to a Python this environment is not being built for
    pylock = textwrap.dedent("""
        lock-version = "1.0"

        [[packages]]
        name = "numpy"
        version = "1.26.4"
        marker = "python_full_version < '3.11'"
        wheels = [{ url = "https://files.pythonhosted.org/packages/aa/bb/numpy-1.26.4-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl" }]

        [[packages]]
        name = "numpy"
        version = "2.3.0"
        marker = "python_full_version >= '3.11'"
        wheels = [{ url = "https://files.pythonhosted.org/packages/cc/dd/numpy-2.3.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl" }]
    """)
    (package,) = _packages_from_pylock(pylock, TAGS, ENVIRONMENT)
    assert "numpy-1.26.4" in package["url"]


def test__packages_from_pylock_refuses_a_wheel_the_environment_cannot_install():
    pylock = textwrap.dedent("""
        lock-version = "1.0"

        [[packages]]
        name = "some-package"
        version = "1.0.0"
        wheels = [{ url = "https://files.pythonhosted.org/packages/ab/cd/some_package-1.0.0-cp310-cp310-win_amd64.whl" }]
    """)
    with pytest.raises(PipException, match="some-package"):
        _packages_from_pylock(pylock, TAGS, ENVIRONMENT)


def test__packages_from_pylock_refuses_a_dependency_a_remote_task_could_not_fetch():
    pylock = textwrap.dedent("""
        lock-version = "1.0"

        [[packages]]
        name = "my-flows"
        version = "0.1.0"
        directory = { path = "." }
    """)
    with pytest.raises(PipException, match="my-flows"):
        _packages_from_pylock(pylock, TAGS, ENVIRONMENT)


def test__enable_uv_pypi_solver_swaps_the_class_metaflow_reaches_for(monkeypatch):
    # monkeypatch restores metaflow's own Pip when the test ends
    monkeypatch.setattr(pip_module, "Pip", pip_module.Pip)
    monkeypatch.setattr(uv_pypi_solver, "_uv_bin", lambda: "/usr/bin/uv")

    assert enable_uv_pypi_solver() is True
    assert pip_module.Pip is UVPip
    # calling it again changes nothing
    assert enable_uv_pypi_solver() is True
    assert pip_module.Pip is UVPip


def test__enable_uv_pypi_solver_leaves_metaflow_alone_when_switched_off(monkeypatch):
    monkeypatch.setattr(pip_module, "Pip", pip_module.Pip)
    monkeypatch.setattr(uv_pypi_solver, "_uv_bin", lambda: "/usr/bin/uv")
    monkeypatch.setenv("DS_PLATFORM_UTILS_UV_PYPI", "0")

    assert enable_uv_pypi_solver() is False
    assert pip_module.Pip is not UVPip


def test__enable_uv_pypi_solver_leaves_metaflow_alone_without_uv(monkeypatch):
    monkeypatch.setattr(pip_module, "Pip", pip_module.Pip)
    monkeypatch.setattr(uv_pypi_solver, "_uv_bin", lambda: None)

    assert enable_uv_pypi_solver() is False
    assert pip_module.Pip is not UVPip


@pytest.mark.slow
def test__uv_resolves_a_real_environment():
    uv = _uv_bin()
    if uv is None:
        pytest.skip("uv is not installed")

    requirements = ["boto3==1.35.0"]
    args = _compile_args(requirements, "x86_64-manylinux_2_38", PYTHON, (None, []))
    with tempfile.TemporaryDirectory() as tmp_dir:
        pylock = os.path.join(tmp_dir, "pylock.toml")
        _run_uv(uv, [*args, "-o", pylock], stdin="\n".join(requirements))
        with open(pylock) as file:
            packages = _packages_from_pylock(file.read(), TAGS, ENVIRONMENT)

    assert {"botocore", "s3transfer"}.issubset({package["url"].split("/")[-1].split("-")[0] for package in packages})
    assert all(package["url"].endswith(".whl") for package in packages)
    assert not any(package["require_build"] for package in packages)
