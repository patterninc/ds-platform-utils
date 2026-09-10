"""How a spec's `env.packages` becomes pip requirement lines.

`@pypi`, `@pypi_base`, `@uv_pypi` and `@uv_pypi_base` all end up in the same
place -- the `env.packages` map in the runner's spec -- but they spell a
dependency differently, and a direct reference in particular arrives in three
shapes depending on which decorator wrote it and how. Every case here has been
seen in a real flow.
"""

import json

import pytest

from metaflow_extensions.remote_step.requirements import (
    build_requirements,
    main,
    requirement_line,
)

# A resolved commit, as @uv_pypi_base reads it out of uv.lock.
SHA = "1578f9d48808d6cedd5fdf101eed05361e45108d"
REPO = "https://github.com/patterninc/ds-platform-utils.git"


@pytest.mark.parametrize(
    ("name", "version", "expected"),
    [
        # --- plain versions, the @pypi / @pypi_base common case ---
        ("pandas", "", "pandas"),
        ("pandas", None, "pandas"),
        ("numpy", "2.2.6", "numpy==2.2.6"),
        # --- specifiers, which must not be pinned again with '==' ---
        ("pyarrow", "<19.0.0", "pyarrow<19.0.0"),
        ("pyarrow", ">=1.0,<2", "pyarrow>=1.0,<2"),
        ("requests", ">=2.21.0", "requests>=2.21.0"),
        ("boto3", "==1.14.0", "boto3==1.14.0"),
        ("torch", "~=2.1", "torch~=2.1"),
        ("urllib3", "!=2.0.0", "urllib3!=2.0.0"),
        # --- direct reference, '@' already on the value.
        # This is what @uv_pypi_base / @uv_pypi derive from uv.lock.
        ("ds-platform-utils", f"@ git+{REPO}@{SHA}", f"ds-platform-utils @ git+{REPO}@{SHA}"),
        # --- direct reference, URL as the value and no '@' ---
        ("ds-platform-utils", f"git+{REPO}", f"ds-platform-utils @ git+{REPO}"),
        ("wheelpkg", "https://host/pkg-1.0-py3-none-any.whl", "wheelpkg @ https://host/pkg-1.0-py3-none-any.whl"),
        ("localpkg", "file:///opt/pkg", "localpkg @ file:///opt/pkg"),
        # --- direct reference, URL as the *key*.
        # Hand-written @pypi_base style; the value is a git ref belonging to
        # the URL, so it is appended with no separator.
        (f"git+{REPO}", "@main", f"git+{REPO}@main"),
        (f"git+{REPO}", f"@{SHA}", f"git+{REPO}@{SHA}"),
        (f"git+{REPO}", "", f"git+{REPO}"),
        # --- surrounding whitespace is not significant ---
        ("  pandas  ", "  2.0.0  ", "pandas==2.0.0"),
    ],
)
def test_renders_one_package_entry(name, version, expected):
    assert requirement_line(name, version) == expected


def test_url_key_beats_at_prefixed_value():
    """Order matters: a URL key with an '@' value must not gain a space.

    Both branches match `{"git+https://...": "@main"}`. Taking the '@' branch
    yields "git+https://... @main", which pip reads as two requirements.
    """
    line = requirement_line(f"git+{REPO}", "@main")
    assert " " not in line
    assert line == f"git+{REPO}@main"


def test_specifier_is_not_double_pinned():
    """`{"pyarrow": "<19.0.0"}` must not become `pyarrow==<19.0.0`."""
    assert "==<" not in requirement_line("pyarrow", "<19.0.0")


def test_boto3_is_always_present():
    """The runner needs its own S3 access even when the flow declares nothing."""
    assert build_requirements({}) == ["boto3"]
    assert build_requirements(None) == ["boto3"]


def test_declared_packages_follow_boto3():
    lines = build_requirements({"pandas": "", "numpy": "2.2.6"})
    assert lines == ["boto3", "pandas", "numpy==2.2.6"]


def test_renders_a_whole_uv_pypi_base_environment():
    """The mixed map a real flow produces, end to end."""
    lines = build_requirements(
        {
            "pandas": "",
            "pyarrow": "<19.0.0",
            "ds-platform-utils": f"@ git+{REPO}@{SHA}",
            f"git+{REPO}": "@main",
        }
    )
    assert lines == [
        "boto3",
        "pandas",
        "pyarrow<19.0.0",
        f"ds-platform-utils @ git+{REPO}@{SHA}",
        f"git+{REPO}@main",
    ]


def test_main_writes_the_file_the_entrypoint_installs_from(tmp_path):
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"env": {"python": "3.12", "packages": {"ds-platform-utils": f"@ git+{REPO}@{SHA}"}}}))
    out = tmp_path / "requirements.txt"

    assert main([str(spec), str(out)]) == 0
    assert out.read_text() == f"boto3\nds-platform-utils @ git+{REPO}@{SHA}\n"


@pytest.mark.parametrize("env", [{}, {"env": {}}, {"env": {"packages": {}}}, {"env": {"packages": None}}])
def test_main_tolerates_a_spec_with_no_packages(tmp_path, env):
    """A flow with no @pypi at all still needs a runnable venv."""
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps(env))
    out = tmp_path / "requirements.txt"

    assert main([str(spec), str(out)]) == 0
    assert out.read_text() == "boto3\n"


def test_main_reports_what_it_wrote(tmp_path, capsys):
    """The driver-side poller attributes install failures from this output."""
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"env": {"packages": {"pyarrow": "<19.0.0"}}}))

    main([str(spec), str(tmp_path / "requirements.txt")])

    assert "pyarrow<19.0.0" in capsys.readouterr().err
