"""How a spec's `env.packages` becomes pip requirement lines.

`@pypi`, `@pypi_base`, `@uv_pypi` and `@uv_pypi_base` all end up in the same
place -- the `env.packages` map in the runner's spec -- but they spell a
dependency differently, and a direct reference in particular arrives in three
shapes depending on which decorator wrote it and how. Every case here has been
seen in a real flow.
"""

import json

import pytest

from remote_step.requirements import (
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


# Every spelling below was taken verbatim from a @pypi_base / @pypi in the DS
# flow repos (pattern-nlp, demand-forecast, data-science-projects). They are
# the shapes that actually have to work.
PRODUCTION_SPELLINGS = {
    # exact pins, including a post-release
    "pandas": "2.1.4",
    "scikit-learn": "1.4.1.post1",
    "sentence-transformers": "5.1.2",
    # unpinned
    "torch": "",
    "logfire": "",
    # specifiers -- lower bounds are the common case, and one real upper bound
    "catboost": ">=1.2.8",
    "fastparquet": ">=2024.11.0",
    "numpy": ">=1.26.1",
    "pyarrow": "<19.0.0",
    # private git repos, ref written on the value
    "git+https://github.com/patterninc/ds-dqv-tool.git": "@v0.0.9",
    "git+https://github.com/patterninc/ds-platform-utils.git": "@main",
}

PRODUCTION_EXPECTED = [
    "pandas==2.1.4",
    "scikit-learn==1.4.1.post1",
    "sentence-transformers==5.1.2",
    "torch",
    "logfire",
    "catboost>=1.2.8",
    "fastparquet>=2024.11.0",
    "numpy>=1.26.1",
    "pyarrow<19.0.0",
    "git+https://github.com/patterninc/ds-dqv-tool.git@v0.0.9",
    "git+https://github.com/patterninc/ds-platform-utils.git@main",
]


def test_renders_the_production_flow_corpus():
    assert build_requirements(PRODUCTION_SPELLINGS) == PRODUCTION_EXPECTED


@pytest.mark.parametrize("version", ["", "@main", "@v1", "@v0.0.9"])
def test_a_private_repo_url_key_stays_one_record(version):
    """All four value forms seen on a git URL key across the flow repos."""
    line = requirement_line("git+https://github.com/patterninc/ds-dqv-tool.git", version)
    assert line == f"git+https://github.com/patterninc/ds-dqv-tool.git{version}"
    assert " " not in line


def test_no_packages_renders_nothing():
    """boto3 is passed by the entrypoint, not rendered here."""
    assert build_requirements({}) == []
    assert build_requirements(None) == []


def test_renders_every_declared_package():
    assert build_requirements({"pandas": "", "numpy": "2.2.6"}) == ["pandas", "numpy==2.2.6"]


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
        "pandas",
        "pyarrow<19.0.0",
        f"ds-platform-utils @ git+{REPO}@{SHA}",
        f"git+{REPO}@main",
    ]


def write_spec(tmp_path, body):
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps(body))
    return str(spec)


def test_main_emits_nul_terminated_records(tmp_path, capsysbinary):
    """entrypoint.sh reads these into a bash array with `read -d ''`."""
    spec = write_spec(tmp_path, {"env": {"packages": {"pandas": "", "pyarrow": "<19.0.0"}}})

    assert main([spec]) == 0
    assert capsysbinary.readouterr().out == b"pandas\0pyarrow<19.0.0\0"


def test_a_direct_reference_survives_as_one_record(tmp_path, capsysbinary):
    """The spaces in a direct reference are why NUL is the separator."""
    spec = write_spec(tmp_path, {"env": {"packages": {"ds-platform-utils": f"@ git+{REPO}@{SHA}"}}})

    main([spec])
    records = capsysbinary.readouterr().out.split(b"\0")[:-1]
    assert records == [f"ds-platform-utils @ git+{REPO}@{SHA}".encode()]


@pytest.mark.parametrize("env", [{}, {"env": {}}, {"env": {"packages": {}}}, {"env": {"packages": None}}])
def test_an_empty_package_set_emits_nothing_at_all(tmp_path, capsysbinary, env):
    """A trailing NUL here would reach uv as "" and fail PEP 508 parsing.

    A step needing only the standard library is legitimate.
    """
    assert main([write_spec(tmp_path, env)]) == 0
    assert capsysbinary.readouterr().out == b""


def test_main_reports_what_it_rendered(tmp_path, capsysbinary):
    """The driver-side poller attributes install failures from this output."""
    spec = write_spec(tmp_path, {"env": {"packages": {"pyarrow": "<19.0.0"}}})

    main([spec])

    assert b"pyarrow<19.0.0" in capsysbinary.readouterr().err
