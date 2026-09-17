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

    [package.optional-dependencies]
    ml = [{ name = "scikit-learn" }]
    excel = [{ name = "pandas", extra = ["excel"] }]
    # a self-referential extra: requesting "all" should pull ml without installing the local project
    all = [{ name = "my-flows", extra = ["ml"] }]
    # coverage[toml] -- the extra packages have to be inferred from coverage's own extras table
    cov = [{ name = "coverage", extra = ["toml"] }]

    [package.dev-dependencies]
    dev = [{ name = "pytest" }]

    [[package]]
    name = "pandas"
    version = "2.3.2"
    source = { registry = "https://pypi.org/simple" }

    [package.optional-dependencies]
    excel = [
        { name = "openpyxl" },
        { name = "pyobjc-core", marker = "sys_platform == 'darwin'" },
    ]

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
    name = "scikit-learn"
    version = "1.5.2"
    source = { registry = "https://pypi.org/simple" }

    [[package]]
    name = "openpyxl"
    version = "3.1.5"
    source = { registry = "https://pypi.org/simple" }

    [[package]]
    name = "coverage"
    version = "7.6.1"
    source = { registry = "https://pypi.org/simple" }

    [package.optional-dependencies]
    toml = [
        { name = "tomli", marker = "python_full_version <= '3.11'" },
    ]

    [[package]]
    name = "tomli"
    version = "2.0.2"
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


def test_uv_lock_excludes_extras_unless_asked(project_root: Path):
    packages = _get_packages_from_uv_lock(project_root=project_root)
    assert "scikit-learn" not in packages
    assert "openpyxl" not in packages
    assert "coverage" not in packages


def test_uv_lock_includes_requested_extras(project_root: Path):
    packages = _get_packages_from_uv_lock(extras=["ml"], project_root=project_root)
    assert packages["scikit-learn"] == "1.5.2"
    # runtime deps are still present
    assert packages["pandas"] == "2.3.2"


def test_uv_lock_bare_string_extra(project_root: Path):
    # a bare string would otherwise iterate character by character
    assert _get_packages_from_uv_lock(extras="ml", project_root=project_root)["scikit-learn"] == "1.5.2"


def test_uv_lock_normalises_extra_names(project_root: Path):
    # PEP 685: Foo_Bar and foo-bar are the same extra; the lock records "ml"
    assert _get_packages_from_uv_lock(extras="ML", project_root=project_root)["scikit-learn"] == "1.5.2"


def test_uv_lock_rejects_unrecorded_extra(project_root: Path):
    with pytest.raises(ValueError, match="extra 'nope' is not recorded in"):
        _get_packages_from_uv_lock(extras=["nope"], project_root=project_root)


def test_uv_lock_extra_drops_dep_gated_to_another_platform(project_root: Path):
    linux = _get_packages_from_uv_lock(extras=["excel"], project_root=project_root)
    assert linux["openpyxl"] == "3.1.5"
    assert "pyobjc-core" not in linux
    darwin = _get_packages_from_uv_lock(extras=["excel"], project_root=project_root, sys_platform="darwin")
    assert darwin["pyobjc-core"] == "10.3.1"


def test_uv_lock_infers_extra_packages_requested_on_a_dependency(project_root: Path):
    # extras=["cov"] pulls coverage[toml]; tomli is not a root extra, it is coverage's extra,
    # and @pypi has nowhere to put extras, so it has to be lifted into the packages map
    packages = _get_packages_from_uv_lock(extras=["cov"], project_root=project_root, python="3.10")
    assert packages["coverage"] == "7.6.1"
    assert packages["tomli"] == "2.0.2"
    # tomli is gated to <=3.11, so a 3.12 bake must not be told to install it
    py312 = _get_packages_from_uv_lock(extras=["cov"], project_root=project_root, python="3.12")
    assert py312["coverage"] == "7.6.1"
    assert "tomli" not in py312


def test_uv_lock_follows_self_referential_extra(project_root: Path):
    # extra "all" depends on the local project with extra "ml" -- the local project is not
    # installable, but ml's packages still have to land in the map
    packages = _get_packages_from_uv_lock(extras=["all"], project_root=project_root)
    assert "my-flows" not in packages
    assert packages["scikit-learn"] == "1.5.2"


def test_uv_lock_infers_extras_on_direct_dependencies(project_root: Path):
    # a root dependency recorded as pandas[excel] must bring excel's packages even when extras=
    # is not passed -- that is the lock saying the extra is required, not optional
    lock = (project_root / "uv.lock").read_text()
    (project_root / "uv.lock").write_text(
        lock.replace('{ name = "pandas" },', '{ name = "pandas", extra = ["excel"] },')
    )
    packages = _get_packages_from_uv_lock(project_root=project_root)
    assert packages["pandas"] == "2.3.2"
    assert packages["openpyxl"] == "3.1.5"
    assert "pyobjc-core" not in packages


def test_raises_when_no_lockfile_is_found(tmp_path: Path):
    # an environment that silently resolves to nothing is worse than a failure that says where
    # it looked, so the client is told rather than handed an empty map
    with pytest.raises(FileNotFoundError, match="no uv.lock found"):
        _get_packages_from_uv_lock(project_root=tmp_path)


def test_missing_lockfile_error_names_where_it_looked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # an explicit root is reported as the one directory that was checked
    with pytest.raises(FileNotFoundError, match=str(tmp_path)):
        _get_packages_from_uv_lock(project_root=tmp_path)
    # without one, the message has to say the search walked upwards
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError, match="and its parents"):
        _get_packages_from_uv_lock()


@pytest.mark.parametrize(
    ("env", "argv"),
    [
        ({"MF_PATHSPEC": "MyFlow/1/start/2"}, ["my_flow.py", "run"]),
        ({}, ["my_flow.py", "step", "start"]),
    ],
)
def test_returns_empty_map_when_a_task_finds_no_lockfile(
    env: dict, argv: list, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # the remote task, re-importing the flow module inside an already-baked image. Raising here
    # would kill every remote run, since metaflow's code package holds only .py files.
    monkeypatch.setattr("sys.argv", argv)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
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


def test_pypi_base_kwargs_passes_extras_through(project_root: Path):
    assert _get_pypi_kwargs(extras=["ml"], project_root=project_root)["packages"]["scikit-learn"] == "1.5.2"


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


def test_uv_pypi_base_passes_extras_through(project_root: Path, pypi_base_spy: dict):
    uv_pypi_base(extras=["ml"], project_root=project_root)(_build_flow())
    assert pypi_base_spy["packages"]["scikit-learn"] == "1.5.2"


def test_uv_pypi_base_combines_extras_and_groups(project_root: Path, pypi_base_spy: dict):
    uv_pypi_base(extras=["ml"], dependency_groups=["dev"], project_root=project_root)(_build_flow())
    assert pypi_base_spy["packages"]["scikit-learn"] == "1.5.2"
    assert pypi_base_spy["packages"]["pytest"] == "8.4.1"


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


def test_uv_pypi_prints_the_step_it_decorates(project_root: Path, capsys: pytest.CaptureFixture):
    def train(self):
        pass

    uv_pypi(project_root=project_root)(step(train))
    assert "@uv_pypi on train:" in capsys.readouterr().out


@pytest.mark.parametrize("command", ["step", "spin-step"])
def test_uv_pypi_base_says_nothing_in_a_task_process(
    command: str,
    project_root: Path,
    pypi_base_spy: dict,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    # metaflow runs one of these per task -- locally, and again inside the container -- and each
    # re-imports the flow module. Without this the summary prints once per step.
    monkeypatch.setattr("sys.argv", ["my_flow.py", "--quiet", command, "start", "--run-id", "1"])
    uv_pypi_base(project_root=project_root)(_build_flow())
    assert capsys.readouterr().out == ""


def test_uv_pypi_base_reports_from_the_client(
    project_root: Path, pypi_base_spy: dict, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
):
    # the invocation that launches the run is the one that should report
    monkeypatch.setattr("sys.argv", ["my_flow.py", "--environment=pypi", "run"])
    uv_pypi_base(project_root=project_root)(_build_flow())
    assert "@uv_pypi_base on MyFlow:" in capsys.readouterr().out


def test_uv_pypi_base_says_nothing_in_a_remote_task(
    project_root: Path, pypi_base_spy: dict, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
):
    # MF_PATHSPEC is exported into every remote task command, so it holds even if argv changes
    # shape. Set argv to the client's to prove this check stands on its own.
    monkeypatch.setattr("sys.argv", ["my_flow.py", "run"])
    monkeypatch.setenv("MF_PATHSPEC", "MyFlow/219386/start/1808602")
    uv_pypi_base(project_root=project_root)(_build_flow())
    assert capsys.readouterr().out == ""


def test_uv_pypi_base_says_nothing_while_a_flow_is_running(
    project_root: Path, pypi_base_spy: dict, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
):
    # `current` is populated once the task runtime starts, so this is the flow module being
    # re-imported mid-run -- e.g. one flow triggering another -- rather than the client
    import metaflow

    monkeypatch.setattr(type(metaflow.current), "is_running_flow", property(lambda self: True))
    uv_pypi_base(project_root=project_root)(_build_flow())
    assert capsys.readouterr().out == ""


def test_uv_pypi_base_says_nothing_when_a_task_finds_no_lock(
    tmp_path: Path, pypi_base_spy: dict, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
):
    # a task re-imports the flow module inside an already-baked image, so there is no lockfile
    # and nothing worth reporting -- and it must not raise there either
    monkeypatch.setattr("sys.argv", ["my_flow.py", "step", "start"])
    uv_pypi_base(project_root=tmp_path)(_build_flow())
    assert capsys.readouterr().out == ""
    assert pypi_base_spy["packages"] == {}


def test_uv_pypi_base_raises_on_the_client_when_no_lock_is_found(
    tmp_path: Path, pypi_base_spy: dict, monkeypatch: pytest.MonkeyPatch
):
    # decoration is import time, so a misconfigured flow fails before anything is scheduled
    monkeypatch.setattr("sys.argv", ["my_flow.py", "run"])
    with pytest.raises(FileNotFoundError, match="no uv.lock found"):
        uv_pypi_base(project_root=tmp_path)(_build_flow())


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


def test_uv_pypi_passes_extras_through(project_root: Path):
    def train(self):
        pass

    decorated = uv_pypi(extras=["ml"], project_root=project_root)(step(train))
    assert decorated.decorators[0].attributes["packages"]["scikit-learn"] == "1.5.2"


def test_finds_project_files_by_walking_up_from_cwd(project_root: Path, monkeypatch: pytest.MonkeyPatch):
    nested = project_root / "flows" / "nested"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    assert _get_packages_from_uv_lock()["pandas"] == "2.3.2"
