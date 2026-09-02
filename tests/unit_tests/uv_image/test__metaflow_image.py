import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest

from metaflow_extensions.pattern.plugins.uv_image import build_metaflow_image, render_metaflow_dockerfile
from metaflow_extensions.pattern.plugins.uv_image.image_builder import DockerNotFoundError, build_image

PYPROJECT = textwrap.dedent("""
    [project]
    name = "my_flows"
    version = "0.1.0"
    requires-python = ">=3.10"
    dependencies = ["packaging", "sqlparse"]

    [dependency-groups]
    dev = ["iniconfig"]

    [build-system]
    requires = ["hatchling"]
    build-backend = "hatchling.build"
""")

# uv runs inside the image, never on the host, so nothing under test parses this. It exists so
# the file is present and gets copied into the build context.
UV_LOCK = textwrap.dedent("""
    version = 1
    revision = 3
    requires-python = ">=3.10"

    [[package]]
    name = "my-flows"
    version = "0.1.0"
    source = { editable = "." }
""")


@pytest.fixture
def project_root(tmp_path: Path) -> Path:
    (tmp_path / "pyproject.toml").write_text(PYPROJECT)
    (tmp_path / "uv.lock").write_text(UV_LOCK)
    return tmp_path


@pytest.fixture
def built(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Capture what a build would hand to docker, stopping short of running one."""
    captured: dict = {}

    def fake_build_image(**kwargs):
        captured.update(kwargs)
        context = Path(kwargs["context_dir"])
        # read through before the caller's TemporaryDirectory takes the context away
        captured["context_files"] = sorted(p.name for p in context.iterdir())
        captured["context_contents"] = {p.name: p.read_text() for p in context.iterdir() if p.is_file()}
        return kwargs["image_name"]

    monkeypatch.setattr(
        "metaflow_extensions.pattern.plugins.uv_image.metaflow_image.build_image",
        fake_build_image,
    )
    return captured


def _instructions(**kwargs) -> list[str]:
    """The Dockerfile's actual instructions, without the comments explaining them."""
    return [line for line in render_metaflow_dockerfile(**kwargs).splitlines() if line and not line.startswith("#")]


def _sync_command(**kwargs) -> str:
    return next(line for line in _instructions(**kwargs) if "uv sync" in line)


def test_installs_dependencies_before_dropping_privileges():
    # syncing after the USER switch would need a writable prefix the task user does not own
    instructions = _instructions(python_version="3.11")
    assert instructions.index("USER 1000") > next(i for i, line in enumerate(instructions) if "uv sync" in line)


def test_task_directories_are_owned_by_the_task_user():
    # Metaflow streams logs into /logs and runs out of HOME, and never elevates to do it
    instructions = _instructions(python_version="3.11")
    assert "RUN mkdir -p /logs /metaflow && chown 1000:1000 /logs /metaflow" in instructions
    assert "ENV HOME=/metaflow" in instructions
    assert "WORKDIR /metaflow" in instructions


def test_sets_no_entrypoint_or_cmd():
    # Metaflow constructs the whole task command; an entrypoint here would be prepended to it
    assert not [line for line in _instructions(python_version="3.11") if line.startswith(("ENTRYPOINT", "CMD"))]


def test_base_image_follows_the_python_version():
    assert _instructions(python_version="3.11")[0] == "FROM python:3.11"
    assert _instructions(python_version="3.10.15")[0] == "FROM python:3.10.15"
    assert _instructions(python_version="3.11", base_image="python:3.11-slim")[0] == "FROM python:3.11-slim"


def test_installs_from_the_lock_without_reresolving():
    # --frozen is what makes the image reproducible: the lock is installed, never recomputed
    assert "--frozen" in _sync_command(python_version="3.11")


def test_does_not_install_the_project_itself():
    # the flow's source ships in Metaflow's code package, and the context holds no project
    # source to install from anyway
    assert "--no-install-project" in _sync_command(python_version="3.11")


def test_syncs_into_the_system_interpreter_rather_than_a_venv():
    # Metaflow's task command runs a plain `python`; a .venv would need PATH surgery to be found
    assert "ENV UV_PROJECT_ENVIRONMENT=/usr/local" in _instructions(python_version="3.11")
    assert not [line for line in _instructions(python_version="3.11") if line.startswith("ENV PATH")]


def test_pins_the_uv_binary():
    # the tool doing the installing must not drift between builds either
    line = next(line for line in _instructions(python_version="3.11") if line.startswith("COPY --from="))
    assert "ghcr.io/astral-sh/uv:" in line
    assert not line.rstrip().endswith(":latest")


def test_excludes_dependency_groups_unless_asked():
    # uv counts `dev` as a default group, so it has to be switched off rather than not asked for
    command = _sync_command(python_version="3.11")
    assert "--no-default-groups" in command
    assert "--group=" not in command


def test_includes_dependency_groups_when_asked():
    command = _sync_command(python_version="3.11", dependency_groups=["dev", "train"])
    assert "--group=dev" in command
    assert "--group=train" in command


def test_accepts_a_bare_group_name():
    # a bare string would otherwise iterate character by character
    assert "--group=dev" in _sync_command(python_version="3.11", dependency_groups="dev")


def test_context_holds_only_the_dependency_files(project_root: Path, built: dict):
    # nothing else from the repo should be reachable by a stray COPY, and the sync layer should
    # cache on the lock alone
    (project_root / "secrets.env").write_text("SHOULD_NOT_BE_COPIED=1")
    build_metaflow_image(project_root, "3.11", "img:tag")
    assert built["context_files"] == ["pyproject.toml", "uv.lock"]
    assert built["context_contents"]["uv.lock"] == UV_LOCK


def test_builds_for_amd64_by_default(project_root: Path, built: dict):
    # leaving this to the builder's architecture is how a build on an Apple Silicon machine
    # produces an arm64 image that cannot start in the cluster
    build_metaflow_image(project_root, "3.11", "img:tag")
    assert built["platform"] == "linux/amd64"


def test_platform_can_be_disabled(project_root: Path, built: dict):
    build_metaflow_image(project_root, "3.11", "img:tag", platform=None)
    assert built["platform"] is None


def test_platform_reaches_the_docker_command(monkeypatch: pytest.MonkeyPatch):
    # the one place worth spying on subprocess: that build_image turns the argument into a flag
    captured: dict = {}
    real_popen = subprocess.Popen

    class SpyPopen(real_popen):
        def __init__(self, command, *args, **kwargs):
            captured["command"] = command
            super().__init__(["cat"], *args, **kwargs)  # consumes the piped Dockerfile, exits 0

    monkeypatch.setattr(subprocess, "Popen", SpyPopen)
    build_image("FROM python:3.11", "x:1", platform="linux/amd64", check_daemon=False, stream=False)
    assert captured["command"][captured["command"].index("--platform") + 1] == "linux/amd64"

    captured.clear()
    build_image("FROM python:3.11", "x:1", check_daemon=False, stream=False)
    assert "--platform" not in captured["command"]


def test_rejects_a_project_root_that_does_not_exist(tmp_path: Path):
    with pytest.raises(NotADirectoryError, match="Project root does not exist"):
        build_metaflow_image(tmp_path / "nope", "3.11", "img:tag")


def test_rejects_a_project_with_no_lockfile(tmp_path: Path):
    # an image built from an unlocked project would pin whatever resolved that day
    (tmp_path / "pyproject.toml").write_text(PYPROJECT)
    with pytest.raises(FileNotFoundError, match="uv.lock"):
        build_metaflow_image(tmp_path, "3.11", "img:tag")


def test_rejects_a_project_with_no_pyproject(tmp_path: Path):
    # uv sync needs both files; a lock alone is not a project
    (tmp_path / "uv.lock").write_text(UV_LOCK)
    with pytest.raises(FileNotFoundError, match="pyproject.toml"):
        build_metaflow_image(tmp_path, "3.11", "img:tag")


def test_reports_a_missing_docker_cli(project_root: Path, monkeypatch: pytest.MonkeyPatch):
    real_which = shutil.which
    monkeypatch.setattr(shutil, "which", lambda name: None if name == "docker" else real_which(name))
    with pytest.raises(DockerNotFoundError, match="was not found on PATH"):
        build_metaflow_image(project_root, "3.11", "img:tag")
