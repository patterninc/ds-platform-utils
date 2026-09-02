import textwrap
from pathlib import Path

import pytest

# Metaflow first, deliberately. This module imports `metaflow.decorators` at module scope, so
# importing it before Metaflow has finished resolving plugins re-enters that resolution while
# this module is still initialising -- and Metaflow fails with "Cannot locate 'UVStepDecorator'
# class for step_decorator plugin". Letting Metaflow load first makes the import safe.
import metaflow  # noqa: F401

from metaflow_extensions.pattern.plugins import uv_decorators as uvd

UV_LOCK = textwrap.dedent("""
    version = 1
    requires-python = ">=3.10"

    [[package]]
    name = "my-flows"
    version = "0.1.0"
    source = { editable = "." }
""")


@pytest.fixture
def project(tmp_path: Path) -> Path:
    (tmp_path / "uv.lock").write_text(UV_LOCK)
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "my-flows"\n')
    return tmp_path


def test_remote_decorators_match_fast_bakerys_rule():
    """Fast Bakery tests only for Kubernetes and Batch (docker_environment.py::_is_remote_deco).

    `resources` is deliberately absent: it sizes a step, it does not place one. A step carrying
    only `@resources` runs locally and needs no image.
    """
    assert uvd.REMOTE_DECORATORS == frozenset(["kubernetes", "batch"])
    assert "resources" not in uvd.REMOTE_DECORATORS


def test_remote_task_is_detected_from_the_pathspec(monkeypatch: pytest.MonkeyPatch):
    """The guard that stops a local @uv step exploding inside somebody else's pod.

    A remote task re-imports the whole flow module, so every step's step_init runs in the
    container -- including local ones. There is no uv.lock there, so resolving one raised and took
    the unrelated remote step down with it.
    """
    monkeypatch.delenv("MF_PATHSPEC", raising=False)
    assert uvd._in_remote_task() is False
    monkeypatch.setenv("MF_PATHSPEC", "Flow/1/start/2")
    assert uvd._in_remote_task() is True


def test_local_worker_is_not_treated_as_a_remote_task(monkeypatch: pytest.MonkeyPatch):
    # MF_PATHSPEC is exported only into *remote* task commands. A local worker shares the
    # client's working directory and can still find the project, so it must not be skipped.
    monkeypatch.delenv("MF_PATHSPEC", raising=False)
    monkeypatch.setattr(uvd.sys, "argv", ["flow.py", "step", "start"])
    assert uvd._in_remote_task() is False


def test_deploying_is_detected_for_a_scheduler_create(monkeypatch: pytest.MonkeyPatch):
    """Deploying has no local steps, and Metaflow attaches @kubernetes after our hooks run.

    Without noticing the command, deployed steps would be handed Metaflow's default image -- with
    none of the project's dependencies -- failing as a ModuleNotFoundError inside a pod.
    """
    monkeypatch.setattr(uvd.sys, "argv", ["flow.py", "argo-workflows", "create"])
    assert uvd._deploying() is True


def test_deploying_is_false_for_a_plain_run(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(uvd.sys, "argv", ["flow.py", "run"])
    assert uvd._deploying() is False


def test_deploying_is_false_for_triggering_an_existing_template(monkeypatch: pytest.MonkeyPatch):
    # `trigger` acts on a template that was already compiled, so it needs no image
    monkeypatch.setattr(uvd.sys, "argv", ["flow.py", "argo-workflows", "trigger"])
    assert uvd._deploying() is False


def test_venv_key_is_stable_and_lock_sensitive(project: Path):
    lock = str(project / "uv.lock")
    before = uvd._env_key(str(project), None, lock)
    assert uvd._env_key(str(project), None, lock) == before
    # a different group is a different environment
    assert uvd._env_key(str(project), "dev", lock) != before
    # editing dependencies must not silently reuse the old venv
    (project / "uv.lock").write_text(UV_LOCK + '\n[[package]]\nname = "extra"\nversion = "1.0"\n')
    assert uvd._env_key(str(project), None, lock) != before


def test_resolve_project_finds_the_lock(project: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.chdir(project)
    project_dir, lock_path = uvd._resolve_project()
    assert Path(project_dir) == project
    assert Path(lock_path) == project / "uv.lock"


def test_resolve_project_accepts_an_explicit_directory(project: Path):
    project_dir, _ = uvd._resolve_project(str(project))
    assert Path(project_dir) == project


def test_resolve_project_requires_a_pyproject_beside_the_lock(tmp_path: Path):
    # a lock is meaningless without the pyproject it was resolved from
    (tmp_path / "uv.lock").write_text(UV_LOCK)
    with pytest.raises(uvd.UVException, match="no pyproject.toml beside it"):
        uvd._resolve_project(str(tmp_path))


def test_resolve_project_reports_a_missing_lock(tmp_path: Path):
    with pytest.raises(uvd.UVException, match="No uv.lock found"):
        uvd._resolve_project(str(tmp_path / "uv.lock"))


def test_venv_metaflow_detection(tmp_path: Path):
    """Whether to expose the host's Metaflow to the venv.

    Injecting it when the venv already has its own makes every extension visible twice, and
    Metaflow then refuses to start: "Conflicts in 'metaflow_extensions' files". A project
    depending on `outerbounds` hits exactly that.
    """
    interpreter = tmp_path / "bin" / "python"
    site = tmp_path / "lib" / "python3.11" / "site-packages"
    site.mkdir(parents=True)
    assert uvd._venv_provides_metaflow(str(interpreter)) is False
    (site / "metaflow").mkdir()
    assert uvd._venv_provides_metaflow(str(interpreter)) is True


def _decorator(**attributes):
    merged = {"group": None, "lock": None}
    merged.update(attributes)
    return uvd.UVStepDecorator(attributes=merged, statically_defined=True)


def _step_init(deco, decos=(), logger=lambda *a, **k: None):
    deco.step_init(None, None, "start", list(decos), None, None, logger)


def test_step_init_stands_down_inside_a_remote_task(project: Path, monkeypatch: pytest.MonkeyPatch):
    """Pins the bug directly, not just the helper that detects it.

    A pod re-imports the whole flow module, so this local decorator's step_init runs there too.
    Resolving a project it cannot find raised and failed the unrelated remote step.
    """
    monkeypatch.chdir(project)
    monkeypatch.setenv("MF_PATHSPEC", "Flow/1/train/2")
    deco = _decorator()
    _step_init(deco)
    assert deco.interpreter is None, "step_init did its local work inside a remote task"


def test_step_init_prepares_a_venv_for_a_local_step(project: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.chdir(project)
    monkeypatch.delenv("MF_PATHSPEC", raising=False)
    deco = _decorator()
    _step_init(deco)
    assert deco.interpreter is not None
    assert uvd.VENV_ROOT in deco.interpreter
    assert deco.interpreter.endswith("/bin/python")


def test_step_init_stands_down_for_a_remote_step(project: Path, monkeypatch: pytest.MonkeyPatch):
    """A step with @kubernetes runs in the baked image; a local venv would be built and unused."""
    monkeypatch.chdir(project)
    monkeypatch.delenv("MF_PATHSPEC", raising=False)

    class Deco:
        name = "kubernetes"

    deco = _decorator()
    _step_init(deco, decos=[Deco()])
    assert deco.interpreter is None


def test_runtime_step_cli_is_a_noop_without_an_interpreter():
    # nothing to retarget for a remote step, and no venv should be created for one
    class Args:
        env = {}
        entrypoint = ["original-python"]

    deco = _decorator()
    deco.interpreter = None
    args = Args()
    deco.runtime_step_cli(args, 0, 0, None)
    assert args.entrypoint == ["original-python"]
    assert args.env == {}
