"""What the decorator ships alongside the flow file.

@uv_pypi_base reads uv.lock and pyproject.toml at class-load time, so those
files have to travel in the code package or `packages` comes back empty
inside the Argo driver pod and the runner installs nothing.

This module exists because of a regression: making the env cache per-flow
introduced a reference to `flow` inside add_to_package, which is not given
one. Every flow then died with

    Internal error
    NameError: name 'flow' is not defined

before any step ran. All 601 unit tests passed, because none of them called
add_to_package -- the packaging hook had no coverage at all.
"""

import os

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
import pytest

from remote_step.plugins.remote_step_decorator import RemoteStepDecorator


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A flow file with the project files beside it."""
    (tmp_path / "uv.lock").write_text("version = 1\n")
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "p"\n')
    (tmp_path / ".python-version").write_text("3.12\n")
    flow_file = tmp_path / "my_flow.py"
    flow_file.write_text("# a flow\n")
    return flow_file


def deco(flow_file, flow_name=None):
    d = RemoteStepDecorator()
    d.attributes = dict(RemoteStepDecorator.defaults)
    d._flow_file_path = lambda: str(flow_file)
    if flow_name is not None:
        d._flow_name_for_cache = flow_name
    return d


def packaged(d):
    return {arc for _path, arc in d.add_to_package()}


def test_it_does_not_raise_when_step_init_never_ran(project):
    """The regression.

    Metaflow calls add_to_package on decorators whose step_init was skipped --
    a start/end sweep, for instance -- so the flow name may never have been
    recorded. That must not be a NameError.
    """
    assert packaged(deco(project)) is not None


def test_the_project_files_travel(project):
    arcs = packaged(deco(project, "MyFlow"))
    assert "uv.lock" in arcs
    assert "pyproject.toml" in arcs
    assert ".python-version" in arcs


def test_every_step_of_the_flow_travels(project):
    """One code package serves the whole flow, so every step's file must ship.

    Metaflow calls add_to_package on whichever decorator instance it likes, so
    yielding only the caller's own step would leave the rest of the flow's
    pods with nothing to read.
    """
    (project.parent / ".remote_step_env.MyFlow.with_group.json").write_text("{}")
    (project.parent / ".remote_step_env.MyFlow.without_group.json").write_text("{}")
    arcs = packaged(deco(project, "MyFlow"))
    assert ".remote_step_env.MyFlow.with_group.json" in arcs
    assert ".remote_step_env.MyFlow.without_group.json" in arcs


def test_a_neighbouring_flows_cache_does_not_travel(project):
    """A directory routinely holds several flows; only this one's may ship."""
    (project.parent / ".remote_step_env.MyFlow.work.json").write_text("{}")
    (project.parent / ".remote_step_env.OtherFlow.work.json").write_text("{}")
    arcs = packaged(deco(project, "MyFlow"))
    assert ".remote_step_env.MyFlow.work.json" in arcs
    assert ".remote_step_env.OtherFlow.work.json" not in arcs, (
        "another flow's cache must not ship"
    )


def test_the_legacy_shared_cache_does_not_travel(project):
    """`.remote_step_env.json` is inert now, so shipping it is dead weight.

    Nothing reads it any more: the reader asks for one exact
    `.remote_step_env.<Flow>.<step>.json` and will not fall back to a less
    specific name, because doing so is what handed fx13 another flow's
    packages and fx20's `with_group` another step's.
    """
    (project.parent / ".remote_step_env.json").write_text("{}")
    assert ".remote_step_env.json" not in packaged(deco(project, "MyFlow"))


def test_missing_files_are_simply_absent(tmp_path):
    """A project with none of them packages nothing, rather than failing."""
    flow_file = tmp_path / "bare_flow.py"
    flow_file.write_text("# a flow\n")
    assert packaged(deco(flow_file, "Bare")) == set()


def test_an_unresolvable_flow_file_is_survivable():
    d = RemoteStepDecorator()
    d.attributes = dict(RemoteStepDecorator.defaults)

    def boom():
        raise RuntimeError("no __main__")

    d._flow_file_path = boom
    assert list(d.add_to_package()) == []


def test_paths_that_travel_are_real_files(project):
    for path, _arc in deco(project, "MyFlow").add_to_package():
        assert os.path.isfile(path), path
