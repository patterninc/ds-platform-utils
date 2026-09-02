"""Bake a uv project's locked dependencies into an image Metaflow can run tasks in.

`build_metaflow_image` is the whole thing --
point it at a uv project and it produces a tagged local image:

```python
build_metaflow_image("~/github/my-flow-repo", python_version="3.11")
```

The image installs with `uv sync --frozen` against the project's own `uv.lock`, so it gets
exactly what `uv sync` gives you locally: the same resolved versions, verified against the same
hashes. Nothing is re-resolved at build time, which is what makes two builds of one lock produce
the same environment.

That is the difference from `@pypi`, which is handed a package list and resolves it itself. It is also why the lock is
copied into the build rather than exported to a requirements.txt first -- an export drops the
hashes, and `uv sync` is the same operation a developer runs, rather than a translation of it.

`render_metaflow_dockerfile` returns the
Dockerfile on its own, for inspecting or committing what a build would run.

The layout of that Dockerfile is dictated by how Metaflow launches a task, per
https://docs.outerbounds.com/build-custom-image/: logs are streamed into `/logs`, the task runs
out of `HOME`, and Metaflow never elevates -- so both directories are owned by the unprivileged
uid the task runs as. Metaflow builds the task command itself, which is why the image sets no
`ENTRYPOINT` and no `CMD`.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from .image_builder import build_image

#: The uid Metaflow's own example images run tasks as. Nothing in Metaflow requires this
#: particular number -- what matters is that the task is not root and owns the directories it
#: writes to, since Metaflow will not chown them for you.
_TASK_UID = 1000

#: Metaflow runs the task out of `HOME`, so this is both the workdir and the home directory.
_TASK_HOME = "/metaflow"

#: Where Metaflow's mflog helper streams task logs.
_TASK_LOG_DIR = "/logs"

#: Remote tasks are scheduled onto linux/amd64 nodes, so that is what an image has to be built
#: for. Worth stating explicitly rather than inheriting the builder's own architecture: a build
#: on an Apple Silicon machine defaults to arm64 and the resulting image fails to start in the
#: cluster with an exec format error.
_DEFAULT_PLATFORM = "linux/amd64"

#: Where the uv binary is copied from. Pinned rather than `:latest`, so the tool doing the
#: installing cannot drift between two builds of the same lock -- the point of building from a
#: lockfile in the first place.
_DEFAULT_UV_IMAGE = "ghcr.io/astral-sh/uv:0.11.14"

#: The dependency files a `uv sync --no-install-project` needs, and nothing else.
_LOCK_FILES = ("pyproject.toml", "uv.lock")

_DOCKERFILE_TEMPLATE = """\
FROM {base_image}

COPY --from={uv_image} /uv /usr/local/bin/uv

# Point uv at the interpreter the base image already put on PATH instead of letting it build a
# .venv. Metaflow's task command runs a plain `python`, which would not find a virtualenv without
# PATH surgery -- and the venv would land in the same directory Metaflow unpacks its code into.
ENV UV_PROJECT_ENVIRONMENT=/usr/local

# Just the two dependency files: --no-install-project keeps the project itself out, since the
# flow's own source ships in Metaflow's code package and baking it in would only pin a second,
# staler copy. Installed as root so the packages land in the system site-packages the task user
# can read; syncing after the USER switch would need a writable prefix it does not own.
COPY {lock_files} /tmp/project/
RUN cd /tmp/project \\
 && uv sync --frozen --no-install-project --no-default-groups{group_flags} \\
 && rm -rf /tmp/project /root/.cache/uv

# Metaflow streams task logs into {log_dir} and runs the task out of HOME, and it never
# elevates -- so the unprivileged uid the task runs as has to own both up front.
RUN mkdir -p {log_dir} {home} && chown {uid}:{uid} {log_dir} {home}
ENV HOME={home}
WORKDIR {home}
USER {uid}

# No ENTRYPOINT and no CMD, deliberately: Metaflow constructs the entire task command and an
# entrypoint here would be prepended to it.
"""


def _normalise_groups(dependency_groups: str | list[str] | None) -> list[str]:
    """Accept a bare group name as well as a list.

    Args:
        dependency_groups: one group name, several, or none

    """
    if dependency_groups is None:
        return []
    if isinstance(dependency_groups, str):
        # a bare string would otherwise iterate character by character
        return [dependency_groups]
    return list(dependency_groups)


def render_metaflow_dockerfile(
    python_version: str,
    base_image: str | None = None,
    dependency_groups: str | list[str] | None = None,
    uv_image: str = _DEFAULT_UV_IMAGE,
) -> str:
    """Render the Dockerfile that `build_metaflow_image` builds.

    Useful for reviewing or committing what a build would run. The Dockerfile expects
    `pyproject.toml` and `uv.lock` in the build context; `build_metaflow_image` copies both
    there from the project.

    Args:
        python_version: interpreter to build on, e.g. `"3.11"`. Selects the default base image.
        base_image: image to build from instead of the default `python:<python_version>`.
            The default is the full Debian-based official image rather than `-slim` because it
            carries a toolchain, so a dependency with no wheel for the target platform still
            builds. Pass `f"python:{python_version}-slim"` for a substantially smaller image
            once you know every dependency ships a wheel.
        dependency_groups: dependency groups to install on top of the runtime dependencies.
            uv counts `dev` as a default group, so groups are switched off wholesale and then
            asked back in by name -- excluded by default, since groups are optional by definition.
        uv_image: image to copy the uv binary from.

    Returns:
        The Dockerfile contents.

    """
    groups = _normalise_groups(dependency_groups)
    return _DOCKERFILE_TEMPLATE.format(
        base_image=base_image or f"python:{python_version}",
        uv_image=uv_image,
        lock_files=" ".join(_LOCK_FILES),
        group_flags="".join(f" --group={group}" for group in groups),
        home=_TASK_HOME,
        log_dir=_TASK_LOG_DIR,
        uid=_TASK_UID,
    )


def build_metaflow_image(
    project_root: str | Path,
    python_version: str,
    image_name: str,
    *,
    dependency_groups: str | list[str] | None = None,
    base_image: str | None = None,
    platform: str | None = _DEFAULT_PLATFORM,
    uv_image: str = _DEFAULT_UV_IMAGE,
    stream: bool = True,
) -> str:
    """Build a Docker image that can run this project's Metaflow tasks.

    The image installs the project's `uv.lock` with `uv sync --frozen` on top of an official
    Python base image, arranged the way Metaflow expects a task container to be arranged. uv runs
    inside the build, so nothing but Docker is needed on the machine doing the building.

    Example usage:

    ```python
    from metaflow_extensions.pattern.plugins.uv_image import build_metaflow_image

    image = build_metaflow_image("~/github/my-flow-repo", python_version="3.11")
    ```

    The image is built locally. Push it to the registry the cluster pulls from before a remote
    task can use it, then name it on the step:

    ```python
    @kubernetes(image="my-registry/my-flow-repo:py3.11")
    ```

    Args:
        project_root: the uv project to build from -- the directory holding `uv.lock`.
        python_version: interpreter to build on, e.g. `"3.11"`. Defaults to the version the
            project pins for itself, read from `.python-version` and then `requires-python`,
            which is the same version the `@uv_pypi` decorators derive. It has to satisfy the
            lock's own `requires-python`, or `uv sync` will refuse it.
        image_name: tag for the resulting image. Defaults to the project's own name and the
            Python version, e.g. `"my-flow-repo:py3.11"`.
        dependency_groups: dependency groups to install on top of the runtime dependencies,
            e.g. `["train"]`. Excluded by default, since groups are optional by definition.
        base_image: image to build from instead of `python:<python_version>`.
        platform: platform to build for. Defaults to `linux/amd64`, which is what remote tasks
            are scheduled onto -- leaving this to the builder's own architecture is how an
            image built on an Apple Silicon machine ends up unable to start in the cluster.
            Pass `None` to build for the host architecture.
        uv_image: image to copy the uv binary from.
        stream: echo build output to stderr as it arrives.

    Returns:
        The image name that was built.

    Raises:
        NotADirectoryError: `project_root` does not exist.
        FileNotFoundError: `project_root` is missing `uv.lock` or `pyproject.toml`.
        DockerNotFoundError: `docker` is not installed or not on PATH.
        DockerDaemonError: the Docker daemon is not running or not reachable.
        DockerBuildError: the build ran but failed -- including when `uv sync` rejects the lock,
            since the sync happens inside it.

    """
    project_root = Path(project_root).expanduser()
    if not project_root.is_dir():
        raise NotADirectoryError(f"Project root does not exist: {project_root}")
    project_root = project_root.resolve()

    missing = [name for name in _LOCK_FILES if not (project_root / name).is_file()]
    if missing:
        raise FileNotFoundError(
            f"{project_root} is missing {', '.join(missing)}, so there is nothing to build an "
            "image from. Run `uv lock` if the project has no lockfile yet, or point "
            "project_root= at the directory holding it."
        )

    dockerfile = render_metaflow_dockerfile(
        python_version, base_image, dependency_groups=dependency_groups, uv_image=uv_image
    )

    # only the dependency files go into the context, so the rest of the repo cannot end up in
    # the image by way of a stray COPY, and the sync layer caches on the lock alone.
    with tempfile.TemporaryDirectory() as context:
        for name in _LOCK_FILES:
            shutil.copy(project_root / name, Path(context) / name)
        return build_image(
            dockerfile=dockerfile,
            image_name=image_name,
            context_dir=context,
            platform=platform,
            stream=stream,
        )
