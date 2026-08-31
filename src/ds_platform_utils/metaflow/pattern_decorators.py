"""Template for the platform's two standard decorators, built on Metaflow's mutator API.

Two decorators, one at each level:

```python
@pattern_flow
class MyFlow(FlowSpec):
    @pattern_step
    def start(self):
        self.next(self.end)
```

[`pattern_flow`][ds_platform_utils.metaflow.pattern_flow] bakes the project's `uv.lock` into a
container image, pushes it to ECR, and puts `@kubernetes(image=...)` on every step. A task then
starts with its dependencies already installed instead of resolving an environment first.

[`pattern_step`][ds_platform_utils.metaflow.pattern_step] is scaffolding for now -- a working
`@step` replacement whose mutator adds nothing yet.

## The tag is a cache key, not just a name

The mutator runs every time Metaflow loads the flow -- `run`, `show`, `argo-workflows create` --
so building unconditionally would add minutes to each. The tag therefore ends in a hash of the
resolved environment: `MyFlow-a1b2c3d4`. If that tag is already in the registry the image is
exactly the one this lock produces, so the build and push are skipped and the cost is one API
call. Editing `uv.lock` changes the hash and the next load builds once.

The hash matters for a second reason. `argo-workflows create` writes the *tag string* into the
Argo template, and the image is resolved from it at pod start, not at deploy time. A tag naming
only the flow would be mutable: pushing after a dependency change would silently swap the image
underneath a workflow that was already deployed and tested. A hashed tag cannot be repointed --
a change produces a new tag, which takes effect only when the flow is deployed again.

## Falling back

The build needs a Docker daemon and push credentials. Without either, the flow still has to run,
so `pattern_flow` warns and falls back to `@pypi_base` -- the environment is then resolved at
task start, which is slower but works anywhere. The image is skipped entirely inside a running
task: the container is already up, and there is no lockfile in it to build from.

Credentials come from the ambient AWS chain. Nothing is stored in the image or in this repo.
Point it at a profile with `@pattern_flow(aws_profile="sandbox")` or `AWS_PROFILE=sandbox`.
"""

import base64
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Optional

from metaflow import FlowMutator, StepMutator

#: ECR Public lives in one region regardless of where anything else runs.
_ECR_PUBLIC_REGION = "us-east-1"

_ECR_PUBLIC_HOST = "public.ecr.aws"

#: The sandbox account's ECR Public registry alias.
_DEFAULT_REGISTRY_ALIAS = "l3p3c6o4"

#: One repository holds every flow's image, told apart by tag. Per-flow repositories would mean
#: creating a repository for every new flow, and this never creates one -- see `_require_image`.
_DEFAULT_REPOSITORY = "outerbounds-images"

#: How much of the environment hash goes in the tag. Eight hex characters is 32 bits, which is
#: plenty to tell one lockfile from another without making the tag unreadable.
_TAG_HASH_LENGTH = 8


def _environment_hash(pypi_kwargs: dict) -> str:
    """Fingerprint a resolved environment, so an unchanged lock reuses its image.

    Sorted and serialised through JSON so the digest depends on the environment's content and
    not on dictionary ordering -- two runs of the same lock have to agree, or the cache never
    hits and every load rebuilds.

    Args:
        pypi_kwargs: the `{"python": ..., "packages": {...}}` map from `_get_pypi_kwargs`

    Returns:
        The first `_TAG_HASH_LENGTH` characters of the digest.

    """
    payload = json.dumps(
        {"python": pypi_kwargs["python"], "packages": dict(sorted(pypi_kwargs["packages"].items()))},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:_TAG_HASH_LENGTH]


def _aws_session(aws_profile: Optional[str]):
    """Open an AWS session from the ambient credential chain.

    Args:
        aws_profile: a named profile, or `None` to use whatever the environment already provides.

    """
    import boto3

    return boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()


def _require_image_absent(session, repository: str, tag: str) -> bool:
    """Say whether the image still needs building.

    Args:
        session: an AWS session
        repository: the ECR Public repository name
        tag: the tag being looked for

    Returns:
        `True` when the tag is not in the registry and so has to be built.

    Raises:
        RuntimeError: the repository does not exist. Creating it is deliberately not done here --
            a decorator that provisions registry infrastructure as a side effect of importing a
            flow module is worse than one that tells you which command to run.

    """
    client = session.client("ecr-public", region_name=_ECR_PUBLIC_REGION)
    try:
        client.describe_images(repositoryName=repository, imageIds=[{"imageTag": tag}])
        return False
    except client.exceptions.ImageNotFoundException:
        return True
    except client.exceptions.RepositoryNotFoundException:
        raise RuntimeError(
            f"ECR Public repository {repository!r} does not exist and will not be created for you. "
            f"Create it once with:\n\n"
            f"    aws ecr-public create-repository --repository-name {repository} "
            f"--region {_ECR_PUBLIC_REGION}\n"
        ) from None


def _push(session, image: str) -> None:
    """Log in to ECR Public and push.

    The registry password is read from the API and piped straight to `docker login` on stdin. It
    is never written to a file, an argument list, or the log.

    Args:
        session: an AWS session
        image: the fully qualified image reference to push

    """
    docker = shutil.which("docker")
    client = session.client("ecr-public", region_name=_ECR_PUBLIC_REGION)
    encoded = client.get_authorization_token()["authorizationData"]["authorizationToken"]
    username, password = base64.b64decode(encoded).decode().split(":", 1)

    login = subprocess.run(
        [docker, "login", "--username", username, "--password-stdin", _ECR_PUBLIC_HOST],
        input=password,
        capture_output=True,
        text=True,
        check=False,
    )
    if login.returncode != 0:
        raise RuntimeError(f"docker login to {_ECR_PUBLIC_HOST} failed: {login.stderr.strip()}")

    push = subprocess.run([docker, "push", image], capture_output=True, text=True, check=False)
    if push.returncode != 0:
        raise RuntimeError(f"docker push of {image} failed: {push.stderr.strip()}")


def _build_and_push(image: str, project_root: Path, python_version: str, session) -> None:
    """Build the image from the project's lock and push it.

    Args:
        image: the fully qualified reference to build and push
        project_root: directory holding `pyproject.toml` and `uv.lock`
        python_version: interpreter to build on
        session: an AWS session

    """
    from ds_platform_utils.docker import build_metaflow_image

    build_metaflow_image(project_root, python_version=python_version, image_name=image, stream=True)
    _push(session, image)


class _PatternFlowStack(FlowMutator):
    """The flow-level half: bakes the environment into an image and puts it on every step."""

    def init(self, *args, **kwargs):
        """Capture the decorator's arguments.

        Metaflow calls this instead of `__init__`, with whatever `@pattern_flow(...)` was given.

        Args:
            *args: positional arguments from the decorator
            **kwargs: `dependency_groups`, `python` and `project_root` are forwarded to
                `_get_pypi_kwargs`; `repository`, `registry_alias` and `aws_profile` control
                where the image goes.

        """
        self.options: dict = dict(kwargs)
        #: set by `pre_mutate`, read by `mutate` -- Metaflow uses one instance for both.
        self.image: Optional[str] = None

    def pre_mutate(self, mutable_flow) -> None:
        """Resolve the environment, make sure its image exists, and fall back if it cannot.

        Flow-level decorators have to be added here rather than in `mutate`. Metaflow rejects an
        `add_decorator` for a *flow* decorator from `mutate` outright. Step decorators are the
        opposite case and belong in `mutate`, which is why this class uses both methods.

        Args:
            mutable_flow: the flow being decorated.

        """
        from .pypi_packages import _find_project_file, _get_pypi_kwargs, _is_running_task

        if _is_running_task():
            # the container is already running, and Metaflow's code package carries no lockfile
            # to build from -- so there is nothing to do and nothing to fall back to.
            return

        pypi_kwargs = _get_pypi_kwargs(
            dependency_groups=self.options.get("dependency_groups"),
            python=self.options.get("python"),
            project_root=self.options.get("project_root"),
        )

        # `_flow_cls` is private, but MutableFlow exposes no public name and the tag has to say
        # which flow it belongs to.
        flow_name = mutable_flow._flow_cls.__name__
        tag = f"{flow_name}-{_environment_hash(pypi_kwargs)}"
        alias = self.options.get("registry_alias", _DEFAULT_REGISTRY_ALIAS)
        repository = self.options.get("repository", _DEFAULT_REPOSITORY)
        image = f"{_ECR_PUBLIC_HOST}/{alias}/{repository}:{tag}"

        try:
            session = _aws_session(self.options.get("aws_profile"))
            if _require_image_absent(session, repository, tag):
                lock = _find_project_file("uv.lock", self.options.get("project_root"))
                print(f"@pattern_flow: {image} not in the registry yet, building it")
                _build_and_push(image, lock.parent, pypi_kwargs["python"], session)
            self.image = image
        except Exception as error:
            # the flow still has to run. @pypi_base resolves the same environment at task start:
            # slower, but it needs neither Docker nor a registry.
            print(
                f"@pattern_flow: falling back to @pypi_base, could not prepare {image}\n  {type(error).__name__}: {error}",
                file=sys.stderr,
            )
            mutable_flow.add_decorator("pypi_base", deco_kwargs=pypi_kwargs, duplicates=mutable_flow.IGNORE)

    def mutate(self, mutable_flow) -> None:
        """Put the image on every step.

        Step decorators belong here rather than in `pre_mutate`, which is the mirror image of the
        rule for flow decorators.

        Args:
            mutable_flow: the flow being decorated.

        """
        if self.image is None:
            # either a task, or the fallback already added @pypi_base
            return

        for _step_name, step in mutable_flow.steps:
            # IGNORE so a step that named its own image keeps it -- this is a default, not policy.
            step.add_decorator("kubernetes", deco_kwargs={"image": self.image}, duplicates=step.IGNORE)


class _PatternStepStack(StepMutator):
    """The step-level half: runs once per decorated step.

    Attached by [`pattern_step`][ds_platform_utils.metaflow.pattern_step] rather than used
    directly, because `@step` has to be applied first and a mutator cannot do that.
    """

    def init(self, *args, **kwargs):
        """Capture the decorator's arguments.

        Args:
            *args: positional arguments from the decorator
            **kwargs: keyword arguments from the decorator, e.g. `compute={"cpu": 8}`

        """
        self.options: dict = dict(kwargs)

    def mutate(self, mutable_step) -> None:
        """Apply per-step policy.

        Args:
            mutable_step: the step being decorated.

        """
        # TODO: per-step policy goes here. A decorator the caller can switch off entirely
        # (`@pattern_step(retry=False)`) reads roughly:
        #
        #     retry = self.options.get("retry", {})
        #     if retry is not False:
        #         mutable_step.add_decorator(
        #             "retry", deco_kwargs=retry or {}, duplicates=mutable_step.IGNORE
        #         )


def pattern_flow(flow=None, **options):
    """Bake the project's uv.lock into an image and run every step in it.

    Both forms work; call it when there is something to configure:

    ```python
    @pattern_flow
    @pattern_flow(aws_profile="sandbox", dependency_groups=["train"])
    ```

    Args:
        flow: the decorated `FlowSpec`, supplied by Python in the bare form. Never pass this
            yourself.
        **options: `dependency_groups`, `python` and `project_root` describe the environment;
            `repository`, `registry_alias` and `aws_profile` say where the image goes.

    Returns:
        The decorated flow, or a decorator when called with keyword arguments.

    """
    if flow is None:
        return _PatternFlowStack(**options)
    return _PatternFlowStack()(flow)


def pattern_step(step_function: Optional[Callable] = None, **options: Any):
    """Metaflow's `@step`, plus the platform's per-step defaults.

    A drop-in replacement for `@step` -- do not stack the two, this applies `@step` itself.

    Args:
        step_function: the decorated step, supplied by Python in the bare form. Never pass this
            yourself.
        **options: forwarded to `_PatternStepStack.init`.

    Returns:
        The decorated step, or a decorator when called with keyword arguments.

    """
    from metaflow import step as metaflow_step

    def decorate(function: Callable) -> Callable:
        # @step first: it is a marker, and the mutator below only ever sees methods already
        # carrying it.
        return _PatternStepStack(**options)(metaflow_step(function))

    return decorate if step_function is None else decorate(step_function)
