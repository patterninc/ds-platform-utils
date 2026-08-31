"""Run a flow's steps against its uv.lock -- locally in your own venv, remotely in a baked image.

Two decorators, mirroring the ones this replaces:

```python
@uv_base
class MyFlow(FlowSpec):
    @step
    def start(self):            # local: runs in the venv that launched the flow
        self.next(self.train)

    @kubernetes(cpu=8)
    @uv(group="train")
    @step
    def train(self):            # remote: runs in an image built from uv.lock + the train group
        self.next(self.end)
```

[`uv_base`][ds_platform_utils.metaflow.uv_base] does the work; [`uv`][ds_platform_utils.metaflow.uv]
only declares a per-step `group` and `lock`, which `uv_base` reads back off the step.

## Local steps are left completely alone

A step with no `@kubernetes` or `@batch` gets nothing attached -- no `@pypi`, no environment built.
It runs in whatever interpreter launched the flow, which under `uv run python flow.py run` is the
project's own uv environment. That is the point: the environment you already have is the one the
step uses, with nothing resolved, downloaded or duplicated.

This is where we deliberately differ from Outerbounds' Fast Bakery. It attaches a `@conda`
fallback to local steps (`docker_environment.py`, `skipped_steps`), which builds a *separate*
local environment. We don't.

## Remote steps get an image, chosen the same way Fast Bakery chooses

Fast Bakery's test is an isinstance check against `KubernetesDecorator` and `BatchDecorator`
(`docker_environment.py::_is_remote_deco`); `@resources` is deliberately not in it, since it is a
sizing hint rather than a placement. `_REMOTE_DECORATORS` below is that same rule by name.

For each remote step the declared *(lock, group)* selects an image built from that lock with that
group installed, and `image=` is merged into the step's existing `@kubernetes` -- preserving the
`cpu`, `memory` and `gpu` it already asked for. A step that named its own image keeps it.

Note that `--with kubernetes` attaches a real `KubernetesDecorator` to every step, and mutators
run after the command line is parsed -- so that flag legitimately turns every step remote and
every step gets the image.

## The tag is a cache key

Images are per *(project, group)*, not per step, so steps sharing a dependency set share one
build. The tag ends in a hash of the resolved environment, so an unchanged lock finds its image
already in the registry and skips the build entirely -- the check is an ECR API call and needs no
Docker. Docker is required only to *create* an image that does not exist yet.
"""

import base64
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

from metaflow import FlowMutator, StepMutator

#: Decorators that move a step off this machine. Matches Fast Bakery's own rule
#: (`_is_remote_deco`), which tests only for Kubernetes and Batch. `resources` is deliberately
#: absent: it sizes a step, it does not place it.
_REMOTE_DECORATORS = frozenset(["kubernetes", "batch"])

#: ECR Public lives in one region regardless of where anything else runs.
_ECR_PUBLIC_REGION = "us-east-1"
_ECR_PUBLIC_HOST = "public.ecr.aws"

#: The sandbox account's ECR Public registry alias.
_DEFAULT_REGISTRY_ALIAS = "l3p3c6o4"

#: One repository holds every flow's image, told apart by tag. Never created automatically --
#: see `_image_missing`.
_DEFAULT_REPOSITORY = "outerbounds-images"

#: Eight hex characters is 32 bits: plenty to tell one lockfile from another, short enough to
#: leave the tag readable.
_TAG_HASH_LENGTH = 8

#: What a step with no declared group is called in a tag.
_NO_GROUP = "default"

#: Scheduler commands that compile the flow into a template, where every step becomes a pod and
#: nothing is local. Paired with `create` -- `trigger` and `list-runs` act on a template that has
#: already been compiled, so they need no image.
_SCHEDULERS = frozenset(["argo-workflows", "step-functions", "airflow"])


def _resolve_project(lock: str | None = None) -> Path:
    """Find the uv project a step's environment comes from.

    Args:
        lock: `None` to search upwards from the working directory, a directory holding `uv.lock`,
            or the path to a `uv.lock` itself.

    Returns:
        The directory holding `uv.lock`.

    Raises:
        FileNotFoundError: no lock was found, or its `pyproject.toml` is missing. A lock is
            meaningless without the pyproject it was resolved from, so both are required here
            rather than midway through a build.

    """
    from .pypi_packages import _find_project_file

    if lock is None:
        lock_path = _find_project_file("uv.lock")
        if lock_path is None:
            raise FileNotFoundError(
                "no uv.lock found in the working directory or any parent. Launch the flow from "
                "inside the project, or pass @uv_base(lock=...) to point at it."
            )
    else:
        expanded = Path(lock).expanduser().resolve()
        lock_path = expanded / "uv.lock" if expanded.is_dir() else expanded
        if not lock_path.is_file():
            raise FileNotFoundError(f"no uv.lock at {lock_path} (from lock={lock!r})")

    if not (lock_path.parent / "pyproject.toml").is_file():
        raise FileNotFoundError(
            f"found {lock_path} but no pyproject.toml beside it; uv needs both to reproduce the environment."
        )
    return lock_path.parent


def _step_spec(step, name: str) -> dict | None:
    """Return a step decorator's attributes, or `None` when it is not on the step.

    Args:
        step: a `MutableStep`
        name: the decorator to look for

    """
    for spec in step.decorator_specs:
        if spec[0] == name:
            return dict(spec[3])
    return None


def _step_is_remote(step) -> bool:
    """Say whether a step executes off this machine.

    Args:
        step: a `MutableStep`

    """
    return any(spec[0] in _REMOTE_DECORATORS for spec in step.decorator_specs)


def _deploying() -> bool:
    """Say whether this invocation is compiling the flow for a scheduler.

    A deployed workflow has no local execution: every step becomes a pod. Metaflow makes that so
    itself, attaching `@kubernetes` to every step -- `argo_workflows_cli.py` calls
    `_attach_decorators(flow, [KubernetesDecorator.name, EnvironmentDecorator.name])`.

    It does that *after* mutators have run, though, so a step that will become a pod still looks
    local here. Fast Bakery does not have this problem because it is an environment, and
    `init_environment` runs after the attachment; a mutator cannot wait that long. Reading the
    command is what is left, and it is enough: the decision is per-invocation, not per-step.

    Without this, deploying a flow whose steps carry no explicit `@kubernetes` would hand those
    steps Metaflow's default image -- which has none of the project's dependencies -- and the
    failure would surface as a ModuleNotFoundError in a pod, minutes from the cause.

    Returns:
        `True` when the flow is being compiled into a scheduler template.

    """
    return "create" in sys.argv and any(scheduler in sys.argv for scheduler in _SCHEDULERS)


def _declared(step, flow_options: dict) -> dict:
    """Merge a step's `@uv` over the flow's `@uv_base`.

    Both default to `None`, so a value set anywhere wins and the step wins over the flow -- the
    same precedence the decorators being replaced used.

    Args:
        step: a `MutableStep`
        flow_options: whatever was passed to `@uv_base`

    """
    step_options = _step_spec(step, "uv") or {}
    return {
        "group": step_options.get("group") or flow_options.get("group"),
        "lock": step_options.get("lock") or flow_options.get("lock"),
    }


def _environment_hash(lock_path: Path, python_version: str, group: str | None) -> str:
    """Fingerprint what the image will contain, so an unchanged lock reuses its image.

    Taken over the raw bytes of `uv.lock`, because that is what `uv sync --frozen` installs from.
    An earlier version hashed the *direct* dependencies instead, which under-invalidated badly:
    the lock records the full closure, so bumping a transitive package -- `uv lock
    --upgrade-package certifi` -- changed the image without changing the hash, and the stale one
    was served from cache. Hashing the file makes any change to the resolution a new tag.

    The interpreter and the group join it because neither is a property of the lock: the same lock
    builds a different image on a different Python, and `--group dev` installs a different subset
    of it.

    The cost is mild over-invalidation -- editing one group's dependencies re-tags the others too,
    since they share the file. That is the safe direction to err.

    Args:
        lock_path: the `uv.lock` the image is built from
        python_version: the interpreter the image is built on
        group: the dependency group installed on top of the runtime dependencies

    """
    digest = hashlib.sha256()
    # read_bytes, not read_text: the exact file is what uv consumes, so encoding and line endings
    # are part of the identity rather than something to normalise away.
    digest.update(lock_path.read_bytes())
    digest.update(b"\0" + python_version.encode())
    digest.update(b"\0" + (group or "").encode())
    return digest.hexdigest()[:_TAG_HASH_LENGTH]


def _aws_session(aws_profile: str | None):
    """Open an AWS session from the ambient credential chain.

    Args:
        aws_profile: a named profile, or `None` to use whatever the environment provides.

    """
    import boto3

    return boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()


def _image_missing(session, repository: str, tag: str) -> bool:
    """Say whether the image still has to be built.

    This is an API call, not a Docker operation -- which is what makes Docker unnecessary in the
    common case where somebody has already pushed the tag.

    Args:
        session: an AWS session
        repository: the ECR Public repository name
        tag: the tag being looked for

    Raises:
        RuntimeError: the repository does not exist. Creating it here is deliberately not done:
            provisioning registry infrastructure as a side effect of importing a flow module is
            worse than being told which command to run.

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


class uv(StepMutator):
    """Declare which uv dependency group and lockfile a step needs.

    Purely declarative: it attaches nothing and changes nothing on its own.
    [`uv_base`][ds_platform_utils.metaflow.uv_base] reads these values back off the step and acts
    on them, so `@uv` is only useful on a flow that also carries `@uv_base`.

    On a local step it has no effect at all -- local steps run in the environment that launched
    the flow. On a remote step the group selects which image the step runs in.

    Example usage:

    ```python
    @kubernetes(cpu=8)
    @uv(group="train")
    @step
    def train(self): ...
    ```

    Args:
        group: dependency group to install on top of the runtime dependencies, as declared under
            `[dependency-groups]` in pyproject.toml. `None` installs the project's dependencies
            only.
        lock: location of the uv.lock to use, either the file or the directory holding it. `None`
            searches upwards from the working directory.

    """

    def init(self, *args, **kwargs):
        """Record the declaration. Metaflow calls this instead of `__init__`."""
        self.group = kwargs.get("group")
        self.lock = kwargs.get("lock")

    def mutate(self, mutable_step) -> None:
        """Do nothing: `uv_base` reads this decorator's arguments and acts on them."""


class uv_base(FlowMutator):
    """Give remote steps an image built from the project's uv.lock, and leave local steps alone.

    Local steps -- anything without `@kubernetes` or `@batch` -- are untouched, so they run in the
    environment that launched the flow. Remote steps get an image containing the locked
    dependencies, with `image=` merged into the `@kubernetes` they already carry.

    Example usage:

    ```python
    @uv_base
    class MyFlow(FlowSpec): ...

    @uv_base(group="train", aws_profile="sandbox")
    class MyFlow(FlowSpec): ...
    ```

    Args:
        group: default dependency group for every step; `@uv(group=...)` overrides it per step.
        lock: default lockfile location; `@uv(lock=...)` overrides it per step.
        aws_profile: AWS profile for the registry. Defaults to the ambient credential chain.
        repository: ECR Public repository holding the images.
        registry_alias: ECR Public registry alias.
        python: interpreter to build on, overriding the project's own pin.

    """

    def init(self, *args, **kwargs):
        """Capture the decorator's arguments. Metaflow calls this instead of `__init__`."""
        self.options: dict = dict(kwargs)

    def mutate(self, mutable_flow) -> None:
        """Build what the remote steps need and point them at it.

        Step decorators are modified here rather than in `pre_mutate`; Metaflow accepts step
        changes from `mutate` and flow-level ones only from `pre_mutate`.

        Args:
            mutable_flow: the flow being decorated.

        """
        from .pypi_packages import _is_running_task

        if _is_running_task():
            # the container is already running and ships no lockfile, so there is nothing to
            # resolve and nothing to build.
            return

        # Deploying makes every step remote, whether or not it says so. See `_deploying`.
        deploying = _deploying()
        remote = [(name, step) for name, step in mutable_flow.steps if deploying or _step_is_remote(step)]
        if not remote:
            # a wholly local flow needs no registry, no credentials and no Docker.
            return

        # `_flow_cls` is private, but MutableFlow exposes no public name and the tag says which
        # flow an image belongs to.
        flow_name = mutable_flow._flow_cls.__name__
        session = _aws_session(self.options.get("aws_profile"))

        # one image per (project, group): steps sharing a dependency set share a build.
        images: dict = {}
        for name, step in remote:
            declared = _declared(step, self.options)
            key = (str(_resolve_project(declared["lock"])), declared["group"])
            if key not in images:
                images[key] = self._ensure_image(flow_name, Path(key[0]), declared["group"], session)
            self._apply_image(step, name, images[key])

    def _ensure_image(self, flow_name: str, project_root: Path, group: str | None, session) -> str:
        """Return the image for one (project, group), building it only if it is not already there.

        Args:
            flow_name: used in the tag, so an image says which flow built it
            project_root: directory holding `uv.lock`
            group: dependency group installed on top of the runtime dependencies
            session: an AWS session

        Raises:
            RuntimeError: the image has to be built and Docker is not available.

        """
        from .pypi_packages import _find_python_version

        # only the interpreter is needed from the project now: the packages are the lock's
        # business, and the lock is hashed whole.
        python_version = self.options.get("python") or _find_python_version(project_root)
        digest = _environment_hash(project_root / "uv.lock", python_version, group)
        tag = f"{flow_name}-{group or _NO_GROUP}-{digest}"
        alias = self.options.get("registry_alias", _DEFAULT_REGISTRY_ALIAS)
        repository = self.options.get("repository", _DEFAULT_REPOSITORY)
        image = f"{_ECR_PUBLIC_HOST}/{alias}/{repository}:{tag}"

        if not _image_missing(session, repository, tag):
            return image

        from ds_platform_utils.docker import build_metaflow_image
        from ds_platform_utils.docker.image_builder import DockerError

        # stderr, not stdout: `argo-workflows create --only-json` writes the template to stdout,
        # and anything printed there would corrupt it for whatever consumes it.
        print(f"@uv_base: {image} not in the registry yet, building it", file=sys.stderr)
        try:
            build_metaflow_image(
                project_root,
                python_version=python_version,
                image_name=image,
                dependency_groups=[group] if group else None,
            )
        except DockerError as error:
            # A hard error, and only reachable when an image is genuinely missing -- a flow whose
            # images are already in the registry never touches Docker. Raised from the build
            # rather than from a `which("docker")` check up front, because the usual failure is a
            # daemon that is installed but not running, which `which` cannot see.
            raise RuntimeError(
                f"{image} has to be built, but Docker is not usable:\n\n"
                f"  {type(error).__name__}: {error}\n\n"
                "Start Docker and retry, or have someone with a working daemon build and push "
                "this tag -- once it is in the registry, nobody else needs Docker for it."
            ) from error

        _push(session, image)
        return image

    def _apply_image(self, step, step_name: str, image: str) -> None:
        """Point a step at its image, whether or not it already has a remote decorator.

        Two cases. A step that wrote its own `@kubernetes` gets `image=` merged into it: read,
        merge, re-add, because `add_decorator` cannot edit one attribute of a decorator already
        there, and replacing it wholesale would drop the `cpu`, `memory` and `gpu` it asked for.

        A step with no remote decorator only reaches here when deploying, where every step becomes
        a pod regardless -- so it gets a `@kubernetes` carrying just the image. Metaflow's own
        later `_attach_decorators` leaves it alone: "statically defined decorators are always
        preferred over runtime decorators" (`decorators.py`), and by then this counts as static.

        Args:
            step: a `MutableStep`
            step_name: used in the message when an explicit image is left alone
            image: the image reference to set

        """
        for name in _REMOTE_DECORATORS:
            attributes = _step_spec(step, name)
            if attributes is None:
                continue
            if attributes.get("image"):
                # the step named its own image and meant it
                print(
                    f"@uv_base: {step_name} already sets image={attributes['image']}, leaving it",
                    file=sys.stderr,
                )
                return
            step.remove_decorator(name)
            step.add_decorator(name, deco_kwargs={**attributes, "image": image})
            return

        # No remote decorator to merge into. Only reachable while deploying, where the step will
        # become a pod anyway and would otherwise be handed the platform default image.
        step.add_decorator("kubernetes", deco_kwargs={"image": image})
