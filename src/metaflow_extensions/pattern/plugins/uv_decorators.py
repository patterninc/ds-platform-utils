"""`@uv` -- give a local step its own uv environment, scoped to a dependency group.

```python
@uv(group="train")
@step
def train(self):        # runs in .metaflow_uv_venvs/<hash>, containing only that group
    ...
```

Without `@uv`, a local step runs in whatever interpreter launched the flow. That is usually the
project's own venv, which `uv sync` populates *including* the dev group -- so a step can import
something that will not exist in a remote image, and the mistake only surfaces after deploying.
`@uv` closes that gap by giving the step an environment built to the same rules the image is.

## Why this is a StepDecorator and not a mutator

Retargeting a local step's interpreter happens in `runtime_step_cli`, which runs on the client
just before the step subprocess is spawned. That hook exists on `StepDecorator` and not on
`StepMutator`/`UserStepDecorator`, so no mutator can do this -- it is the one piece of
ds-platform-utils that has to be a registered Metaflow plugin. Remote steps are still handled by
the `@uv_base` mutator, which bakes them an image; the two halves do not overlap.

## Why it does not require `--environment=`

Core's `@pypi` splits the work: a `MetaflowEnvironment` builds the environment and the decorator
points at it, which is why it insists on `--environment=pypi`. This decorator does both itself,
so a flow needs no environment flag. The venv is created on first use and reused after, keyed on
the project, the group and the lock's contents.
"""

import hashlib
import os
import subprocess
import sys

from metaflow.decorators import FlowDecorator, StepDecorator
from metaflow.exception import MetaflowException

#: Where the per-group virtual environments live, relative to the working directory. One venv per
#: (project, group, lock) so steps asking for different groups never clobber each other -- unlike
#: a plain `uv sync`, which mutates a single project venv.
VENV_ROOT = ".metaflow_uv_venvs"

#: Decorators that move a step off this machine. A local venv is meaningless for those: they run
#: in the image `@uv_base` baked for them.
REMOTE_DECORATORS = frozenset(["kubernetes", "batch"])


class UVException(MetaflowException):
    headline = "uv error"


def _in_remote_task():
    """Say whether this process is a remote task rather than the client that launched one.

    `MF_PATHSPEC` is exported by Metaflow's mflog helper into every *remote* task command, so it
    is present in a Kubernetes pod, Batch job or Argo container and absent from the local worker.
    That distinction is what matters here: a local worker shares the client's working directory
    and can still find the project, while a container cannot.
    """
    return bool(os.environ.get("MF_PATHSPEC"))


def _find_upwards(filename):
    """Search for a file from the working directory upwards.

    Args:
        filename: the file to look for, e.g. `"uv.lock"`

    """
    current = os.getcwd()
    while True:
        candidate = os.path.join(current, filename)
        if os.path.isfile(candidate):
            return candidate
        parent = os.path.dirname(current)
        if parent == current:
            raise UVException(
                "Could not find %s in the current directory or any parent. Pass an explicit "
                "location with @uv(lock=...)." % filename
            )
        current = parent


def _resolve_project(lock=None):
    """Locate the uv project a step's environment comes from.

    A uv.lock is meaningless without the pyproject.toml it was resolved from, so both are located
    together and required together.

    Args:
        lock: `None` to search upwards, a directory holding `uv.lock`, or the lock itself.

    Returns:
        `(project_dir, lock_path)`, both absolute.

    """
    if lock is None:
        lock_path = _find_upwards("uv.lock")
    else:
        expanded = os.path.abspath(os.path.expanduser(lock))
        lock_path = os.path.join(expanded, "uv.lock") if os.path.isdir(expanded) else expanded
        if not os.path.isfile(lock_path):
            raise UVException("No uv.lock found at '%s' (from lock=%r)." % (lock_path, lock))

    project_dir = os.path.dirname(lock_path)
    if not os.path.isfile(os.path.join(project_dir, "pyproject.toml")):
        raise UVException(
            "Found %s but no pyproject.toml beside it. uv needs both to reproduce the environment." % lock_path
        )
    return project_dir, lock_path


def _env_key(project_dir, group, lock_path):
    """Identify a virtual environment by what goes into it.

    The lock's *contents* are hashed, not its path, so editing dependencies produces a new
    environment rather than silently reusing a stale one -- the same reason the image tag hashes
    the lock rather than the resolved dependency list.

    Args:
        project_dir: the project the environment is built from
        group: the dependency group installed on top of the runtime dependencies
        lock_path: the `uv.lock` to hash

    """
    digest = hashlib.sha256()
    digest.update(project_dir.encode())
    digest.update(b"\0" + (group or "").encode())
    with open(lock_path, "rb") as lock:
        digest.update(b"\0" + lock.read())
    return digest.hexdigest()[:12]


def _venv_path(key):
    return os.path.abspath(os.path.join(VENV_ROOT, key))


def _venv_provides_metaflow(interpreter):
    """Say whether the venv already has Metaflow of its own.

    Args:
        interpreter: the venv's python

    """
    import glob

    venv = os.path.dirname(os.path.dirname(interpreter))
    return bool(glob.glob(os.path.join(venv, "lib", "python*", "site-packages", "metaflow")))


def _host_metaflow_pythonpath():
    """Expose the Metaflow running this process to a venv that has none of its own.

    A lock built for a flow has no reason to contain Metaflow, so the venv usually cannot import
    it. Same machine, same files, so pointing at the running installation is cheaper than
    installing a second copy.

    Only safe when the venv really has none. A project that depends on Metaflow -- anything
    pulling in `outerbounds`, which this repo does -- installs it into the venv too, and adding
    the host's copy on top makes every extension visible twice. Metaflow refuses to start at all
    then: "Conflicts in 'metaflow_extensions' files ... define the same configuration module".
    Hence the caller checks `_venv_provides_metaflow` first.

    Remote tasks get Metaflow from the code package, so none of this applies to them.
    """
    import metaflow

    roots = []
    for entry in getattr(metaflow, "__path__", []):
        parent = os.path.dirname(entry)
        if parent not in roots:
            roots.append(parent)

    existing = os.environ.get("PYTHONPATH")
    if existing:
        roots.append(existing)
    return os.pathsep.join(roots)


class UVStepDecorator(StepDecorator):
    """Run this step in its own uv environment.

    Only affects steps that execute locally. A step carrying `@kubernetes` or `@batch` runs in the
    image `@uv_base` built for it, so this decorator stands aside.

    Parameters
    ----------
    group : str, optional, default None
        Dependency group to install on top of `[project.dependencies]`, as declared under
        `[dependency-groups]` in pyproject.toml. None installs the project's dependencies only --
        which is the point: it excludes the dev group your own venv almost certainly has.
    lock : str, optional, default None
        Location of the uv.lock, either the file or the directory holding it. None searches
        upwards from the working directory.
    """

    name = "uv"
    defaults = {"group": None, "lock": None}

    def __init__(self, attributes=None, statically_defined=False, inserted_by=None):
        self.interpreter = None
        self.project_dir = None
        self.lock_path = None
        super().__init__(attributes, statically_defined, inserted_by)

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        """Resolve the project and decide whether this step wants a local venv at all.

        Resolving here rather than at spawn time means a missing or malformed project fails on the
        client, with the step name to hand, instead of midway through launching it.
        """
        self.logger = logger

        if any(deco.name in REMOTE_DECORATORS for deco in decos):
            # the step runs in a baked image; a local venv would be built and never used
            return

        if _in_remote_task():
            # A remote task re-imports the whole flow module, so *every* step's step_init runs
            # inside the pod -- including local ones like this. There is no uv.lock in a container
            # (Metaflow's code package carries only .py files), so resolving one raises and takes
            # the unrelated remote step down with it. Nothing local is needed there anyway:
            # runtime_step_cli only ever runs on the client.
            return

        self.project_dir, self.lock_path = _resolve_project(self.attributes["lock"])
        key = _env_key(self.project_dir, self.attributes["group"], self.lock_path)
        self.interpreter = os.path.join(_venv_path(key), "bin", "python")

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries, ubf_context):
        """Point the step's subprocess at the venv, creating it the first time.

        Created here rather than in `step_init` so a flow only pays for the environments it
        actually runs -- `step_init` fires for every step on every invocation, including `show`.
        """
        if not self.interpreter:
            return

        if not os.path.exists(self.interpreter):
            self._sync()

        # PYTHONNOUSERSITE stops ~/.local leaking into the venv, which would defeat the isolation
        # this decorator exists to provide.
        cli_args.env["PYTHONNOUSERSITE"] = "1"
        if not _venv_provides_metaflow(self.interpreter):
            # only when the venv has none of its own -- see _host_metaflow_pythonpath
            cli_args.env["PYTHONPATH"] = _host_metaflow_pythonpath()
        cli_args.entrypoint[0] = self.interpreter

    def _sync(self):
        """Build the venv with uv, containing exactly the declared group."""
        group = self.attributes["group"]
        target = os.path.dirname(os.path.dirname(self.interpreter))

        command = ["uv", "sync", "--frozen", "--project", self.project_dir]
        # --no-dev is what makes this worth having: it is the difference between the step's
        # environment and the developer's own venv, which uv sync populates *with* dev.
        if group != "dev":
            command.append("--no-dev")
        if group:
            command.extend(["--group", group])

        self.logger("Creating uv environment for step (group=%s)..." % (group or "default"))
        env = dict(os.environ, UV_PROJECT_ENVIRONMENT=target)
        result = subprocess.run(command, env=env, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise UVException(
                "uv sync failed for project '%s'%s.\n\n%s\n%s"
                % (
                    self.project_dir,
                    " (group '%s')" % group if group else "",
                    result.stdout.strip(),
                    result.stderr.strip(),
                )
            )

        if not os.path.exists(self.interpreter):
            raise UVException("uv sync reported success but produced no interpreter at %s" % self.interpreter)


#: Scheduler commands that compile the flow into a template, where every step becomes a pod and
#: nothing is local. Paired with `create` -- `trigger` and `list-runs` act on an already-compiled
#: template and need no image.
SCHEDULERS = frozenset(["argo-workflows", "step-functions", "airflow"])


def _deploying():
    """Say whether this invocation is compiling the flow for a scheduler.

    A deployed workflow has no local execution: every step becomes a pod. Metaflow makes that so
    itself, attaching @kubernetes to every step (argo_workflows_cli.py calls _attach_decorators)
    -- but it does so *after* both flow_init and step_init have run, so a step that will become a
    pod still looks local to us. Only a MetaflowEnvironment runs late enough to observe it, and
    that path demands --environment=. Reading the command is what is left, and it suffices: the
    decision is per-invocation, not per-step.

    Without this, deploying a flow whose steps carry no explicit @kubernetes hands those steps
    Metaflow's default image -- no project dependencies -- and the failure surfaces as a
    ModuleNotFoundError in a pod, far from its cause.
    """
    return "create" in sys.argv and any(scheduler in sys.argv for scheduler in SCHEDULERS)


class UVFlowDecorator(FlowDecorator):
    """Give every remote step an image built from the project's uv.lock.

    Local steps are untouched: they run in the interpreter that launched the flow, or -- if they
    carry `@uv` -- in the venv that decorator builds for them. Only steps going to Kubernetes or
    Batch get an image, and the reference is written into the `@kubernetes` they already carry so
    its cpu/memory/gpu survive.

    Parameters
    ----------
    group : str, optional, default None
        Default dependency group for every step; `@uv(group=...)` overrides it per step.
    lock : str, optional, default None
        Default lockfile location; `@uv(lock=...)` overrides it per step.
    aws_profile : str, optional, default None
        AWS profile for the registry. Defaults to the ambient credential chain.
    repository : str, optional, default "outerbounds-images"
        ECR Public repository holding the images.
    registry_alias : str, optional, default the sandbox alias
        ECR Public registry alias.
    python : str, optional, default None
        Interpreter to build on, overriding the project's own pin.
    """

    name = "uv_base"
    defaults = {
        "group": None,
        "lock": None,
        "aws_profile": None,
        "repository": None,
        "registry_alias": None,
        "python": None,
    }

    def flow_init(self, flow, graph, environment, flow_datastore, metadata, logger, echo, options):
        """Make sure each remote step's image exists, and point the step at it."""
        from .uv_image import registry

        if _in_remote_task() or any(command in sys.argv for command in ("step", "spin-step")):
            # already running a task: the environment is settled and no lockfile is present
            return

        deploying = _deploying()
        targets = [step for step in flow if deploying or self._is_remote(step)]
        if not targets:
            # a wholly local flow needs no registry, no credentials and no Docker
            return

        session = registry.aws_session(self.attributes["aws_profile"])
        images = {}
        for step in targets:
            group, lock = self._declared_for(flow, step)
            project_dir, lock_path = _resolve_project(lock)
            key = (project_dir, group)
            if key not in images:
                images[key] = self._ensure_image(flow, project_dir, lock_path, group, session, logger)
            self._apply_image(step, images[key], logger)

    @staticmethod
    def _is_remote(step):
        return any(deco.name in REMOTE_DECORATORS for deco in step.decorators)

    def _declared_for(self, flow, step):
        """Merge the step's `@uv` over this decorator's defaults.

        A step's own declaration wins; both default to None so any set value takes effect.
        """
        group, lock = self.attributes["group"], self.attributes["lock"]
        for deco in step.decorators:
            if deco.name == "uv":
                group = deco.attributes.get("group") or group
                lock = deco.attributes.get("lock") or lock
        return group, lock

    def _ensure_image(self, flow, project_dir, lock_path, group, session, logger):
        """Return the image for one (project, group), building it only if it is not already there."""
        from .uv_image import build_metaflow_image
        from .uv_image.image_builder import DockerError
        from .uv_image import registry

        python_version = self.attributes["python"] or registry.python_version_for(project_dir)
        digest = registry.environment_hash(lock_path, python_version, group)
        repository = self.attributes["repository"] or registry.DEFAULT_REPOSITORY
        tag = "%s-%s-%s" % (flow.name, group or registry.NO_GROUP, digest)
        image = registry.image_reference(flow.name, group, digest, self.attributes["registry_alias"], repository)

        if not registry.image_missing(session, repository, tag):
            return image

        logger("Building %s ..." % image)
        try:
            build_metaflow_image(
                project_dir,
                python_version,
                image,
                dependency_groups=[group] if group else None,
            )
        except DockerError as error:
            # A hard error, and only reachable when an image is genuinely missing -- a flow whose
            # images are already pushed never touches Docker. Raised from the build rather than a
            # `which("docker")` check, because the usual failure is a daemon that is installed but
            # not running, which `which` cannot see.
            raise UVException(
                "%s has to be built, but Docker is not usable:\n\n  %s: %s\n\n"
                "Start Docker and retry, or have someone with a working daemon build and push "
                "this tag -- once it is in the registry, nobody else needs Docker for it."
                % (image, type(error).__name__, error)
            ) from error

        registry.push(session, image)
        return image

    @staticmethod
    def _apply_image(step, image, logger):
        """Set `image` on the step's remote decorator, adding one when deploying if absent."""
        for deco in step.decorators:
            if deco.name in REMOTE_DECORATORS:
                if deco.attributes.get("image"):
                    logger("%s already sets image=%s, leaving it" % (step.name, deco.attributes["image"]))
                    return
                deco.attributes["image"] = image
                return

        # No remote decorator to write into. Only reachable while deploying, where the step becomes
        # a pod anyway and would otherwise get the platform default image.
        from metaflow import decorators

        decorators._attach_decorators_to_step(step, ["kubernetes"])
        for deco in step.decorators:
            if deco.name == "kubernetes":
                deco.attributes["image"] = image
                return
