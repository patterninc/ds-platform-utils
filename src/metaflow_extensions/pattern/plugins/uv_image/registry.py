###
# Naming, finding and pushing the images a remote step runs in.
#
# No Metaflow imports at module scope: this is reached while Metaflow is still
# resolving plugins, and importing it there re-enters that resolution.
###

import base64
import hashlib
import os
import shutil
import subprocess

#: ECR Public lives in one region regardless of where anything else runs.
ECR_PUBLIC_REGION = "us-east-1"
ECR_PUBLIC_HOST = "public.ecr.aws"

#: The sandbox account's ECR Public registry alias.
DEFAULT_REGISTRY_ALIAS = "l3p3c6o4"

#: One repository holds every flow's image, told apart by tag. Never created automatically --
#: see `image_missing`.
DEFAULT_REPOSITORY = "outerbounds-images"

#: Eight hex characters is 32 bits: plenty to tell one lockfile from another, short enough to
#: leave the tag readable.
TAG_HASH_LENGTH = 8

#: What a step with no declared group is called in a tag.
NO_GROUP = "default"


def environment_hash(lock_path, python_version, group):
    """Fingerprint what the image will contain, so an unchanged lock reuses its image.

    Taken over the raw bytes of `uv.lock`, because that is what `uv sync --frozen` installs from.
    Hashing the *resolved direct dependencies* instead under-invalidates badly: the lock carries
    the full closure, so `uv lock --upgrade-package certifi` changes the image without changing
    the hash, and the stale one gets served from cache.

    The interpreter and the group join it because neither is a property of the lock: the same lock
    builds a different image on a different Python, and `--group dev` installs a different subset.

    Args:
        lock_path: the `uv.lock` the image is built from
        python_version: the interpreter the image is built on
        group: the dependency group installed on top of the runtime dependencies

    """
    digest = hashlib.sha256()
    # the exact file is what uv consumes, so encoding and line endings are part of the identity
    with open(lock_path, "rb") as lock:
        digest.update(lock.read())
    digest.update(b"\0" + python_version.encode())
    digest.update(b"\0" + (group or "").encode())
    return digest.hexdigest()[:TAG_HASH_LENGTH]


def image_reference(flow_name, group, digest, alias=None, repository=None):
    """Build the fully qualified image reference for one (flow, group, lock)."""
    alias = alias or DEFAULT_REGISTRY_ALIAS
    repository = repository or DEFAULT_REPOSITORY
    return "%s/%s/%s:%s-%s-%s" % (
        ECR_PUBLIC_HOST,
        alias,
        repository,
        flow_name,
        group or NO_GROUP,
        digest,
    )


def aws_session(aws_profile=None):
    """Open an AWS session from the ambient credential chain.

    Nothing is stored in this repo or in the image; point at a profile with
    `@uv_base(aws_profile=...)` or `AWS_PROFILE`.
    """
    import boto3

    return boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()


def image_missing(session, repository, tag):
    """Say whether the image still has to be built.

    An API call, not a Docker operation -- which is what makes Docker unnecessary in the common
    case where somebody has already pushed the tag.

    Raises:
        RuntimeError: the repository does not exist. Creating it here is deliberately not done:
            provisioning registry infrastructure as a side effect of importing a flow module is
            worse than being told which command to run.

    """
    client = session.client("ecr-public", region_name=ECR_PUBLIC_REGION)
    try:
        client.describe_images(repositoryName=repository, imageIds=[{"imageTag": tag}])
        return False
    except client.exceptions.ImageNotFoundException:
        return True
    except client.exceptions.RepositoryNotFoundException:
        raise RuntimeError(
            "ECR Public repository '%s' does not exist and will not be created for you. "
            "Create it once with:\n\n"
            "    aws ecr-public create-repository --repository-name %s --region %s\n"
            % (repository, repository, ECR_PUBLIC_REGION)
        ) from None


def push(session, image):
    """Log in to ECR Public and push.

    The registry password is read from the API and piped straight to `docker login` on stdin. It
    is never written to a file, an argument list, or the log.
    """
    docker = shutil.which("docker")
    client = session.client("ecr-public", region_name=ECR_PUBLIC_REGION)
    encoded = client.get_authorization_token()["authorizationData"]["authorizationToken"]
    username, password = base64.b64decode(encoded).decode().split(":", 1)

    login = subprocess.run(
        [docker, "login", "--username", username, "--password-stdin", ECR_PUBLIC_HOST],
        input=password,
        capture_output=True,
        text=True,
        check=False,
    )
    if login.returncode != 0:
        raise RuntimeError("docker login to %s failed: %s" % (ECR_PUBLIC_HOST, login.stderr.strip()))

    result = subprocess.run([docker, "push", image], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError("docker push of %s failed: %s" % (image, result.stderr.strip()))


def python_version_for(project_dir):
    """Read the interpreter a project pins for itself.

    `.python-version` first -- the exact interpreter uv pinned and built its venv from -- then
    `requires-python` in pyproject.toml, whose floor is the only concrete thing in a range.

    Deliberately does not import ds_platform_utils for this. That package's __init__ pulls in
    pandas and snowflake at module scope, and this code runs while Metaflow is resolving plugins.
    """
    pin = os.path.join(project_dir, ".python-version")
    if os.path.isfile(pin):
        with open(pin) as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if line:
                    # uv allows an implementation prefix, e.g. "cpython@3.11"
                    return line.rpartition("@")[2]

    import sys

    if sys.version_info >= (3, 11):
        import tomllib
    else:
        import tomli as tomllib

    pyproject = os.path.join(project_dir, "pyproject.toml")
    if os.path.isfile(pyproject):
        with open(pyproject, "rb") as f:
            requires = tomllib.load(f).get("project", {}).get("requires-python")
        if requires:
            from packaging.specifiers import SpecifierSet
            from packaging.version import Version

            floors = [
                Version(spec.version.rstrip(".*"))
                for spec in SpecifierSet(requires)
                if spec.operator in (">=", "==", "~=")
            ]
            if floors:
                floor = max(floors)
                return "%d.%d" % (floor.major, floor.minor)

    return "%d.%d" % (sys.version_info.major, sys.version_info.minor)
