"""Runs inside the job container, whichever service started it. Never imported on the Metaflow side.

Every compute service hands a container its input differently -- SageMaker mounts S3 channels,
Batch and ECS mount nothing at all -- so relying on any of those conventions ties the runtime to
one backend. Instead this takes three S3 URIs from the environment and does its own transport:

- ``REMOTE_STEP_PAYLOAD_URI``  the pickled call
- ``REMOTE_STEP_CODE_URI``     a zip of the packages the body imports
- ``REMOTE_STEP_RESULT_URI``   where to put the pickled result

That is the entire contract. A backend that can run a container with environment variables and an
IAM role that reads and writes S3 can use this unchanged, which is what makes adding Batch, ECS or
anything else a matter of submitting a job rather than writing a new runtime.

Sequence: fetch the payload, unpack the code onto ``sys.path``, install requested packages, run
the body via :func:`ds_platform_utils.metaflow.external_compute.execute_remote_step`, upload the result.

Failures are deliberately loud and unswallowed: the traceback goes to stdout, which is where
CloudWatch -- and therefore the Metaflow step's log stream -- picks it up.
"""

import os
import pickle
import subprocess
import sys
import zipfile
from pathlib import Path

CODE_DIR = Path("/opt/ml/code")
WORK_DIR = Path("/tmp/remote-step")

PAYLOAD_URI_VAR = "REMOTE_STEP_PAYLOAD_URI"
CODE_URI_VAR = "REMOTE_STEP_CODE_URI"
RESULT_URI_VAR = "REMOTE_STEP_RESULT_URI"


def _split_uri(uri: str) -> tuple:
    """Split an ``s3://bucket/key`` URI.

    :param uri: The URI.
    :return: ``(bucket, key)``.
    """
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


def _download(client, uri: str, destination: Path) -> Path:
    """Fetch an object to a local path.

    :param client: A boto3 S3 client.
    :param uri: Source ``s3://`` URI.
    :param destination: Where to write it.
    :return: The destination.
    """
    bucket, key = _split_uri(uri)
    destination.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, str(destination))
    return destination


def _install(requirements: list) -> None:
    """Install pip requirements before the body runs.

    :param requirements: pip requirement strings.
    """
    if not requirements:
        return
    print(f"[remote_step] installing {len(requirements)} package(s)", flush=True)
    # No --upgrade. The list is mostly the flow's own @pypi_base packages, and the image is built
    # to already satisfy the common ones -- pip skips those in seconds, which is the whole reason
    # inheriting the flow's packages is affordable. Anything that must be a specific version says
    # so: the backend pins cloudpickle explicitly, and pip honours a pin without --upgrade.
    subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", *requirements], check=True)


def _unpack_code(archive: Path) -> None:
    """Extract the shipped packages and make them importable.

    :param archive: Zip written by the backend, one top-level directory per package.
    """
    CODE_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as zipped:
        zipped.extractall(CODE_DIR)
    sys.path.insert(0, str(CODE_DIR))
    print(f"[remote_step] code on sys.path: {sorted(p.name for p in CODE_DIR.iterdir())}", flush=True)


def main() -> None:
    """Fetch, unpack, install, run the body, and upload the result."""
    import boto3

    missing = [name for name in (PAYLOAD_URI_VAR, CODE_URI_VAR, RESULT_URI_VAR) if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"container started without {', '.join(missing)}; the backend must set all three")

    client = boto3.client("s3")
    print(f"[remote_step] fetching payload from {os.environ[PAYLOAD_URI_VAR]}", flush=True)

    payload_path = _download(client, os.environ[PAYLOAD_URI_VAR], WORK_DIR / "payload.pkl")
    code_path = _download(client, os.environ[CODE_URI_VAR], WORK_DIR / "code.zip")

    payload = pickle.loads(payload_path.read_bytes())
    _unpack_code(code_path)
    _install(payload.get("pip_requirements", []))

    # Imported only once the shipped code is on sys.path.
    from ds_platform_utils.metaflow.external_compute import execute_remote_step

    result = execute_remote_step(
        payload["fn_bytes"],
        payload["inputs"],
        payload["write_names"],
        payload["step_names"],
        payload["path_bundle"],
        payload["path_map"],
        tuple(payload["python_version"]),
    )

    bucket, key = _split_uri(os.environ[RESULT_URI_VAR])
    client.put_object(Bucket=bucket, Key=key, Body=result)
    print(f"[remote_step] result written to {os.environ[RESULT_URI_VAR]}", flush=True)


if __name__ == "__main__":
    main()
