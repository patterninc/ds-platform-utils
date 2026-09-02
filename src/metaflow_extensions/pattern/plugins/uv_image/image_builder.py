"""Build a Docker image from a Dockerfile held in memory."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# Substrings the Docker CLI uses when it cannot reach the daemon. Matched
# case-insensitively against build output as a backstop for the pre-flight check.
_DAEMON_ERROR_MARKERS = (
    "cannot connect to the docker daemon",
    "failed to connect to the docker api",
    "is the docker daemon running",
    "docker daemon is not running",
    "error during connect",
    "open //./pipe/docker_engine",
)


class DockerError(RuntimeError):
    """Base class for every failure raised by this module."""


class DockerNotFoundError(DockerError):
    """Raised when the `docker` executable is not on PATH."""


class DockerDaemonError(DockerError):
    """Raised when the CLI is installed but the daemon is unreachable."""


class DockerBuildError(DockerError):
    """Raised when the build itself fails (bad instruction, failing RUN, ...)."""

    def __init__(self, message: str, returncode: int, output: str) -> None:
        super().__init__(message)
        self.returncode = returncode
        self.output = output


def _looks_like_daemon_failure(output: str) -> bool:
    lowered = output.lower()
    return any(marker in lowered for marker in _DAEMON_ERROR_MARKERS)


def _assert_daemon_reachable(docker: str, timeout: float = 20.0) -> None:
    """Fail fast if the daemon is not answering, before we start a build."""
    try:
        result = subprocess.run(
            [docker, "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise DockerDaemonError(
            f"The Docker daemon did not respond within {timeout:g}s. It may still be starting up."
        ) from None

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise DockerDaemonError(
            "The docker CLI is installed but cannot reach the daemon. Start Docker "
            "(Docker Desktop, Colima, or `sudo systemctl start docker`) and retry."
            + (f"\n\n{detail}" if detail else "")
        )


def build_image(
    dockerfile: str,
    image_name: str,
    context_dir: str | Path | None = None,
    build_args: dict[str, str] | None = None,
    stream: bool = True,
    check_daemon: bool = True,
    platform: str | None = None,
) -> str:
    """Build a Docker image locally from a Dockerfile given as a string.

    The Dockerfile is piped to `docker build` on stdin, so nothing is written
    into the build context.

    Args:
        dockerfile: Contents of the Dockerfile.
        image_name: Tag for the resulting image, e.g. "myapp:latest".
        context_dir: Build context. Defaults to an empty temp directory, which
            is what you want unless the Dockerfile has COPY/ADD instructions.
        build_args: Values passed through as --build-arg.
        stream: Echo build output to stderr as it arrives. Output is captured
            either way and attached to DockerBuildError.output.
        check_daemon: Run a `docker version` pre-flight so an unreachable
            daemon fails immediately with a clear error. Set False to skip the
            extra round trip when building in a tight loop.
        platform: Target platform, e.g. "linux/amd64". Defaults to the build
            machine's own architecture, which is worth overriding whenever the
            image runs somewhere else -- an image built on an Apple Silicon Mac
            is arm64 and will not start on an amd64 host.

    Returns:
        The image name that was built.

    Raises:
        DockerNotFoundError: `docker` is not installed or not on PATH.
        DockerDaemonError: The daemon is not running or not reachable.
        DockerBuildError: The build ran but failed.
        NotADirectoryError: `context_dir` was given but does not exist.

    """
    docker = shutil.which("docker")
    if docker is None:
        raise DockerNotFoundError(
            "The 'docker' command was not found on PATH. Install Docker and "
            "make sure the CLI is available before building images."
        )

    if context_dir is not None and not Path(context_dir).is_dir():
        raise NotADirectoryError(f"Build context does not exist: {context_dir}")

    if check_daemon:
        _assert_daemon_reachable(docker)

    with tempfile.TemporaryDirectory() as empty_context:
        context = str(context_dir) if context_dir is not None else empty_context

        cmd = [docker, "build", "--tag", image_name, "--file", "-"]
        if platform is not None:
            cmd += ["--platform", platform]
        for key, value in (build_args or {}).items():
            cmd += ["--build-arg", f"{key}={value}"]
        cmd.append(context)

        captured: list[str] = []
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        try:
            try:
                proc.stdin.write(dockerfile)
            except BrokenPipeError:
                pass  # docker bailed out before reading the Dockerfile
            finally:
                proc.stdin.close()

            for line in proc.stdout:
                captured.append(line)
                if stream:
                    sys.stderr.write(line)
                    sys.stderr.flush()

            returncode = proc.wait()
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()

    if returncode != 0:
        output = "".join(captured)
        if _looks_like_daemon_failure(output):
            raise DockerDaemonError(
                "Lost the connection to the Docker daemon during the build. "
                "Check that Docker is still running.\n\n" + output.strip()
            )
        raise DockerBuildError(
            f"docker build failed for image '{image_name}' (exit code {returncode}).\n\n{output.strip()}",
            returncode=returncode,
            output=output,
        )

    return image_name


if __name__ == "__main__":
    try:
        build_image(
            dockerfile="FROM alpine:3.20\nRUN echo hello > /greeting\n",
            image_name="demo:latest",
        )
    except DockerError as exc:
        sys.exit(f"{type(exc).__name__}: {exc}")
