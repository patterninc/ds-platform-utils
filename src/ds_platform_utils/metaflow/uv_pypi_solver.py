"""Resolve a Metaflow `@pypi` environment with uv instead of pip.

Metaflow builds a `@pypi` environment in four steps -- solve, download, create, cache -- and
hands the first three to a `Pip` object that `CondaEnvironment.validate_environment` constructs
on the fly. The solve is the slow one: pip re-resolves the whole dependency graph from the
network on every environment it has not seen before.

[`UVPip`][ds_platform_utils.metaflow.uv_pypi_solver.UVPip] is a drop-in replacement for that
object. It answers the same four calls, but runs `uv pip compile` for the solve and
`uv pip install` for the install. Everything else about `@pypi` is untouched: metaflow still
downloads the wheels, still caches them in the datastore, and the remote task still bootstraps
from that cache exactly as before. Nothing about the resulting environment changes -- only who
computes it, and how fast.

[`enable_uv_pypi_solver`][ds_platform_utils.metaflow.enable_uv_pypi_solver] is how a flow turns
it on, before metaflow builds the environment:

```python
from ds_platform_utils.metaflow import enable_uv_pypi_solver, uv_pypi_base

enable_uv_pypi_solver()

@uv_pypi_base
class MyFlow(FlowSpec): ...
```

uv is asked to resolve for the platform the image is baked for -- `--python-platform` and
`--python-version` -- rather than for the machine launching the flow, which is what lets a Mac
resolve a linux-64 environment. Its answer comes back as a PEP 751 `pylock.toml`, which records
the resolved wheel URL and hash for every package, so metaflow gets the same
`{"url": ..., "require_build": ...}` list it gets from pip's `--report`.

Where uv cannot answer, this falls back to pip rather than failing the flow: no uv on `PATH`, a
platform metaflow has tags for but uv has no target for, or a free-threaded interpreter.

Known gaps, both of which fall back to pip's behaviour rather than breaking:

- private indexes whose credentials come from a keyring helper installed *inside* the conda
  environment (the GCP artifact registry setup metaflow supports) are not visible to uv, which
  runs as its own binary. Index URLs configured in pip's config *are* forwarded.
- uv resolves against the first index that has a package rather than the best match across all
  of them, which is pip's rule. That is uv's safer default and it is left alone here.
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlsplit

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from metaflow.plugins.pypi.pip import INSTALLATION_MARKER, Pip, PipException
from metaflow.plugins.pypi.utils import pip_tags, wheel_tags

from .pypi_packages import _marker_environment as _base_marker_environment

#: Set to a falsy value to keep metaflow on pip without touching the flow, e.g. to compare a
#: resolve against the one pip produces.
_ENABLED_ENV_VAR = "DS_PLATFORM_UTILS_UV_PYPI"

#: Path to a `uv` binary to use instead of the one on `PATH`.
_UV_BIN_ENV_VAR = "DS_PLATFORM_UTILS_UV_BIN"

_OFF = ("0", "false", "no", "off")

#: `--python-platform` targets uv accepts for macOS, keyed by the conda platform metaflow names
#: the environment with.
_UV_MACOS_TARGETS = {"osx-64": "x86_64-apple-darwin", "osx-arm64": "aarch64-apple-darwin"}

#: ... and the architecture half of the linux ones, whose glibc half is chosen per resolve.
_UV_LINUX_ARCHES = {"linux-64": "x86_64", "linux-aarch64": "aarch64"}

#: glibc versions uv ships a `<arch>-manylinux_2_<minor>` target for -- from
#: `uv pip compile --help`. The one used is the newest of these that metaflow would also accept
#: a wheel for, so uv never resolves to a wheel metaflow's own tag list would reject.
_UV_MANYLINUX_TARGETS = (17, 28, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40)

_MANYLINUX_TAG = re.compile(r"manylinux_2_(\d+)_")

#: `(sys_platform, platform_machine)` for each platform metaflow builds environments for, used
#: to decide which of uv's marker-gated packages belong in this one.
_PLATFORM_MARKERS = {
    "linux-64": ("linux", "x86_64"),
    "linux-aarch64": ("linux", "aarch64"),
    "osx-64": ("darwin", "x86_64"),
    "osx-arm64": ("darwin", "arm64"),
}


def _uv_bin() -> Optional[str]:
    """Locate the uv binary, or `None` when there is none to run."""
    return os.environ.get(_UV_BIN_ENV_VAR) or shutil.which("uv")


def _is_enabled() -> bool:
    """Say whether the off switch has been thrown."""
    return os.environ.get(_ENABLED_ENV_VAR, "1").strip().lower() not in _OFF


def _requirement_lines(packages: dict) -> list:
    """Render `@pypi(packages=...)` as the requirement lines uv reads on stdin.

    Follows metaflow's own rules, so uv is asked for exactly what pip would have been asked
    for: a bare version is a pin, anything opening with a specifier or a direct reference is
    passed through, and an empty version leaves the package unconstrained.

    Args:
        packages: the decorator's `packages` map, e.g. `{"pandas": "2.3.3", "polars": ">=1.0"}`

    Returns:
        One PEP 508 requirement per package.

    """
    lines = []
    for package, version in packages.items():
        if version.startswith(("<", ">", "!", "~", "@")):
            lines.append(f"{package}{version}")
        elif not version:
            lines.append(package)
        else:
            lines.append(f"{package}=={version}")
    return lines


def _uv_python_platform(mamba_platform: str, python_version: str) -> Optional[str]:
    """Translate the platform metaflow builds for into the `--python-platform` uv resolves for.

    macOS maps straight across. Linux does not: uv wants a glibc version baked into the target,
    and picking one too new would let uv choose a wheel that metaflow's own tag list -- the one
    it hands `pip download` later -- would refuse. So the target is the newest glibc uv offers
    that metaflow would still accept a wheel for.

    Args:
        mamba_platform: the conda platform of the environment, e.g. `"linux-64"`
        python_version: the interpreter version resolved into that environment, e.g. `"3.11.9"`

    Returns:
        A uv `--python-platform` value, or `None` when uv has no target for the platform and the
        resolve should go back to pip.

    """
    if mamba_platform in _UV_MACOS_TARGETS:
        return _UV_MACOS_TARGETS[mamba_platform]

    arch = _UV_LINUX_ARCHES.get(mamba_platform)
    if arch is None:
        return None

    ceiling = max(
        (
            int(match.group(1))
            for tag in pip_tags(python_version, mamba_platform)
            for match in [_MANYLINUX_TAG.match(tag.platform)]
            if match
        ),
        default=0,
    )
    target = max((minor for minor in _UV_MANYLINUX_TARGETS if minor <= ceiling), default=None)
    # manylinux2014 is glibc 2.17 under its old name, and every platform metaflow supports has
    # tags for it -- so there is always a target to fall back on.
    return f"{arch}-manylinux_2_{target}" if target else f"{arch}-manylinux2014"


def _marker_environment(python_version: str, mamba_platform: str) -> dict:
    """Describe the environment being built, in the variables PEP 508 markers are written in.

    Args:
        python_version: the interpreter resolved into the environment, e.g. `"3.11.9"`
        mamba_platform: the conda platform being built for, e.g. `"linux-64"`

    Returns:
        A marker environment to evaluate uv's markers against.

    """
    sys_platform, machine = _PLATFORM_MARKERS[mamba_platform]
    environment = _base_marker_environment(python_version, sys_platform)
    # the shared helper assumes the usual architecture for each platform; say it outright,
    # since aarch64 images are the reason a marker would discriminate on it.
    environment["platform_machine"] = machine
    return environment


def _compile_args(
    requirements: list,
    python_platform: str,
    python_version: str,
    indices: tuple,
    interpreter: Optional[str] = None,
) -> list:
    """Build the `uv pip compile` invocation for one environment, minus the output path.

    Args:
        requirements: the lines fed to uv on stdin, from `_requirement_lines`
        python_platform: the uv target from `_uv_python_platform`
        python_version: the interpreter version to resolve for, e.g. `"3.11.9"`
        indices: `(index_url, extra_index_urls)` as metaflow reads them out of pip's config
        interpreter: the environment's own python, used when uv has to build a source
            distribution to read its metadata

    Returns:
        Arguments to pass after the `uv` binary. The caller appends `-o <path>/pylock.toml`;
        uv insists the output file be named that way.

    """
    index_url, extra_index_urls = indices
    args = [
        "pip",
        "compile",
        "-",
        "--format",
        "pylock.toml",
        "--quiet",
        "--no-progress",
        "--python-platform",
        python_platform,
        "--python-version",
        python_version,
    ]
    if interpreter:
        args += ["--python", interpreter]
    if not any("@" in requirement for requirement in requirements):
        # wheels only, the same contract @pypi states -- except for direct references, which
        # are a URL or a git ref that has to be built before there is a wheel to speak of.
        args.append("--only-binary=:all:")
    if index_url:
        args += ["--index-url", index_url]
    for extra in dict.fromkeys(extra_index_urls):
        args += ["--extra-index-url", extra]
    return args


def _run_uv(uv: str, args: list, stdin: Optional[str] = None) -> str:
    """Run uv, turning a failure into the exception metaflow already knows how to report.

    Args:
        uv: path to the uv binary
        args: arguments to pass to it
        stdin: text to feed the process, e.g. the requirements a compile reads from `-`

    Returns:
        Whatever uv wrote to stdout.

    """
    try:
        return subprocess.check_output(
            [uv, *args],
            input=stdin,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "NO_COLOR": "1", "UV_NO_PROGRESS": "1"},
        ).strip()
    except subprocess.CalledProcessError as error:
        stderr = error.stderr or ""
        if "Building source distributions" in stderr or "no wheels are available" in stderr:
            raise PipException(
                "Unable to find a binary distribution compatible with this environment.\n\n"
                "Note: ***@pypi*** does not currently support source distributions\n\n" + stderr
            ) from None
        raise PipException(
            "command '{cmd}' returned error ({code})\n{stderr}".format(
                cmd=" ".join([uv, *args]), code=error.returncode, stderr=stderr
            )
        ) from None


def _wheel_filename(url: str) -> str:
    """Recover the wheel's filename from the URL uv resolved it to."""
    return os.path.basename(unquote(urlsplit(url).path))


def _select_wheel(name: str, wheels: list, tags: list) -> dict:
    """Pick the wheel to install out of the ones uv resolved a package to.

    uv usually narrows a package to a single wheel once a target platform is named, but a
    package can still offer several that all fit -- an abi3 wheel next to a version-specific
    one, say. `tags` is metaflow's own preference order, so this picks what the rest of the
    `@pypi` machinery would have picked.

    Args:
        name: the package name, for the error message
        wheels: the `[[packages]].wheels` entries from the pylock
        tags: `pip_tags(...)` for the environment, most preferred first

    Returns:
        The chosen wheel entry.

    """
    ranking = {tag: rank for rank, tag in enumerate(tags)}
    best = None
    for wheel in wheels:
        filename = wheel.get("name") or _wheel_filename(wheel["url"])
        rank = min((ranking[tag] for tag in wheel_tags(filename) if tag in ranking), default=None)
        if rank is not None and (best is None or rank < best[0]):
            best = (rank, wheel)
    if best is None:
        raise PipException(
            f"uv resolved {name!r} to a wheel this environment cannot install: "
            + ", ".join(_wheel_filename(wheel["url"]) for wheel in wheels)
        )
    return best[1]


def _packages_from_pylock(pylock: str, tags: list, environment: dict) -> list:
    """Read uv's resolution into the package list metaflow's `@pypi` machinery consumes.

    Metaflow describes a resolved package as its download URL plus whether a wheel has to be
    built from it first, which is what pip's `--report` gives it. A PEP 751 lock says the same
    thing in a different shape: a registry package carries its wheels, a git dependency carries
    the commit uv resolved it to, and anything else carries an archive to build.

    uv is asked to resolve for one interpreter but answers for every version at or above it,
    tagging any package whose answer differs across that range with the marker it holds under.
    Only the ones that hold for the interpreter actually being built are kept -- otherwise a
    package resolved one way for 3.10 and another for 3.11 would be installed twice.

    Args:
        pylock: the contents of the `pylock.toml` uv wrote
        tags: `pip_tags(...)` for the environment, used to choose between wheels
        environment: the marker environment from `_marker_environment`

    Returns:
        `[{"url": ..., "require_build": ...}, ...]`, with a `"hash"` on the entries that need
        building -- metaflow stores the wheel it builds under that hash.

    """
    from packaging.markers import Marker

    resolved = []
    seen = set()
    for package in tomllib.loads(pylock).get("packages", []):
        name = package["name"]
        marker = package.get("marker")
        if marker and not Marker(marker).evaluate(environment):
            # resolved for a Python version or platform other than the one being built
            continue
        if name in seen:
            raise PipException(
                f"uv resolved {name!r} to more than one distribution for this environment, which "
                "@pypi has no way to install. This is a bug in how the resolution was requested."
            )
        seen.add(name)
        if package.get("wheels"):
            resolved.append({"url": _select_wheel(name, package["wheels"], tags)["url"], "require_build": False})
        elif "vcs" in package:
            vcs = package["vcs"]
            commit = vcs.get("commit-id") or vcs.get("requested-revision")
            url = "{type}+{url}@{commit}".format(type=vcs["type"], url=vcs["url"], commit=commit)
            if vcs.get("subdirectory"):
                url = f"{url}#subdirectory={vcs['subdirectory']}"
            # the commit both pins the build and, being unique, keeps two builds of the same
            # repo from landing on the same path in the datastore.
            resolved.append({"url": url, "require_build": True, "hash": commit})
        elif "sdist" in package or "archive" in package:
            source = package.get("sdist") or package["archive"]
            resolved.append(
                {
                    "url": source["url"],
                    "require_build": True,
                    "hash": source.get("hashes", {}).get("sha256", package.get("version", name)),
                }
            )
        else:
            raise PipException(
                f"uv resolved {name!r} to something @pypi cannot fetch from a remote task: "
                f"{sorted(key for key in package if key not in ('name', 'version', 'marker'))}. "
                "Only registry, git, url and archive dependencies can be installed remotely."
            )
    return resolved


class UVPip(Pip):
    """Metaflow's pip wrapper, with uv doing the resolving and the installing.

    Only `solve` and `create` are replaced. `download` and `metadata` are inherited untouched:
    they fetch and account for wheels rather than resolve them, and metaflow's versions already
    handle building direct references and the private-index credentials pip is configured with.
    """

    def solve(self, id_, packages, python, platform):
        """Resolve `packages` for `platform` with uv, falling back to pip when uv cannot.

        Args:
            id_: metaflow's hash of the environment being built
            packages: the `@pypi` packages map
            python: the Python version the environment asks for, e.g. `"3.11"`
            platform: the conda platform being resolved for, e.g. `"linux-64"`

        Returns:
            The resolved packages, in the shape metaflow's manifest and cache expect.

        """
        uv = _uv_bin()
        if uv is None or python.endswith("t"):
            # no uv to run, or a free-threaded interpreter, which uv names differently than
            # metaflow does -- pip already handles both.
            return super().solve(id_, packages, python, platform)

        prefix = self.micromamba.path_to_environment(id_)
        if prefix is None:
            raise PipException(f"Unable to locate a Micromamba managed virtual environment\nfor id {id_}")
        resolved_python = self._get_resolved_python_version(prefix)
        if not resolved_python:
            raise PipException("Could not determine Python version from conda environment")

        python_platform = _uv_python_platform(platform, resolved_python)
        if python_platform is None:
            self.logger(f"uv has no target for {platform}, resolving {id_} with pip instead")
            return super().solve(id_, packages, python, platform)

        requirements = _requirement_lines(packages)
        args = _compile_args(
            requirements,
            python_platform,
            resolved_python,
            self.indices(prefix),
            os.path.join(prefix, "bin", "python"),
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            # uv rejects an output file that is not named pylock*.toml.
            pylock = os.path.join(tmp_dir, "pylock.toml")
            _run_uv(uv, [*args, "-o", pylock], stdin="\n".join(requirements))
            return _packages_from_pylock(
                Path(pylock).read_text(),
                pip_tags(resolved_python, platform),
                _marker_environment(resolved_python, platform),
            )

    def create(self, id_, packages, python, platform):
        """Install the downloaded wheels into the environment with `uv pip install`.

        Nothing is resolved or fetched here: every wheel is already on disk, and uv is pointed
        at the environment's own interpreter with the index turned off. Cross-platform
        environments are only ever assembled remotely, so this records that they are done
        without installing anything, exactly as metaflow's own does.

        Args:
            id_: metaflow's hash of the environment being built
            packages: the resolved packages from `solve`
            python: the Python version the environment asks for
            platform: the conda platform the packages were resolved for

        """
        uv = _uv_bin()
        if uv is None:
            return super().create(id_, packages, python, platform)

        prefix = self.micromamba.path_to_environment(id_)
        installation_marker = INSTALLATION_MARKER.format(prefix=prefix)
        metadata = self.metadata(id_, packages, python, platform)
        if os.path.isfile(installation_marker):
            return
        if self.micromamba.platform() == platform:
            args = [
                "pip",
                "install",
                "--python",
                os.path.join(prefix, "bin", "python"),
                "--no-deps",
                "--no-index",
                "--quiet",
                "--no-progress",
                # the wheels live under the environment's own prefix; copying keeps the
                # environment self-contained if that cache is ever cleaned up.
                "--link-mode=copy",
            ]
            _run_uv(uv, args + [metadata[package["url"]] for package in packages])
        with open(installation_marker, "w") as file:
            file.write(json.dumps({"id": id_}))


def enable_uv_pypi_solver() -> bool:
    """Have metaflow resolve `@pypi` environments with uv from here on.

    Call it once, before the flow runs -- importing the flow module is early enough, since
    metaflow only builds environments after that. It swaps the `Pip` class
    `CondaEnvironment.validate_environment` reaches for, which is the one seam that catches
    every `@pypi` and `@conda` environment in the flow without touching the decorators
    themselves.

    Calling it twice is harmless. Setting `DS_PLATFORM_UTILS_UV_PYPI=0` in the environment
    turns it back off without editing the flow.

    Returns:
        Whether uv is now doing the resolving. `False` means nothing was changed -- either the
        off switch is set or there is no uv binary to run -- and metaflow carries on with pip.

    """
    if not _is_enabled() or _uv_bin() is None:
        return False

    from metaflow.plugins.pypi import pip as pip_module

    pip_module.Pip = UVPip
    return True
