# `enable_uv_pypi_solver`

Source: `ds_platform_utils.metaflow.uv_pypi_solver`

Has Metaflow resolve `@pypi` environments with **uv** instead of pip. Same environment, same
cache, same remote bootstrap — just a much faster solve.

## Start here

Call it once, before the flow runs. Import time is early enough, since Metaflow only builds
environments after the module is loaded:

```python
from metaflow import FlowSpec, step

from ds_platform_utils.metaflow import enable_uv_pypi_solver, uv_pypi_base

enable_uv_pypi_solver()


@uv_pypi_base
class MyFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass
```

Nothing else changes. The decorators, the `--environment=pypi` flag, the images that get baked
and the tasks that run against them are all untouched.

It returns whether uv is now doing the resolving — `False` means nothing was changed and
Metaflow carries on with pip, because either there is no `uv` binary on `PATH` or the off
switch is set.

## What it replaces

Metaflow builds a `@pypi` environment in four steps and hands the first three to a `Pip` object
it constructs in `CondaEnvironment.validate_environment`. This swaps that class:

| Step | Before | After |
| --- | --- | --- |
| **solve** — resolve the dependency graph for the target platform | `pip install --dry-run --report` | `uv pip compile --format pylock.toml` |
| **download** — fetch the wheels | pip | pip, unchanged |
| **create** — install them into the environment | `pip install` | `uv pip install` |
| **cache** — upload the wheels to the datastore | Metaflow | Metaflow, unchanged |

The download step is deliberately left alone: it fetches wheels rather than resolving them, and
Metaflow's version already handles building direct references and the private-index credentials
pip is configured with.

The remote side is untouched too. A task still bootstraps from the wheels cached in the
datastore, so nothing about this has to be installed or available in the container.

## Why it is faster

pip re-resolves the whole graph over the network for every environment it has not seen before.
Measured on this repo's own dependency set — 43 resolved packages, `linux-64` from macOS:

| | Cold | Warm |
| --- | --- | --- |
| pip | 3.09s | 1.92s |
| uv | 0.56s | 0.37s |

Both produced the **same 43 wheel URLs**, with one benign exception: for `cryptography`, uv
chose the `manylinux2014` wheel where pip chose the `manylinux_2_28` build of the same version.
Both install; uv's is the more portable of the two.

That saving is per environment, per `run` — on every launch where the environment has changed.

## Resolving for the platform being baked

Metaflow bakes remote images for Linux, so the environment has to be resolved for a platform
that is usually not the one launching the flow. uv is told which one explicitly:

| Metaflow platform | uv `--python-platform` |
| --- | --- |
| `linux-64` | `x86_64-manylinux_2_38` |
| `linux-aarch64` | `aarch64-manylinux_2_38` |
| `osx-64` | `x86_64-apple-darwin` |
| `osx-arm64` | `aarch64-apple-darwin` |

The glibc version is not a constant: it is the newest target uv offers that Metaflow would also
accept a wheel for, read out of Metaflow's own tag list. That keeps uv from resolving to a wheel
the rest of the `@pypi` machinery would later refuse.

uv answers for every Python version **at or above** the one it was asked about, tagging any
package whose answer differs across that range with the marker it holds under. Only the entries
that hold for the interpreter actually being built are kept — otherwise a package resolved one
way for 3.10 and another for 3.11 would be installed twice.

## When it falls back to pip

Silently, per environment, rather than failing the flow:

| Condition | Why |
| --- | --- |
| No `uv` binary found | Nothing to run. |
| A platform uv has no target for | Metaflow supports platforms uv cannot be asked to resolve for. |
| A free-threaded interpreter (`python="3.13t"`) | uv names these differently than Metaflow does. |

## Switches

| Variable | Effect |
| --- | --- |
| `DS_PLATFORM_UTILS_UV_PYPI=0` | Keeps Metaflow on pip without editing the flow — useful to compare a resolve against pip's, or to rule this out while debugging one. |
| `DS_PLATFORM_UTILS_UV_BIN` | Path to a `uv` binary to use instead of the one on `PATH`. |

## Limitations

- **Keyring-authenticated private indexes.** Index URLs configured in pip's config are forwarded
  to uv, but credentials that come from a keyring helper installed *inside* the Conda
  environment — the GCP artifact registry setup Metaflow supports — are not visible to uv, which
  runs as its own binary. Set `DS_PLATFORM_UTILS_UV_PYPI=0` if a flow depends on that.
- **Index strategy.** uv resolves against the first index that has a package; pip picks the best
  match across all of them. uv's default is the safer of the two and is left alone here.
- **Outerbounds fast bakery.** A flow running with `--environment=fast-bakery` resolves in
  Outerbounds' bakery service rather than locally, so none of this applies to it.

## Errors

Everything surfaces as `PipException`, the exception Metaflow already knows how to report, so a
failed resolve reads the same as it did before:

- a package with no wheel for the target platform, with the same "does not currently support
  source distributions" note pip's path gives
- a wheel this environment cannot install, named alongside what uv resolved
- a dependency a remote task could not fetch — a local directory, say — rather than one that
  would fail after the image was baked
