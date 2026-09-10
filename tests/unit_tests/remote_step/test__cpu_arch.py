"""Which architecture a step runs on.

arm64 is the default — Graviton is ~20% cheaper and usually faster for ML CPU
kernels. The GPU NodePool is amd64 only, so a GPU ask overrides that default
rather than failing on a choice the user never made.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
import pytest

from remote_step.plugins.remote_step_decorator import RemoteStepDecorator


def deco(cpu_arch=None, user_set=()):
    """A decorator instance with attributes filled in like Metaflow would."""
    d = RemoteStepDecorator()
    d.attributes = dict(RemoteStepDecorator.defaults)
    if cpu_arch is not None:
        d.attributes["cpu_arch"] = cpu_arch
    d._user_defined_attributes = set(user_set)
    return d


def test_the_default_is_arm64():
    assert RemoteStepDecorator.defaults["cpu_arch"] == "arm64"


def test_a_cpu_step_stays_on_arm64():
    assert deco()._effective_cpu_arch(gpu=0, step_name="s") == "arm64"


def test_an_explicit_x86_step_stays_on_x86():
    assert deco("x86_64")._effective_cpu_arch(gpu=0, step_name="s") == "x86_64"


def test_a_gpu_ask_falls_back_to_x86(capsys):
    """The GPU NodePool is amd64 only, and arm64 here is only the default."""
    assert deco()._effective_cpu_arch(gpu=1, step_name="train") == "x86_64"
    err = capsys.readouterr().err
    assert "running on x86_64" in err
    assert "amd64 only" in err


@pytest.mark.parametrize("gpus", [1, 2, 8])
def test_any_number_of_gpus_falls_back(gpus):
    assert deco()._effective_cpu_arch(gpu=gpus, step_name="s") == "x86_64"


def test_an_explicit_arm64_with_a_gpu_is_left_to_fail():
    """Asking for something impossible should not be silently rewritten.

    resolve() raises with its own message; this only has to not mask it.
    """
    d = deco("arm64", user_set={"cpu_arch"})
    assert d._effective_cpu_arch(gpu=1, step_name="s") == "arm64"


def test_an_explicit_x86_with_a_gpu_is_untouched():
    d = deco("x86_64", user_set={"cpu_arch"})
    assert d._effective_cpu_arch(gpu=1, step_name="s") == "x86_64"


def test_a_missing_user_defined_set_does_not_crash():
    """Older Metaflow may not populate _user_defined_attributes."""
    d = deco()
    del d._user_defined_attributes
    assert d._effective_cpu_arch(gpu=1, step_name="s") == "x86_64"
