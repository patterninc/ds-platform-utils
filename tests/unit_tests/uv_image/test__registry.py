import re
import textwrap
from pathlib import Path

import pytest

from metaflow_extensions.pattern.plugins.uv_image import registry

#: A lock with a transitive-only package in it. `certifi` is nowhere in the root project's
#: dependencies, which is exactly what makes it useful here.
UV_LOCK = textwrap.dedent("""
    version = 1
    revision = 3
    requires-python = ">=3.10"

    [[package]]
    name = "my-flows"
    version = "0.1.0"
    source = { editable = "." }
    dependencies = [
        { name = "requests" },
    ]

    [[package]]
    name = "requests"
    version = "2.34.2"
    source = { registry = "https://pypi.org/simple" }

    [[package]]
    name = "certifi"
    version = "2026.7.22"
    source = { registry = "https://pypi.org/simple" }
""")


@pytest.fixture
def lock(tmp_path: Path) -> Path:
    path = tmp_path / "uv.lock"
    path.write_text(UV_LOCK)
    return path


def test_hash_is_stable_for_the_same_inputs(lock: Path):
    # an unstable digest would miss the cache on every load and rebuild the image each time
    assert registry.environment_hash(lock, "3.11", None) == registry.environment_hash(lock, "3.11", None)


def test_hash_changes_with_the_interpreter(lock: Path):
    # the same lock on a different Python is a different image
    assert registry.environment_hash(lock, "3.11", None) != registry.environment_hash(lock, "3.12", None)


def test_hash_changes_with_the_group(lock: Path):
    # --group dev installs a different subset of the same lock
    assert registry.environment_hash(lock, "3.11", None) != registry.environment_hash(lock, "3.11", "dev")


def test_hash_changes_when_a_transitive_dependency_moves(lock: Path):
    """The regression this function exists to prevent.

    An earlier version hashed the resolved *direct* dependencies, so bumping an indirect package
    -- `uv lock --upgrade-package certifi` -- left the tag identical while `uv sync --frozen`
    installed something different. The cache then served an image that did not match the lock.
    """
    before = registry.environment_hash(lock, "3.11", None)
    lock.write_text(re.sub(r'(name = "certifi"\nversion = ")[^"]+(")', r"\g<1>99.9.9\g<2>", UV_LOCK))
    assert registry.environment_hash(lock, "3.11", None) != before


def test_hash_is_short_enough_to_read(lock: Path):
    digest = registry.environment_hash(lock, "3.11", None)
    assert len(digest) == registry.TAG_HASH_LENGTH
    assert re.fullmatch(r"[0-9a-f]+", digest)


def test_image_reference_names_the_flow_and_group():
    reference = registry.image_reference("MyFlow", "train", "abc12345")
    assert reference == "public.ecr.aws/l3p3c6o4/outerbounds-images:MyFlow-train-abc12345"


def test_image_reference_labels_an_absent_group():
    # "default" rather than an empty segment, so the tag stays readable
    assert registry.image_reference("MyFlow", None, "abc12345").endswith(":MyFlow-default-abc12345")


def test_image_reference_honours_overrides():
    reference = registry.image_reference("MyFlow", None, "abc12345", alias="other", repository="repo")
    assert reference == "public.ecr.aws/other/repo:MyFlow-default-abc12345"


def test_python_version_prefers_the_pin(tmp_path: Path):
    # .python-version is the exact interpreter uv pinned and built its venv from
    (tmp_path / ".python-version").write_text("3.12.4\n")
    (tmp_path / "pyproject.toml").write_text('[project]\nrequires-python = ">=3.9"\n')
    assert registry.python_version_for(str(tmp_path)) == "3.12.4"


def test_python_version_strips_an_implementation_prefix(tmp_path: Path):
    # uv allows "cpython@3.11"; the image only wants the version
    (tmp_path / ".python-version").write_text("cpython@3.11\n")
    assert registry.python_version_for(str(tmp_path)) == "3.11"


def test_python_version_falls_back_to_the_requires_python_floor(tmp_path: Path):
    # a range says nothing concrete except its floor, which is what the project runs on
    (tmp_path / "pyproject.toml").write_text('[project]\nrequires-python = ">=3.11,<3.13"\n')
    assert registry.python_version_for(str(tmp_path)) == "3.11"


def test_python_version_ignores_upper_bounds(tmp_path: Path):
    # "<3.13" is not a version the project runs on
    (tmp_path / "pyproject.toml").write_text('[project]\nrequires-python = "<3.13"\n')
    import sys

    assert registry.python_version_for(str(tmp_path)) == "%d.%d" % sys.version_info[:2]


def test_missing_repository_says_which_command_to_run():
    """The repository is never created as a side effect of importing a flow module."""

    class NotFound(Exception):
        pass

    class Client:
        class exceptions:  # noqa: N801 - mirrors botocore's client.exceptions shape
            ImageNotFoundException = type("ImageNotFoundException", (Exception,), {})
            RepositoryNotFoundException = NotFound

        def describe_images(self, **_):
            raise NotFound

    class Session:
        def client(self, *_, **__):
            return Client()

    with pytest.raises(RuntimeError, match="aws ecr-public create-repository"):
        registry.image_missing(Session(), "outerbounds-images", "tag")
