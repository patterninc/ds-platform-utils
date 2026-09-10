"""A RemoteArtifact has to behave like the value it points at.

A `@remote_step` writes its outputs as refs so the driver's memory stays
flat, and a downstream *non-remote* step then holds a ref rather than the
value. The proxy forwarded attribute access, indexing, iteration and
truthiness -- but not comparison, arithmetic or string conversion, so
ordinary downstream code was quietly wrong:

    self.result == 499500     ->  False   (dataclass __eq__ compared refs)
    f"{self.total}"           ->  "RemoteArtifact(kind=..., uri=...)"
    self.count > 100          ->  TypeError
    self.total + 1            ->  TypeError

The first two are the dangerous ones: they look like data bugs in the user's
own code, on lines that work in a plain step.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
import pytest

from remote_step.artifact import RemoteArtifact


def ref(value, kind="builtins.int"):
    """A ref that is already loaded, so nothing touches S3."""
    a = RemoteArtifact(s3_uri="s3://bucket/k.pkl", size_bytes=8, kind=kind, sha256="x")
    a._cached = value
    a._loaded = True
    return a


# ------------------------------------------------------------------ equality


def test_equality_against_a_plain_value():
    assert ref(499500) == 499500


def test_inequality_against_a_plain_value():
    assert ref(499500) != 1


def test_equality_between_two_refs_compares_the_reference():
    """What our own code and manifests mean by equality."""
    a = ref(1)
    b = RemoteArtifact(s3_uri="s3://bucket/k.pkl", size_bytes=8, kind="builtins.int", sha256="x")
    assert a == b


def test_two_refs_at_different_uris_differ():
    a = ref(1)
    b = RemoteArtifact(s3_uri="s3://bucket/other.pkl", size_bytes=8, kind="builtins.int", sha256="y")
    assert a != b


def test_a_string_ref_compares_to_a_string():
    assert ref("prod", kind="builtins.str") == "prod"


def test_a_bool_ref_compares_to_a_bool():
    assert ref(True, kind="builtins.bool") == True  # noqa: E712


# ------------------------------------------------------------------- hashing


def test_a_ref_can_be_a_dict_key_lookup():
    """`self.next({True: ...}, condition=...)` needs this."""
    assert ref(True, kind="builtins.bool") in {True: "yes", False: "no"}
    assert {True: "yes", False: "no"}[ref(False, kind="builtins.bool")] == "no"


def test_hash_matches_the_underlying_value():
    assert hash(ref(7)) == hash(7)


# ---------------------------------------------------------------- comparison


@pytest.mark.parametrize(
    ("expr", "want"),
    [
        (lambda r: r > 100, True),
        (lambda r: r >= 499500, True),
        (lambda r: r < 500000, True),
        (lambda r: r <= 499500, True),
        (lambda r: r > 500000, False),
    ],
)
def test_ordering(expr, want):
    assert expr(ref(499500)) is want


# ---------------------------------------------------------------- arithmetic


def test_arithmetic_both_ways():
    r = ref(10)
    assert r + 1 == 11
    assert 1 + r == 11
    assert r - 1 == 9
    assert 20 - r == 10
    assert r * 2 == 20
    assert 2 * r == 20
    assert r / 2 == 5
    assert r // 3 == 3
    assert r % 3 == 1
    assert r**2 == 100
    assert -r == -10
    assert abs(ref(-5)) == 5


def test_conversions():
    assert int(ref(7)) == 7
    assert float(ref(7)) == 7.0
    assert list(range(ref(3))) == [0, 1, 2]  # __index__


def test_summing_refs_like_a_join_would():
    """`sum(i.total for i in inputs)` is the shape that matters."""
    assert sum([ref(1), ref(2), ref(3)]) == 6


# ------------------------------------------------------- string conversion


def test_str_and_format_show_the_value():
    r = ref(499500)
    assert str(r) == "499500"
    assert f"{r}" == "499500"
    assert f"{ref(3.14159, kind='builtins.float'):.2f}" == "3.14"


def test_repr_still_describes_the_reference():
    """A debugger, a traceback or one of our log lines must stay cheap."""
    text = repr(ref(499500))
    assert "RemoteArtifact" in text
    assert "s3://bucket/k.pkl" in text


# ------------------------------------------------------------- unchanged bits


def test_the_original_proxying_still_works():
    r = ref({"a": [1, 2, 3]}, kind="builtins.dict")
    assert r["a"] == [1, 2, 3]
    assert "a" in r
    assert len(r) == 1
    assert bool(r) is True
    assert list(r) == ["a"]


def test_a_ref_is_still_not_callable():
    """callable() must stay False -- artifact filters skip callables."""
    assert not callable(ref(1))
