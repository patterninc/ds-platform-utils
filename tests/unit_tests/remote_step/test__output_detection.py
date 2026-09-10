"""Which attributes travel back from the pod.

Assignment is the rule: the body assigned it, so the step produced it. An
input the step only read is not re-uploaded, which is what keeps a wide
foreach join affordable.

This replaced comparing object identity against the inputs. Identity is an
address and an address can be reused, so a genuine reassignment could compare
equal to its own input and never be uploaded.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct import below

from remote_step.runner_entry import _FakeSelf, _detect_outputs


def seeded(**inputs):
    """A fake self with inputs hydrated and recording armed, as main() does."""
    fake = _FakeSelf()
    for k, v in inputs.items():
        setattr(fake, k, v)
    fake._begin_recording()
    return fake


def outputs_of(fake):
    return _detect_outputs(vars(fake), fake._assigned)


def test_a_new_attribute_is_an_output():
    fake = seeded(df=[1, 2])
    fake.result = 42
    assert outputs_of(fake) == {"result": 42}


def test_an_untouched_input_is_not_an_output():
    """The whole point: no re-upload of data the step only read."""
    fake = seeded(df=[1, 2, 3])
    _ = fake.df  # read it
    assert outputs_of(fake) == {}


def test_a_reassigned_input_is_an_output():
    fake = seeded(df=[1, 2, 3])
    fake.df = [9, 9]
    assert outputs_of(fake) == {"df": [9, 9]}


def test_assigning_none_to_an_input_is_an_output():
    """`self.df = None` must not be dropped.

    A `v is not None` guard used to exclude this before the identity check
    ran, so the driver kept the stale upstream value and downstream steps read
    data the step had explicitly cleared.
    """
    fake = seeded(df=[1, 2, 3])
    fake.df = None
    assert outputs_of(fake) == {"df": None}


def test_freeing_the_input_before_building_the_replacement_is_still_an_output():
    """The address-reuse case that identity comparison got wrong.

    Idiomatic memory management: drop the input so peak memory holds one copy
    rather than two. Under identity comparison the replacement could land on
    the freed object's address, whereupon the attribute compared equal to its
    own input and was never uploaded.
    """
    fake = seeded(df={"data": list(range(50))})
    big = fake.df
    fake.df = None  # free the input
    del big
    fake.df = {"data": list(range(50))}  # may reuse the freed address
    assert "df" in outputs_of(fake)


def test_reassignment_is_detected_even_at_a_recycled_address():
    """Belt and braces: force the identity collision and assert we still catch it."""
    fake = _FakeSelf()
    original = {"payload": list(range(100))}
    addr = id(original)
    setattr(fake, "df", original)
    fake._begin_recording()
    fake.df = None
    del original
    # Allocate until something lands on the original address, then assign it.
    for _ in range(20000):
        candidate = {"payload": list(range(100))}
        if id(candidate) == addr:
            fake.df = candidate
            assert "df" in outputs_of(fake), "reassignment lost at a recycled address"
            return
        del candidate
    # Never collided in this run; the mechanism does not depend on addresses
    # either way, so nothing to assert.


def test_a_new_attribute_set_to_none_is_an_output():
    fake = seeded()
    fake.flag = None
    assert outputs_of(fake) == {"flag": None}


def test_underscore_attributes_never_travel():
    fake = seeded()
    fake._internal = 1
    fake.real = 3
    assert outputs_of(fake) == {"real": 3}


def test_an_input_reassigned_to_an_equal_but_distinct_object_is_an_output():
    """Equality is not the test. A fresh object assigned is a new value."""
    fake = seeded(df=[1, 2, 3])
    fake.df = [1, 2, 3]
    assert "df" in outputs_of(fake)


def test_reassigning_the_same_object_still_counts():
    """`self.df = self.df` is a no-op in content but an explicit assignment."""
    fake = seeded(df=[1, 2, 3])
    fake.df = fake.df
    assert "df" in outputs_of(fake)


def test_in_place_mutation_of_an_input_is_NOT_detected():
    """Documented limitation, asserted so it cannot change silently.

    A normal Metaflow step pickles everything on `self` at task end, so an
    in-place mutation persists there. Here it does not: nothing was assigned.
    Users must rebind (`self.df = self.df.dropna()`).
    """
    fake = seeded(df=[1, 2, 3])
    fake.df.append(4)
    assert outputs_of(fake) == {}


def test_hydrating_inputs_before_recording_does_not_mark_them():
    """Seeding an input is not the step producing it."""
    fake = _FakeSelf()
    setattr(fake, "df", [1, 2, 3])
    fake._begin_recording()
    assert outputs_of(fake) == {}


def test_the_foreach_input_is_not_an_output():
    fake = _FakeSelf(foreach_input="us")
    fake._begin_recording()
    assert fake.input == "us"
    assert outputs_of(fake) == {}
