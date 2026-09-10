"""Proving a value is picklable must not cost a copy of it.

`_collect_flow_attrs` pickled every candidate attribute to a bytes object
purely to see whether it *could* be pickled, then dropped the result --
and `payload.build_spec` pickled it again to actually ship it. On the
Small-tier driver (2 vCPU, 8 GB) a 3 GB DataFrame therefore peaked at ~3 GB
before the real work began, which is enough to OOM the driver on an artifact
it was only inspecting.
"""

import pickle
import threading
import tracemalloc

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.plugins.remote_step_decorator import (
    _NullSink,
    _is_picklable_streaming,
    _pickleable,
)


def test_the_probe_keeps_nothing():
    """The point of the change, measured rather than asserted by inspection."""
    payload = bytearray(16 * 1024 * 1024)

    tracemalloc.start()
    try:
        _is_picklable_streaming(payload)
        streaming_peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.reset_peak()
        pickle.dumps(payload, protocol=5)
        dumps_peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()

    assert streaming_peak < len(payload) // 8, streaming_peak
    assert dumps_peak > len(payload), dumps_peak


def test_a_picklable_value_is_accepted():
    assert _is_picklable_streaming({"a": [1, 2, 3], "b": (4, 5)})


def test_an_unpicklable_value_is_rejected():
    """A lock is the classic one to find nested inside a user's object."""
    assert not _is_picklable_streaming(threading.Lock())


def test_a_value_holding_an_unpicklable_member_is_rejected():
    class Holder:
        def __init__(self):
            self.lock = threading.Lock()

    assert not _is_picklable_streaming(Holder())


def test_none_and_falsey_values_are_picklable():
    """`if not _is_picklable(...)` must not confuse "falsey" with "cannot"."""
    for value in (None, 0, "", [], {}, False):
        assert _is_picklable_streaming(value), repr(value)


def test_the_cheap_type_check_still_rejects_modules_and_functions():
    """Unchanged; the streaming probe is the layer after it."""
    assert not _pickleable(threading)
    assert not _pickleable(lambda: None)
    assert _pickleable({"a": 1})


def test_the_sink_discards_and_reports_nothing():
    sink = _NullSink()
    assert sink.write(b"anything") is None


def test_a_generator_is_rejected_rather_than_consumed():
    """Pickling a generator raises; it must not be silently drained."""
    gen = (i for i in range(3))
    assert not _is_picklable_streaming(gen)
    assert list(gen) == [0, 1, 2]
