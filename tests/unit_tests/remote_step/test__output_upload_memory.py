"""Uploading an output must not hold the whole pickle in RAM.

Outputs are uploaded _OUTPUTS_PARALLELISM at a time, and each one pickled into
a BytesIO -- one complete copy per upload, on top of the live objects `fake`
still references. A step ending with four 6 GB frames held 24 GB of objects
plus 24 GB of pickles and was OOM-killed at STAGE=persist_outputs: after every
bit of the compute, with nothing saved, and @retry reproduced it exactly.

Spilling above a threshold caps RAM at roughly
_OUTPUTS_PARALLELISM x SPILL_TO_DISK_BYTES regardless of artifact size.
"""

import io
import pickle
import tempfile
import tracemalloc

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.runner_entry import (
    _OUTPUTS_PARALLELISM,
    SPILL_TO_DISK_BYTES,
    _put_pickle,
)


class FakeS3:
    """Accepts an upload and remembers what it was handed."""

    def __init__(self):
        self.uploads = []

    def put_object(self, **kw):
        body = kw.get("Body")
        self.uploads.append(("put_object", body.read() if hasattr(body, "read") else body))
        return {}

    def upload_fileobj(self, fileobj, bucket, key, **kw):
        self.uploads.append(("upload_fileobj", fileobj.read()))
        return None


def peak_pickling_into(make_buf, payload):
    tracemalloc.start()
    tracemalloc.reset_peak()
    buf = make_buf()
    try:
        pickle.dump(payload, buf, protocol=5)
        return tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
        buf.close()


def test_peak_is_bounded_by_the_threshold_not_the_artifact():
    """The property that matters: RAM stops scaling with output size."""
    payload = [bytes(100 * 1024) for _ in range(400)]  # ~40 MB, many writes
    in_memory = peak_pickling_into(io.BytesIO, payload)
    spooled = peak_pickling_into(lambda: tempfile.SpooledTemporaryFile(max_size=4 * 1024 * 1024), payload)

    assert in_memory > 30 * 1024 * 1024, in_memory
    assert spooled < 8 * 1024 * 1024, spooled


def test_the_threshold_is_small_against_a_pod_and_the_parallelism():
    """Four uploads in flight must stay a rounding error on a real pod."""
    worst_case = _OUTPUTS_PARALLELISM * SPILL_TO_DISK_BYTES
    assert worst_case <= 512 * 1024 * 1024, worst_case


def test_a_small_output_round_trips():
    s3 = FakeS3()
    size, sha = _put_pickle({"a": [1, 2, 3]}, "bucket", "key.pkl", s3)
    assert size > 0
    assert len(sha) == 64
    assert len(s3.uploads) == 1
    kind, blob = s3.uploads[0]
    assert pickle.loads(blob) == {"a": [1, 2, 3]}


def test_a_small_output_stays_in_memory():
    """No disk I/O for the overwhelming majority of outputs."""
    buf = tempfile.SpooledTemporaryFile(max_size=SPILL_TO_DISK_BYTES)
    pickle.dump({"small": True}, buf, protocol=5)
    assert buf._rolled is False
    buf.close()


def test_an_output_above_the_threshold_spills():
    buf = tempfile.SpooledTemporaryFile(max_size=1024)
    pickle.dump(bytes(64 * 1024), buf, protocol=5)
    assert buf._rolled is True
    buf.close()


def test_the_sha_matches_the_bytes_that_were_uploaded():
    """A mismatch here is a silent corruption downstream, so pin it."""
    import hashlib

    s3 = FakeS3()
    payload = {"rows": list(range(5000))}
    size, sha = _put_pickle(payload, "bucket", "key.pkl", s3)
    _, blob = s3.uploads[0]
    assert len(blob) == size
    assert hashlib.sha256(blob).hexdigest() == sha


def test_a_large_output_still_round_trips_after_spilling():
    s3 = FakeS3()
    payload = [bytes(64 * 1024) for _ in range(40)]  # a few MB, many writes
    size, sha = _put_pickle(payload, "bucket", "big.pkl", s3)
    _, blob = s3.uploads[0]
    assert len(blob) == size
    assert pickle.loads(blob) == payload
