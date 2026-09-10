"""`current.run.add_tags(...)` from inside the runner pod.

A real `Run` needs a Metaflow client, and the pod has no credentials for
Outerbounds' metadata service — so the call had nothing to talk to and did
nothing at all. The pod records the intent; the driver, which is
authenticated, performs the write.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
from remote_step.runner_entry import _RunRecorder, _save_run_tags


def test_nothing_recorded_by_default():
    assert _RunRecorder().pending() == {"added": [], "removed": []}


def test_add_tags_records_a_list():
    rec = _RunRecorder()
    rec.add_tags(["alpha", "beta"])
    assert rec.pending()["added"] == ["alpha", "beta"]


def test_add_tag_records_one():
    rec = _RunRecorder()
    rec.add_tag("solo")
    assert rec.pending()["added"] == ["solo"]


def test_a_bare_string_is_not_treated_as_characters():
    """`add_tags("abc")` must not record a, b, c."""
    rec = _RunRecorder()
    rec.add_tags("abc")
    assert rec.pending()["added"] == ["abc"]


def test_remove_tags_records_separately():
    rec = _RunRecorder()
    rec.add_tags(["keep"])
    rec.remove_tags(["drop"])
    assert rec.pending() == {"added": ["keep"], "removed": ["drop"]}


def test_replace_is_a_remove_plus_an_add():
    rec = _RunRecorder()
    rec.replace_tag("old", "new")
    assert rec.pending() == {"added": ["new"], "removed": ["old"]}


def test_duplicates_are_collapsed_so_replay_is_idempotent():
    rec = _RunRecorder()
    rec.add_tags(["x", "x"])
    rec.add_tag("x")
    assert rec.pending()["added"] == ["x"]


def test_order_is_preserved():
    rec = _RunRecorder()
    for tag in ("c", "a", "b"):
        rec.add_tag(tag)
    assert rec.pending()["added"] == ["c", "a", "b"]


def test_non_string_tags_are_stringified():
    rec = _RunRecorder()
    rec.add_tags([1, 2])
    assert rec.pending()["added"] == ["1", "2"]


class FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kwargs):  # noqa: D102
        self.puts.append(kwargs)


def test_nothing_is_written_when_no_tags_were_touched():
    s3 = FakeS3()
    _save_run_tags(
        _RunRecorder(),
        {"output_prefix": "p", "output_bucket": "b"},
        s3_client=s3,
    )
    assert s3.puts == []


def test_recorded_tags_are_written_for_the_driver():
    import json

    rec = _RunRecorder()
    rec.add_tags(["alpha"])
    s3 = FakeS3()
    _save_run_tags(rec, {"output_prefix": "p", "output_bucket": "b"}, s3_client=s3)

    assert len(s3.puts) == 1
    put = s3.puts[0]
    assert put["Bucket"] == "b"
    assert put["Key"].endswith("run_tags.json")
    assert json.loads(put["Body"])["added"] == ["alpha"]


def test_a_spec_without_an_output_location_writes_nothing():
    """Nothing to fail over — the step itself already succeeded."""
    rec = _RunRecorder()
    rec.add_tag("x")
    s3 = FakeS3()
    _save_run_tags(rec, {}, s3_client=s3)
    assert s3.puts == []
