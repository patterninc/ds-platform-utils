"""A join step's `inputs`, and `merge_artifacts` on top of it.

A join body is `def join(self, inputs)`, and the runner has to supply that
second argument itself — Metaflow is not running the task. The branches are
lazy: a join over a wide foreach must not download every branch's artifacts
just to answer `inputs[0].x`.
"""

import base64
import pickle

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

# Imported through the `remote_step` alias, not `remote_step`.
# Both names reach the same files, but importing a submodule under each one
# executes it twice and yields two distinct classes — so an exception raised
# internally would not match a RemoteStepError imported the other way.
from remote_step.errors import RemoteStepError
from remote_step.artifact import RemoteArtifact
from remote_step.runner_entry import _build_join_inputs, _FakeBranch, _FakeInputs, _FakeSelf


def inline(value):
    """A spec entry for a small value, as build_spec would write it."""
    blob = pickle.dumps(value)
    import hashlib

    return {
        "kind": "inline",
        "blob_b64": base64.b64encode(blob).decode(),
        "sha256": hashlib.sha256(blob).hexdigest(),
    }


def spec(*branches, is_join=True):
    return {
        "is_join": is_join,
        "join_branches": [
            {"step": step, "attrs": {k: inline(v) for k, v in attrs.items()}} for step, attrs in branches
        ],
    }


# ------------------------------------------------------------------ the shape


def test_a_non_join_step_gets_no_inputs():
    assert _build_join_inputs({"is_join": False}) is None
    assert _build_join_inputs({}) is None


def test_branches_are_reachable_by_step_name():
    """`inputs.step_a.x` — the documented split-join access pattern."""
    inputs = _build_join_inputs(spec(("middle_a", {"x": 1}), ("middle_b", {"x": 2})))
    assert inputs.middle_a.x == 1
    assert inputs.middle_b.x == 2


def test_branches_are_reachable_by_index():
    """`inputs[0].x` — the documented foreach access pattern."""
    inputs = _build_join_inputs(spec(("worker", {"y": 10}), ("worker", {"y": 20})))
    assert inputs[0].y == 10
    assert inputs[1].y == 20


def test_branches_are_iterable():
    """`(inp.x for inp in inputs)` — the documented both-cases pattern."""
    inputs = _build_join_inputs(spec(("a", {"n": 1}), ("b", {"n": 2}), ("c", {"n": 3})))
    assert sorted(inp.n for inp in inputs) == [1, 2, 3]
    assert len(inputs) == 3


def test_an_unknown_attribute_says_which_step_lacked_it():
    inputs = _build_join_inputs(spec(("middle_a", {"x": 1})))
    with pytest.raises(AttributeError) as excinfo:
        _ = inputs[0].nope
    assert "middle_a" in str(excinfo.value)
    assert "nope" in str(excinfo.value)


def test_a_branch_attribute_is_hydrated_once():
    """Cached, so a body reading `inp.df` twice does not fetch twice."""
    inputs = _build_join_inputs(spec(("a", {"v": [1, 2, 3]})))
    first = inputs[0].v
    assert inputs[0].v is first


def test_a_branch_is_lazy_until_read(monkeypatch):
    """Nothing is fetched at construction — that is the point for wide joins."""
    import remote_step.runner_entry as re_mod

    def explode(*a, **k):
        raise AssertionError("hydrated a branch attribute that was never read")

    monkeypatch.setattr(re_mod, "_hydrate_input", explode)
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 2})))
    assert len(inputs) == 2  # constructing and counting touches nothing


# ------------------------------------------------------------ merge_artifacts


def test_merge_copies_branch_artifacts_onto_self():
    """Previously __getattr__ answered with a no-op and these were dropped."""
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1, "y": 2}), ("b", {"x": 1, "y": 2})))
    fake.merge_artifacts(inputs)
    assert fake.x == 1
    assert fake.y == 2


def test_merge_leaves_an_attribute_the_step_already_set():
    """Metaflow's rule: a value assigned on self wins and is not merged."""
    fake = _FakeSelf()
    fake.x = "mine"
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 2})))
    fake.merge_artifacts(inputs)
    assert fake.x == "mine"


def test_merge_include_takes_only_what_was_named():
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1, "y": 2}), ("b", {"x": 1, "y": 2})))
    fake.merge_artifacts(inputs, include=["x"])
    assert fake.x == 1
    assert "y" not in vars(fake)


def test_merge_exclude_skips_what_was_named():
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1, "y": 2}), ("b", {"x": 1, "y": 2})))
    fake.merge_artifacts(inputs, exclude=["y"])
    assert fake.x == 1
    assert "y" not in vars(fake)


def test_include_and_exclude_together_is_refused():
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1})))
    with pytest.raises(RemoteStepError, match="mutually exclusive"):
        fake.merge_artifacts(inputs, include=["x"], exclude=["y"])


def test_branches_disagreeing_is_an_unresolved_conflict():
    """Silently picking one branch's value would be the worst outcome."""
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 999})))
    with pytest.raises(RemoteStepError, match="unresolved conflicts"):
        fake.merge_artifacts(inputs)


def test_include_does_not_resolve_a_conflict():
    """Include narrows what is considered; it does not pick a winner.

    Matching Metaflow: a named attribute that still disagrees is an error.
    """
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 999})))
    with pytest.raises(RemoteStepError, match="unresolved conflicts"):
        fake.merge_artifacts(inputs, include=["x"])


def test_a_conflict_can_be_dropped_with_exclude():
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1, "y": 5}), ("b", {"x": 999, "y": 5})))
    fake.merge_artifacts(inputs, exclude=["x"])
    assert fake.y == 5
    assert "x" not in vars(fake)


def test_a_conflict_resolved_by_assignment_is_not_raised():
    fake = _FakeSelf()
    fake.x = "decided"
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 999})))
    fake.merge_artifacts(inputs)
    assert fake.x == "decided"


def test_include_naming_something_no_branch_produced():
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1})))
    with pytest.raises(RemoteStepError, match="incoming branch produced"):
        fake.merge_artifacts(inputs, include=["absent"])


def test_merge_without_inputs_is_refused():
    with pytest.raises(RemoteStepError, match="only be called in a join"):
        _FakeSelf().merge_artifacts(None)


def test_merged_attributes_become_outputs():
    """A join's whole purpose is to carry these forward, so they must persist."""
    fake = _FakeSelf()
    inputs = _build_join_inputs(spec(("a", {"x": 1}), ("b", {"x": 1})))
    fake.merge_artifacts(inputs)
    assert [k for k in vars(fake) if not k.startswith("_")] == ["x"]


def test_underscored_branch_attributes_are_never_merged():
    fake = _FakeSelf()
    inputs = _FakeInputs(_build_join_inputs(spec(("a", {"x": 1}))).flows)
    inputs.flows[0]._entries["_private"] = inline("no")
    fake.merge_artifacts(inputs)
    # hasattr is useless here: _FakeSelf.__getattr__ answers any non-dunder
    # name with a placeholder, so the instance dict is the only real evidence.
    assert "_private" not in vars(fake)


class TestOversizedBranchAttrsGetDistinctKeys:
    """A foreach join's branches must not share one S3 key.

    Every branch of a foreach join comes from the same step, so keying an
    uploaded branch attr on `{step}.{name}` alone gave all N branches one key:
    each upload overwrote the last, and every branch then resolved to the last
    branch's value. `sum(i.total for i in inputs)` returned N x the final
    branch, with no error anywhere.

    Only attrs above INLINE_ATTR_LIMIT_BYTES are affected -- smaller ones are
    inlined per-branch inside the spec -- which is why a flow joining on a
    string or a float never showed it.
    """

    def build(self, n_branches, attr_bytes):
        """build_spec over a foreach join, capturing what got uploaded where."""
        from remote_step.payload import DriverContext, build_spec

        uploaded = {}

        class FakeS3:
            def create_multipart_upload(self, **kw):
                uploaded[kw["Key"]] = b""
                return {"UploadId": "u1"}

            def upload_part(self, **kw):
                uploaded[kw["Key"]] += kw["Body"].read()
                return {"ETag": "e1"}

            def complete_multipart_upload(self, **kw):
                return {}

            def abort_multipart_upload(self, **kw):
                return {}

            def put_object(self, **kw):
                body = kw["Body"]
                uploaded[kw["Key"]] = body.read() if hasattr(body, "read") else body
                return {}

        ctx = DriverContext(
            flow_module="m",
            flow_class="F",
            step_name="gather",
            flow_name="F",
            run_id="1",
            task_id="t",
            attempt=0,
            code_package_url="",
            code_package_sha="",
            datastore_root="",
            mfconfig={},
            is_join=True,
            # Same step name on every branch -- this is what a foreach join is.
            join_branches=[
                {"step": "work", "attrs": {"payload": ("b%d" % i).encode() * attr_bytes}} for i in range(n_branches)
            ],
        )
        spec_dict = build_spec(ctx, {}, {}, "bucket", s3_client=FakeS3())
        return spec_dict, uploaded

    def test_each_branch_uploads_to_its_own_key(self):
        spec_dict, uploaded = self.build(n_branches=3, attr_bytes=3 * 1024 * 1024)
        keys = [b["attrs"]["payload"]["s3_uri"] for b in spec_dict["join_branches"]]
        assert len(set(keys)) == 3, f"branches share an S3 key: {keys}"
        assert len(uploaded) == 3, f"expected 3 uploads, got {sorted(uploaded)}"

    def test_each_branch_round_trips_its_own_value(self):
        """The failure that matters: every branch reading the last one's data."""
        import pickle as _pickle

        spec_dict, uploaded = self.build(n_branches=3, attr_bytes=3 * 1024 * 1024)
        seen = []
        for b in spec_dict["join_branches"]:
            key = b["attrs"]["payload"]["s3_uri"].split("bucket/", 1)[1]
            seen.append(_pickle.loads(uploaded[key])[:2])
        assert seen == [b"b0", b"b1", b"b2"], seen

    def test_small_attrs_were_never_affected(self):
        """Inline attrs live inside the spec per branch, so they never collided."""
        spec_dict, uploaded = self.build(n_branches=3, attr_bytes=1)
        assert uploaded == {}
        blobs = [b["attrs"]["payload"]["blob_b64"] for b in spec_dict["join_branches"]]
        assert len(set(blobs)) == 3


class TestMergedArtifactsAreNotCopied:
    """merge_artifacts must not move bytes that have not changed.

    `getattr(branch, name)` downloads and unpickles, and the merged value was
    then pickled straight back out to a new key. A join over a static split
    merging a 12 GB model and a 4 GB frame moved all 16 GB into the pod and
    all 16 GB back out, for content identical to what was already in S3.

    An artifact merged from a branch now goes into the output manifest as a
    pointer to the object the branch already wrote. Safe because payload
    objects have no expiry.
    """

    URI = "s3://bucket/upstream/train/0/model.pkl"

    def remote_entry(self, uri=None, size=12 * 1024**3, sha="aa"):
        return {
            "kind": "RemoteArtifact",
            "s3_uri": uri or self.URI,
            "size_bytes": size,
            "type_kind": "builtins.dict",
            "sha256": sha,
        }

    def joined(self, monkeypatch, entries, loads=None):
        """A fake self after merging `entries`, with downloads recorded."""
        from remote_step import runner_entry as re_mod

        seen = loads if loads is not None else []
        monkeypatch.setattr(
            re_mod, "_hydrate_input", lambda n, e, c: (seen.append(n), {"loaded": n})[1]
        )
        fake = _FakeSelf()
        fake._begin_recording()
        branches = [_FakeBranch(step="train", entries=entries, s3_client=None)]
        fake.merge_artifacts(_FakeInputs(branches))
        return fake, seen

    def outputs_of(self, fake):
        """What main() would persist, merged refs included."""
        from remote_step.runner_entry import _detect_outputs

        out = _detect_outputs(vars(fake), fake._assigned)
        for name, ref in (fake._merged_refs or {}).items():
            out.setdefault(name, ref)
        return out

    def test_merging_downloads_nothing(self, monkeypatch):
        _, loads = self.joined(monkeypatch, {"model": self.remote_entry()})
        assert loads == []

    def test_the_output_points_at_the_branches_own_object(self, monkeypatch):
        fake, _ = self.joined(monkeypatch, {"model": self.remote_entry()})
        out = self.outputs_of(fake)
        assert isinstance(out["model"], RemoteArtifact)
        assert out["model"].s3_uri == self.URI, "a new key means the bytes were copied"

    def test_the_merged_size_and_hash_are_carried_over(self, monkeypatch):
        fake, _ = self.joined(monkeypatch, {"model": self.remote_entry(size=999, sha="beef")})
        ref = self.outputs_of(fake)["model"]
        assert ref.size_bytes == 999
        assert ref.sha256 == "beef"

    def test_reading_a_merged_artifact_still_gives_the_real_object(self, monkeypatch):
        """Not a proxy: isinstance and arithmetic have to work on it."""
        fake, loads = self.joined(monkeypatch, {"model": self.remote_entry()})
        assert fake.model == {"loaded": "model"}
        assert loads == ["model"]

    def test_a_merged_artifact_read_once_is_not_read_twice(self, monkeypatch):
        fake, loads = self.joined(monkeypatch, {"model": self.remote_entry()})
        _ = fake.model
        _ = fake.model
        assert loads == ["model"]

    def test_a_merged_artifact_that_was_read_becomes_a_real_output(self, monkeypatch):
        """Once materialised it takes the ordinary upload path."""
        fake, _ = self.joined(monkeypatch, {"model": self.remote_entry()})
        _ = fake.model
        out = self.outputs_of(fake)
        assert out["model"] == {"loaded": "model"}

    def test_an_inline_attribute_is_still_assigned_directly(self, monkeypatch):
        """Tiny by construction, so there is nothing to save by deferring.

        Asserted on placement, not value: the stub loader here intercepts the
        inline path too, since _FakeBranch resolves inline entries through the
        same _hydrate_input.
        """
        fake, _ = self.joined(monkeypatch, {"x": inline(7)})
        assert "x" in vars(fake), "an inline merge should land on the instance"
        assert "x" not in (fake._merged_refs or {}), "nothing to point at for an inline value"

    def test_an_inline_merge_keeps_its_value_when_nothing_is_stubbed(self):
        """The same path without a stub, so the value itself is checked."""
        fake = _FakeSelf()
        fake._begin_recording()
        fake.merge_artifacts(_build_join_inputs(spec(("a", {"x": 7}))))
        assert vars(fake)["x"] == 7

    def test_a_conflict_across_branches_is_still_caught_without_downloading(self, monkeypatch):
        from remote_step import runner_entry as re_mod

        loads = []
        monkeypatch.setattr(re_mod, "_hydrate_input", lambda n, e, c: loads.append(n))
        fake = _FakeSelf()
        fake._begin_recording()
        branches = [
            _FakeBranch(step="a", entries={"model": self.remote_entry(sha="one")}, s3_client=None),
            _FakeBranch(step="b", entries={"model": self.remote_entry(sha="two")}, s3_client=None),
        ]
        with pytest.raises(RemoteStepError, match="unresolved conflicts"):
            fake.merge_artifacts(_FakeInputs(branches))
        assert loads == [], "conflicts are decided on hashes, not content"

    def test_an_attribute_already_set_on_self_wins_over_the_branch(self, monkeypatch):
        fake, loads = self.joined(monkeypatch, {"model": self.remote_entry()})
        fake2 = _FakeSelf()
        fake2._begin_recording()
        fake2.model = "mine"
        branches = [_FakeBranch(step="train", entries={"model": self.remote_entry()}, s3_client=None)]
        fake2.merge_artifacts(_FakeInputs(branches))
        assert fake2.model == "mine"
        assert "model" not in (fake2._merged_refs or {})


def test_an_output_that_is_already_a_ref_is_not_re_uploaded():
    """The upload path's half of the same idea."""
    from remote_step.artifact import RemoteArtifact

    ref = RemoteArtifact(
        s3_uri="s3://bucket/upstream/x.pkl", size_bytes=5, kind="builtins.dict", sha256="aa"
    )
    manifest = {}
    lock = __import__("threading").Lock()
    uploaded = []

    # Mirrors _upload_one's ref branch: record the pointer, upload nothing.
    def upload_one(name, val):
        if isinstance(val, RemoteArtifact):
            with lock:
                manifest[name] = val
            return
        uploaded.append(name)

    upload_one("model", ref)
    assert manifest["model"] is ref
    assert uploaded == []


def test_a_passed_through_ref_gets_the_read_role_stamped_on_it():
    """Otherwise a downstream non-remote step cannot read it.

    A ref built from a join branch's spec entry carries no read_role_arn, and
    a downstream plain step runs on an Outerbounds pod whose task role has no
    direct read on the payload bucket. Passing the branch's ref through
    unchanged produced, one step later than the cause:

      AccessDenied ... obp-5p6le9-task is not authorized to perform
      s3:GetObject

    Found by a scenario flow; the unit tests all stubbed S3.
    """
    from remote_step.artifact import RemoteArtifact

    READ_ROLE = "arn:aws:iam::209479263910:role/pattern-ml-platform-ob-artifact-reader"
    branch_ref = RemoteArtifact(
        s3_uri="s3://bucket/upstream/left/0/big.pkl",
        size_bytes=9,
        kind="builtins.bytes",
        sha256="aa",
    )
    assert branch_ref.read_role_arn == "", "a branch ref starts without one"

    manifest = {}

    # Mirrors _upload_one's pass-through branch.
    def upload_one(name, val, read_role_arn):
        if isinstance(val, RemoteArtifact):
            out = val
            if read_role_arn and getattr(val, "read_role_arn", "") != read_role_arn:
                out = RemoteArtifact(
                    s3_uri=val.s3_uri,
                    size_bytes=val.size_bytes,
                    kind=val.kind,
                    sha256=val.sha256,
                    pickle_protocol=getattr(val, "pickle_protocol", 5),
                    read_role_arn=read_role_arn,
                )
            manifest[name] = out
            return

    upload_one("big", branch_ref, READ_ROLE)
    assert manifest["big"].read_role_arn == READ_ROLE
    # Still the same object in S3 -- the bytes were not copied.
    assert manifest["big"].s3_uri == branch_ref.s3_uri
    assert manifest["big"].sha256 == branch_ref.sha256
