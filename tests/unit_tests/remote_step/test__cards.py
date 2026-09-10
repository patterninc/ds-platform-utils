"""`current.card` from inside the runner pod (gap 6).

`@card` renders on the driver task, so `current.card` does not exist in the
runner — `current.card.append(...)` raised there, and a step that guarded the
call rendered an empty card anyway. The pod records what the body appends and
the driver replays it into the real card.
"""

import pickle

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
import pytest
from metaflow.cards import Artifact, Markdown, Table
from remote_step.runner_entry import _CardRecorder, _save_card_components


def loaded(recorder, card_id=_CardRecorder.DEFAULT_ID):
    """Unpickle what the recorder captured for one card."""
    return [pickle.loads(b) for b in recorder.pending().get(card_id, [])]


def test_nothing_recorded_by_default():
    assert _CardRecorder().pending() == {}


def test_append_records_a_component():
    rec = _CardRecorder()
    rec.append(Markdown("# hello"))
    assert [type(c).__name__ for c in loaded(rec)] == ["Markdown"]


def test_order_is_preserved():
    """A card is a document — the order the body appended in is the content."""
    rec = _CardRecorder()
    for i in range(4):
        rec.append(Markdown(f"line {i}"))
    assert len(loaded(rec)) == 4


def test_extend_records_each_component():
    rec = _CardRecorder()
    rec.extend([Markdown("a"), Table([[1]], headers=["h"])])
    assert [type(c).__name__ for c in loaded(rec)] == ["Markdown", "Table"]


def test_extend_tolerates_none():
    rec = _CardRecorder()
    rec.extend(None)
    assert rec.pending() == {}


def test_a_named_card_is_recorded_separately():
    """`current.card['gpu_profile']` — @gpu_profile's own card id."""
    rec = _CardRecorder()
    rec.append(Markdown("default"))
    rec["gpu_profile"].append(Markdown("named"))

    pending = rec.pending()
    assert set(pending) == {_CardRecorder.DEFAULT_ID, "gpu_profile"}
    assert [type(c).__name__ for c in loaded(rec, "gpu_profile")] == ["Markdown"]


def test_per_id_views_share_one_sink():
    """One save has to collect every card the body touched."""
    rec = _CardRecorder()
    rec["a"].append(Markdown("x"))
    rec["b"].append(Markdown("y"))
    assert set(rec.pending()) == {"a", "b"}


def test_setitem_replaces_a_card_contents():
    rec = _CardRecorder()
    rec["a"].append(Markdown("old"))
    rec["a"] = [Markdown("new")]
    assert len(loaded(rec, "a")) == 1


def test_clear_empties_a_card():
    rec = _CardRecorder()
    rec.append(Markdown("x"))
    rec.clear()
    assert rec.pending() == {}


def test_refresh_is_a_no_op_that_says_so(capsys):
    """A live refresh cannot reach the driver's card mid-step."""
    rec = _CardRecorder()
    rec.refresh(force=True)
    assert "does nothing in a remote step" in capsys.readouterr().out


def test_refresh_only_warns_once(capsys):
    """Bodies call refresh in a loop; one note is information, many is noise."""
    rec = _CardRecorder()
    for _ in range(5):
        rec.refresh()
    assert capsys.readouterr().out.count("does nothing") == 1


def test_an_unpicklable_component_becomes_an_honest_note():
    """`Artifact` holds a module reference and will not pickle.

    Dropping it silently would leave a hole in the card with no explanation.
    """
    rec = _CardRecorder()
    rec.append(Artifact({"k": "v"}))

    components = loaded(rec)
    assert [type(c).__name__ for c in components] == ["Markdown"]
    assert "Artifact" in str(vars(components[0]))


def test_an_unpicklable_component_does_not_lose_its_neighbours():
    rec = _CardRecorder()
    rec.append(Markdown("before"))
    rec.append(Artifact({"k": "v"}))
    rec.append(Markdown("after"))
    assert len(loaded(rec)) == 3


class FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kwargs):  # noqa: D102
        self.puts.append(kwargs)


def test_nothing_is_written_when_the_body_used_no_card():
    s3 = FakeS3()
    _save_card_components(_CardRecorder(), {"output_prefix": "p", "output_bucket": "b"}, s3_client=s3)
    assert s3.puts == []


def test_recorded_components_are_written_for_the_driver():
    rec = _CardRecorder()
    rec.append(Markdown("# report"))
    s3 = FakeS3()
    _save_card_components(rec, {"output_prefix": "p", "output_bucket": "b"}, s3_client=s3)

    assert len(s3.puts) == 1
    put = s3.puts[0]
    assert put["Key"].endswith("card_components.pkl")
    restored = pickle.loads(put["Body"])
    assert set(restored) == {_CardRecorder.DEFAULT_ID}


@pytest.mark.parametrize("spec", [{}, {"output_prefix": "p"}, {"output_bucket": "b"}])
def test_a_spec_without_an_output_location_writes_nothing(spec):
    rec = _CardRecorder()
    rec.append(Markdown("x"))
    s3 = FakeS3()
    _save_card_components(rec, spec, s3_client=s3)
    assert s3.puts == []


# ----------------------------------------- attribute-rendering cards (type=html)


class Deco:
    def __init__(self, name, **attributes):
        self.name = name
        self.attributes = attributes


def test_a_card_that_renders_an_attribute_is_detected():
    """`@card(type="html", options={"attribute": "html"})` reads self.html."""
    from remote_step.plugins.remote_step_decorator import _find_card_attributes

    decos = [Deco("card", type="html", id="dqv_report", options={"attribute": "html"})]
    assert _find_card_attributes(decos) == {"html"}


def test_a_plain_card_names_no_attribute():
    from remote_step.plugins.remote_step_decorator import _find_card_attributes

    assert _find_card_attributes([Deco("card", type="blank")]) == set()
    assert _find_card_attributes([Deco("resources", cpu=2)]) == set()


def test_several_cards_contribute_their_attributes():
    from remote_step.plugins.remote_step_decorator import _find_card_attributes

    decos = [
        Deco("card", type="html", options={"attribute": "html"}),
        Deco("card", type="json", options={"attribute": "summary"}),
    ]
    assert _find_card_attributes(decos) == {"html", "summary"}


class FakeRef:
    """Stands in for a RemoteArtifact ref."""

    def __init__(self, value, size_bytes):
        self._value = value
        self.size_bytes = size_bytes
        self.loads = 0

    def load(self):
        self.loads += 1
        return self._value


def test_a_small_card_attribute_is_loaded(monkeypatch, capsys):
    """Otherwise the card renders `RemoteArtifact(...)` instead of the report."""
    import remote_step.plugins.remote_step_decorator as deco_mod

    monkeypatch.setattr(deco_mod, "RemoteArtifact", FakeRef)
    ref = FakeRef("<h1>report</h1>", 2048)

    assert deco_mod._hydrate_for_card("html", ref) == "<h1>report</h1>"
    assert ref.loads == 1
    assert "loaded 'html' for its @card" in capsys.readouterr().out


def test_a_huge_card_attribute_is_left_as_a_reference(monkeypatch, capsys):
    """The driver is Small tier — loading a 10 GB artifact would OOM it."""
    import remote_step.plugins.remote_step_decorator as deco_mod

    monkeypatch.setattr(deco_mod, "RemoteArtifact", FakeRef)
    ref = FakeRef("huge", deco_mod.MAX_CARD_ATTR_BYTES + 1)

    assert deco_mod._hydrate_for_card("df", ref) is ref
    assert ref.loads == 0
    out = capsys.readouterr().out
    assert "left as a reference" in out


def test_a_failed_load_falls_back_to_the_reference(monkeypatch, capsys):
    """A card is a report; failing to render one must not fail the step."""
    import remote_step.plugins.remote_step_decorator as deco_mod

    class Exploding(FakeRef):
        def load(self):
            raise RuntimeError("s3 down")

    monkeypatch.setattr(deco_mod, "RemoteArtifact", Exploding)
    ref = Exploding("x", 10)

    assert deco_mod._hydrate_for_card("html", ref) is ref
    assert "could not load 'html'" in capsys.readouterr().out


def test_a_non_reference_value_passes_through(monkeypatch):
    import remote_step.plugins.remote_step_decorator as deco_mod

    monkeypatch.setattr(deco_mod, "RemoteArtifact", FakeRef)
    assert deco_mod._hydrate_for_card("html", "already a value") == "already a value"


def test_the_gpu_profile_card_is_cleared_before_replay(monkeypatch):
    """@gpu_profile's wrapper fills that card on the driver, which has no GPU.

    It cannot be dropped — it is a user_step_decorator, absent from the list
    step_init sees — so it writes "Drivers: unknown / unknown" and "No GPU
    devices found" at task start. Clearing first leaves only the real readings.
    """
    import remote_step.plugins.remote_step_decorator as deco_mod
    from remote_step.runner_entry import CARD_COMPONENTS_FILENAME  # noqa: F401

    calls = []

    class FakeCardManager:
        def __init__(self, card_id):
            self.card_id = card_id

        def clear(self):
            calls.append(("clear", self.card_id))

        def append(self, component):
            calls.append(("append", self.card_id))

    class FakeCollector:
        def __getitem__(self, card_id):
            return FakeCardManager(card_id)

        def append(self, component):
            calls.append(("append", "_default"))

        def refresh(self, force=False):
            pass

    class FakeS3:
        def get_object(self, Bucket, Key):  # noqa: N803
            body = pickle.dumps({"gpu_profile": [pickle.dumps(Markdown("real numbers"))]})

            class B:
                def read(self_inner):
                    return body

            return {"Body": B()}

    monkeypatch.setattr("metaflow.current.card", FakeCollector(), raising=False)
    deco_mod._replay_card_components("bucket", "prefix", s3_client=FakeS3())

    assert ("clear", "gpu_profile") in calls
    assert calls.index(("clear", "gpu_profile")) < calls.index(("append", "gpu_profile"))
