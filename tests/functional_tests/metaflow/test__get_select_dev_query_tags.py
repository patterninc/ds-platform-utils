import types
import warnings

import pytest

from src.ds_platform_utils.metaflow import write_audit_publish


def _make_dummy_current(*, tags):
    """Use SimpleNamespace so we can quickly mock a Metaflow's `current` object attributes used by the `get_select_dev_query_tags()` function."""
    cur = types.SimpleNamespace()
    cur.tags = tags
    cur.flow_name = "DummyFlow"
    cur.project_name = "dummy-project"
    cur.step_name = "dummy-step"
    cur.run_id = "123"
    cur.username = "tester"
    cur.namespace = "user:tester"
    cur.is_production = False
    cur.card = None
    return cur


def test_warns_when_either_required_tag_missing(monkeypatch):
    """Raise warning if either `'ds.domain'` or `'ds.project'` is missing."""
    dummy_current = _make_dummy_current(tags={"ds.project:foo"})
    monkeypatch.setattr(write_audit_publish, "current", dummy_current)

    with pytest.warns(UserWarning, match=r"one or both required Metaflow user tags"):
        write_audit_publish.get_select_dev_query_tags()


def test_warns_when_no_tags(monkeypatch):
    """Raise warning if no tags are present."""
    dummy_current = _make_dummy_current(tags=set())
    monkeypatch.setattr(write_audit_publish, "current", dummy_current)

    with pytest.warns(UserWarning, match=r"one or both required Metaflow user tags"):
        write_audit_publish.get_select_dev_query_tags()


def test_no_warning_when_both_required_tags_present(monkeypatch):
    """No warning when both required tags are present."""
    dummy_current = _make_dummy_current(tags={"ds.domain:operations", "ds.project:myproj"})
    monkeypatch.setattr(write_audit_publish, "current", dummy_current)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        write_audit_publish.get_select_dev_query_tags()
        assert not w  # check no warnings captured
