import os
from unittest.mock import MagicMock

import pytest

from ds_platform_utils.sql_utils import get_select_dev_query_tags


def _make_mock_current(tags=None):
    """Create a mock Metaflow current object for query tag tests."""
    mock_current = MagicMock("metaflow.current")
    mock_current.tags = tags if tags is not None else ["ds.domain:testing", "ds.project:unit-tests"]
    mock_current.flow_name = "DummyFlow"
    mock_current.project_name = "dummy-project"
    mock_current.step_name = "dummy-step"
    mock_current.run_id = "123"
    mock_current.username = "tester"
    mock_current.is_production = False
    mock_current.namespace = "user:tester"
    mock_current.is_running_flow = True
    mock_current.card = []
    return mock_current


@pytest.fixture
def patched_current():
    """Provide a mocked Metaflow current object for this test module."""
    mock_current = _make_mock_current()
    os.environ["OB_CURRENT_PERIMETER"] = "default"
    yield mock_current
    os.environ.pop("OB_CURRENT_PERIMETER", None)


def test_get_select_dev_query_tags(patched_current):
    """No warning when both required tags are present."""
    query_tags = get_select_dev_query_tags(current_obj=patched_current)

    assert query_tags["app"] == "testing"
    assert query_tags["workload_id"] == "unit-tests"
    assert query_tags["flow_name"] == "DummyFlow"
    assert query_tags["project"] == "dummy-project"
    assert query_tags["step_name"] == "dummy-step"
    assert query_tags["run_id"] == "123"
    assert query_tags["user"] == "tester"
    assert query_tags["domain"] == "testing"
    assert query_tags["namespace"] == "user:tester"
    assert query_tags["perimeter"] == "default"
    assert query_tags["is_production"] == "False"
    assert query_tags["team"] == "data-science"


def test_get_select_dev_query_tags_missing_required_tags_prints_warning(capsys):
    """Print warning and return defaults when ds.domain / ds.project tags are absent."""
    mock_current = _make_mock_current(tags=[])
    os.environ["OB_CURRENT_PERIMETER"] = "default"

    query_tags = get_select_dev_query_tags(current_obj=mock_current)
    captured = capsys.readouterr()

    assert "Warning: ds-platform-utils attempted to add query tags" in captured.out
    assert query_tags["app"] == "unknown"
    assert query_tags["workload_id"] == "unknown"
    assert query_tags["domain"] == "unknown"

    os.environ.pop("OB_CURRENT_PERIMETER", None)


def test_get_select_dev_query_tags_uses_obp_perimeter_fallback():
    """Use OBP_PERIMETER when OB_CURRENT_PERIMETER is not set."""
    mock_current = _make_mock_current()
    os.environ.pop("OB_CURRENT_PERIMETER", None)
    os.environ["OBP_PERIMETER"] = "fallback-perimeter"

    query_tags = get_select_dev_query_tags(current_obj=mock_current)

    assert query_tags["perimeter"] == "fallback-perimeter"

    os.environ.pop("OBP_PERIMETER", None)
