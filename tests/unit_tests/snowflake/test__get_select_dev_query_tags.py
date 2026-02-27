import os
from unittest.mock import MagicMock

import pytest

from ds_platform_utils.sql_utils import get_select_dev_query_tags


@pytest.fixture(scope="module")
def patched_current():
    """Patch Metaflow `current` object for modules used in this test file."""
    mock_current = MagicMock("metaflow.current")
    mock_current.tags = ["ds.domain:testing", "ds.project:unit-tests"]
    mock_current.flow_name = "DummyFlow"
    mock_current.project_name = "dummy-project"
    mock_current.step_name = "dummy-step"
    mock_current.run_id = "123"
    mock_current.username = "tester"
    mock_current.is_production = False
    mock_current.namespace = "user:tester"
    mock_current.is_running_flow = True
    mock_current.card = []
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
