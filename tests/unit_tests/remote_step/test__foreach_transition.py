"""A `@remote_step` that itself emits the foreach split.

    self.scenario_list = [...]                       # in a @remote_step body
    self.next(self.process_scenario, foreach="scenario_list")

Every scenario flow puts the foreach *body* on a remote step and emits the
split from a plain one, so this combination -- the splitting step being the
remote one -- was never exercised. excess-inventory's `prepare_scenarios` is
exactly it, and under Argo the run died with

    File ".../argo_workflows_decorator.py", line 122, in task_finished
      json.dump(list(range(flow._foreach_num_splits)), file)
    TypeError: 'NoneType' object cannot be interpreted as an integer

`DAGNode.num_parallel` is 0 on *every* node (graph.py), not None, so the
driver's `if num_parallel is not None` replayed an ordinary foreach as a
parallel one and never passed `foreach=`. Metaflow's next() gates on
`num_parallel >= 1`, so it skipped the foreach block, left
`_foreach_num_splits` at None, and Argo fell over one step later.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct import below
import pytest

from metaflow import FlowSpec, step
from remote_step.plugins.remote_step_decorator import _foreach_next_kwargs


# The value graph.DAGNode assigns every node it builds. Named so that a
# Metaflow change to the default fails here rather than in an Argo pod.
DAGNODE_DEFAULT_NUM_PARALLEL = 0


def test_an_ordinary_foreach_replays_with_foreach():
    """The regression: 0 is not None, so this used to pick num_parallel."""
    assert _foreach_next_kwargs(DAGNODE_DEFAULT_NUM_PARALLEL, "scenario_list") == {"foreach": "scenario_list"}


def test_metaflow_still_reports_zero_for_a_plain_foreach_node():
    """The premise, read off a real graph rather than assumed.

    This is what the driver captures at task_decorate time. If Metaflow ever
    switches the default to None, this fails here instead of in an Argo pod.
    """
    from metaflow.graph import FlowGraph

    node = FlowGraph(ForeachFlow)["start"]
    assert node.type == "foreach"
    assert node.foreach_param == "scenario_list"
    assert node.num_parallel == DAGNODE_DEFAULT_NUM_PARALLEL


def test_a_parallel_foreach_still_replays_with_num_parallel():
    assert _foreach_next_kwargs(4, None) == {"num_parallel": 4}


def test_num_parallel_wins_when_both_are_present():
    assert _foreach_next_kwargs(4, "shards") == {"num_parallel": 4}


def test_none_num_parallel_is_treated_like_zero():
    """getattr(node, 'num_parallel', None) on a node that lacks it."""
    assert _foreach_next_kwargs(None, "shards") == {"foreach": "shards"}


def test_neither_yields_no_kwargs():
    """A linear replay, rather than a foreach with nothing to split on."""
    assert _foreach_next_kwargs(None, None) == {}


class ForeachFlow(FlowSpec):
    """Minimal flow whose `start` splits, as prepare_scenarios does."""

    @step
    def start(self):
        self.scenario_list = [{"key": "a"}, {"key": "b"}, {"key": "c"}]
        self.next(self.work, foreach="scenario_list")

    @step
    def work(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def _replay(flow, **kwargs):
    """Drive next() the way the driver's replay does."""
    flow._current_step = "start"
    flow._transition = None
    flow._foreach_num_splits = None
    flow.next(flow.work, **kwargs)
    return flow._foreach_num_splits


@pytest.fixture
def flow():
    f = ForeachFlow.__new__(ForeachFlow)
    f.scenario_list = [{"key": "a"}, {"key": "b"}, {"key": "c"}]
    return f


def test_the_replay_sets_the_split_count_argo_reads(flow):
    """What Argo's task_finished does with the result must not raise."""
    splits = _replay(flow, **_foreach_next_kwargs(DAGNODE_DEFAULT_NUM_PARALLEL, "scenario_list"))
    assert splits == 3
    assert list(range(splits)) == [0, 1, 2]


def test_the_old_behaviour_left_argo_with_none(flow):
    """An executable record of the defect."""
    splits = _replay(flow, num_parallel=DAGNODE_DEFAULT_NUM_PARALLEL)
    assert splits is None
    with pytest.raises(TypeError):
        range(splits)
