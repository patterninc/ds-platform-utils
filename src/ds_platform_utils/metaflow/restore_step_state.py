from typing import Generic, TypeVar, Union, cast

from metaflow import Flow, FlowSpec, Run
from metaflow import namespace as _namespace

T = TypeVar("T", bound=FlowSpec)


class RestoredFlowState(Generic[T]):
    def __init__(self, run: Run, step_name: str):
        self.run = run
        self._step_name = step_name
        self._step = run[step_name]
        self._task_data = self._step.task.data if self._step.task else None

    def __getattr__(self, item):
        # Access attributes from the task's data
        if self._task_data and hasattr(self._task_data, item):
            return getattr(self._task_data, item)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item}'")


def restore_step_state(
    flow_class: type[T],
    step_to_run: str,
    flow_run_id: Union[str, None] = None,
    namespace: Union[str, None] = None,
) -> T:
    """Restore a Metaflow run and return a typed wrapper around the flow object.

    flow_run_id:
        - if None, defaults to latest run EVEN IF IT FAILED
        - if "latest_successful_run" use latest_successful_run
        - else use the run_id provided

    Behavior:
    - export @secrets as env vars [PENDING ⏳]
    - return an object with the following properties [DONE ✅]
        - anything on self.x in the flow, which would be on Flow("name").run.data will be
          available on self.x [DONE ✅]
        - Other attributes of self. [DONE ✅]
          - tags and system tags [DONE ✅, through self.tags, self.system_tags]
          - identity, e.g. user:vinay.shende@pattern.com [PENDING ⏳, don't know how to get this yet]
          - run and flow and step ids [DONE ✅]
          - whatever else we can grab (eventually list out things) [DONE ✅]
    - from metaflow import current
        - monkeypatched/magic mocked with
            - .run_id
            - .is_production (should always be False)
            - card <-- an object on which you can call any method and it's just a no-op
            - mock any other attributes you can find which make sense
        - we will know if this works if the publish() function works

    supported decorators
    - @secrets: via the secrets=[...] argument. Exports secret's key-val pairs as env vars

    Unsupported decorators
    - @pypi: instead, simply have the libs you need installed wherever this fn is run
    - @resources: this will always use your laptop's resources
    - @step: implicit
    """
    _namespace(namespace)
    flow_name = flow_class.__name__
    flow = Flow(flow_name)
    flow_run_id = (
        ""  # flow_run_id or flow.latest_successful_run.id if flow.latest_successful_run else flow.latest_run.id
    )
    run = flow[flow_run_id]

    if step_to_run is None:
        step_to_run = input(f"Choose a step to restore from ({list(run.steps.keys())}): ")

    return cast(T, RestoredFlowState(run, step_to_run))


from metaflow import FlowSpec, step


class MyFlow(FlowSpec):
    @step
    def start(self):
        import pandas as pd

        self.df = pd.DataFrame({"x": [1, 2, 3]})
        self.next(self.end)

    @step
    def end(self):
        print("Done.")


self = restore_step_state(MyFlow, flow_run_id="1685226790169915", step_to_run="start")

# ✅ You will now get autocompletion for state.df
print(self.df)
