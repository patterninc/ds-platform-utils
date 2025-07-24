import os
from typing import FrozenSet, Literal, Optional, TypeVar, Union, cast

from metaflow import Flow, FlowSpec, Run, current
from metaflow import namespace as _namespace
from metaflow.plugins.secrets import get_secret

from ds_platform_utils.metaflow.dotdict import convert_to_dotdict


class RestoredFlowState:
    """Container meant to mock the FlowSpec `self` instance in a Metaflow step.

    Anything you can access on `self.` in a Metaflow @step-decorated function
    should be accessible on this class instance.
    """

    def __init__(self, run: Run, step_name: str):
        self.run = run
        self._step_name = step_name
        self._step = run[step_name]
        self._task_data = self._step.task.data if self._step.task else None

    def __getattr__(self, item: str):
        """Proxy access to the `run.data` attributes."""
        if self._task_data and hasattr(self._task_data, item):
            attr = getattr(self._task_data, item)

            # make nested dict[str, Any] accessible using dot notation;
            # use case: self.config and other flow configs
            if isinstance(attr, dict):
                attr = convert_to_dotdict(attr)
            return attr

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item}'")


T = TypeVar("T", bound=FlowSpec)


def restore_step_state(  # noqa: PLR0913 (too-many-arguments; desired in this instance)
    flow_class: Optional[type[T]] = None,
    flow_name: Optional[str] = None,
    # TODO: implement step_name; currently the state from ALL steps are loaded
    # not just the steps before the selected step_name
    step_name: str = "end",
    flow_run_id: Union[Literal["latest_successful_run", "latest"], str] = "latest_successful_run",
    secrets: Optional[list[str]] = None,
    namespace: Union[str, None] = None,
) -> T:
    """Restore a Metaflow run and return a typed wrapper around the flow object.

    flow_class: if provided,
        - the returned value of this function will have autocompletion for the flow class.
          In other words, `self.x` will be recognized by your IDE if `self.x = ...` is assigned
          in the flow. (assuming you set `self = restore_step_state(MyFlow, ...)`)
        - flow_name will default to the class name if flow_name is not provided
    flow_name: if provided, will override the flow_class name.
        This name is used to query the Metaflow metadata service to retrieve the attributes for the
        flow run.
    flow_run_id:
        - if "latest_successful_run", use the latest successful run
        - if "latest", use the latest run (even if it failed)
        - else use the run_id provided
    step_name: the name of the step which will be run. The state will be restored from the
        step BEFORE this step. So that when you run the code from this step line by line,
        it will be a faithful representation of what the state would be if this step were
        running as part of the flow. E.g. "end" would result in `self.` having all of the
        attributes as they were in the step before "end".
    secrets: a list of secrets to export as environment variables. Example
        `secrets=["outerbounds.brain-api-pattern-nlp"]` will export the secrets as environment variables
        so that you can use them in your flow code. In this case `BRAIN_API_KEY=...".
        See the full list of our dev secrets here:
        https://ui.pattern.obp.outerbounds.com/dashboard/integrations/p/default
    namespace: Useful if you want to filter the metaflow namespace considered by this
        function when fetching the flow state. Learn more about metaflow namespaces here:
        https://docs.metaflow.org/scaling/tagging

    Example usage:

    ```python
    from your_flow import YourFlow

    self = restore_step_state(
        YourFlow,
        secrets=["outerbounds.some-secret-in-the-outerbounds-ui"],
    )

    print(f"self.config=")
    print(f"self.df=")
    print(f"self.some_flow_param=")

    # copy/paste step code here and run/debug it line by line!
    # ...
    ```

    supported decorators
    - `@secrets`: via the `secrets=[...]` argument. Exports secret's key-val pairs as env vars
    - `@pypi`: the user should install any needed libs in whichever environment calls this function
    - `@resources`: will use the user's laptop's resources
    - `@step`: implicit
    """
    _namespace(namespace)

    if flow_class is None and flow_name is None:
        raise ValueError("Either flow_class or flow_name must be provided.")

    flow_name = flow_name or flow_class.__name__  # type: ignore
    flow = Flow(flow_name)
    run: Run = _try_get_run(flow, flow_run_id, flow_name)

    if secrets:
        _try_export_secrets_to_env_vars(secrets)

    _patch_current_with_mocked_values(
        flow_name=flow_name,
        flow_run_id=flow_run_id,
        step_name=step_name,
        tags=run.tags,
        namespace=namespace,
    )

    # cast the return type to LIE about the type this function is returning.
    # even though the function is actually returning an object of type RestoredFlowState,
    # we declare that the function is returning an instance of T, AKA the Flow class (flow_class)
    # defined by the data scientist. This is a trick that makes it so attributes on self,
    # e.g. self.x or self.df will be recognized by IDE's that call this function
    # (the example assumes users use `self = restore_step_state(MyFlow, ...)`)
    return cast(T, RestoredFlowState(run, step_name))


def _try_export_secrets_to_env_vars(secrets: list[str]):
    """Export the key-val pairs of each secret in the secrets list as environment variables.

    Raises:
        MetaflowException: If the secret cannot be found in the Metaflow/Outerbounds secrets manager.

    """
    for secret_source in secrets:
        secret: dict[str, str] = get_secret(source=secret_source)
        if secret:
            for key, value in secret.items():
                os.environ[key] = value


def _try_get_run(
    flow: Flow, flow_run_id: Union[Literal["latest_successful_run", "latest"], str], flow_name: str
) -> Run:
    if flow_run_id == "latest_successful_run":
        run = flow.latest_successful_run
        if not run:
            raise ValueError(
                f"No successful runs found for flow '{flow_name}'. Run it at least once or set flow_run_id='latest'."
            )
    elif flow_run_id == "latest":
        run = flow.latest_run
        if not run:
            raise ValueError(
                f"No (un)successful runs found for flow '{flow_name}' with ID '{flow_run_id}'. Run it at least once."
            )
    else:
        run = flow[flow_run_id]

    if not run:
        raise ValueError(f"No run found for flow '{flow_name}' with ID '{flow_run_id}'")

    return run


def _patch_current_with_mocked_values(
    flow_name: str,
    flow_run_id: str,
    tags: FrozenSet[str],
    step_name: str = "end",
    namespace: Union[str, None] = None,
):
    """Monkeypatch the current object to simulate a Metaflow run context using mocks.

    This creates global patches that persist until explicitly stopped or the process ends.
    Designed for use in notebooks where the patches should remain active across cells.
    """
    current._flow_name = flow_name  # type: ignore[attr-defined]
    current._run_id = flow_run_id  # type: ignore[attr-defined]
    current._step_name = step_name  # type: ignore[attr-defined]
    current._task_id = "task id not implemented"  # type: ignore[attr-defined]
    current._retry_count = -1  # type: ignore[attr-defined]
    current._origin_run_id = "origin run id not implemented"  # type: ignore[attr-defined]
    current._namespace = namespace  # type: ignore[attr-defined]
    current._username = "username not implemented"  # type: ignore[attr-defined]
    current._metadata_str = "metadata str not implemented"  # type: ignore[attr-defined]
    current._is_running = True  # type: ignore[attr-defined]
    current._tags = tags  # type: ignore[attr-defined]
    current.project_name = "project_name not implemented"  # type: ignore
    current.card = None  # type: ignore
    current.is_production = False  # type: ignore

    return current
