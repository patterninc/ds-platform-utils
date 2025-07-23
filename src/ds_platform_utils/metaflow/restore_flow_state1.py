import sys
from typing import Dict, Optional

from metaflow import Run, Step
from metaflow import namespace as _namespace


def restore_step_state(
    flow_name: str,
    flow_run_id: str,
    step_to_run: str,
    namespace: Optional[str] = None,
) -> Step:
    """Restores the execution state of a Metaflow run up to a specified step and enables debugging.

    This function:
    1. Loads artifacts from all previous steps in the specified run
    2. Accumulates parameters/artifacts across steps (later steps override earlier
       artifacts with same names to ensure the latest state)
    3. Loads artifacts from all preceding steps before the target step into both the caller's
       global namespace and a returned Step object with artifacts as dynamic attributes
       to access attributes via for dot-access (accessible via step_obj.artifact_name)
    5. Preserves Metaflow's namespace isolation requirements

    Parameters
    ----------
    flow_name : str
        Name of the Metaflow flow to restore (must match the class name)
    flow_run_id : str
        Unique identifier of the specific run
    step_to_run : str
        Target step name to restore state up to (but not including the step itself)
    namespace : Optional[str], default=None
        Metaflow namespace containing the run. Required for production namespaces (non-user runs).
        - Use None for user namespace
        - Format: 'argo-<project>.<env>' for Argo executions

    Returns
    -------
    Step
        Metaflow Step object for the specified step with artifacts attached as attributes.
        Artifacts can be accessed via dot notation (e.g., step_obj.predict_df)

    Raises
    ------
    MetaflowNamespaceMismatch
        If the requested run isn't in the specified namespace
    ValueError
        - If specified step doesn't exist in the run
        - If any previous step task failed
        - If no tasks found for a step

    Notes
    -----
    - Requires all previous steps to have at least one successful task
    - Processes steps in reverse execution order (end to start) to match DAG dependencies
    - Artifacts from later steps override earlier ones with the same name
    - Modifies caller's global namespace by injecting artifact variables
      - but that's not an issue since user will use this utility function to debug most of the
        time in a jupyter notebook

    Example
    -------
    >>> flow_state = restore_step_state(
            flow_name="TrainingFlow",
            flow_run_id="TrainingFlow/123456789",
            step_to_run="model_training",
            namespace="argo-training.prod"
        )
    >>> # Access artifacts via Step object
    >>> print(flow_state.training_data.head())
    >>> # Or via direct global access
    >>> print(model_config)

    """
    _namespace(namespace)

    # Create the flow run object using the Run class
    flow_run = Run(f"{flow_name}/{flow_run_id}")

    # Get all steps in execution order (reversed for proper DAG order)
    all_steps = [step.id for step in flow_run]
    all_steps = all_steps[::-1]
    print("All steps in the flow: ", all_steps)

    # Validate that the step exists in the run
    if step_to_run not in all_steps:
        raise ValueError(f"Step '{step_to_run}' not found in run '{flow_run_id}'")

    step_index = all_steps.index(step_to_run)
    loading_previous_steps = all_steps[:step_index]
    print(f"Loading previous steps: {loading_previous_steps}")

    accumulated_params: Dict[str, any] = {}  # Accumulate params across steps
    for step_name in loading_previous_steps:
        print(f"Loading step -> {step_name}")

        step_path = f"{flow_name}/{flow_run_id}/{step_name}"

        print(f"Getting parameters for -> {step_path}")

        current_step = Step(step_path)

        # Get first task (handle multi-task steps if needed)
        task = next(iter(current_step.tasks()), None)
        if not task:
            raise ValueError(f"No tasks found for step '{step_name}'")

        if not task.successful:
            raise ValueError(f"Task in step '{step_name}' did not succeed")

        data = task.data
        step_params = {k: getattr(data, k) for k in getattr(data, "_artifacts", [])}
        print("All variables: ", list(step_params.keys()))
        accumulated_params.update(step_params)  # Accumulate/override
        print("-" * 60)

    # Create Step object and attach artifacts
    step_obj = Step(f"{flow_name}/{flow_run_id}/{step_to_run}")
    for key, val in accumulated_params.items():
        setattr(step_obj, key, val)  # Directly attach to Step object

    # Inject into caller's globals
    current_frame = sys._getframe(1)
    for k, v in accumulated_params.items():
        current_frame.f_globals[k] = v

    print("\nNow, You can now begin debugging any line of code from the main codebase.")
    return step_obj
