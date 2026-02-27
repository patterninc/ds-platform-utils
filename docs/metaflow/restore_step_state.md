# `restore_step_state`

Source: `ds_platform_utils.metaflow.restore_step_state.restore_step_state`

Restores Metaflow run state so step logic can be reproduced locally (for debugging/notebooks).

## Signature

```python
restore_step_state(
    flow_class: type[T] | None = None,
    flow_name: str | None = None,
    step_name: str = "end",
    flow_run_id: Literal["latest_successful_run", "latest"] | str = "latest_successful_run",
    secrets: list[str] | None = None,
    namespace: str | None = None,
) -> T
```

## What it does

- Loads run artifacts from Metaflow metadata.
- Exposes restored values as `self.<artifact>` style attributes.
- Optionally exports requested Metaflow secrets into env vars.
- Patches `metaflow.current` with a mock context for local execution.
