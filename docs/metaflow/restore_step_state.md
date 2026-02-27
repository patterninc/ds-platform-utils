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

## Parameters

| Parameter     | Type                                                | Required | Description                                                                             |
| ------------- | --------------------------------------------------- | -------: | --------------------------------------------------------------------------------------- |
| `flow_class`  | `type[T] \| None`                                   |       No | Optional flow class used for typed return/autocomplete and default flow name inference. |
| `flow_name`   | `str \| None`                                       |       No | Optional explicit flow name used to fetch metadata; overrides class-derived name.       |
| `step_name`   | `str`                                               |       No | Target step context name for restoring/patching state (default: `"end"`).               |
| `flow_run_id` | `Literal["latest_successful_run", "latest"] \| str` |       No | Run selector: latest successful, latest, or an explicit run id.                         |
| `secrets`     | `list[str] \| None`                                 |       No | Optional Metaflow secret sources to export as environment variables.                    |
| `namespace`   | `str \| None`                                       |       No | Optional Metaflow namespace filter when locating flow runs.                             |

**Returns:** `T` typed flow-like state object with restored run artifacts.
