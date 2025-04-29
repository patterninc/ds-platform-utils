import json
from typing import Callable

import tomllib
from pydantic import BaseModel


def make_pydantic_parser_fn(pydantic_model: type[BaseModel]) -> Callable[[str], dict]:
    """Return a function that can be passed to as `parser=` for a Metaflow config.

    It is great to use Pydantic models to validate config files for 2 reasons:

    1. It gives you autocompletion when you access `self.config.some_field`
    2. Pydantic is a well-known framework for writing data validation, so using it
       standardizes how our validation logic looks and where it is located

    To use Pydantic models to validate our flow configs, use this function like so:

    Example usage:

    ```python
    from ds_platform_utils.metaflow import make_pydantic_parser_fn

    class PydanticFlowConfig(BaseModel):

        n_rows: int = Field(..., ge=1)


    @pypi_base(
        python="3.11",
        packages={}
    )
    @project(name="ds_projen")
    class PydanticFlow(FlowSpec):
        '''A sample flow.'''

        config: PydanticFlowConfig = Config(
            name="config",
            default="./configs/default.toml",
            parser=make_pydantic_parser_fn(PydanticFlowConfig)
        ) # type: ignore[assignment]
    ```
    """

    def _parse_config(config_txt: str) -> dict:
        # Try to parse the config as JSON
        try:
            cfg = json.loads(config_txt)
        except json.JSONDecodeError:
            # If JSON parsing fails, try to parse as TOML
            try:
                cfg = tomllib.loads(config_txt)
            except tomllib.TOMLDecodeError as e:
                raise ValueError("Config parsing failed. Ensure it is valid JSON or TOML.") from e

        pydantic_model.model_validate(cfg)

        return cfg

    return _parse_config
