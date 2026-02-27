# `make_pydantic_parser_fn`

Source: `ds_platform_utils.metaflow.validate_config.make_pydantic_parser_fn`

Creates a Metaflow `Config(..., parser=...)` parser backed by a Pydantic model.

## Signature

```python
make_pydantic_parser_fn(
    pydantic_model: type[BaseModel],
) -> Callable[[str], dict]
```

## What it does

- Parses config content as JSON, TOML, or YAML.
- Validates and normalizes with Pydantic.
- Returns a dict with applied defaults from the model.

## Typical usage

```python
config: MyConfig = Config(
    name="config",
    default="./configs/default.yaml",
    parser=make_pydantic_parser_fn(MyConfig),
)
```
