import textwrap

import pydantic
import pytest
from pydantic import BaseModel, Field

from ds_platform_utils.metaflow import make_pydantic_parser_fn


class TestConfig(BaseModel):
    name: str
    value: int = Field(ge=0)
    optional_field: str = "default"


def test_valid_json():
    parser = make_pydantic_parser_fn(TestConfig)
    config = parser("""
        {
            "name": "test_json",
            "value": 42,
            "optional_field": "custom"
        }
    """)
    assert config["name"] == "test_json"
    assert config["value"] == 42
    assert config["optional_field"] == "custom"


def test_valid_yaml():
    parser = make_pydantic_parser_fn(TestConfig)
    yaml_str = textwrap.dedent("""
        name: test_yaml
        value: 100
        optional_field: custom_yaml
    """)
    config = parser(yaml_str)
    assert config["name"] == "test_yaml"
    assert config["value"] == 100
    assert config["optional_field"] == "custom_yaml"


def test_valid_toml():
    parser = make_pydantic_parser_fn(TestConfig)
    toml_str = textwrap.dedent("""
        name = "test_toml"
        value = 200
        optional_field = "custom_toml"
    """)
    config = parser(toml_str)
    assert config["name"] == "test_toml"
    assert config["value"] == 200
    assert config["optional_field"] == "custom_toml"


def test_invalid_json():
    parser = make_pydantic_parser_fn(TestConfig)
    with pytest.raises(ValueError):
        parser("{bad json")


def test_invalid_yaml():
    parser = make_pydantic_parser_fn(TestConfig)
    with pytest.raises(ValueError):
        parser("name: test\nvalue: [bad yaml")


def test_invalid_schema():
    parser = make_pydantic_parser_fn(TestConfig)
    with pytest.raises(pydantic.ValidationError):  # Pydantic will raise validation error
        parser('{"name": "test", "value": -1}')  # value must be >= 0


@pytest.mark.parametrize(
    "input_data",
    [
        "value: 42",  # YAML
        '{"value": 42}',  # JSON
        "value = 42",  # TOML
    ],
)
def test_missing_required_field(input_data):
    parser = make_pydantic_parser_fn(TestConfig)
    with pytest.raises(pydantic.ValidationError):
        parser(input_data)
