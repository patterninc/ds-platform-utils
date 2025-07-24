"""Helpers used by restore_step_state.

metaflow.Config objects in a flow are meant to be accessible using dot notation, e.g.

```
dict1.key1.key2
```

as opposed to

```
dict1["key1"]["key2"]
```

When step code is wrapped in a @step-decorated function, this works for config objects
out of the box, due to post-processing of the config dict under the hood.

However, when the same config object is fetched from the metadata service via the metaflow
SDK, i.e. `Flow("MyFlow")["run_id"].data["config"]`, the config object is a regular dict
and there does not support dot notation.

We use the DotDict defined in this file to wrap all dicts whose keys are strings
so that the attributes like `self.config` are accessible using dot notation when
`self = restore_step_state(...)`.
"""

from __future__ import annotations

from typing import Any, Mapping, MutableMapping


class DotDict(dict):
    """A dictionary subclass that allows attribute-style access to keys.

    ``DotDict`` behaves like a normal ``dict`` but lets you access keys
    using dot notation (``obj.key``) as long as the key is a valid
    Python identifier and does not shadow an existing attribute or
    method name on ``dict``.
    """

    def __getattr__(self, attr: str) -> Any:
        """Retrieve a value from the dictionary via attribute access."""
        try:
            # First attempt normal attribute access on the dict itself.
            return super().__getattribute__(attr)
        except AttributeError:
            # Fallback: if the attribute name is a key in the dict,
            # return the associated value. Otherwise propagate the error.
            if attr in self:
                return self[attr]
            raise

    def __setattr__(self, attr: str, value: Any) -> None:
        """Assign a value to a key via attribute access."""
        # Protect built-in attributes and methods as well as private attributes
        if attr.startswith("_") or hasattr(dict, attr):
            super().__setattr__(attr, value)
        else:
            self[attr] = value

    def __delattr__(self, attr: str) -> None:
        """Delete a key via attribute access."""
        if attr in self:
            del self[attr]
        else:
            super().__delattr__(attr)


def _all_string_keys(mapping: Mapping[Any, Any]) -> bool:
    """Return ``True`` if all keys in ``mapping`` are strings."""
    for k in mapping:
        if not isinstance(k, str):
            return False
    return True


def convert_to_dotdict(obj: Any) -> Any:
    """Recursively convert dictionaries with all-string keys into ``DotDict``.

    This function walks through ``obj`` and converts any nested mapping
    (``dict`` or ``MutableMapping``) whose keys are all strings into an
    instance of ``DotDict``. For mappings that have one or more
    non-string keys, the mapping is left as-is, though the function
    still recursively inspects and converts any nested mappings within
    the values.

    Lists, tuples, sets and other iterables containing dictionaries are
    also traversed, and their elements are individually converted if they
    qualify. Immutable sequences (e.g., tuples) are returned as the same
    type with their contents converted; mutable sequences (e.g., lists)
    are returned as lists with converted contents.

    :param obj: The object to convert.

    :return: A converted version of ``obj`` where eligible mappings are
        replaced by ``DotDict`` instances.
    """
    # Check for mapping types first. We intentionally support both dict
    # instances and other mutable mappings. Immutable mappings (from
    # typing.Mapping) are also accepted.
    if isinstance(obj, MutableMapping):
        # Determine if all keys are strings; if not, we still need to
        # recursively convert nested structures in the values.
        if _all_string_keys(obj):
            # Construct a new DotDict and recursively convert children.
            converted = DotDict()
            for key, val in obj.items():
                converted[key] = convert_to_dotdict(val)
            return converted
        else:
            # Keep the original mapping type but convert nested mappings.
            new_mapping: MutableMapping[Any, Any] = obj.__class__()
            for key, val in obj.items():
                new_mapping[key] = convert_to_dotdict(val)
            return new_mapping

    # Convert lists, tuples, sets, and other iterables that are not strings
    # or bytes. Strings and bytes are iterable but should be treated as
    # atomic values.
    if isinstance(obj, (list, tuple, set)):
        # Convert each element individually.
        converted_iterable = [convert_to_dotdict(elem) for elem in obj]
        # Preserve the original type for tuples and sets; lists remain lists.
        if isinstance(obj, tuple):
            return tuple(converted_iterable)
        if isinstance(obj, set):
            return set(converted_iterable)
        return converted_iterable

    # Non-iterable or non-convertible types are returned unchanged.
    return obj


__all__ = ["DotDict", "convert_to_dotdict"]
