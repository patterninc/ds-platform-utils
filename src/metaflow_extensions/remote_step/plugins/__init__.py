"""Metaflow plugin registration for @remote_step.

Metaflow reads `STEP_DECORATORS_DESC` from
`metaflow_extensions.<name>.plugins.__init__` and installs each entry as a
step decorator, making it importable as `from metaflow import <name>`.
"""

STEP_DECORATORS_DESC = [
    ("remote_step", ".remote_step_decorator.RemoteStepDecorator"),
]
