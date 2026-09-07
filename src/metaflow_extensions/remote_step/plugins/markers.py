"""Marker decorators — carry no behaviour, their presence is the signal.

`@remote_step` needs a way for an operator to say "not this time" from the
command line. Metaflow's `--with` cannot express that by configuring the
decorator already in the source: `_attach_decorators_to_step` appends a
`--with` decorator only when the step does not already have one of that name,
and otherwise **silently ignores it** — so `--with remote_step:local=true`
would do nothing at all on a step that already carries `@remote_step`.

A separate no-op decorator sidesteps that. It also propagates correctly:
`--with` travels in `top_level_options`, so it reaches the command Metaflow
builds for a remote step. An environment variable would not, which matters for
`--with local_step --with kubernetes` — the step body runs in an Outerbounds
pod, step_init runs again there, and the answer has to come out the same.

Named `local_step` rather than `local`: Metaflow already overloads "local"
for its metadata and datastore backends, and every registered decorator also
becomes importable as `from metaflow import <name>`.
"""

from __future__ import annotations

from metaflow.decorators import StepDecorator


class LocalStepMarker(StepDecorator):
    """Opt a step out of running its body on the EKS cluster.

        run --with local_step                    # runs in-process
        run --with local_step --with kubernetes  # runs in an Outerbounds pod

    `@remote_step` becomes inert: it leaves every sibling decorator untouched
    and returns the step function unwrapped, so Metaflow does whatever it
    would have done without it.

    Applying this to a step that has no `@remote_step` does nothing.
    """

    name = "local_step"
    defaults: dict = {}
