"""A custom FlowMutator and @exit_hook alongside @remote_step.

out-of-stock's predict flow carries `@output_table_cleanup_mutator`, a
FlowMutator that adds a `user_step_decorator` to every step -- so this is a
real pattern here, and it is the same machinery @gpu_profile uses. Mutators
are precisely what made that decorator hard: by the time step_init runs, the
mutator has rewritten the step's decorator list, so what @remote_step sees is
not what the source said.

Two things have to hold. @remote_step must survive a sibling list built by
someone else, and the mutator's own injected decorator must still take
effect -- the latter being the bug that made a @card written above
@remote_step silently vanish, because removing a decorator shifted the list
Metaflow was iterating.
"""

from metaflow import FlowMutator, FlowSpec, current, remote_step, resources, step, user_step_decorator

from _check import check

WRAPPED = []


@user_step_decorator
def marker_decorator(step_name, flow, inputs=None, attributes=None):
    """Wraps a step, exactly as the real cleanup decorator does."""
    WRAPPED.append(step_name)
    print(f"[fx29] wrapper entered for {step_name}", flush=True)
    try:
        yield
    except Exception:
        print(f"[fx29] wrapper saw {step_name} fail", flush=True)
        raise
    print(f"[fx29] wrapper saw {step_name} succeed", flush=True)


class marker_mutator(FlowMutator):  # noqa: N801
    """Adds the wrapper to every step in the flow."""

    def mutate(self, mutable_flow):  # noqa: D102
        for _, s in mutable_flow.steps:
            s.add_decorator(marker_decorator, duplicates=s.IGNORE)


@marker_mutator
class Fx29MutatorAndHooks(FlowSpec):
    @step
    def start(self):
        self.next(self.work)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        # The wrapper runs on the *driver* -- the step body itself is in the
        # pod -- so this proves the two coexist rather than the wrapper
        # reaching the pod.
        check("the step ran remotely", current.step_name, "work")
        check("flow name intact", current.flow_name, "Fx29MutatorAndHooks")
        self.ran = True
        self.next(self.end)

    @step
    def end(self):
        check("the remote step completed", bool(self.ran), True)
        # Every step in this flow was wrapped by the mutator, including the
        # one @remote_step rewrote.
        print(f"[fx29] wrapper saw steps: {WRAPPED}", flush=True)
        check("the mutator wrapped the remote step too", "work", predicate=lambda s: s in WRAPPED)
        print("[fx29] OK")


if __name__ == "__main__":
    Fx29MutatorAndHooks()
