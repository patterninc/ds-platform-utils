"""Card components written in the pod, read back and asserted.

`@card` renders on the *driver*, so a `current.card.append(...)` in the pod
had no card to reach -- it raised there, or rendered nothing. The pod records
its components and the driver replays them into the real card.

This is the flow that actually proves it. fx06 writes to a card but asserts
nothing about the result, and that gap has bitten before: an earlier check
asserted on `card.get()`, which returns a ~1 MB HTML bundle of JavaScript, so
substring searching it produced both a false pass and a false fail. The
component data lives in `card.get_data()`; that is what gets asserted here.
"""

import json

from metaflow import FlowSpec, card, current, remote_step, resources, step
from metaflow.cards import Markdown, Table

from _check import check

CARD_ID = "fx30"
MARKER = "written-inside-the-runner-pod"


def card_text(task, card_id):
    """Every string in a task's card data, flattened."""
    from metaflow.cards import get_cards

    found = []
    for c in get_cards(task, id=card_id):
        data = c.get_data() or {}
        found.append(json.dumps(data, default=str))
    return "\n".join(found)


class Fx30CardReadback(FlowSpec):
    @step
    def start(self):
        self.next(self.render)

    @card(type="blank", id=CARD_ID)
    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def render(self):
        c = current.card[CARD_ID]
        c.append(Markdown(f"# {MARKER}"))
        c.append(Markdown(f"run {current.run_id}, step {current.step_name}"))
        c.append(
            Table(
                data=[["rows", "1234"], ["status", "ok"]],
                headers=["metric", "value"],
            )
        )
        print(f"[fx30] appended 3 components from the pod", flush=True)
        self.rendered = True
        self.next(self.verify)

    @step
    def verify(self):
        """Read the card back through the client and prove the pod's work is in it."""
        from metaflow import Task

        task = Task(f"{current.flow_name}/{current.run_id}/render/{self.index_of_render()}")
        text = card_text(task, CARD_ID)
        print(f"[fx30] card data length: {len(text)}", flush=True)
        check("the card exists and has data", len(text), predicate=lambda n: n > 0)
        # get_data(), not get(): the HTML is a JS bundle and searching it lies.
        check("the pod's marker is in the card", MARKER in text, True)
        check("the pod's table reached the card", "1234" in text, True)
        check("the table header reached the card", "metric" in text, True)
        self.card_len = len(text)
        self.next(self.end)

    def index_of_render(self):
        """The render step's task id, found via the client."""
        from metaflow import Run

        for s in Run(f"{current.flow_name}/{current.run_id}"):
            if s.id == "render":
                return next(iter(s)).id
        raise AssertionError("no render step found")

    @step
    def end(self):
        check("card had content", int(self.card_len), predicate=lambda n: n > 0)
        print("[fx30] OK")


if __name__ == "__main__":
    Fx30CardReadback()
