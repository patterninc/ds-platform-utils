"""Sibling decorators: @card above @remote_step, with @kubernetes present.

Metaflow hands step_init the list it is iterating, so removing the sibling
@kubernetes shifted the list underneath that loop and the decorator written
directly above @remote_step never got step_init at all. With @card there,
the card was silently never registered.

Also covers @kubernetes disk / shared_memory, which were read and discarded:
disk gave the pod a 40 GiB ephemeral limit whatever was asked for, and
shared_memory left /dev/shm at the container default of 64 MB.
"""

import os
import shutil
import subprocess

from metaflow import FlowSpec, card, current, kubernetes, remote_step, step
from metaflow.cards import Markdown

from _check import check


class Fx06Decorators(FlowSpec):
    @step
    def start(self):
        self.next(self.work)

    @card(type="blank", id="fx06")
    @remote_step
    @kubernetes(cpu=2, memory=8000, disk=61440, shared_memory=2048)
    @step
    def work(self):
        # /dev/shm sized by shared_memory rather than the 64 MB default.
        shm = shutil.disk_usage("/dev/shm")
        shm_mb = shm.total / 1024 / 1024
        print(f"[fx06] /dev/shm total = {shm_mb:.0f} MB", flush=True)
        check("shared_memory honoured", shm_mb, predicate=lambda m: m > 1024)

        # Scratch space reflects disk=61440 (60 GiB), not the 40 GiB default.
        df = subprocess.run(["df", "-BG", "/tmp"], capture_output=True, text=True).stdout
        print(f"[fx06] df /tmp:\n{df}", flush=True)

        # A card written from the pod, replayed onto the driver's card.
        current.card["fx06"].append(Markdown("# written from the runner pod"))
        current.card["fx06"].append(Markdown(f"/dev/shm = {shm_mb:.0f} MB"))

        self.shm_mb = shm_mb
        self.next(self.end)

    @step
    def end(self):
        check("shm reported back", self.shm_mb, predicate=lambda m: m > 1024)
        print("[fx06] OK")


if __name__ == "__main__":
    Fx06Decorators()
