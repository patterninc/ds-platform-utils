"""Assertion helper shared by the scenario flows.

Prints one line per check so a passing run is readable in the Outerbounds UI,
and raises on the first failure so the step actually fails rather than logging
a problem nobody reads.
"""


def check(label, got, want=None, predicate=None):
    if predicate is not None:
        ok = predicate(got)
        detail = f"{got!r}"
    else:
        ok = got == want
        detail = f"{got!r} == {want!r}"
    print(f"[check] {'PASS' if ok else 'FAIL'}  {label}: {detail}", flush=True)
    if not ok:
        raise AssertionError(f"{label}: {detail}")
    return got
