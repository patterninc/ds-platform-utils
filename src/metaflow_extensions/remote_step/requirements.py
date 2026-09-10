"""Turn a spec's `env.packages` map into pip requirement lines.

The runner pod installs the step's environment from `spec.json`, whose
`env.packages` comes from whatever `@pypi_base` / `@pypi` / `@uv_pypi_base` /
`@uv_pypi` put on the flow. Those four spell a dependency several different
ways, and the mapping to a requirement line is not obvious in every case:

    {"pandas": ""}                             -> pandas
    {"numpy": "2.2.6"}                         -> numpy==2.2.6
    {"pyarrow": "<19.0.0"}                     -> pyarrow<19.0.0
    {"pkg": "@ git+https://host/r.git@<sha>"}  -> pkg @ git+https://host/r.git@<sha>
    {"git+https://host/r.git": "@main"}        -> git+https://host/r.git@main
    {"pkg": "git+https://host/r.git"}          -> pkg @ git+https://host/r.git

The last three are all direct references, and the difference between them is
only *where* the URL and the '@' were written. `@uv_pypi_base` derives the
fourth form from uv.lock; the fifth is what a hand-written `@pypi_base` tends
to look like. Getting any of them wrong yields a requirement pip rejects, so
the whole step fails to install rather than installing the wrong thing.

`main()` writes the rendered specs to stdout as NUL-terminated records, which
entrypoint.sh reads into a bash array. NUL rather than newline because a PEP
508 direct reference contains spaces, and an unquoted shell expansion would
split one of those into three arguments.
"""

from __future__ import annotations

import json
import sys

URL_SCHEMES = ("git+", "http://", "https://", "file://")

# A version that already starts with one of these is a specifier, not a bare
# version, so it must be appended rather than pinned with '=='.
SPECIFIER_STARTS = ("<", ">", "=", "!", "~")


def requirement_line(name: str, version: str) -> str:
    """Render one `packages` entry as a pip requirement line."""
    name = (name or "").strip()
    version = (version or "").strip()

    # URL written as the key. Any value is a git ref belonging to that URL, so
    # it is appended with no separator -- and this has to be tested before the
    # '@' case below, which would otherwise claim it and insert a space.
    if name.startswith(URL_SCHEMES):
        return f"{name}{version}"

    # PEP 508 direct reference with the '@' already on the value. This is what
    # @uv_pypi_base derives from uv.lock.
    if version.startswith("@"):
        return f"{name} {version}"

    # URL as the value, without the '@'.
    if version.startswith(URL_SCHEMES):
        return f"{name} @ {version}"

    if version.startswith(SPECIFIER_STARTS):
        return f"{name}{version}"

    return f"{name}=={version}" if version else name


def build_requirements(packages: dict[str, str] | None) -> list[str]:
    """Render every `packages` entry.

    `boto3` is not included: the entrypoint passes it separately, since the
    runner needs S3 access whatever the flow declares.
    """
    return [requirement_line(name, version) for name, version in (packages or {}).items()]


def main(argv: list[str] | None = None) -> int:
    """Read a spec, write NUL-terminated specs to stdout. `main(spec_path)`."""
    args = list(sys.argv[1:] if argv is None else argv)
    spec_path = args[0] if args else "/payload/spec.json"

    with open(spec_path) as f:
        spec = json.load(f)
    packages = (spec.get("env") or {}).get("packages") or {}
    specs = build_requirements(packages)

    sys.stderr.write(
        f"[remote_step] spec env packages ({len(packages)}): "
        f"{list(packages.items())[:20]}\n"
        "[remote_step] requirements:\n" + "".join(f"  {s}\n" for s in specs)
    )

    # Only terminate when there is something to terminate. A trailing NUL on an
    # empty set emits one empty record, which reaches uv as "" and fails with
    #   error: Failed to parse: ``
    #   Caused by: Empty field is not allowed for PEP508
    # A step needing only the standard library is legitimate, so an empty set
    # has to mean 'install nothing'.
    if specs:
        sys.stdout.buffer.write(("\0".join(specs) + "\0").encode())
        sys.stdout.buffer.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
