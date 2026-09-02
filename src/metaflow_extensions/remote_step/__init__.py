"""remote-step — Metaflow extension providing @remote_step.

The full runtime library lives here so Metaflow's extension packaging
bundles it into the flow's code package (and thus every Argo pod).

For ergonomic use in flow code, we also register this package under the
short alias `remote_step` in `sys.modules` so `from remote_step.X import Y`
works everywhere the extension is installed.
"""

from __future__ import annotations

import sys as _sys

# Alias BEFORE any submodule import so `from remote_step.X import Y` inside
# our own submodules resolves cleanly against this package's __path__.
_this = _sys.modules[__name__]
if _sys.modules.get("remote_step") is not _this:
    _sys.modules["remote_step"] = _this

TL_PACKAGE = "metaflow_extensions.remote_step"
__version__ = "0.1.0"
