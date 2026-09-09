"""Build hooks for the MkDocs documentation site.

The repo `README.md` is the single source of truth for the docs landing page, so it is
injected into the site as `index.md` at build time rather than being duplicated into
`docs/`. Its links are written to resolve on github.com (`docs/metaflow/publish.md`),
which must be rebased because the generated `index.md` sits at the docs root.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mkdocs.config.defaults import MkDocsConfig
    from mkdocs.structure.files import Files

REPO_ROOT = Path(__file__).parent.parent
README_PATH = REPO_ROOT / "README.md"

# `README.md` lives one level above `docs/`, so its links carry a `docs/` prefix that the
# generated `index.md` does not need: `](docs/metaflow/x.md)` -> `](metaflow/x.md)`.
GITHUB_LINK_PREFIX = "](docs/"
SITE_LINK_PREFIX = "]("


def on_files(files: Files, config: MkDocsConfig) -> Files:
    """Add the README to the site as the generated landing page, `index.md`."""
    from mkdocs.structure.files import File

    markdown = README_PATH.read_text(encoding="utf-8").replace(GITHUB_LINK_PREFIX, SITE_LINK_PREFIX)
    index = File.generated(config, "index.md", content=markdown)

    # Point "edit this page" at the README, not the nonexistent `docs/index.md`. MkDocs
    # builds the URL as `edit_uri + file.edit_uri` and then urljoin()s it onto `repo_url`,
    # which resolves the `..` -- so this yields `<repo>/edit/main/README.md`. Generated
    # files default to no edit link at all, so this must be set explicitly.
    index.edit_uri = "../README.md"

    files.append(index)
    return files
