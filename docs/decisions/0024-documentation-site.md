# 0024: A documentation site built by Zensical, hosted on GitHub Pages

**Status:** accepted  
**Date:** 2026-10-01T14:43:57-03:00

## Context
Usage was documented in a long README and the Markdown files under `docs/`; the API was
documented only in docstrings. Users need guides, an options and tables reference, and an
API reference of the public surface (ADR 0021), published somewhere. Requirements: the
existing `docs/` Markdown works unchanged (GitHub renders it too), the API reference comes
from the docstrings without importing the package (`mssql_cdc` imports PySpark, which a docs
build should not need, nor Java), and CI fails on broken links.

The state of the tools in October 2026:

* MkDocs 1.6.1 (August 2024) is its last release and unmaintained; the announced
  "MkDocs 2.0" drops the plugin system, and ProperDocs continues 1.6 as a drop-in fork
  ([ProperDocs discussion](https://github.com/orgs/ProperDocs/discussions/33),
  [The Slow Collapse of MkDocs](https://fpgmaas.com/blog/collapse-of-mkdocs/)).
* Material for MkDocs (9.7.7) is in maintenance mode since November 2025: critical bugs and
  security issues for at least 12 months, no new features
  ([Material blog](https://squidfunk.github.io/mkdocs-material/blog/2025/11/11/insiders-now-free-for-everyone/)).
  Its team builds Zensical instead, MIT-licensed, which reads `mkdocs.yml`
  ([announcement](https://squidfunk.github.io/mkdocs-material/blog/2025/11/05/zensical/)).
* Zensical (0.0.67, 2026-09-30) ships the Material features (search, palette toggle, code
  copy, admonitions, Mermaid) and supports mkdocstrings since 0.0.11
  ([plugins](https://zensical.org/docs/compatibility/mkdocs/plugins/)); `zensical build
  --strict` fails on links to missing pages or anchors
  ([validation](https://zensical.org/docs/setup/validation/)).
* mkdocstrings-python (2.0.9) reads the source with Griffe; `allow_inspection: false`
  forbids importing a module ([options](https://mkdocstrings.github.io/python/usage/configuration/general/)).
* Sphinx (9.1, Python 3.12+) with Furo needs MyST-Parser to read Markdown, and autodoc
  imports the modules it documents
  ([autodoc](https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html);
  sphinx-autoapi does not). Read the Docs hosts either, as a second service to configure
  next to GitHub.

## Decision
* Zensical builds the site from `mkdocs.yml` at the repository root, `docs_dir: docs`, into
  `site/` (ignored). The existing pages stay where they are; `mkdocs.yml` lists every page in
  its `nav`. Configuration stays in the MkDocs format, which mkdocstrings documents and the
  MkDocs-compatible tools read.
* `docs/reference/api.md` is the API reference: mkdocstrings-python directives over the
  public surface of ADR 0021, read statically from `src/` (`allow_inspection: false`).
  Listing an object there makes it public (ADR 0021, amended to point here).
* Usage is documented once, on the site. The README keeps the pitch, the install, a minimal
  example and links to the site's pages; the options and the output schema live only in
  `docs/reference/`.
* `docs/changelog.md` and `docs/contributing.md` include the root `CHANGELOG.md` and
  `CONTRIBUTING.md` through `pymdownx.snippets`, with `check_paths` so a missing file fails
  the build.
* A `docs` dependency group (`zensical`, `mkdocstrings-python`). The build runs with that
  group alone: `uv run --only-group docs zensical build --strict`, no PySpark, no package.
* CI's `docs` job builds in strict mode on every push and pull request; `docs.yml` builds
  and deploys to GitHub Pages on every push to `main`. One version, `main`'s; versioned docs
  when a release needs them.

## Consequences
* Zensical is 0.0.x: the lock pins it, and an upgrade is checked with a strict build.
  Should it stall, the same `mkdocs.yml` is the starting point for ProperDocs with a theme.
* The validator checks the page sources, not text a snippet pulls in: a relative link inside
  `CHANGELOG.md` resolves against `docs/changelog.md` on the site, unchecked, and a link that
  works on GitHub (`docs/decisions/...`) breaks there. So `CHANGELOG.md` and `CONTRIBUTING.md`
  link to docs pages by their absolute site URL. Links leaving `docs_dir` (`../README.md`)
  fail the strict build.
* Docstrings render as Markdown: reST roles such as `:mod:` show literally.
* A page missing from `nav` still builds, reachable only by links: new pages and ADRs get a
  `nav` line.
* GitHub Pages must be enabled once, with "GitHub Actions" as the source.
