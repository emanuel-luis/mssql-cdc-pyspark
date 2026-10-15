# 0024: A documentation site built by Zensical, hosted on GitHub Pages

**Status:** accepted  
**Date:** 2026-10-01T14:43:57-03:00  
**Amended:** 2026-10-14T09:21:02-03:00, one site per version with mike: `dev` from main, each release, `stable` (amendment 1)  
**Amended:** 2026-10-15T09:43:36-03:00, a final release's run deletes its release candidates from the selector (amendment 2)  
**Amended:** 2026-10-15T12:20:00-03:00, each run's Pages deployment is named after its gh-pages commit, so a tag on main's last commit publishes (amendment 3)

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

## Amendment 1: one site per version
1.0 freezes the public API (ADR 0021), and users on a release need the pages that describe
it while `main` moves on: the single site, `main`'s, describes the next release, not theirs.

Zensical has no versioning of its own yet. Its documented path is a fork of mike, the tool
Material for MkDocs used, kept by the Zensical team until native versioning ships
([Zensical: versioning](https://zensical.org/docs/compatibility/mkdocs/mike/),
[squidfunk/mike](https://github.com/squidfunk/mike)): it builds with `zensical build` and
commits each version to a directory of the `gh-pages` branch, with a `versions.json` at its
root that the theme's version selector reads (`extra.version.provider: mike`). The fork is
not on PyPI and gets compatibility fixes only.

* The `docs` group pins the fork to a commit (`uv.lock` records it). pip-audit audits what
  `uv.lock` pins by hash, and a git commit has none and no PyPI entry: `security.yml` leaves
  that one package out of the export; its dependencies stay in.
* `docs.yml` publishes one version per run, all from one concurrency group, so the runs
  that commit to `gh-pages` never overlap:
  * a push to `main` publishes `dev`;
  * a tag `vX.Y.Z` publishes `X.Y.Z` and moves the `stable` alias to it, unless `stable`
    is on a newer version already (a run again from an older tag publishes its version
    only);
  * a tag `vX.Y.ZrcN` publishes `X.Y.ZrcN` and leaves `stable` where it is, so a release
    candidate in use has its pages without becoming the default;
  * the site's root opens `stable`, or `dev` until the first final release.
  A tag publishes only when `release.yml` would release it: its version is the one in
  `pyproject.toml`, its commit is on `main`, and `CHANGELOG.md` has its section. A tag
  pushed by mistake (`v1.0.1` for a 1.0.0) never shows as a version.
  Versions start with this amendment: the 0.x releases are not published.
* GitHub Pages keeps "GitHub Actions" as its source. `gh-pages` is the store of every
  version; each run commits to it, pushes it, and deploys its whole tree as the Pages
  artifact (an alias's symlink becomes a copy there). The job that builds, which runs the
  third-party code of the docs group (Zensical, mkdocstrings, the mike fork), has a
  read-only token: on a hosted runner every step of a job can read its token, so a token
  given to one step only is given to the job. mike commits to a local `gh-pages`, and the job
  hands the new commits on as a git bundle. A second job, which runs nothing but git,
  holds `contents: write` and pushes them, without force: `gh-pages` only, and only as a
  fast-forward.
* Links from before the versions (`/mssql-cdc-pyspark/guides/streaming/`, in the README of
  every 0.x release on PyPI and in `CHANGELOG.md`) keep working: the site's root `404.html`
  (`.github/pages/404.html`, added to the artifact) sends a path whose first segment is not
  a version on to the same path in `stable`, or `dev`. A missing page inside a version stays
  a 404.
* mike's build is not strict, so `docs.yml` runs the strict build first.

Consequences: the `github-pages` environment needs a `v*` tag rule next to `main`, or a
tag's deploy is refused (its version is on `gh-pages` regardless, and the next push to
`main` deploys it). A tag publishes its version as soon as it is pushed, before PyPI's
reviewer approves the release; a tag deleted and pushed again publishes over it. A version
that passes the checks but never reaches PyPI (abandoned for the next number) stays in the
selector, and in `stable`, until removed by hand (`RELEASING.md`). When
Zensical ships native versioning, the fork goes, and the `gh-pages` layout (a directory per
version, `versions.json`) is what a replacement has to keep or redirect.

## Amendment 2: a final release replaces its candidates
A release candidate's pages are for the weeks it runs before its final release (ADR 0021
amendment 11). Once `X.Y.Z` is out they describe the same version, and a selector that keeps
`X.Y.Zrc1`, `X.Y.Zrc2`... next to it grows with every release and offers pages nobody should
pick. So the `docs` run of a tag `vX.Y.Z`, after it publishes `X.Y.Z` (and moves `stable`),
deletes `X.Y.ZrcN` with `mike delete`, in the same commits the push job pushes: the build job
stays read-only. A run again from the same tag finds none left. Other versions stay until
removed by hand (`RELEASING.md`).

## Amendment 3: a deployment per run, not per commit
`actions/deploy-pages` names a Pages deployment after the commit (`GITHUB_SHA`), with no
input to change it, and Pages answers a second deployment of the same commit with the first
one, as a success. A release tag is pushed on the commit `main` has just published (the
release commit, once CI passes), so 1.0.0rc1's run reported success and left the site as
`main`'s run had it: the version was on `gh-pages`, not on the site, until the next push to
`main`. The deploy job now calls the Pages API itself (create the deployment from the build
job's artifact with the job's OIDC token, then wait for `succeed`), naming it after the
`gh-pages` commit the run pushed: mike always commits (`--allow-empty`), so every run, and
a re-run, publishes its tree. (Pages takes only a commit of the repository as that name.)
