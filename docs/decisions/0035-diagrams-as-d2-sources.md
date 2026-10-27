# 0035: Diagrams as D2 sources, rendered to committed SVGs

**Status:** accepted  
**Date:** 2026-10-27T16:24:28-03:00

## Context
The documentation had four inline Mermaid charts: the pipeline in `README.md` and in
`docs/index.md`, and the components and the micro-batch sequence in `docs/ARCHITECTURE.md`.
GitHub and the site render Mermaid in the browser; PyPI does not, so the package page showed
the README's chart as code (the README is the package's long description). The charts shared
no style, gave no control over their layout, and nothing rendered them in CI; as the library
grew they got hard to read and to debug.

The owner's repositories move to one standard: diagrams as code in D2, rendered to SVG in CI,
the same version and style everywhere, with `de-lakehouse-pipeline` as the pilot.

## Decision
* Sources in `docs/diagrams/<name>.d2`, each starting with `...@_style` and a comment naming
  the files it is grounded in; the rendered `docs/diagrams/<name>.svg` committed next to it.
  `docs/diagrams/_style.d2` (not rendered) holds the `d2-config`: the ELK layout engine,
  theme 0 with dark theme 200, padding, and color overrides, and one class per kind of element
  (process, source, store, actor, external, check; `meta` for dashed edges). No class sets a
  fill: the SVG carries both themes and follows the page's color scheme.
* D2 v0.9.0, pinned in the `Makefile` by version and by the sha256 of its Linux release archive
  (from the release's `SHA256SUMS`). `make d2-install` installs it checksum-checked;
  `make diagrams` renders every diagram with `env -i` (no `D2_*` variable overrides the
  config) and `--scale 1` (natural size); `make diagrams-check` renders, then fails on an SVG
  that is stale, missing or has no source; `make diagrams-watch NAME=<name>` previews one live.
* CI's `diagrams (D2 render check)` job runs `make d2-install` and `make diagrams-check` and
  uploads the SVGs as an artifact, even when the check fails. `main`'s branch protection
  requires it next to the other checks, from the pull request that adds it: the context is
  added once the job has reported there, before that pull request merges.
* `.gitattributes`: `*.svg -text` (CI compares the bytes D2 writes) and
  `docs/diagrams/*.svg linguist-generated=true` (collapsed in pull request diffs).
* The set, each next to the text it explains:

  | Diagram | Shows | Where |
  |---|---|---|
  | `architecture` | SQL Server CDC, the stream and snapshot readers, the sink, the tables, the consumers | README, home page |
  | `components` | the modules and how they call each other | Architecture |
  | `data-flow` | one micro-batch from an LSN range to rows, with its checks and run metadata | Architecture |
  | `micro-batch` | the same batch call by call, with the T-SQL | Architecture |
  | `data-model` | the CDC tables the source reads and the state it keeps | Architecture |
  | `data-model-tables` | the Delta tables the library writes, keys only | Architecture, Tables |
  | `user-flows-query-lifecycle` | a first run with a bootstrap, then a restart that replays a batch | Streaming into Delta |
  | `user-flows-finalization` | `advance`, `track`, silver's verdict and `is_final` | Finalization |

* No Mermaid is left: the two pipeline charts are replaced by `architecture`, the components
  chart by `components` and the micro-batch sequence by `micro-batch`; `mkdocs.yml` drops its
  `mermaid` fence.

Where this repository departs from the pilot's setup:

1. **Paths.** The docs pages use relative paths (`diagrams/<name>.svg`, `../diagrams/...`
   from guides and the reference). `docs/diagrams/` is inside `docs_dir`, so Zensical copies
   the SVGs and sources into the site and rewrites the paths for directory URLs, and the same
   paths work on GitHub; the strict build checks them. Each image on a docs page links to its
   SVG: the site's content column is narrower than most diagrams and has no zoom. The README
   uses absolute URLs on `main` instead, raw.githubusercontent.com for the image and the GitHub
   page for the source, as its other links already are: a relative path does not resolve on
   PyPI.
2. **dagre for one diagram.** `data-model-tables` sets `layout-engine: dagre` in its own
   `d2-config`. In that graph ELK lays the `sql_table` shapes out as a staircase over 2,600 px
   wide (their edges leave from column ports); dagre keeps it at about 1,270 px. dagre draws
   edges from table to table, so each key column names the column it points at.
3. **`.gitattributes` holds the SVG lines only.** The repository has no `* text=auto eol=lf`
   rule, and adding one would renormalize files this change does not touch
   (`docs/guides/finalization.md` is stored with CRLF).
4. **No `docs/DECISIONS.md`.** This repository records decisions as ADRs; the D2 version and
   the setup are recorded here.

## Consequences
* Changing a diagram means editing its `.d2`, running `make diagrams` and committing both
  files; CI fails otherwise. `make d2-install` covers Linux x86_64 (WSL on Windows); elsewhere,
  install D2 v0.9.0 and pass `D2=/path/to/d2`. The SVG records `data-d2-version`, so a D2
  upgrade re-renders every diagram, in one change with the new pin.
* Tests do not check the diagrams against the code: review does, and each `.d2` names the
  files it is drawn from. A change to what a diagram shows updates it in the same change.
* The README's image is `main`'s: on a branch, and on a release's PyPI page, it shows the
  diagram as `main` has it, like the README's other links.
* In Edge (Chromium), an SVG inside a page follows the page's color scheme, the site's palette
  toggle included; opened alone, it follows the browser's. raw.githubusercontent.com serves an
  SVG under a sandboxing Content-Security-Policy: inside the README its fonts and dark theme
  work, opened alone its text falls back to a serif font.
* Rendered widths stay under about 1,300 px and each diagram under about 25 nodes; the tall
  ones (the micro-batch sequence, the data flow) are read by scrolling, not by shrinking text.
