# Contributing

1. Read `CLAUDE.md` (invariants) and `docs/ARCHITECTURE.md`.
2. `uv sync`, then `uv run ruff check`, `uv run ruff format`, `uv run mypy`,
   `uv run pyright --verifytypes mssql_cdc --ignoreexternal` and `uv run pytest -q`; with
   Docker running, also `uv run pytest -m sqlserver`. CI runs the first four (the formatter
   with `--check`) in its `lint` job before anything else.
3. Behaviour changes in the source need a test in `tests/test_source_fake.py`; T-SQL
   changes need `tests/test_client_sql.py` updates and a lab check run.
4. Design changes need an ADR in `docs/decisions/`.
5. Keep `docs/reference/options.md` and `docs/reference/output-schema.md` in sync with the
   options read in `src/mssql_cdc/source.py` and `client.make_client`; usage is documented
   once, on the site (`docs/`), and the README links to it.
6. User-visible changes get an entry under `## [Unreleased]` in `CHANGELOG.md`; breaking
   ones follow ADR 0021. Releases: `docs/RELEASING.md`.
7. Security issues are reported privately, never in a public issue: see `SECURITY.md`.
