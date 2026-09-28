# Contributing

1. Read `CLAUDE.md` (invariants) and `docs/ARCHITECTURE.md`.
2. `uv sync`, then `uv run pytest -q`; with Docker running, also `uv run pytest -m sqlserver`.
3. Behaviour changes in the source need a test in `tests/test_source_fake.py`; T-SQL
   changes need `tests/test_client_sql.py` updates and a lab check run.
4. Design changes need an ADR in `docs/decisions/`.
5. Keep the README options table and output schema in sync with `src/mssql_cdc/source.py`.
