# 0031: Strict typing: mypy strict on `src`, `pyright --verifytypes` on the public API

**Status:** accepted  
**Date:** 2026-10-07T10:49:11-03:00

## Context
Through 0.3 mypy ran with `check_untyped_defs`: it read the bodies of unannotated functions,
but most of `src` had no annotations, `spark` parameters, the clients' methods and many
results among them. A user's type checker saw those as unknown. 0.4 adds typed API (the
protocols of [ADR 0030](0030-protocols-for-the-pluggable-seams.md), the payload `TypedDict`s
of [ADR 0021](0021-compatibility-policy-for-0x.md) amendment 7, `SourceOptions`), which helps
only if what surrounds it is typed too, and stays so. Measured before this change: mypy in
strict mode found 356 errors in 17 files; `pyright --verifytypes mssql_cdc
--ignoreexternal` scored the public API 76.4% type-complete (134 of 589 exported symbols
unknown).

## Decision
* mypy runs with `strict = true` on `src` and `tests/typing_*.py` (one config in
  `pyproject.toml`): every def annotated, no bare generics, no `Any` returned as a typed
  value, a name imported into a module exported only through its `__all__`, strict
  equality. A `# type: ignore` names its code and says why (`ignore-without-code` stays on).
* Values whose type is the data's are `Any`, and only those: a driver's row
  (`dict[str, Any]`), a key (`tuple[Any, ...]`, one value per key column), a JSON payload's
  bound, an option read from a mapping. Where the code's logic guarantees what a type cannot
  see, a `cast` or an `assert` says so in a comment, as the code already did (`assert row is
  not None  # a global aggregate always returns one row`).
* CI's `lint` job runs `pyright --verifytypes mssql_cdc --ignoreexternal`, which fails below
  100%: every public symbol (the exported names, and the functions, classes and attributes
  of the public modules) has a known type. `--ignoreexternal`: `pyarrow` ships no types.
  pyright is in the dev group, pinned by `uv.lock` as ruff and mypy are, and runs in the
  project's environment (`uv run`), where it finds the package and its dependencies.
* A type follows what the code does. Where an annotation found a value its type said could
  not be there, the type changed, not the code: `CdcClient.max_lsn()` returns `Lsn | None`
  (NULL on a database capture has not written to; every caller already handled it), and
  `WaveChunk.high_lsn` is `str | None` (a chunk whose task could not read `max_lsn` writes
  null). Both came with 0.4's unreleased types.

Considered:

* A completeness floor that may only rise: not needed once this change reached 100%.
* `uvx pyright==<version> --verifytypes ...`: run from `uvx`'s isolated environment it found
  no `mssql_cdc` and scored 0% ("No py.typed file found"), even with `--pythonpath` naming the
  project's interpreter; run in the project's environment it finds it.
* Strict mode on every test: fixtures and fakes would gain annotations no reader needs. The
  `tests/typing_*.py` files are the API's contract, and are strict.

## Consequences
* A new public function, attribute or module constant without a type fails CI; an instance
  attribute or constant whose inferred type a checker could read otherwise needs an
  annotation.
* Nothing changed at run time but where an annotation needed it: a module that annotates
  with a name imported under `TYPE_CHECKING` has `from __future__ import annotations`;
  `cast` returns its argument; the asserts hold wherever the code ran before. One planning
  metric skips its query when `max_lsn` is NULL: it asked for the commit time of no LSN, which
  was None.
