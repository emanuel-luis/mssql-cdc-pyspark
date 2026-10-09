# 0021: Compatibility policy for 0.x

**Status:** accepted  
**Date:** 2026-09-30T11:07:08-03:00  
**Amended:** 2026-10-01T15:55:00-03:00, the public surface is the documentation site's reference (API, options, output schema), not the README  
**Amended:** 2026-10-05T06:01:09-03:00, an older release keeps writing tables a newer one migrated, unless a migration sets `mssql_cdc.min_version` (amendment 2, ADR 0013)  
**Amended:** 2026-10-05T12:35:09-03:00, every release keeps the state its wheel writes under `tests/compat/<version>`, and the current code must resume it (amendment 3)  
**Amended:** 2026-10-05T12:49:29-03:00, the state contract covers the silver and reconcile schemas, the facts `event` values, the JSON of snapshot rows and a backfill wave's userMetadata, whose keys are only added; the public surface is what `docs/reference/api.md` lists (amendment 4)  
**Amended:** 2026-10-05T20:51:10-03:00, the kept state includes a chunked snapshot left open, and the `fake` backend's files in it stay readable (amendment 3)  
**Amended:** 2026-10-07T01:34:59-03:00, the API shape from 0.3.0: options and flags keyword-only, modes typed as `Literal`s, results as `TypedDict`s whose keys are only added (amendment 5)  
**Amended:** 2026-10-07T01:41:18-03:00, a micro-batch row's `detail` is a payload too, with the added key `warnings` (amendment 6, ADR 0023 Amendment 6)  
**Amended:** 2026-10-07T09:14:31-03:00, the payloads and the source options get types, and a 'data_skipped' row's `detail` joins the listed payloads (amendment 7)  
**Amended:** 2026-10-09T11:39:56-03:00, a wave's measured values in its userMetadata may be null where the platform refuses caching (amendment 8, ADR 0032)

## Context
0.1.0 is the first release on PyPI. Semantic Versioning promises nothing before 1.0, but
a stream leaves state behind that outlives any version: offsets in Spark checkpoints,
checkpoint directories, and Delta tables in users' catalogs. A user who upgrades a patch
or a minor and finds a checkpoint that no longer resumes, or a facts table the new version
cannot write, has lost more than an API: they have to re-snapshot, or rebuild by hand.
The Python API, on the other hand, is young and will change.

## Decision
* Minor releases (0.x to 0.x+1) may break the public Python API. Every break is listed
  under "Breaking" in `CHANGELOG.md`, with what to change.
* Patch releases (0.x.y to 0.x.y+1) are fixes only.
* The state contract never breaks without a migration path and an ADR, in any release:
  - the offset JSON `{"lsn", "commit_ts"}` (invariant 1,
    [ADR 0002](0002-lsn-offsets-with-commit-time.md));
  - the checkpoint layout, including `<checkpoint>/_generations/<n>`, the state file
    `<checkpoint>/_mssql_cdc_generation.json` and the `<app_id>.g<n>` app ids
    ([ADR 0018](0018-automatic-resnapshot-after-data-loss.md));
  - the schemas of the facts, control, bronze, silver and reconcile report tables: changed
    only by appending migrations ([ADR 0013](0013-schema-migrations-per-table-kind.md)). A
    release keeps writing a table a newer one migrated, so the jobs that share it upgrade,
    or roll back, one at a time; only a migration that sets `mssql_cdc.min_version` makes
    older releases refuse it, and its release notes say so (amendment 2);
  - the facts table's `event` values (NULL for a micro-batch, `'bootstrap'`,
    `'resnapshot'`, `'snapshot_open'`, `'snapshot_plan'`, `'snapshot_chunk'`,
    `'schema_change'`, `'capture_instance_switched'`, `'data_skipped'`) and the keys of the JSON a later
    release reads back (amendment 4):
    - `detail` of `'snapshot_open'`: `mode`, `kind`, `generation`, `lost_from_ts`,
      `lost_to_ts`, and on a chunked one `keys` and `plan`;
    - `detail` of `'snapshot_plan'`: `snapshot`, `kind`, `keys`, `chunk_rows`, `chunks`;
    - `detail` of `'snapshot_chunk'`: `snapshot`, `chunk`, `wave`, `lo`, `hi`, `last`;
    - `detail` of a chunked snapshot's `'bootstrap'` or `'resnapshot'` row: `snapshot`,
      `chunks`, `rows`, `last_lsn`;
    - the userMetadata of a backfill wave's bronze commit: `backfill`, `wave`, `lsn`,
      `attempt`, `chunks`, each chunk with `chunk`, `lo`, `hi`, `last`, `rows`, `high_lsn`,
      `read_seconds`, `read_mb`
      ([ADR 0028](0028-chunked-snapshot-next-to-the-stream.md));
    - `detail` of a micro-batch row (`event` NULL): `warnings`, added in 0.3.0
      (amendment 6);
    - `detail` of `'data_skipped'`: `from`, `to`, `certain`, and from a task that found
      cleanup had run while it read, `reason`
      ([ADR 0018](0018-automatic-resnapshot-after-data-loss.md), amendment 7).

    Keys are only added: never renamed, removed or given another meaning. A reader takes a
    key added after its payload first shipped with a default (`.get`), so rows an older
    release wrote stay readable, as the `mode` of a `'snapshot_open'` row (missing or
    unknown reads as chunked) and a chunk's `last` (missing reads as false) already are.

  A migration path means the new version reads the old state and carries it forward on its
  own (as appended table migrations do), or the release notes give the exact steps.
* Every release entry in `CHANGELOG.md` has a "State compatibility" line: what it does to
  offsets, checkpoint layout, table migrations and event payloads, with ADR links.
* Every release keeps the state it writes (amendment 3): `tests/compat/generate.py`, run
  with the published wheel, writes a checkpoint with two generations and the bronze,
  silver, facts and control tables to `tests/compat/<version>`, and from 0.2.0 on a
  second stream's chunked snapshot left open after one wave (amendment 4's payloads); and
  `tests/compat/test_compat.py` resumes each such directory with the current code, and
  finishes that snapshot ([RELEASING.md](../RELEASING.md), step 6). Tests that rebuild an
  old table from today's column lists follow any edit of those lists; bytes a release wrote
  do not. 0.1.0's were written from its PyPI wheel after the fact. The directories also
  hold the source the release's `fake` backend wrote (`src/`), which the test reads with
  today's fake: the fake stays internal, but a change to its files must keep reading the
  old ones.
* The public surface is what the documentation site's reference documents (amendment, ADR
  0024): the objects `docs/reference/api.md` lists, the formats `mssql_cdc` and
  `mssql_cdc_snapshot`, the options in `docs/reference/options.md` and the output schema in
  `docs/reference/output-schema.md` (amendment 4: the page is the list; the copy kept here
  fell behind it). `CdcStream` is public only as what `stream()` returns, not its name or
  constructor. Everything else is internal and may change in any release, including names
  exported from `mssql_cdc` that the API reference does not list, and the `fake` backend
  with its `fakePath` option.

## Consequences
* Upgrading within 0.x never costs a re-snapshot or a new checkpoint unless an ADR and the
  CHANGELOG say so, with the steps.
* Changing the state is expensive on purpose: reading the old format stays in the code.
* Documenting something in the reference pages makes it public; keep internals out of them
  or accept the contract. The guides and the README may show public things only.
* 1.0 will freeze the Python API as well; nothing here decides when.

## Amendment 4: the snapshot payloads are state
A chunked snapshot stays open for days or weeks, so its `'snapshot_open'` and
`'snapshot_plan'` rows are often written by one release and read by the next, and a wave
interrupted before its facts rows is rebuilt from its commit's userMetadata, maybe by a
newer release. `backfill()`, `apply_changes` and `reconcile` all read these payloads, yet the
contract listed only offsets, the checkpoint layout and the facts, control and bronze
schemas, although the silver and reconcile tables have migration kinds of their own.

* Every payload a release wrote has the shape listed above: 0.2.0rc1 introduced them and no
  release has changed one since. An unreleased build put the kind in `mode` and wrote no
  `kind`; `backfill()` now completes such an open, taking generation 0 as a bootstrap and a
  later one as a re-snapshot.
* Considered: a version field in every payload. Additive keys read with defaults need none,
  and a version adds a second rule to keep. A change that cannot be additive gets a new
  key, or a new event value, and an ADR.
* Considered: one module that owns the payloads and derives a snapshot's state for every
  reader. The readers' rules differ on purpose: `backfill()` and `apply_changes` take any
  `'bootstrap'` or `'resnapshot'` row at or after S as complete (a newer full re-snapshot
  supersedes the chunks), while `reconcile` checks the chunks only against a completion row
  at S itself.

## Amendment 5: the API shape from 0.3.0
0.2 grew `to_delta` to eleven parameters, all positional, each new one appended so that
positional calls kept working: `to_delta(t, a, c, None, None, None, True)` was a bootstrap,
and `snapshot("bronze.x", True)` a re-snapshot. Its siblings took options by keyword only,
but `apply_changes` took four strings in a row. Modes were plain `str` and results untyped
dicts, so a misspelt mode or key surfaced only at run time.

* Leading arguments stay positional: what a call is about, such as `to_delta(target,
  app_id, checkpoint, facts_table)` ([ADR 0014](0014-network-and-read-metrics-in-facts.md)'s
  form), `apply_changes(spark, bronze, target)`, `reconcile(spark, options, silver)`,
  `advance(spark, control_table, table_name, end_offset)`, `get_spark(app_name, master)`.
  Everything after them, options, flags and modes, is keyword-only. Breaking, in 0.3.0:
  `to_delta` after `facts_table`; `snapshot`'s `resnapshot`; `apply_changes`'s
  `capture_instance` and `keys`; `reconcile`'s `keys`; `granularity` of `advance`, `track`,
  `candidate` and the `FinalizationListener` constructor; `timeout` of `await_all` and
  `FinalizationListener.join`; `end_offset_from_progress`'s `source_index`; `delta_sink`'s
  `metrics_path`; `get_spark`'s `delta`.
* Every mode parameter is a `Literal` alias defined once in `mssql_cdc.types` and exported
  from `mssql_cdc`: `SnapshotMode`, `OnDataLoss`, `Isolation`, `Granularity`, and
  `BackfillState` for `backfill()`'s `state`. The run-time check stays: a wrong value raises
  `ValueError` naming the allowed ones. A `Literal` names the canonical spelling; where the
  check ignores case (`isolation`, `granularity`) it still does, so `"readcommitted"` runs
  but does not type-check. `backfill()`'s `isolation` and `candidate`'s `granularity` took
  any `str` before: a type-check break, listed under "Breaking".
* Results are `TypedDict`s, plain dicts at run time: `Offset` (`snapshot()`, `seed()`,
  `end_offset_from_progress()`),
  `BackfillStatus`, `ApplyResult`, `ReconcileResult`. Their keys are public API: a minor
  release may add one, and removing or renaming one is a break listed under "Breaking".
  Parameters that take an offset accept any mapping, so an `Offset` and a parsed progress
  offset both fit.
* Considered: a release with shims that warn (`FutureWarning`) on the old positional forms,
  as pandas 2.0 did. A positional call fails at once with Python's own `TypeError` naming
  the call, before anything runs, so the change cannot go unnoticed or half-run; a shim
  would be code kept for one release only.
* `tests/typing_api.py`, checked by mypy, pins the result types and keeps the old forms and
  wrong modes type errors.

## Amendment 6: a micro-batch row's warnings
0.3.0 writes a micro-batch row's `detail` for the first time: JSON `{"warnings": [...]}`, the
warnings the reader logged on the driver, when there were any
([ADR 0023](0023-schema-changes-and-capture-instance-switching.md) Amendment 6). An added
payload, no change to a shipped one: rows written before, and batches without warnings,
keep `detail` NULL, so a query reads the key with a default. The key is state; the messages
in the list are text for people and may change in any release.

## Amendment 7: types for the payloads and the options
0.4.0 types what the library writes and reads back. The payloads listed above were dicts
built inline at each writer and parsed at each reader, so a writer that dropped or misspelt a
key failed only when a later call, maybe a later release, read the row.

* Every listed payload is a `TypedDict` in `mssql_cdc.payloads`, the row payloads also
  exported from `mssql_cdc` for queries of the facts table: `SnapshotOpenDetail`,
  `SnapshotPlanDetail`, `SnapshotChunkDetail`, `SnapshotCompletionDetail`,
  `DataSkippedDetail`, `BatchDetail` and `WaveMetadata` (its chunks `WaveChunk`). Writers
  build them as these types, so mypy fails one that leaves out a key; readers annotate what
  `json.loads` returns and keep their `.get` defaults. `tests/test_payloads.py` keeps every
  key listed here in its type. The types describe the state, they do not change it: keys,
  formats and meanings are as before, and the rule stays that a key is only added.
* A key a payload does not always have, a chunked open's `keys` and `plan` or a task's
  `reason`, or one added after its payload first shipped, is optional in its type. Not
  through `NotRequired`, which `typing` has from Python 3.11 while the package supports
  3.10 without depending on `typing_extensions`: the required keys sit in a private base
  class and the optional ones in a `total=False` subclass, which gives the same keys at run
  time on every version.
* A 'data_skipped' row's `detail`, written since 0.2.0 and documented with the facts table,
  joins the list: no release reads it back, but a query of the facts does. Its `reason` is
  text for people, like the warnings.
* `SourceOptions`, a `TypedDict` with `total=False`, names every source option with a `str`
  value, as Spark passes them, and `KNOWN_OPTIONS` is derived from it, so the two cannot
  drift. The functions that take options (`stream`, `reconcile`, `apply_changes`,
  `start_many`) take `SourceOptions | Mapping[str, Any]`: wider than 0.3's `dict`, so every
  call that type-checked still does, and a dict annotated as `SourceOptions` gets its names
  checked. Considered: `Literal` values for `backend`, `isolationLevel` or the booleans; the
  reader takes them in any case, and booleans in several spellings, so a `Literal` would
  refuse values that run.
* Considered: frozen dataclasses for the payloads. They are JSON that other releases and
  users' queries read, so a dataclass would need converting both ways. Internal records
  nothing persists are frozen dataclasses instead: the chunks `backfill()` counts, and the
  chunks a 'snapshot_chunk' row announces to `apply_changes`.
* A chunked 'snapshot_open' row now writes `keys` and `plan` after `lost_to_ts` rather than
  after `kind`. A JSON object has no order, and every reader takes its keys by name.
* `tests/typing_payloads.py`, checked by mypy with `tests/typing_api.py`, pins the payloads'
  key types and keeps a misspelt option in a `SourceOptions` a type error.

## Amendment 8: a wave's counts may be null
Where the platform refuses to cache a wave (Databricks serverless), `backfill()` writes the
wave's commit while it reads it, so the commit's userMetadata cannot hold what the read
measures ([ADR 0032](0032-facts-without-caching.md)). Not additive, so recorded here: the
keys stay, and a chunk's `rows`, `high_lsn`, `read_seconds` and `read_mb` are null in such a
commit (`WaveChunk.rows` becomes `int | None`; the other three were nullable already). The
wave's 'snapshot_chunk' facts rows hold the values. A reader takes null as not known:
`backfill()` counts the chunk's rows in bronze, as it does for a commit log cleanup dropped.
A release before 0.6.0 that finds such a commit (only after a crash between the wave's
append and its facts rows) writes NULL counts for its chunks.
