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
**Amended:** 2026-10-10T18:02:10-03:00, a chunked snapshot's facts rows leave its key values to a plans table next to bronze: keys removed from the rows 0.7.0 writes only, readers take them from either (amendment 9, ADR 0028 Amendment 4)  
**Amended:** 2026-10-13T18:45:19-03:00, a chunked snapshot opened after a capture instance switch adds `switch_batch_id` to its 'snapshot_open' payload, and its rows take keys by its S (amendment 10, ADR 0023 Amendment 7)  
**Amended:** 2026-10-14T09:06:30-03:00, the 1.x policy: Semantic Versioning from 1.0, the state contract frozen but for additions, a deprecation before every removal (amendment 11)

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
  - the schemas of the facts, control, bronze, silver, reconcile report and (from 0.7.0)
    snapshot plans tables: changed
    only by appending migrations ([ADR 0013](0013-schema-migrations-per-table-kind.md)). A
    release keeps writing a table a newer one migrated, so the jobs that share it upgrade,
    or roll back, one at a time; only a migration that sets `mssql_cdc.min_version` makes
    older releases refuse it, and its release notes say so (amendment 2);
  - the facts table's `event` values (NULL for a micro-batch, `'bootstrap'`,
    `'resnapshot'`, `'snapshot_open'`, `'snapshot_plan'`, `'snapshot_chunk'`,
    `'schema_change'`, `'capture_instance_switched'`, `'data_skipped'`) and the keys of the JSON a later
    release reads back (amendment 4):
    - `detail` of `'snapshot_open'`: `mode`, `kind`, `generation`, `lost_from_ts`,
      `lost_to_ts`, and on a chunked one `keys` and `plan`, and from 0.8.0 on one opened
      after a capture instance switch `switch_batch_id` (amendment 10);
    - `detail` of `'snapshot_plan'`: `snapshot`, `kind`, `keys`, `chunk_rows`, `chunks`, and
      from 0.7.0 `chunk_count`;
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
      ([ADR 0018](0018-automatic-resnapshot-after-data-loss.md), amendment 7);
    - from 0.7.0, the snapshot plans table next to bronze, `<target>_snapshot_plans`, its
      columns `snapshot`, `event` and `detail`, `detail` the three chunked payloads above
      whole (amendment 9).

    Keys are only added: never renamed, removed or given another meaning. A reader takes a
    key added after its payload first shipped with a default (`.get`), so rows an older
    release wrote stay readable, as the `mode` of a `'snapshot_open'` row (missing or
    unknown reads as chunked) and a chunk's `last` (missing reads as false) already are.
    The one removal is amendment 9's: the key values of a chunked snapshot's facts rows,
    from the rows 0.7.0 writes only, which the plans table holds instead.

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
* 1.0 will freeze the Python API as well; nothing here decides when (amendment 11 does).

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

## Amendment 9: key values out of the facts
0.7.0 moves a chunked snapshot's key values out of the facts table, into a plans table next
to bronze with bronze's access ([ADR 0028](0028-chunked-snapshot-next-to-the-stream.md)
Amendment 4). Not additive, so recorded here:

* From the rows 0.7.0 writes for a snapshot it opens, the facts payloads drop `keys` and
  `plan` ('snapshot_open'), `chunks` ('snapshot_plan') and `lo` and `hi`
  ('snapshot_chunk'); 'snapshot_plan' gains `chunk_count`. The plans table's `detail` holds
  each payload whole, under the same keys, so a query reads them there as it read them in
  the facts. Their types make those keys optional (`SnapshotPlanDetail.chunks`,
  `SnapshotChunkDetail.lo` and `hi`; the open's were optional already).
* Rows written before stay as they are and are read as before: a reader takes a payload
  from the plans table, or from the facts row when it has its key values. A snapshot opened
  before 0.7.0 is finished by 0.7.0 in the shape it started with, its key values in the
  facts and nothing in a plans table, so that every job sharing those facts reads it,
  0.6's included; `tests/compat` finishes each release's open snapshot so.
* The other way round, a release before 0.7.0 on a snapshot 0.7.0 opened: `apply_changes`
  finds no `keys` on the open and applies the waves without range deletes, the rebuild at
  completion deleting what they would have; `backfill()` and `reconcile` fail with a
  `KeyError`. Upgrade the jobs that run those with the stream; none of them writes a wrong
  row. No migration sets `mssql_cdc.min_version` for it: the facts table's schema is
  unchanged, and the stream of an older release keeps writing it.

## Amendment 10: a snapshot after a switch
0.8.0 lets `to_delta(snapshot="chunked", snapshot_on_switch=True)` open a chunked snapshot
after the batch that first reads a newer capture instance
([ADR 0023](0023-schema-changes-and-capture-instance-switching.md) Amendment 7). Additive:

* Its 'snapshot_open' payload adds `switch_batch_id`, the `batch_id` of the switch's
  'capture_instance_switched' row (`SnapshotOpenDetail.switch_batch_id`, optional); `kind` is
  `'bootstrap'`, an existing value, and its completion a `'bootstrap'` row, so a consumer
  that filters `event IN ('bootstrap', 'resnapshot')` sees it, and so does an older
  release's `apply_changes`.
* Its 'snapshot_open' and completion rows take Delta keys by its S (`<app_id>#snapopen.<S>`
  and `<app_id>#snapdone.<S>`, version 0), as the generation's own may be taken by its
  bootstrap or re-snapshot. A reader that finds no `switch_batch_id` (every snapshot opened
  before 0.8.0) keeps the generation's keys.
* Facts migration 14 and plans migration 1 change comments only; no `mssql_cdc.min_version`.
* The other way round: a release before 0.8.0 whose `backfill()` finishes a snapshot 0.8.0
  opened after a switch writes its completion under the generation's key, which Delta skips
  when the generation's own snapshot took it: the snapshot then never completes, silver
  keeps its verdict held and a full run raises (one mode while it is open). Run `backfill()`
  with 0.8.0, which writes the row; nothing wrong is written meanwhile.

## Amendment 11: the 1.x policy
1.0 freezes the public Python API and the state contract. 1.0.0rc1 comes first and runs a
real pipeline for a few weeks; 1.0.0 ships when nothing that run finds needs an API change,
and a change it does need goes into another release candidate, not after 1.0.0. From
1.0.0 on, this amendment replaces the Decision's first two bullets (what a minor and a
patch may change); the state contract, the "State compatibility" line and `tests/compat`
stay, with the additions below.

* Semantic Versioning, strictly:
  - a patch (1.x.y) only fixes: it adds no public name, parameter, option, payload key or
    table column;
  - a minor (1.x) adds: public names, parameters whose default keeps today's behaviour,
    source options, keys of a result's `TypedDict`, values of a `Literal` (a mode,
    `BackfillState`), facts `event` values, payload keys, table columns by an appended
    migration; and it deprecates;
  - a major (2.0) breaks: it removes or renames any of those, changes a signature so that
    a call or a type check that passed fails, changes what a default does, or changes the
    state contract other than by adding to it.

  A type-only break is a break (amendment 5): a narrower parameter type, a wider return
  type, a member added to a protocol that a class implements without inheriting
  ([ADR 0030](0030-protocols-for-the-pluggable-seams.md)).
* What is public does not change: what the reference documents (amendment 4), the formats
  `mssql_cdc` and `mssql_cdc_snapshot`, and `CdcStream` only as what `stream()` returns.
  Every name `mssql_cdc` exports is in `docs/reference/api.md` (`tests/test_api_shape.py`).
  The four it exported before 1.0 that the reference never listed, `HAS_ADMISSION_CONTROL`,
  `OPERATIONS`, `MssqlCdcDataSource` and `make_client`, leave its `__all__` and warn when
  imported from it: internal, as this ADR already made them, they stay in
  `mssql_cdc.source` and `mssql_cdc.client`, and leave `mssql_cdc` in 2.0.
  `FinalizationListener` is public for `join`, `last_error` and `failures`; `track()`
  creates it, and its constructor is not public: a listener it built never started its
  worker.
* Readers handle what they do not know: in any minor, a result or a payload may gain keys,
  and `BackfillState` and the facts `event` may gain values. A consumer that matches on
  them keeps a default branch, and reads a key added after its payload first shipped with
  a default (`.get`), as the library does.
* A deprecation before every removal. Nothing public is removed, renamed or made to behave
  otherwise in 2.0 unless a 1.x minor deprecated it first: from then on it kept working and
  warned, naming what replaces it, with a `DeprecationWarning` attributed to the caller
  (`stacklevel=2`, so that a notebook or a test run shows it) for Python, and a WARNING
  logged where the option is read for a source option. The changelog lists it under
  "Deprecated", and the major that removes it under "Removed". A deprecated name stays for
  the rest of 1.x.
* The state contract only grows. Besides the Decision's list and amendments 4 to 10, it
  names what a later release reads back that the list left implicit:
  - the keys of the generation state file `<checkpoint>/_mssql_cdc_generation.json`:
    `generation`, `snapshot_lsn`, `commit_ts`, `at`, `recovering`, `failed_at`;
  - the Delta `txnAppId`s and versions that make each write idempotent, since a release
    that wrote under another would write a replayed row twice. The sink's app_id
    (`<app_id>`, or `<app_id>.g<n>` in generation n) with the batch id, on bronze and, as
    `<sink app_id>#facts`, on the facts table. From the stream's `app_id`, on the facts
    table: `#events` and `#snapshots` (the generation), `#snapopen.<S>`, `#snapplan.<S>`
    and `#snapdone.<S>` (0), `#snapchunks.<S>` (the wave); on the plans table
    `#snapopen.<S>` and `#snapplan.<S>` (0), `#snapchunks.<S>` (the wave); on bronze, a
    wave's `#snap.<S>` (the wave).

  Table schemas change only by appended migrations that add columns or rewrite comments,
  and no 1.x migration sets `mssql_cdc.min_version`: every 1.x release reads and writes
  every 1.x table, so the jobs that share one upgrade, or roll back, one at a time. A change
  that cannot be additive (a key renamed, a column dropped or retyped, a `min_version`) is a
  2.0, with an ADR and a migration path; so is a change that fails `tests/compat`.
* Text for people is not state: the messages in a micro-batch row's `warnings`, a task's
  `reason`, the text of an error, and the `detail` of a 'capture_instance_switched' row
  (`old -> new`, then the columns no longer captured) may be reworded in any release. A
  'schema_change' row's `detail` is the DDL statement as SQL Server recorded it (its first
  500 characters).

Considered:

* Keeping the 0.x rule, a minor may break the API, through 1.x. What 1.0 tells a user is
  that an upgrade within the major needs no change to their code; a break waits for 2.0,
  and the deprecation before it says what to change.
* A deprecation window in time (six months). Releases are small and irregular; a window in
  releases can be checked from the changelog alone, and a deprecated name stays for the rest
  of 1.x anyway.
* Freezing the internal modules too (`client`, `events`, the sink but `delta_sink`, the
  fake). They change with every feature; only the reference is promised, and an internal
  name a user needs is listed there in a minor, which freezes it from then on.
