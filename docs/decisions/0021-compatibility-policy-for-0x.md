# 0021: Compatibility policy for 0.x

**Status:** accepted  
**Date:** 2026-09-30T11:07:08-03:00  
**Amended:** 2026-10-01T15:55:00-03:00, the public surface is the documentation site's reference (API, options, output schema), not the README  
**Amended:** 2026-10-05T06:01:09-03:00, an older release keeps writing tables a newer one migrated, unless a migration sets `mssql_cdc.min_version` (amendment 2, ADR 0013)  
**Amended:** 2026-10-05T12:35:09-03:00, every release keeps the state its wheel writes under `tests/compat/<version>`, and the current code must resume it (amendment 3)  
**Amended:** 2026-10-05T12:49:29-03:00, the state contract covers the silver and reconcile schemas, the facts `event` values, the JSON of snapshot rows and a backfill wave's userMetadata, whose keys are only added; the public surface is what `docs/reference/api.md` lists (amendment 4)  
**Amended:** 2026-10-05T20:51:10-03:00, the kept state includes a chunked snapshot left open, and the `fake` backend's files in it stay readable (amendment 3)

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
      ([ADR 0028](0028-chunked-snapshot-next-to-the-stream.md)).

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
