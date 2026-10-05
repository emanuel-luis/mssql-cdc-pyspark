# 0021: Compatibility policy for 0.x

**Status:** accepted  
**Date:** 2026-09-30T11:07:08-03:00  
**Amended:** 2026-10-01T15:55:00-03:00, the public surface is the documentation site's reference (API, options, output schema), not the README  
**Amended:** 2026-10-05T06:01:09-03:00, an older release keeps writing tables a newer one migrated, unless a migration sets `mssql_cdc.min_version` (amendment 2, ADR 0013)  
**Amended:** 2026-10-05T12:35:09-03:00, every release keeps the state its wheel writes under `tests/compat/<version>`, and the current code must resume it (amendment 3)

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
  - the schemas of the facts, control and bronze tables: changed only by appending
    migrations ([ADR 0013](0013-schema-migrations-per-table-kind.md)). A release keeps
    writing a table a newer one migrated, so the jobs that share it upgrade, or roll back,
    one at a time; only a migration that sets `mssql_cdc.min_version` makes older releases
    refuse it, and its release notes say so (amendment 2).

  A migration path means the new version reads the old state and carries it forward on its
  own (as appended table migrations do), or the release notes give the exact steps.
* Every release entry in `CHANGELOG.md` has a "State compatibility" line: what it does to
  offsets, checkpoint layout and table migrations, with ADR links.
* Every release keeps the state it writes (amendment 3): `tests/compat/generate.py`, run
  with the published wheel, writes a checkpoint with two generations and the bronze,
  silver, facts and control tables to `tests/compat/<version>`, and
  `tests/compat/test_compat.py` resumes each such directory with the current code
  ([RELEASING.md](../RELEASING.md), step 6). Tests that rebuild an old table from today's
  column lists follow any edit of those lists; bytes a release wrote do not. 0.1.0's were
  written from its PyPI wheel after the fact.
* The public surface is what the documentation site's reference documents (amendment, ADR
  0024): the objects listed in `docs/reference/api.md` (`stream()`, the `.to_delta()` and
  `.snapshot()` methods of what it returns, `register()`, `apply_changes()`,
  `finalization.advance()`, `is_final()`, `candidate()` and `end_offset_from_progress()`,
  `sink.delta_sink()`, `DataLossError`, `SchemaChangedError`, and `spark.get_spark()` for
  local sessions), the formats `mssql_cdc` and `mssql_cdc_snapshot`, the options in
  `docs/reference/options.md` and the output schema in `docs/reference/output-schema.md`.
  `CdcStream` is public only as what `stream()` returns, not its name or constructor.
  Everything else is internal and may change in any release, including names exported from
  `mssql_cdc` that the API reference does not list (`make_client`, `MssqlCdcDataSource`,
  `HAS_ADMISSION_CONTROL`, `OPERATIONS`) and the `fake` backend with its `fakePath` option.

## Consequences
* Upgrading within 0.x never costs a re-snapshot or a new checkpoint unless an ADR and the
  CHANGELOG say so, with the steps.
* Changing the state is expensive on purpose: reading the old format stays in the code.
* Documenting something in the reference pages makes it public; keep internals out of them
  or accept the contract. The guides and the README may show public things only.
* 1.0 will freeze the Python API as well; nothing here decides when.
