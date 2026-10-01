# Roadmap

## v0.1: validate what exists

- [x] Run `LAB.md` against SQL Server 2022: t1–t7 green (in CI; t1 and t3 also locally),
      code and fake fixed where real CDC differed (ADRs 0008–0010).
- [x] Delta tests passing locally (`tests/test_delta_sink.py`).
- [x] Publish to GitHub; CI `unit`, `integration` and `lab` jobs green.
- [x] Databricks classic (DBR 18.2, dedicated): t5, t6 `--schema`.
- [ ] Databricks classic: t7 `--schema` (needs a lab SQL Server reachable from the cluster).
- [x] Fill the results table in `LAB.md` with links to result files.

## v0.2: production concerns

- [x] **Initial snapshot / bootstrap**: record `max_lsn`, snapshot the table through the
      same backend, start the stream at that LSN (ADR 0016).
- [x] **Automatic re-snapshot after data loss**: `to_delta(on_data_loss="resnapshot")`,
      checkpoint generations, loss events in the facts, at most one per interval (ADR 0018).
- [x] Snapshot partitions for composite or non-integer keys: NTILE tiles of the rows,
      bounds bound typed (ADR 0016 amendment).
- [x] **Seeding from an existing copy**: a helper for tables too big to snapshot within the
      CDC retention (at 4-11k rows/s, billions of rows do not fit in 3 days):
      `CdcStream.seed(target, df, as_of)` writes the copy as the target's snapshot at the LSN
      of the time it started, then `to_delta(bootstrap=True)` starts from it (ADR 0025).
- [ ] NTILE tiles for sparse single integer keys, if uneven MIN..MAX ranges show up
      (NTILE scans and spools the key, MIN..MAX is two seeks).
- [x] **Schema changes**: DDL detected on the driver through `sys.sp_cdc_get_ddl_history`
      (a type change fails the batch before it reads, other DDL is a facts event); the
      stream follows a second capture instance of the table from its start LSN without
      losing changes, bronze takes new columns (`mergeSchema`), optional snapshot at the
      switch (ADR 0023, `sql/switch_capture_instance.sql`).
- [ ] Lab check t9 for the switch under a continuous writer, also against SQL Server 2017;
      on production SQL Server 2016, check that the change tables have `__$command_id`.
- [x] **Silver helper**: apply changes to a target with MERGE, latest image per key by
      `(_start_lsn, _command_id, _seqval, _operation)`, deletes honoured, operation 0
      (snapshot) as an upsert, rebuild from the newest snapshot after a re-snapshot; propagate
      `finalized_until` (`apply_changes`, ADR 0019).
- [x] **Continuous mode finalization**: `finalization.track` registers a
      `StreamingQueryListener` that advances the verdict on progress from a worker thread,
      off the listener bus (ADR 0026).
- [x] Retention headroom in the facts (`retention_watermark_ts`, `retention_headroom_hours`,
      ADR 0017), measured from the batch's end offset (`end_lsn`, `end_commit_ts`).
- [x] **Operational metrics**: capture lag (`now - map_lsn_to_time(max_lsn)`) and ingestion
      lag (`max_lsn` vs the batch's end offset) in the facts table (`source_max_commit_ts`,
      `capture_lag_seconds`, `ingestion_lag_seconds`, ADR 0020); `reportLatestOffset` shows
      `max_lsn` and its commit time in the query progress on every trigger. Batches that
      read no rows write facts too, so facts that stop arriving mean the stream or capture
      stopped.
- [ ] `arrow-odbc` backend covered in CI (install msodbcsql18 in the job).
- [x] **Many tables**: one stream per capture instance, started together by `start_many`
      (`await_all`, `stop_all`), with its own checkpoint, `app_id` and bronze, sharing the
      facts table; a stream over several capture instances was rejected (ADR 0027,
      `docs/guides/many-tables.md`).

## v0.3

- [x] **PyPI release** of 0.1.0 (2026-10-01; steps in `docs/RELEASING.md`):
  - [x] Package metadata, `py.typed`, an sdist without the tests.
  - [x] `release.yml`: a tag publishes to PyPI with Trusted Publishing behind a reviewed
        environment; a manual run publishes to TestPyPI.
  - [x] `CHANGELOG.md` and the 0.x compatibility policy (ADR 0021).
  - [x] Pending publishers on PyPI and TestPyPI, GitHub environments `pypi` and `testpypi`.
  - [x] TestPyPI dry run, then tag `v0.1.0`.
  - [x] `docs/DATABRICKS.md`: install with the `pypi` library type (checked on DBR 18.2).

## Later

- [ ] Scala/Java DSv2 `Changelog` (`TableCatalog.loadChangelog`) so
      `SELECT ... CHANGES FROM VERSION ...` works over SQL Server (Spark 4.2+). Deferred
      until the API settles (ADR 0022).
