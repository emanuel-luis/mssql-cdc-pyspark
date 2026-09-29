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
- [ ] Snapshot partitions for composite or non-integer keys, and NTILE tiles for sparse
      ones (today: uniform ranges of an integer leading key, else one partition).
- [ ] **Schema changes**: detect `cdc.ddl_history`; support switching to a second
      capture instance without losing changes.
- [ ] **Silver helper**: apply changes to a target with MERGE, latest image per key by
      `(_start_lsn, _command_id, _seqval, _operation)`, deletes honoured, operation 0
      (snapshot) as an upsert, rebuild from the newest snapshot after a re-snapshot; propagate
      `finalized_until`.
- [ ] **Continuous mode finalization**: `StreamingQueryListener` that advances the
      verdict on progress (check it does not block the listener bus).
- [x] Retention headroom in the facts (`retention_watermark_ts`, `retention_headroom_hours`,
      ADR 0017).
- [ ] **Operational metrics**: capture lag (`now - map_lsn_to_time(max_lsn)`), ingestion
      lag (`max_lsn` vs end offset), via `reportLatestOffset` and the facts table.
- [ ] `arrow-odbc` backend covered in CI (install msodbcsql18 in the job).
- [ ] Multiple capture instances per stream (same schema), or a documented fan-out
      pattern.

## v0.3

- [ ] Scala/Java DSv2 `Changelog` (`TableCatalog.loadChangelog`) so
      `SELECT ... CHANGES FROM VERSION ...` works over SQL Server (Spark 4.2+).
- [ ] PyPI release.
