# Validation

`reconcile` checks that a silver table equals its SQL Server table: after a bootstrap, a
re-snapshot, or on a schedule. It reads the source through the stream's options, as the
snapshot does (READ COMMITTED, never `NOLOCK`), and writes nothing to SQL Server.

## Smallest example

```python
from mssql_cdc import reconcile

result = reconcile(
    spark,
    options,  # the stream's
    "silver.orders",
    bronze="bronze.orders",
    facts_table="ops.ingestion_facts",
    report_table="ops.reconcile_report",
)
# {"run_id": "...", "buckets": 120, "match": 120, "in_flight": 0, "mismatch": 0,
#  "hashed": 2, "failures": {}, "report": DataFrame, ...}
```

`failures` counts what differs by kind; an empty dict and `mismatch == 0` mean silver
matched. `report` holds one row per bucket and one per key or chunk that failed; with
`report_table` they are appended there too ([Tables](../reference/tables.md#reconcile-report)).

## How it compares

1. **Counts per bucket.** For a one-column integer or date key, one scan on SQL Server
   counts the rows and sums the keys per bucket of the key's range (`COUNT_BIG`, `SUM` as
   `decimal(38,0)`, a date as its day number), and Spark computes the same over silver.
   Buckets hold about `bucket_rows` rows. A bucket whose counts and sums are equal is a
   MATCH, else a MISMATCH. Any other key (composite, or a string a collation orders) gets
   one count of the whole table: SQL Server and Spark do not order it alike.
2. **Rows.** Every MISMATCH bucket, and a `sample` of the MATCH ones (for other keys, a
   sample of key ranges SQL Server cuts), are read from the source and joined with silver on
   the key, comparing a SHA-256 of each row's captured columns computed by the same Spark
   function on both sides:
   - MISSING_TARGET: the key is only in the source, an insert silver never applied;
   - MISSING_SOURCE: the key is only in silver, a delete it never applied, or a stale key;
   - RECORD_DIFF: other values, an update it never applied; `detail` names the columns.
3. **In flight.** A bucket or key that differs while bronze holds a change to it newer than
   what either side read is IN_FLIGHT: silver has not applied it yet, or the source read
   came before it. Run again later; it clears once silver catches up.
4. **Chunks.** With `facts_table`, bronze's newest [chunked snapshot](bootstrap.md#chunked-snapshots)
   is checked against its facts rows, without reading SQL Server:
   - CHUNK_TILING: the chunks leave a gap or overlap: a chunk missing or recorded twice, the
     first not open below, one not starting where the one before ended, or the last of a
     complete snapshot not open above;
   - CHUNK_ROWS: a chunk's `snapshot_chunk` row counts other rows than bronze holds of it;
   - CHUNK_STAMP: a chunk stamped below the snapshot's LSN.

   A wave still being written is no failure: the facts are read before bronze, which
   commits a wave's rows before its facts rows.

## Parameters

- `keys`: the key columns; by default the capture instance's unique index.
- `bronze`: the table silver is applied from, whose newer changes make a difference
  IN_FLIGHT.
- `facts_table`: the stream's, for the chunk checks.
- `bucket_rows`: rows per bucket (default 1,000,000).
- `sample`: the fraction of MATCH buckets also compared row by row (default 0.01; 0 none,
  1 all). Equal counts and key sums can hide an update, so keep a sample.
- `report_table`: where the report is appended; `seed`: the sample's random seed.

## Pitfalls

- Run it while the stream keeps up: a change the stream has not read yet cannot be seen,
  and shows as a MISMATCH until a later run.
- The row comparison reads the source rows of the buckets it compares. Keep `sample` small
  on a large table, and run it off-peak.
- A string or composite key is counted as a whole table, and its rows are compared from the
  source's side: a key only silver holds shows in that count alone, and one missed insert
  with one missed delete cancel out there. Only the sampled ranges find the insert.

## See also

- [Silver tables](silver.md): what is compared.
- [Bootstrap](bootstrap.md): the snapshots it validates.
- [`reconcile`](../reference/api.md#mssql_cdc.reconcile) in the API reference.
