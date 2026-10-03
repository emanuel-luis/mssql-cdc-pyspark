"""Is a silver table equal to its SQL Server table? Count by key range, then compare rows.

    from mssql_cdc import reconcile
    result = reconcile(spark, options, "silver.orders", bronze="bronze.orders",
                       control_table="ops.table_finalization",
                       report_table="ops.reconcile_report")

``options`` are the stream's; ``silver`` is the table ``apply_changes`` builds from ``bronze``.
Nothing is written to SQL Server, and the reads are the snapshot's (READ COMMITTED, never
NOLOCK).

* Tier 1, every row: one scan on SQL Server counts the rows and sums the keys (``COUNT_BIG``,
  ``SUM`` as ``decimal(38,0)``) per bucket of the key's range, and Spark the same over silver;
  adjacent buckets are merged until they hold ``bucket_rows`` rows. Only for a one-column
  integer or date key, which SQL Server and Spark order alike. Any other key (composite, or a
  string a collation orders) gets one count of the whole table.
* Tier 2, row by row: every MISMATCH bucket and a ``sample`` of the MATCH ones (for any other
  key, a sample of ranges of about ``bucket_rows`` rows cut by ``NTILE``) are read through the
  snapshot reader (``snapshotChunks``, on ``keys``: ``snapshotKeys``) and joined with silver on the key, comparing
  ``sha2(to_json(struct(<captured columns>)), 256)`` computed by the same Spark function on
  both sides. MISSING_TARGET: the key only in the source (an insert not applied);
  MISSING_SOURCE: only in silver (a delete not applied, a stale key); RECORD_DIFF: other
  values (an update not applied). For any other key silver is joined from the source's rows,
  which finds only the first and the last: no range Spark cuts of silver is SQL Server's.
* IN_FLIGHT: M is ``max_lsn`` read just before the source is read, E silver's
  ``applied_lsn`` in ``control_table``, read before the silver version compared, which holds
  every change up to it (not its newest ``_start_lsn``: a chunk row's stamp can be ahead of
  the changes applied). A bucket or a key that differs while bronze holds a change to it
  (not a snapshot row) after the older of the two is IN_FLIGHT: check it again later. While
  a chunked snapshot is open, the keys of the chunks silver lacks are MISSING_TARGET. Equal counts are a MATCH even then. A change the stream has
  not read yet cannot be seen: run it while the stream keeps up; a MISMATCH that causes
  clears on the next run.
* Chunks, with ``facts_table``: bronze's newest chunked snapshot (ADR 0028) is checked
  against its 'snapshot_chunk' facts rows, without reading SQL Server. CHUNK_TILING: the
  chunks leave a gap or overlap (each starts where the one before ended, the first open
  below and, once complete, the last the plan's final one); CHUNK_ROWS: a chunk's facts row counts
  other rows than bronze holds of it; CHUNK_STAMP: a chunk stamped below the snapshot's LSN
  S. The facts are read before bronze, which commits a wave's rows before its facts rows,
  so a wave still being written is no failure.

Returns ``{"run_id", "silver_version", "silver_lsn", "source_lsn", "buckets", "match",
"in_flight", "mismatch", "hashed", "failures": {failure_type: rows}, "report"}``, ``report``
the run's report as a DataFrame, which ``report_table`` also gets (``REPORT_COLUMNS``).
"""

from __future__ import annotations

import json
import math
import random
import uuid
from bisect import bisect_right
from collections.abc import Sequence
from contextlib import closing
from datetime import date, timedelta
from decimal import Decimal
from itertools import pairwise

from . import migrations
from .silver import _one, _q, _source_keys
from .tables import delta_table, table_ref

REPORT_COMMENT = (
    "Report of mssql-cdc-pyspark's reconcile(), which compares a silver table with its SQL "
    "Server table. Each run adds one row per bucket of the key's range (the rows and the key "
    "sum on each side) and one row per key whose row differs in the buckets compared row by "
    "row; run_id groups them."
)
REPORT_COLUMNS = [
    ("run_id", "STRING", "Id of the reconcile() run (a UUID): every row it wrote has it."),
    ("run_at", "TIMESTAMP_NTZ", "When the run started, UTC."),
    ("silver", "STRING", "The silver table compared (a name or a path)."),
    ("silver_version", "BIGINT", "The Delta version of silver compared, the same in the run."),
    (
        "silver_lsn",
        "STRING",
        (
            "Silver's applied_lsn (0x + 20 hex) in the control table, read before that "
            "version: silver holds the changes up to it."
        ),
    ),
    (
        "source_lsn",
        "STRING",
        (
            "sys.fn_cdc_get_max_lsn() read just before the source was read: the source held "
            "every commit up to it. Bucket rows: before the counts; key rows: before the rows; "
            "NULL on chunk rows."
        ),
    ),
    (
        "bucket_lo",
        "STRING",
        (
            "First key of the bucket, inclusive, as JSON: a value for a one-column key, a list "
            "for a composite one. NULL: open (both NULL: the whole table, counted as one). "
            "Chunk rows: the chunk's first key."
        ),
    ),
    (
        "bucket_hi",
        "STRING",
        "Key the bucket ends before, exclusive, as JSON. NULL: open. Chunk rows: the chunk's.",
    ),
    ("source_rows", "BIGINT", "Bucket rows: the bucket's rows in the source table."),
    (
        "source_key_sum",
        "DECIMAL(38,0)",
        (
            "Bucket rows: the sum of the bucket's keys in the source table (a date as its day "
            "number from 1970-01-01); NULL on a whole-table count."
        ),
    ),
    ("silver_rows", "BIGINT", "Bucket rows: the bucket's rows in silver."),
    ("silver_key_sum", "DECIMAL(38,0)", "Bucket rows: the sum of the bucket's keys in silver."),
    (
        "status",
        "STRING",
        (
            "Bucket rows: MATCH (rows and key sums equal), IN_FLIGHT (they differ, and bronze "
            "holds a change to the bucket newer than what one side read: check again later) or "
            "MISMATCH (they differ). NULL on key and chunk rows."
        ),
    ),
    (
        "hashed",
        "BOOLEAN",
        (
            "Bucket rows: whether its rows were also compared one by one (every MISMATCH and a "
            "sample of the rest); the key rows of the run in it are what differs."
        ),
    ),
    (
        "key",
        "STRING",
        (
            "Key rows: the key of a row that differs, as a JSON object. NULL on bucket and "
            "chunk rows."
        ),
    ),
    (
        "failure_type",
        "STRING",
        (
            "Key rows: MISSING_TARGET (in the source, not in silver: an insert not applied), "
            "MISSING_SOURCE (in silver, not in the source: a delete not applied, or a stale "
            "key), RECORD_DIFF (in both with other values: an update not applied) or IN_FLIGHT "
            "(differs, and bronze holds a change to the key newer than what one side read: "
            "check again later). Chunk rows (with facts_table, for the newest chunked snapshot "
            "of bronze): CHUNK_TILING (its chunks leave a gap or overlap: an index missing or "
            "twice, the first not open below, one not starting where the one before ended, the "
            "last of a complete snapshot not the plan's final one), CHUNK_ROWS (a chunk's "
            "'snapshot_chunk' facts row counts other rows than bronze holds of it) or "
            "CHUNK_STAMP (a chunk stamped below the snapshot's LSN)."
        ),
    ),
    (
        "detail",
        "STRING",
        (
            "Key rows: JSON with the silver row's _start_lsn (silver_start_lsn) and, when both "
            "sides have the row, the columns that differ (columns). Chunk rows: JSON with the "
            "snapshot's LSN (snapshot), the chunk and what was found."
        ),
    ),
]
_SCHEMA = ", ".join(f"`{n}` {t}" for n, t, _ in REPORT_COLUMNS)
_EPOCH = date(1970, 1, 1)
_DAYS = ((date.min - _EPOCH).days, (date.max - _EPOCH).days)  # a date key's ordinals
# ponytail: about this many buckets come back from the count query (and from silver's), merged
# into buckets of bucket_rows rows on the driver: a bucket is never finer than 1/_FINE of the
# key's range. Raise it if very skewed keys need finer buckets.
_FINE = 100_000
# Spark typeName() of the key types SQL Server and Spark order alike, by kind of ordinal
_KINDS = {"byte": "int", "short": "int", "integer": "int", "long": "int", "date": "date"}


def _ordinal_sql(kind: str, key: str) -> str:
    """Spark SQL of the key's ordinal, as ``SqlCdcClient.key_buckets`` computes it."""
    if kind == "int":
        return f"CAST({_q(key)} AS BIGINT)"
    return f"CAST(datediff({_q(key)}, DATE'1970-01-01') AS BIGINT)"


def _bound(kind: str, o: int):
    """The key at ordinal ``o``, as JSON holds it: a snapshotChunks bound, a report column. A
    bucket's ends can pass a date key's range (9999-12-31): below it, its first day; above, None
    (open), which is the same rows."""
    if kind == "int":
        return o
    if o > _DAYS[1]:
        return None
    return (_EPOCH + timedelta(days=max(o, _DAYS[0]))).isoformat()


def _latest(spark, table: str):
    """``table``'s latest version and a read pinned to it."""
    version = int(delta_table(spark, table).history(1).first()["version"])
    return version, spark.sql(f"SELECT * FROM {table_ref(table)} VERSION AS OF {version}")


def _after(bronze, lower: str):
    """``bronze``'s changes after ``lower``: what one side may not hold yet. Snapshot rows are
    no change: a chunk's stamp can be above all silver applied once it is rebuilt."""
    from pyspark.sql import functions as F

    return bronze.where((F.col("_operation") != 0) & (F.col("_start_lsn") > lower))


def _sample(rng: random.Random, items: list[int], fraction: float) -> list[int]:
    return rng.sample(items, math.ceil(fraction * len(items)))


def _range_buckets(spark, client, source, key, kind, target, bronze, lower, bucket_rows):
    """Tier 1 for an integer or date key: buckets of about ``bucket_rows`` rows, as dicts of
    ordinals ``lo``/``hi`` [lo, hi), (rows, key_sum) of ``source``/``silver``, ``moved``
    (bronze changed it after ``lower``) and the ``fine`` bucket ids it holds; and the Spark
    expression of a row's fine bucket id."""
    from pyspark.sql import functions as F

    sql = _ordinal_sql(kind, key)
    o = F.expr(sql)
    lo, hi = client.key_range(source.schema, source.table, key)
    ends = [v if kind == "int" else (v - _EPOCH).days for v in (lo, hi) if v is not None]
    ends += [v for v in target.agg(F.min(o), F.max(o)).first() if v is not None]
    if not ends:  # both empty
        return [], F.lit(None)
    width = max(1, -(-(max(ends) - min(ends) + 1) // _FINE))
    fine = F.expr(f"({sql} - pmod({sql}, {width})) div {width}")  # floor, as the T-SQL's
    in_source = client.key_buckets(source.schema, source.table, key, kind, width)
    _, changes = _latest(spark, bronze)  # after the count: holds what it may have seen
    counted = target.where(o.isNotNull()).groupBy(fine)
    in_silver = counted.agg(F.count(F.lit(1)), F.sum(o.cast("decimal(38,0)"))).collect()
    sides = {
        "source": {b: (n, s) for b, n, s in in_source},
        "silver": {b: (n, s) for b, n, s in in_silver},
    }
    moved = _after(changes, lower).select(fine).distinct().collect()
    ids = sorted(sides["source"].keys() | sides["silver"].keys())
    starts, rows = [], bucket_rows
    for b in ids:
        if rows >= bucket_rows:
            starts.append(b)
            rows = 0
        rows += max(sides["source"].get(b, (0,))[0], sides["silver"].get(b, (0,))[0])
    end = ids[-1] + 1
    buckets = [
        {"lo": a * width, "hi": z * width, "fine": [], "moved": False}
        | {side: [0, Decimal(0)] for side in sides}
        for a, z in zip(starts, [*starts[1:], end])
    ]
    for b in ids:
        bucket = buckets[bisect_right(starts, b) - 1]
        bucket["fine"].append(b)
        for side, counts in sides.items():
            n, s = counts.get(b, (0, 0))
            bucket[side] = [bucket[side][0] + n, bucket[side][1] + s]
    for (b,) in moved:
        if b is not None and starts[0] <= b < end:
            buckets[bisect_right(starts, b) - 1]["moved"] = True
    return buckets, fine


def reconcile(
    spark,
    options: dict,
    silver: str,
    keys: Sequence[str] | None = None,
    *,
    bronze: str,
    control_table: str,
    facts_table: str | None = None,
    bucket_rows: int = 1_000_000,
    sample: float = 0.01,
    report_table: str | None = None,
    seed: int | None = None,
) -> dict:
    """Compare ``silver`` with the table the capture instance in ``options`` tracks (see the
    module docstring).

    ``keys``: the source's key columns; without them, the capture instance's unique index.
    ``bronze``: the table silver is applied from; its newer changes make a difference
    IN_FLIGHT. ``control_table``: ``apply_changes``'s, for silver's ``applied_lsn``.
    ``facts_table``: the stream's, to check the chunks of bronze's newest chunked
    snapshot. ``bucket_rows``: rows per bucket. ``sample``: the fraction of the MATCH buckets
    also compared row by row (0: none, 1: all). ``seed``: of that sample.
    """
    from pyspark.sql import functions as F

    from .client import _json_key, make_client
    from .lsn import ZERO_LSN
    from .sink import _utc_now, _write
    from .source import METADATA_COLUMNS, _opt

    if bucket_rows < 1 or not 0 <= sample <= 1:
        raise ValueError("bucket_rows must be at least 1, and sample between 0 and 1")
    run_id, run_at, rng = str(uuid.uuid4()), _utc_now(), random.Random(seed)
    schema = spark.read.format("mssql_cdc_snapshot").options(**options).load().schema
    meta = {n for n, _ in METADATA_COLUMNS} | {"_chunk"}
    columns = [f for f in schema if f.name not in meta]
    ci = _opt(options, "captureInstance")
    keys = list(keys) if keys else _source_keys(ci, options)
    missing = [k for k in keys if k not in schema.fieldNames()]
    if missing:
        raise ValueError(f"keys {missing} are not captured columns: {schema.fieldNames()}")
    kind = _KINDS.get(schema[keys[0]].dataType.typeName()) if len(keys) == 1 else None
    control = delta_table(spark, control_table).toDF()  # before silver: it holds this much
    silver_lsn = _one(control.where(F.col("table_name") == silver).select("applied_lsn"))
    version, target = _latest(spark, silver)

    def json_or_null(v) -> str | None:
        return None if v is None else json.dumps(v)

    with closing(make_client(options)) as client:
        source = client.source_table(ci)
        source_lsn = client.max_lsn() or ZERO_LSN  # M, before the source is read
        lower = min(source_lsn, silver_lsn or ZERO_LSN)
        if kind:
            found, fine = _range_buckets(
                spark, client, source, keys[0], kind, target, bronze, lower, bucket_rows
            )
            parts = [(_bound(kind, b["lo"]), _bound(kind, b["hi"])) for b in found]
        else:  # one count of the whole table; Tier 2 on ranges SQL Server cuts
            rows = client.key_buckets(source.schema, source.table, None, None, 1)[0][1]
            _, changes = _latest(spark, bronze)
            moved = _one(_after(changes, lower).select(F.lit(True)))
            found = [{"source": [rows, None], "silver": [target.count(), None], "moved": moved}]
            tiles: list | None = []  # the first key of each range after the first
            if rows > bucket_rows:
                types = client.key_types(ci, keys)
                # ponytail: NTILE reads and spools the whole key; keyset bounds if it shows up.
                # A key with no type to CAST a bound to gets no ranges.
                n = math.ceil(rows / bucket_rows)
                tiles = (
                    None
                    if None in types
                    else [  # as snapshotChunks takes them
                        _json_key(b, types)
                        for b in client.key_tiles(source.schema, source.table, keys, n)
                    ]
                )
            parts = list(pairwise([None, *tiles, None])) if tiles is not None else []
        for b in found:
            b["status"] = (
                "MATCH" if b["source"] == b["silver"] else "IN_FLIGHT" if b["moved"] else "MISMATCH"
            )
        if kind:
            match = [i for i, b in enumerate(found) if b["status"] == "MATCH"]
            chosen = [i for i, b in enumerate(found) if b["status"] == "MISMATCH"]
            chosen = sorted(chosen + _sample(rng, match, sample))
        else:
            chosen = sorted(_sample(rng, list(range(len(parts))), sample))
        failures = None
        if chosen:
            read_lsn = client.max_lsn() or ZERO_LSN  # M again, before the rows are read
            rows_read = (
                spark.read.format("mssql_cdc_snapshot")
                # no chunk metrics: they are backfill()'s, and a stream's directory folds them
                .options(**{k: v for k, v in options.items() if k.lower() != "metricspath"})
                .option("snapshotChunks", json.dumps([[i, *parts[i]] for i in chosen]))
                .option("snapshotKeys", json.dumps(keys))  # the bounds' columns
                .option("snapshotLsn", read_lsn)
                .load()
                .localCheckpoint()  # read once, before bronze is pinned
            )
            _, changes = _latest(spark, bronze)
            labels = spark.createDataFrame(
                [(i, json_or_null(parts[i][0]), json_or_null(parts[i][1])) for i in chosen],
                "_rc_idx INT, bucket_lo STRING, bucket_hi STRING",
            )
            if kind:  # silver's rows of the chosen buckets, by their fine buckets
                fines = spark.createDataFrame(
                    [(b, i) for i in chosen for b in found[i]["fine"]],
                    "_rc_fine BIGINT, _rc_idx INT",
                )

                def label(df):
                    return df.withColumn("_rc_fine", fine).join(F.broadcast(fines), "_rc_fine")

                theirs, how = target.transform(label), "full_outer"
            else:
                theirs, how = target.withColumn("_rc_idx", F.lit(None).cast("int")), "left"
            ours = rows_read.withColumnRenamed("_chunk", "_rc_idx")  # the chunk is the bucket
            failures = _differences(
                ours, theirs, keys, columns, how, changes, min(read_lsn, silver_lsn or ZERO_LSN)
            ).join(F.broadcast(labels), "_rc_idx", "left")

    common = {"run_id": run_id, "run_at": run_at, "silver": silver}
    common |= {"silver_version": version, "silver_lsn": silver_lsn}
    rows_out = []
    for i, b in enumerate(found):
        lo, hi = parts[i] if kind else (None, None)
        row = common | {
            "source_lsn": source_lsn,
            "bucket_lo": json_or_null(lo),
            "bucket_hi": json_or_null(hi),
            "source_rows": b["source"][0],
            "source_key_sum": b["source"][1],
            "silver_rows": b["silver"][0],
            "silver_key_sum": b["silver"][1],
            "status": b["status"],
            "hashed": i in chosen if kind else bool(chosen),
        }
        rows_out.append(tuple(row.get(n) for n, _, _ in REPORT_COLUMNS))
    chunks = _chunk_checks(spark, bronze, facts_table) if facts_table else []
    rows_out += [tuple((common | c).get(n) for n, _, _ in REPORT_COLUMNS) for c in chunks]
    report = spark.createDataFrame(rows_out, _SCHEMA)
    counts: dict = {}
    for c in chunks:
        counts[c["failure_type"]] = counts.get(c["failure_type"], 0) + 1
    if failures is not None:
        values = {k: F.lit(v) for k, v in common.items()} | {"source_lsn": F.lit(read_lsn)}
        values["run_at"] = F.lit(run_at.isoformat())  # cast to NTZ: no session time zone
        typed = [
            values.get(n, F.col(n) if n in failures.columns else F.lit(None)).cast(t).alias(n)
            for n, t, _ in REPORT_COLUMNS
        ]
        failures = failures.select(*typed)
        counts |= dict(failures.groupBy("failure_type").count().collect())
        report = report.unionByName(failures)
    if report_table:
        migrations.ensure(spark, report_table, "reconcile", REPORT_COLUMNS, REPORT_COMMENT)
        _write(report, report_table, None, None)
    statuses = [b["status"] for b in found]
    return {
        "run_id": run_id,
        "silver_version": version,
        "silver_lsn": silver_lsn,
        "source_lsn": source_lsn,
        "buckets": len(found),
        "match": statuses.count("MATCH"),
        "in_flight": statuses.count("IN_FLIGHT"),
        "mismatch": statuses.count("MISMATCH"),
        "hashed": len(chosen),
        "failures": counts,
        "report": report,
    }


def _differences(ours, theirs, keys, columns, how, changes, lower):
    """The keys whose rows differ between the source's rows ``ours`` and silver's ``theirs``
    (both with ``_rc_idx``), by hash: ``keys``, ``_rc_idx``, ``failure_type``, ``detail``."""
    from pyspark.sql import functions as F

    def side(df, *extra):
        have = set(df.columns)
        values = [
            (F.col(_q(f.name)) if f.name in have else F.lit(None)).cast(f.dataType).alias(f.name)
            for f in columns
        ]
        digest = F.sha2(F.to_json(F.struct(*values)), 256).alias("_rc_hash")
        return df.select(*values, digest, "_rc_idx", *extra)

    def col(alias: str, name: str):
        return F.col(f"{alias}.{_q(name)}")

    s, t = side(ours).alias("s"), side(theirs, "_start_lsn").alias("t")
    on = [col("s", k).eqNullSafe(col("t", k)) for k in keys]
    both = col("s", "_rc_hash").isNotNull() & col("t", "_rc_hash").isNotNull()
    differ = [
        F.when(~col("s", f.name).eqNullSafe(col("t", f.name)), F.lit(f.name)) for f in columns
    ]
    found = (
        s.join(t, on, how)
        .where(~both | (col("s", "_rc_hash") != col("t", "_rc_hash")))
        .select(
            *[F.coalesce(col("s", k), col("t", k)).alias(k) for k in keys],
            F.coalesce(col("s", "_rc_idx"), col("t", "_rc_idx")).alias("_rc_idx"),
            F.when(col("t", "_rc_hash").isNull(), "MISSING_TARGET")
            .when(col("s", "_rc_hash").isNull(), "MISSING_SOURCE")
            .otherwise("RECORD_DIFF")
            .alias("_rc_type"),
            F.to_json(
                F.struct(
                    col("t", "_start_lsn").alias("silver_start_lsn"),
                    F.when(both, F.concat_ws(", ", *differ)).alias("columns"),
                )
            ).alias("detail"),
        )
    )
    # a key bronze changed after the older read: either side may not have that change yet.
    # ponytail: joined by =, so a NULL key's change is not seen (one row, if a unique index has it)
    moved = (
        _after(changes, lower)
        .select(*[F.col(_q(k)) for k in keys])
        .distinct()
        .withColumn("_rc_moved", F.lit(True))
    )
    return found.join(moved, keys, "left").select(
        F.to_json(F.struct(*[F.col(_q(k)) for k in keys])).alias("key"),
        "_rc_idx",
        F.when(F.col("_rc_moved"), "IN_FLIGHT").otherwise(F.col("_rc_type")).alias("failure_type"),
        "detail",
    )


def _chunk_checks(spark, bronze: str, facts_table: str) -> list[dict]:
    """The chunk rows of the report for bronze's newest chunked snapshot (see the module
    docstring): ``bucket_lo``, ``bucket_hi``, ``failure_type`` and ``detail``, one per failure."""
    from collections import Counter

    from pyspark.sql import functions as F

    from .tables import exists

    if not exists(spark, facts_table):
        return []
    kinds = ("snapshot_open", "snapshot_chunk", "bootstrap", "resnapshot")
    facts = delta_table(spark, facts_table).toDF()
    rows = (
        facts.where((F.col("target") == bronze) & F.col("event").isin(*kinds))
        .select("event", "rows", "min_lsn", "max_lsn", "detail")
        .collect()
    )
    opens = [r["max_lsn"] for r in rows if r["event"] == "snapshot_open"]
    if not opens:
        return []
    s = max(opens)  # a newer open abandons an older one
    complete = any(r["event"] in ("bootstrap", "resnapshot") and r["max_lsn"] == s for r in rows)
    found = [
        d | {"rows": r["rows"], "lsn": r["min_lsn"]}
        for r in rows
        if r["event"] == "snapshot_chunk" and (d := json.loads(r["detail"]))["snapshot"] == s
    ]
    # bronze after the facts: it holds every wave they announce, whose rows commit first
    held: dict = {}
    pinned = _latest(spark, bronze)[1] if exists(spark, bronze) else None
    if pinned is not None and "_chunk" in pinned.columns:
        snap = pinned.where((F.col("_operation") == 0) & (F.col("_snapshot") == s))
        held = {
            i: (n, low)
            for i, n, low in snap.groupBy("_chunk")
            .agg(F.count(F.lit(1)), F.min("_start_lsn"))
            .collect()
        }
    out: list[dict] = []

    def fail(kind: str, i: int, chunk: dict | None, **what) -> None:
        lo, hi = (chunk["lo"], chunk["hi"]) if chunk else (None, None)
        out.append(
            {
                "bucket_lo": None if lo is None else json.dumps(lo),
                "bucket_hi": None if hi is None else json.dumps(hi),
                "failure_type": kind,
                "detail": json.dumps({"snapshot": s, "chunk": i, **what}),
            }
        )

    times = Counter(c["chunk"] for c in found)
    chunks = {c["chunk"]: c for c in sorted(found, key=lambda c: c["chunk"])}
    for i in range(max(chunks, default=-1) + 1):
        c = chunks.get(i)
        if c is None:
            fail("CHUNK_TILING", i, None, problem="no 'snapshot_chunk' facts row")
            continue
        if times[i] > 1:
            fail("CHUNK_TILING", i, c, problem=f"{times[i]} 'snapshot_chunk' facts rows")
        before = chunks.get(i - 1)
        if i == 0 and c["lo"] is not None:
            fail("CHUNK_TILING", i, c, problem="the first chunk is not open below")
        elif before and (before.get("last") or c["lo"] != before["hi"]):
            fail("CHUNK_TILING", i, c, problem="it does not start where the one before ended")
        n, low = held.get(i, (0, None))
        if c["rows"] != n:
            fail("CHUNK_ROWS", i, c, facts_rows=c["rows"], bronze_rows=n)
        stamp = min((x for x in (c["lsn"], low) if x), default=s)
        if stamp < s:
            fail("CHUNK_STAMP", i, c, lsn=stamp)
    if complete and chunks and not chunks[max(chunks)].get("last"):
        last = max(chunks)
        fail("CHUNK_TILING", last, chunks[last], problem="the last chunk is not the plan's final")
    for i in sorted(set(held) - set(chunks)) if complete else []:
        fail("CHUNK_ROWS", i, None, facts_rows=None, bronze_rows=held[i][0])
    return out
