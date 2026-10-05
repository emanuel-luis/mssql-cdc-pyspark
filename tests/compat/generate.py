"""Write the state a released mssql-cdc-pyspark leaves behind, for test_compat.py (ADR 0021).

Run it with the released wheel, never the repository's code: in a fresh virtual environment
holding ``mssql-cdc-pyspark==X.Y.Z`` and the PySpark and delta-spark of that release's
``uv.lock`` (docs/RELEASING.md), from the repository root:

    python tests/compat/generate.py tests/compat/X.Y.Z

On the fake backend, with ``to_delta(bootstrap=True, on_data_loss="resnapshot")`` and the
bronze verdict and ``apply_changes`` after every run: a bootstrap, batches in generation 0,
CDC cleanup purging changes the stream has not read, the re-snapshot into generation 1 (its
state file), batches in it. Then a second stream of the table, into its own bronze, opens a
chunked snapshot and ``backfill()`` reads one wave of it, leaving it open: its
'snapshot_open' and 'snapshot_plan' rows and the wave's userMetadata are state too (ADR 0021
amendment 4). It writes the fake source (``src/``), the checkpoints (``ckpt/``,
``ckpt-chunked/``), the bronze, silver, facts and control Delta tables (``delta/<kind>``,
catalog tables ``compat.<kind>``), the chunked stream's bronze (``compat.bronze_chunked``)
and ``manifest.json``, which tells the test what it needs to resume them.
"""

import importlib.metadata
import json
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

KEY, VALUE = "order_id", "status"
OPTIONS = {"captureInstance": "dbo_orders", "maxCommitsPerBatch": "1"}
APP_ID = "compat"
TABLES = {kind: f"compat.{kind}" for kind in ("bronze", "silver", "facts", "control")}
# a second stream of the table, its facts in TABLES["facts"]
CHUNKED = {"bronze": "compat.bronze_chunked", "app_id": "compat-chunked", "ckpt": "ckpt-chunked"}
T0 = datetime(2026, 9, 28, 13, 50)


def main(out: Path) -> None:
    import mssql_cdc

    if mssql_cdc.__version__ != out.name or "site-packages" not in mssql_cdc.__file__:
        sys.exit(
            f"{out}: run with mssql-cdc-pyspark=={out.name} installed, not "
            f"{mssql_cdc.__version__} from {mssql_cdc.__file__}"
        )
    if out.exists():
        sys.exit(f"{out} exists: remove it first")
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    from mssql_cdc import apply_changes, finalization, stream
    from mssql_cdc.fake import FakeCdcDatabase

    out = out.resolve()
    src, ckpt = str(out / "src"), str(out / "ckpt")
    spark = configure_spark_with_delta_pip(
        SparkSession.builder.master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.snapshotPartitions", "1")  # tiny tables
    ).getOrCreate()
    spark.sql(f"CREATE DATABASE compat LOCATION '{(out / 'delta').as_uri()}'")

    ci = OPTIONS["captureInstance"]
    db = FakeCdcDatabase(src, [ci], keys={ci: KEY}, columns={ci: f"{KEY} INT, {VALUE} STRING"})
    options = {**OPTIONS, "backend": "fake", "fakePath": src}
    minutes = iter(range(1000))

    def at():
        return T0 + timedelta(minutes=next(minutes))

    def commit(*changes):
        db.commit(ci, [(op, {KEY: k, VALUE: v}) for op, k, v in changes], at=at())

    def run():
        q = stream(spark, options).to_delta(
            TABLES["bronze"],
            APP_ID,
            ckpt,
            TABLES["facts"],
            trigger={"availableNow": True},
            bootstrap=True,
            on_data_loss="resnapshot",
        )
        q.awaitTermination()
        end = finalization.end_offset_from_progress(q.lastProgress)
        finalization.advance(spark, TABLES["control"], TABLES["bronze"], end)
        apply_changes(
            spark,
            TABLES["bronze"],
            TABLES["silver"],
            ci,
            [KEY],
            control_table=TABLES["control"],
            facts_table=TABLES["facts"],
        )

    for k in range(3):
        commit((2, k, "new"))
    run()  # the bootstrap
    commit((3, 1, "new"), (4, 1, "paid"))
    commit((2, 3, "new"))
    commit((1, 0, "new"))
    run()  # batches in generation 0
    commit((2, 4, "new"))
    commit((1, 2, "new"))
    db.cleanup(ci, db.idle(at=at()))  # purged before the stream read them
    run()  # the re-snapshot: generation 1
    commit((3, 3, "new"), (4, 3, "paid"))
    commit((2, 5, "new"))
    run()  # batches in generation 1
    chunked = stream(spark, {**options, "numPartitions": "2"})
    chunked.to_delta(
        CHUNKED["bronze"],
        CHUNKED["app_id"],
        str(out / CHUNKED["ckpt"]),
        TABLES["facts"],
        trigger={"availableNow": True},
        bootstrap=True,
        snapshot="chunked",
    ).awaitTermination()  # opens the snapshot at S and streams from there
    status = chunked.backfill(
        CHUNKED["bronze"],
        app_id=CHUNKED["app_id"],
        facts_table=TABLES["facts"],
        chunk_rows=1,
        max_waves=1,
    )
    assert status["state"] == "running", status  # left open, for the next release to finish
    spark.stop()

    # Hadoop's checksums of local files, and Delta's optional version checksums: more than
    # half the bytes, and not state
    for crc in out.rglob("*.crc"):
        crc.unlink()
    manifest = {
        "mssql_cdc": out.name,
        "pyspark": importlib.metadata.version("pyspark"),
        "delta_spark": importlib.metadata.version("delta-spark"),
        "options": OPTIONS,
        "app_id": APP_ID,
        "key": KEY,
        "value": VALUE,
        "tables": TABLES,
        "chunked": CHUNKED,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    files = [p for p in out.rglob("*") if p.is_file()]
    print(f"{out}: {len(files)} files, {sum(p.stat().st_size for p in files) // 1024} KiB")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(Path(sys.argv[1]))
