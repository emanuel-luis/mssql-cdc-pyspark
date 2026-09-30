"""PySpark Python Data Source (DataSource V2) for SQL Server Change Data Capture.

Streaming offsets are commit LSNs. Each micro-batch reads the closed interval
``[fn_cdc_increment_lsn(start), end]`` from the change table ``cdc.<ci>_CT``.
``end`` never exceeds ``sys.fn_cdc_get_max_lsn()``, the last LSN the capture
process has processed, so every batch is a prefix of the source commit history.

The offset also carries ``commit_ts``, the UTC commit time of the end LSN. The end
LSN can be a CDC "dummy" entry with no change rows (written while the database is
idle), so the batch rows alone cannot tell how far capture progressed; the offset
can. That is what the finalization layer uses.

Usage::

    from mssql_cdc import register
    register(spark)
    df = (spark.readStream.format("mssql_cdc")
            .option("connectionString", "Server=...;Database=...;UID=...;PWD=...;Encrypt=yes")
            .option("captureInstance", "dbo_orders")
            .load())  # columns inferred from CDC metadata; "columns" (DDL) overrides

When ``columns`` is omitted, the captured columns and their types come from
``sys.sp_cdc_get_captured_columns`` at ``load()`` time, on the driver.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from itertools import pairwise

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    InputPartition,
)

try:  # Spark 4.2+ (and runtimes that backported SPARK-55304)
    from pyspark.sql.streaming.datasource import (
        ReadAllAvailable,
        ReadMaxRows,
        SupportsTriggerAvailableNow,
    )

    HAS_ADMISSION_CONTROL = True
except ImportError:  # pragma: no cover - older Spark
    HAS_ADMISSION_CONTROL = False

    class SupportsTriggerAvailableNow:  # type: ignore[no-redef]
        pass


METADATA_COLUMNS = [
    ("_capture_instance", "STRING"),
    ("_start_lsn", "STRING"),
    ("_seqval", "STRING"),
    ("_operation", "INT"),
    ("_command_id", "INT"),
    ("_commit_ts", "TIMESTAMP_NTZ"),
]
# 1-4 are SQL Server's __$operation codes; 0 marks a snapshot row (ADR 0016)
OPERATIONS = {0: "snapshot", 1: "delete", 2: "insert", 3: "update_before", 4: "update_after"}


def _opt(options, key: str, default=None):
    lowered = {k.lower(): v for k, v in dict(options).items()}
    return lowered.get(key.lower(), default)


def _truthy(value) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def _write_metrics(path: str, partition: LsnRange, metrics: dict) -> None:
    """One JSON per partition; a task retry overwrites its file. Best effort: a metric
    must never fail a read."""
    import json

    try:
        os.makedirs(path, exist_ok=True)
        name = os.path.join(path, f"{partition.from_lsn}-{partition.to_lsn}.json")
        with open(name + ".tmp", "w", encoding="utf-8") as fh:
            json.dump({"from_lsn": partition.from_lsn, "to_lsn": partition.to_lsn, **metrics}, fh)
        os.replace(name + ".tmp", name)
    except OSError:
        pass


@dataclass
class LsnRange(InputPartition):
    capture_instance: str
    from_lsn: str  # inclusive
    to_lsn: str  # inclusive


class MssqlCdcDataSource(DataSource):
    """``spark.readStream.format("mssql_cdc")``."""

    default_num_partitions: int | None = None  # set by register() from the session's cores

    @classmethod
    def name(cls) -> str:
        return "mssql_cdc"

    def schema(self) -> str:
        columns = _opt(self.options, "columns") or self._captured_columns()
        include_cmd = _truthy(_opt(self.options, "includeCommandId", "true"))
        meta = [f"{n} {t}" for n, t in METADATA_COLUMNS if include_cmd or n != "_command_id"]
        return ", ".join(meta) + ", " + columns

    def _captured_columns(self) -> str:
        """Without ``columns``, read the captured columns from CDC metadata (driver side)."""
        from .client import make_client

        ci = _opt(self.options, "captureInstance")
        if not ci:
            raise ValueError("Option 'captureInstance' is required (e.g. 'dbo_orders')")
        client = make_client(self.options)
        try:
            return client.captured_columns(ci)
        finally:
            client.close()

    def streamReader(self, schema):
        cls = MssqlCdcStreamReader if HAS_ADMISSION_CONTROL else MssqlCdcLegacyStreamReader
        return cls(dict(self.options), schema, self.default_num_partitions)


class MssqlCdcSnapshotDataSource(MssqlCdcDataSource):
    """``spark.read.format("mssql_cdc_snapshot")``: the tracked table's current rows in the
    stream's schema, as operation 0 at one LSN (ADR 0016). ``CdcStream.snapshot`` writes
    them to Delta and returns the offset the stream starts from."""

    @classmethod
    def name(cls) -> str:
        return "mssql_cdc_snapshot"

    def reader(self, schema):
        return MssqlCdcSnapshotReader(dict(self.options), schema, self.default_num_partitions)


class _Common:
    """What the stream and snapshot readers share: options, the schema, a lazy client."""

    def __init__(self, options: dict, schema, default_num_partitions: int | None = None):
        self.options = options
        self.capture_instance = _opt(options, "captureInstance")
        if not self.capture_instance:
            raise ValueError("Option 'captureInstance' is required (e.g. 'dbo_orders')")
        self.include_command_id = _truthy(_opt(options, "includeCommandId", "true"))
        self.fail_on_data_loss = _truthy(_opt(options, "failOnDataLoss", "true"))
        num_partitions = str(_opt(options, "numPartitions", "auto")).strip().lower()
        # auto: the session's cores (register()), else this driver node's CPUs. Each
        # partition opens its own connection to SQL Server.
        self.num_partitions = (
            max(1, default_num_partitions or os.cpu_count() or 1)
            if num_partitions == "auto"
            else int(num_partitions)
        )
        self.batch_size = int(_opt(options, "arrowBatchSize", "10000"))
        # optional: a directory (local or FUSE, e.g. /Volumes/...) where each partition that
        # read rows leaves its metrics for delta_sink(metrics_path=...) to fold into the batch facts
        self.metrics_path = _opt(options, "metricsPath")
        meta_names = {n for n, _ in METADATA_COLUMNS}
        self.field_names = list(schema.fieldNames())
        self.source_columns = [f for f in self.field_names if f not in meta_names]
        self.schema = schema
        self._client = None

    # A live DB connection must never be pickled to executors.
    def __getstate__(self):
        state = self.__dict__.copy()
        state["_client"] = None
        return state

    @property
    def client(self):
        if self._client is None:
            from .client import make_client

            self._client = make_client(self.options)
        return self._client


class _BaseReader(_Common, DataSourceStreamReader):
    # -- offsets --------------------------------------------------------------
    def _offset(self, lsn: str) -> dict:
        return {"lsn": lsn, "commit_ts": self.client.lsn_to_time(lsn) or ""}

    def initialOffset(self) -> dict:
        start = (_opt(self.options, "startingLsn", "earliest") or "earliest").strip()
        if start.lower() == "earliest":
            # offsets hold the last *processed* LSN, so start just before min_lsn
            lsn = self.client.decrement_lsn(self.client.min_lsn(self.capture_instance))
        elif start.lower() == "latest":
            lsn = self.client.max_lsn()
        else:
            from .lsn import normalize

            lsn = normalize(start)
        return self._offset(lsn)

    def partitions(self, start: dict, end: dict):
        if end["lsn"] <= start["lsn"]:
            return []
        from_lsn = self.client.increment_lsn(start["lsn"])
        # failOnDataLoss=false: skip ahead to what cleanup left
        from_lsn = max(from_lsn, self._guard_retention(self.client, from_lsn))
        to_lsn = end["lsn"]
        if from_lsn > to_lsn:  # invariant 3: cleanup left nothing up to end
            return []
        if self.num_partitions <= 1:
            return [LsnRange(self.capture_instance, from_lsn, to_lsn)]
        bounds = [
            b
            for b in self.client.split_points(
                self.capture_instance, from_lsn, to_lsn, self.num_partitions
            )
            if b
        ]
        if not bounds or bounds[-1] != to_lsn:
            bounds.append(to_lsn)
        ranges, lo = [], from_lsn
        for hi in bounds:
            if hi < lo:
                continue
            ranges.append(LsnRange(self.capture_instance, lo, hi))
            lo = self.client.increment_lsn(hi)
        return ranges

    def _guard_retention(self, client, from_lsn: str) -> str:
        """Invariant 4: fail when CDC cleanup purged change data at or after ``from_lsn``."""
        min_lsn = client.min_lsn(self.capture_instance)
        if from_lsn < min_lsn and self.fail_on_data_loss:
            from .client import DataLossError

            raise DataLossError(
                f"{self.capture_instance}: change data from {from_lsn} was purged by CDC "
                f"cleanup (current min_lsn is {min_lsn}). A re-snapshot is required. "
                "Set failOnDataLoss=false to skip ahead (loses changes)."
            )
        return min_lsn

    # -- data (runs on executors) ---------------------------------------------
    def read(self, partition: LsnRange) -> Iterator:  # type: ignore[override]  # partitions() only plans LsnRange
        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_schema

        target = to_arrow_schema(self.schema, timezone="UTC")  # TIMESTAMP columns are UTC instants
        client = self.client
        started, rows, nbytes = time.perf_counter(), 0, 0
        try:
            if self.metrics_path:  # one round trip and the session's wait so far, before reading
                rtt = client.ping(1)
                wait_before = client.network_wait_ms()
            for batch in client.iter_changes(
                partition.capture_instance,
                partition.from_lsn,
                partition.to_lsn,
                self.source_columns,
                self.include_command_id,
                self.batch_size,
            ):
                if batch.num_rows == 0:
                    continue
                table = pa.Table.from_batches([batch])
                table = table.append_column(
                    "_capture_instance",
                    pa.array([partition.capture_instance] * table.num_rows, pa.string()),
                )
                table = table.select(self.field_names).cast(target)
                rows, nbytes = rows + table.num_rows, nbytes + table.nbytes
                yield from table.to_batches()
            # Cleanup may have run since partitions() checked. It moves min_lsn before it
            # deletes rows, so min_lsn past from_lsn now means rows may be missing.
            min_lsn = self._guard_retention(client, partition.from_lsn)
            if (
                self.metrics_path and rows
            ):  # the sink never folds (or removes) an empty batch's file
                wait_after = client.network_wait_ms()
                try:
                    watermark = client.lsn_to_time(min_lsn)
                except Exception:  # noqa: BLE001 - a metric must never fail a read
                    watermark = None
                _write_metrics(
                    self.metrics_path,
                    partition,
                    {
                        "rows": rows,
                        "bytes": nbytes,
                        "seconds": time.perf_counter() - started,
                        "rtt_ms": rtt[0] if rtt else None,
                        "network_wait_ms": (
                            None
                            if wait_before is None or wait_after is None
                            else wait_after - wait_before
                        ),
                        # what cleanup has deleted up to, as a commit time (ADR 0017)
                        "retention_watermark_ts": watermark,
                    },
                )
        finally:
            client.close()
            self._client = None

    def commit(self, end: dict) -> None:
        # SQL Server CDC retention is time-based: there is nothing to acknowledge.
        pass


class MssqlCdcStreamReader(_BaseReader, SupportsTriggerAvailableNow):
    """Spark 4.2+: admission control (maxCommitsPerBatch) and Trigger.AvailableNow."""

    def __init__(self, options, schema, default_num_partitions=None):
        super().__init__(options, schema, default_num_partitions)
        self._target = None
        max_commits = _opt(options, "maxCommitsPerBatch")
        self._max_commits = int(max_commits) if max_commits else None

    def getDefaultReadLimit(self):
        # ReadMaxRows is reused with "commits" semantics: custom ReadLimits are not
        # supported for Python sources, and a batch must end on a commit boundary.
        return ReadMaxRows(self._max_commits) if self._max_commits else ReadAllAvailable()

    def prepareForTriggerAvailableNow(self) -> None:
        self._target = self.client.max_lsn()

    def latestOffset(self, start: dict, limit) -> dict:
        upper = self._target or self.client.max_lsn()
        if isinstance(limit, ReadMaxRows):
            nth = self.client.nth_commit_after(start["lsn"], limit.max_rows)
            if nth is not None and nth < upper:
                upper = nth
        if upper <= start["lsn"]:
            return start
        return self._offset(upper)

    def reportLatestOffset(self):
        # Surfaces capture progress (max_lsn) as latestOffset in query progress.
        return self._offset(self.client.max_lsn())


class MssqlCdcLegacyStreamReader(_BaseReader):  # pragma: no cover - Spark < 4.2
    """Spark 4.0/4.1 without admission control: every batch reads up to max_lsn."""

    def latestOffset(self) -> dict:  # type: ignore[override]  # Spark < 4.2 signature; stubs are 4.2
        return self._offset(self.client.max_lsn())


# --------------------------------------------------------------------------- #
# Snapshot of the tracked table (ADR 0016)
# --------------------------------------------------------------------------- #
def snapshot_lsn(client, source) -> str:
    """The LSN a snapshot is stamped with, recorded *before* the table is read.

    Every commit up to ``max_lsn`` is already in the table when the read starts; a commit the
    read also sees comes later and has a larger LSN, so the stream from here replays it and
    the downstream MERGE absorbs the overlap. A capture instance that capture has not reached
    yet (``max_lsn`` below its first LSN: a quiet database, just after the enable) starts
    the stream at its first LSN instead; ``fn_cdc_get_min_lsn`` is NULL until then, the
    instance's ``start_lsn`` in ``source`` (a ``SourceTable``) is not. ``max_lsn`` itself is
    NULL on a database capture has not written to yet.
    """
    from .lsn import ZERO_LSN

    max_lsn = client.max_lsn() or ZERO_LSN
    if source.start_lsn is None:
        return max_lsn  # no low endpoint yet; the stream's retention guard still checks it
    return max(max_lsn, client.decrement_lsn(source.start_lsn))


@dataclass
class KeyRange(InputPartition):
    capture_instance: str
    lsn: str  # the snapshot's LSN and commit time, stamped on every row
    commit_ts: str | None
    schema: str
    table: str
    keys: list[str]  # []: the whole table in one partition
    types: list[str] | None  # the keys' SQL types, bounds bound as CAST(? AS type); None: integers
    lo: tuple | None  # inclusive, one value per key; None: open, plus the rows that sort first
    hi: tuple | None  # exclusive; None: open


class MssqlCdcSnapshotReader(_Common, DataSourceReader):
    def partitions(self):
        client = self.client
        from .lsn import normalize

        source = client.source_table(self.capture_instance)
        given = _opt(self.options, "snapshotLsn")
        lsn = normalize(given) if given else snapshot_lsn(client, source)
        commit_ts = client.lsn_to_time(lsn)
        schema, table, keys = source.schema, source.table, source.keys

        def ranges(keys, types, bounds):
            return [
                KeyRange(self.capture_instance, lsn, commit_ts, schema, table, keys, types, a, b)
                for a, b in pairwise([None, *bounds, None])
            ]

        if self.num_partitions <= 1 or not keys or not set(keys) <= set(self.source_columns):
            return ranges([], None, [])
        if len(keys) == 1:
            # One integer key: uniform ranges over MIN..MAX, two seeks. NTILE would read and
            # spool the whole key to count and tile it; sparse or skewed keys give uneven
            # ranges instead. ponytail: a single non-integer key pays these two seeks too.
            lo, hi = client.key_range(schema, table, keys[0])
            if lo is None or lo == hi:  # empty, or one row
                return ranges([], None, [])
            if all(isinstance(v, int) and not isinstance(v, bool) for v in (lo, hi)):
                n = min(self.num_partitions, hi - lo + 1)
                return ranges(keys, None, [(lo + (hi - lo + 1) * i // n,) for i in range(1, n)])
        # Composite or non-integer key: tiles of the rows by NTILE, bounds typed per column.
        types = client.key_types(self.capture_instance, keys)
        if None in types:
            return ranges([], None, [])
        return ranges(keys, types, client.key_tiles(schema, table, keys, self.num_partitions))

    def read(self, partition: KeyRange) -> Iterator:  # type: ignore[override]  # partitions() only plans KeyRange
        from datetime import datetime

        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_schema

        target = to_arrow_schema(self.schema, timezone="UTC")
        commit_ts = datetime.fromisoformat(partition.commit_ts) if partition.commit_ts else None
        meta = {  # constant per snapshot; _seqval and _command_id have no meaning here
            "_capture_instance": (partition.capture_instance, pa.string()),
            "_start_lsn": (partition.lsn, pa.string()),
            "_seqval": (None, pa.string()),
            "_operation": (0, pa.int32()),
            "_command_id": (None, pa.int32()),
            "_commit_ts": (commit_ts, pa.timestamp("us")),
        }
        client = self.client
        try:
            for batch in client.iter_table(
                partition.schema,
                partition.table,
                self.source_columns,
                partition.keys,
                partition.types,
                partition.lo,
                partition.hi,
                self.batch_size,
            ):
                if batch.num_rows == 0:
                    continue
                table = pa.Table.from_batches([batch])
                for name in self.field_names:
                    if name in meta:
                        value, typ = meta[name]
                        table = table.append_column(name, pa.array([value] * table.num_rows, typ))
                yield from table.select(self.field_names).cast(target).to_batches()
        finally:
            client.close()
            self._client = None
