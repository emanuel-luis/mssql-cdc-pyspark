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

from dataclasses import dataclass
from typing import Iterator

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition

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
OPERATIONS = {1: "delete", 2: "insert", 3: "update_before", 4: "update_after"}


def _opt(options, key: str, default=None):
    lowered = {k.lower(): v for k, v in dict(options).items()}
    return lowered.get(key.lower(), default)


def _truthy(value) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "y")


@dataclass
class LsnRange(InputPartition):
    capture_instance: str
    from_lsn: str  # inclusive
    to_lsn: str  # inclusive


class MssqlCdcDataSource(DataSource):
    """``spark.readStream.format("mssql_cdc")``."""

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
        return cls(dict(self.options), schema)


class _BaseReader(DataSourceStreamReader):
    def __init__(self, options: dict, schema):
        self.options = options
        self.capture_instance = _opt(options, "captureInstance")
        if not self.capture_instance:
            raise ValueError("Option 'captureInstance' is required (e.g. 'dbo_orders')")
        self.include_command_id = _truthy(_opt(options, "includeCommandId", "true"))
        self.fail_on_data_loss = _truthy(_opt(options, "failOnDataLoss", "true"))
        self.num_partitions = int(_opt(options, "numPartitions", "1"))
        self.batch_size = int(_opt(options, "arrowBatchSize", "10000"))
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
        if self.num_partitions <= 1:
            return [LsnRange(self.capture_instance, from_lsn, to_lsn)]
        bounds = [b for b in self.client.split_points(from_lsn, to_lsn, self.num_partitions) if b]
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
    def read(self, partition: LsnRange) -> Iterator:
        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_schema

        target = to_arrow_schema(self.schema, timezone="UTC")  # TIMESTAMP columns are UTC instants
        client = self.client
        try:
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
                yield from table.to_batches()
            # Cleanup may have run since partitions() checked. It moves min_lsn before it
            # deletes rows, so min_lsn past from_lsn now means rows may be missing.
            self._guard_retention(client, partition.from_lsn)
        finally:
            client.close()
            self._client = None

    def commit(self, end: dict) -> None:
        # SQL Server CDC retention is time-based: there is nothing to acknowledge.
        pass


class MssqlCdcStreamReader(_BaseReader, SupportsTriggerAvailableNow):
    """Spark 4.2+: admission control (maxCommitsPerBatch) and Trigger.AvailableNow."""

    def __init__(self, options, schema):
        super().__init__(options, schema)
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

    def latestOffset(self) -> dict:
        return self._offset(self.client.max_lsn())
