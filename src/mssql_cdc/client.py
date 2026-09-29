"""Access to SQL Server CDC metadata and change tables.

``CdcClient`` is the interface the Spark data source depends on. ``SqlCdcClient``
implements it with plain T-SQL on top of a small ``Backend`` that knows how to run a
query and return Apache Arrow record batches. Two backends ship:

* ``mssql-python`` (default): Microsoft's official driver. ``pip`` only; it bundles
  the ODBC driver and fetches natively into Arrow via ``cursor.arrow_batch()``.
* ``arrow-odbc``: needs unixODBC and msodbcsql18 on every worker.

All LSNs cross the driver boundary as hex strings and are converted server-side
with ``CONVERT(binary(10), ?, 1)``, so both backends bind parameters the same way.
Changes are read from the change table ``cdc.<capture_instance>_CT`` (ADR 0009).
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Iterator, Sequence

import pyarrow as pa

from . import lsn as _lsn

_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")
_TZ_RE = re.compile(r"^[A-Za-z0-9 ._+\-/()]+$")


class DataLossError(RuntimeError):
    """Raised when requested change data was already purged by CDC cleanup."""


def _check_ident(name: str, what: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"Invalid {what}: {name!r}")
    return name


def _check_column(name: str) -> str:
    if "]" in name or not name:
        raise ValueError(f"Invalid column name: {name!r}")
    return name


# Default Spark type per SQL Server type, for schemas inferred from CDC metadata.
# The `columns` option overrides them.
_SPARK_TYPES = {
    "bit": "BOOLEAN",
    "tinyint": "SMALLINT",  # 0..255 does not fit Spark's signed TINYINT
    "smallint": "SMALLINT",
    "int": "INT",
    "bigint": "BIGINT",
    "real": "FLOAT",
    "float": "DOUBLE",
    "money": "DECIMAL(19,4)",
    "smallmoney": "DECIMAL(10,4)",
    "date": "DATE",
    "datetime": "TIMESTAMP_NTZ",
    "datetime2": "TIMESTAMP_NTZ",
    "smalldatetime": "TIMESTAMP_NTZ",
    "datetimeoffset": "TIMESTAMP",
    "time": "STRING",
    "char": "STRING",
    "varchar": "STRING",
    "nchar": "STRING",
    "nvarchar": "STRING",
    "text": "STRING",
    "ntext": "STRING",
    "xml": "STRING",
    "uniqueidentifier": "STRING",
    "binary": "BINARY",
    "varbinary": "BINARY",
    "image": "BINARY",
    "timestamp": "BINARY",  # rowversion
}


def _spark_type(sql_type: str, precision: int, scale: int) -> str:
    t = (sql_type or "").lower()
    if t in ("decimal", "numeric"):
        return f"DECIMAL({int(precision)},{int(scale)})"
    if t not in _SPARK_TYPES:
        raise ValueError(f"no default Spark type for SQL Server type {sql_type!r}")
    return _SPARK_TYPES[t]


# --------------------------------------------------------------------------- #
# Interface
# --------------------------------------------------------------------------- #
class CdcClient(ABC):
    """What the data source needs from SQL Server. All LSNs are hex strings."""

    @abstractmethod
    def max_lsn(self) -> str: ...

    @abstractmethod
    def min_lsn(self, capture_instance: str) -> str: ...

    @abstractmethod
    def increment_lsn(self, lsn: str) -> str: ...

    @abstractmethod
    def decrement_lsn(self, lsn: str) -> str: ...

    @abstractmethod
    def lsn_to_time(self, lsn: str) -> str | None:
        """Commit time of an LSN as ISO-8601 UTC string (millisecond precision)."""

    @abstractmethod
    def nth_commit_after(self, lsn: str, n: int) -> str | None:
        """The n-th commit LSN strictly after ``lsn`` in cdc.lsn_time_mapping."""

    @abstractmethod
    def split_points(self, capture_instance: str, from_lsn: str, to_lsn: str, n: int) -> list[str]:
        """Up to ``n`` commit-aligned upper bounds that split [from, to] into ranges holding
        about the same number of change rows of ``capture_instance``."""

    @abstractmethod
    def iter_changes(
        self,
        capture_instance: str,
        from_lsn: str,
        to_lsn: str,
        columns: Sequence[str],
        include_command_id: bool,
        batch_size: int,
    ) -> Iterator[pa.RecordBatch]:
        """Changes in the closed interval [from_lsn, to_lsn], in commit order."""

    def captured_columns(self, capture_instance: str) -> str:
        """Spark DDL of the captured columns, in capture order."""
        raise ValueError(
            "Option 'columns' is required for this backend: a DDL list of the captured "
            "source columns, e.g. 'order_id INT, status STRING, amount DECIMAL(18,2)'."
        )

    def ping(self, samples: int = 3) -> list[float]:
        """Round-trip times in milliseconds of ``samples`` trivial queries; [] when not a server."""
        return []

    def network_wait_ms(self) -> int | None:
        """Milliseconds this connection's server session has waited on the client so far
        (``ASYNC_NETWORK_IO``); None when unknown."""
        return None

    def close(self) -> None:  # pragma: no cover - default no-op
        pass


# --------------------------------------------------------------------------- #
# Backends: "run this SQL, give me Arrow"
# --------------------------------------------------------------------------- #
class Backend(ABC):
    @abstractmethod
    def batches(self, sql: str, params: Sequence[str], batch_size: int) -> Iterator[pa.RecordBatch]: ...

    def scalar(self, sql: str, params: Sequence[str] = ()):
        for batch in self.batches(sql, params, 1):
            if batch.num_rows:
                return batch.column(0)[0].as_py()
        return None

    def close(self) -> None:
        pass


class MssqlPythonBackend(Backend):
    """Microsoft ``mssql-python`` driver with native Arrow fetch (>= 1.5.0)."""

    def __init__(self, connection_string: str, timeout: int = 30):
        import mssql_python  # imported lazily: this runs on driver and executors

        self._conn = mssql_python.connect(connection_string, autocommit=True, timeout=timeout)

    def batches(self, sql, params, batch_size):
        cur = self._conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            while True:
                batch = cur.arrow_batch(batch_size)
                if batch.num_rows == 0:
                    break
                yield batch
        finally:
            cur.close()

    def scalar(self, sql, params=()):
        cur = self._conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
            return None if row is None else row[0]
        finally:
            cur.close()

    def close(self):
        self._conn.close()


class ArrowOdbcBackend(Backend):
    """``arrow-odbc``. Requires unixODBC + Microsoft ODBC Driver 18 on the worker."""

    def __init__(self, connection_string: str, max_bytes_per_batch: int = 64 * 1024 * 1024):
        import arrow_odbc

        self._conn = arrow_odbc.connect(connection_string)
        self._max_bytes = max_bytes_per_batch

    def batches(self, sql, params, batch_size):
        reader = self._conn.read_arrow_batches(
            sql,
            batch_size=batch_size,
            parameters=list(params),
            max_bytes_per_batch=self._max_bytes,
            fetch_concurrently=False,
        )
        for batch in reader:
            if batch.num_rows:
                yield batch


# --------------------------------------------------------------------------- #
# T-SQL implementation
# --------------------------------------------------------------------------- #
def _check_tz(name) -> str:
    if not isinstance(name, str) or not _TZ_RE.match(name):
        raise ValueError(f"Invalid sourceTimeZone: {name!r}")
    return name


class SqlCdcClient(CdcClient):
    def __init__(self, backend: Backend, source_timezone: str = "auto"):
        self._b = backend
        self._tz = None if source_timezone.lower() == "auto" else _check_tz(source_timezone)
        self._offset_min: int | None = None  # set instead of _tz by the pre-2022 fallback

    # -- helpers --------------------------------------------------------------
    @property
    def timezone(self) -> str:
        """Time zone of the server clock; with ``auto``, detected once per client.

        SQL Server 2022+ and Azure SQL name it (``CURRENT_TIMEZONE_ID()``), and ``AT TIME
        ZONE`` applies the daylight-saving rules in force at each commit. Older versions
        have no such function; then the server's current UTC offset
        (``SYSDATETIMEOFFSET()``) is applied to every commit, returned as ``UTC-03:00``.
        That is exact for zones without daylight saving; elsewhere, name the zone.
        """
        # ponytail: one query per client (driver and every task); ship the detected zone
        # to executors in the options if it ever shows up in profiles.
        if self._tz is None and self._offset_min is None:
            try:
                self._tz = _check_tz(self._b.scalar("SELECT CURRENT_TIMEZONE_ID()"))
            except ValueError:
                raise
            except Exception:  # noqa: BLE001 - no such function before 2022; driver-specific type
                self._offset_min = int(self._b.scalar("SELECT DATEPART(TZOFFSET, SYSDATETIMEOFFSET())"))
        if self._tz is not None:
            return self._tz
        sign, minutes = ("+" if self._offset_min >= 0 else "-"), abs(self._offset_min)
        return f"UTC{sign}{minutes // 60:02d}:{minutes % 60:02d}"

    def _utc(self, expr: str, offset_min: int | None = None) -> str:
        """``tran_end_time`` is a timezone-less datetime in the server's clock.

        ``offset_min``: a UTC offset known to hold for every value of ``expr``; a plain
        ``DATEADD`` then replaces ``AT TIME ZONE``, which costs ~2.7x the read (lab t8).
        """
        zone = self.timezone
        offset_min = self._offset_min if offset_min is None else offset_min
        if offset_min is not None:
            return f"CAST(DATEADD(minute, {-int(offset_min)}, {expr}) AS datetime2(3))"
        if zone.upper() == "UTC":
            return f"CAST({expr} AS datetime2(3))"
        return f"CAST(({expr} AT TIME ZONE N'{zone}') AT TIME ZONE 'UTC' AS datetime2(3))"

    def _range_offset(self, from_lsn: str, to_lsn: str) -> int | None:
        """The named zone's UTC offset over a range of commits, when it is one offset.

        Equal offsets at both ends of a range shorter than 7 days mean no daylight-saving
        change in between (no zone changes twice within a week). None otherwise, or when
        the range has no commits: the caller then converts row by row.
        """
        zone = self.timezone
        if self._offset_min is not None or zone.upper() == "UTC":
            return None
        value = self._b.scalar(
            "SELECT CASE WHEN DATEDIFF(day, MIN(tran_end_time), MAX(tran_end_time)) < 7 "
            f"AND DATEPART(TZOFFSET, MIN(tran_end_time) AT TIME ZONE N'{zone}') "
            f"= DATEPART(TZOFFSET, MAX(tran_end_time) AT TIME ZONE N'{zone}') "
            f"THEN DATEPART(TZOFFSET, MIN(tran_end_time) AT TIME ZONE N'{zone}') END "
            "FROM cdc.lsn_time_mapping "
            "WHERE start_lsn BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)",
            (from_lsn, to_lsn),
        )
        return None if value is None else int(value)

    def _hex(self, value) -> str | None:
        return None if value is None else _lsn.normalize(value)

    # -- metadata -------------------------------------------------------------
    def max_lsn(self):
        return self._hex(self._b.scalar("SELECT CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1)"))

    def min_lsn(self, capture_instance):
        value = self._hex(
            self._b.scalar(
                "SELECT CONVERT(varchar(22), sys.fn_cdc_get_min_lsn(?), 1)",
                (_check_ident(capture_instance, "capture instance"),),
            )
        )
        if value in (None, _lsn.ZERO_LSN):
            raise ValueError(
                f"Capture instance {capture_instance!r} not found, the login lacks "
                "permission to read it, or capture has not processed its creation yet "
                "(sys.fn_cdc_get_min_lsn returned 0x00...). Right after "
                "sys.sp_cdc_enable_table, retry once the capture job has run."
            )
        return value

    def increment_lsn(self, lsn):
        return self._hex(
            self._b.scalar(
                "SELECT CONVERT(varchar(22), sys.fn_cdc_increment_lsn(CONVERT(binary(10), ?, 1)), 1)",
                (lsn,),
            )
        )

    def decrement_lsn(self, lsn):
        return self._hex(
            self._b.scalar(
                "SELECT CONVERT(varchar(22), sys.fn_cdc_decrement_lsn(CONVERT(binary(10), ?, 1)), 1)",
                (lsn,),
            )
        )

    def lsn_to_time(self, lsn):
        value = self._b.scalar(
            "SELECT CONVERT(varchar(23), "
            + self._utc("sys.fn_cdc_map_lsn_to_time(CONVERT(binary(10), ?, 1))")
            + ", 126)",
            (lsn,),
        )
        return value or None

    def nth_commit_after(self, lsn, n):
        n = int(n)
        if n <= 0:
            raise ValueError("n must be positive")
        return self._hex(
            self._b.scalar(
                "SELECT CONVERT(varchar(22), MAX(start_lsn), 1) FROM ("
                f"SELECT TOP ({n}) start_lsn FROM cdc.lsn_time_mapping "
                "WHERE start_lsn > CONVERT(binary(10), ?, 1) ORDER BY start_lsn) t",
                (lsn,),
            )
        )

    def split_points(self, capture_instance, from_lsn, to_lsn, n):
        # Tiles of the change table's own rows, not of cdc.lsn_time_mapping's commits: those
        # are database-wide, and on a real table they left the largest range with ~2x the
        # mean rows (ADR 0015). Each bound is the last commit LSN of its tile, so a commit
        # whose rows straddle two tiles stays whole in the first range.
        ci = _check_ident(capture_instance, "capture instance")
        n = int(n)
        sql = (
            "SELECT CONVERT(varchar(22), MAX(__$start_lsn), 1) AS b FROM ("
            f"SELECT __$start_lsn, NTILE({n}) OVER (ORDER BY __$start_lsn) AS g "
            f"FROM cdc.[{ci}_CT] "
            "WHERE __$start_lsn BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)"
            ") x GROUP BY g ORDER BY b"
        )
        points = []
        for batch in self._change_table_batches(ci, sql, (from_lsn, to_lsn), 1000):
            points.extend(self._hex(v) for v in batch.column(0).to_pylist())
        return points

    def ping(self, samples=3):
        import time

        times = []
        for _ in range(samples):
            t0 = time.perf_counter()
            self._b.scalar("SELECT 1")
            times.append((time.perf_counter() - t0) * 1000)
        return times

    def network_wait_ms(self):
        # sys.dm_exec_session_wait_stats (2016+): a session sees its own row without
        # VIEW SERVER STATE. No row yet means no wait so far.
        try:
            value = self._b.scalar(
                "SELECT wait_time_ms FROM sys.dm_exec_session_wait_stats "
                "WHERE session_id = @@SPID AND wait_type = 'ASYNC_NETWORK_IO'")
        except Exception:  # noqa: BLE001 - a metric must never fail a read
            return None
        return int(value or 0)

    def captured_columns(self, capture_instance):
        # The documented API, not cdc.captured_columns: it needs only what the query
        # functions need (SELECT on the source columns, gating role if any).
        ci = _check_ident(capture_instance, "capture instance")
        not_found = (
            f"Capture instance {ci!r} not found, or the login lacks SELECT on its source "
            "columns (or membership in its gating role). Pass 'columns' explicitly."
        )
        try:
            rows = [r for batch in self._b.batches(
                "EXEC sys.sp_cdc_get_captured_columns @capture_instance = ?", (ci,), 1000)
                for r in batch.to_pylist()]
        except Exception as exc:  # noqa: BLE001 - Error 22981, driver-specific type
            raise ValueError(not_found) from exc
        if not rows:
            raise ValueError(not_found)
        ddl = []
        for r in sorted(rows, key=lambda r: r["column_ordinal"]):
            name = _check_column(r["column_name"])
            try:
                typ = _spark_type(r["data_type"], r["numeric_precision"], r["numeric_scale"])
            except ValueError as e:
                raise ValueError(f"{ci}.{name}: {e}. Pass 'columns' explicitly.") from None
            ddl.append(f"`{name.replace('`', '``')}` {typ}")
        return ", ".join(ddl)

    # -- data -----------------------------------------------------------------
    def iter_changes(self, capture_instance, from_lsn, to_lsn, columns, include_command_id, batch_size):
        # The change table itself, not cdc.fn_cdc_get_all_changes_<ci>: the function does
        # not return __$command_id (ADR 0009). Unlike the function, the table does not
        # reject a range that cleanup purged; the reader re-checks min_lsn after reading.
        ci = _check_ident(capture_instance, "capture instance")
        cols = ", ".join(f"c.[{_check_column(c)}]" for c in columns)
        cmd_select = "c.[__$command_id] AS _command_id, " if include_command_id else ""
        cmd_order = "c.[__$command_id], " if include_command_id else ""
        sql = (
            "SELECT "
            "CONVERT(varchar(22), c.[__$start_lsn], 1) AS _start_lsn, "
            "CONVERT(varchar(22), c.[__$seqval], 1) AS _seqval, "
            "c.[__$operation] AS _operation, "
            f"{cmd_select}"
            f"{self._utc('m.tran_end_time', self._range_offset(from_lsn, to_lsn))} AS _commit_ts"
            f"{', ' + cols if cols else ''} "
            f"FROM cdc.[{ci}_CT] c "
            "JOIN cdc.lsn_time_mapping m ON m.start_lsn = c.[__$start_lsn] "
            "WHERE c.[__$start_lsn] BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1) "
            f"ORDER BY c.[__$start_lsn], {cmd_order}c.[__$seqval], c.[__$operation]"
        )
        yield from self._change_table_batches(ci, sql, (from_lsn, to_lsn), batch_size)

    def _change_table_batches(self, ci: str, sql: str, params, batch_size: int):
        """Batches of a query on cdc.[<ci>_CT]; a denied read names the grant it needs."""
        try:
            yield from self._b.batches(sql, params, batch_size)
        except Exception as exc:
            if "denied" in str(exc) and f"{ci}_CT" in str(exc):
                raise PermissionError(
                    f"The login cannot read the change table cdc.[{ci}_CT]. Beyond what the CDC "
                    f"query functions need, the reader needs: GRANT SELECT ON cdc.[{ci}_CT] "
                    "TO <user> (ADR 0009)."
                ) from exc
            raise

    def close(self):
        self._b.close()


# --------------------------------------------------------------------------- #
# Factory (called on the driver and inside every executor task)
# --------------------------------------------------------------------------- #
def make_client(options) -> CdcClient:
    opts = {k.lower(): v for k, v in dict(options).items()}
    backend = opts.get("backend", "mssql-python").lower()
    tz = opts.get("sourcetimezone", "auto")
    if backend == "fake":
        from .fake import FakeCdcClient

        return FakeCdcClient(opts["fakepath"])
    conn = opts.get("connectionstring")
    if not conn:
        raise ValueError("Option 'connectionString' is required")
    if backend == "mssql-python":
        return SqlCdcClient(MssqlPythonBackend(conn, int(opts.get("connecttimeout", "30"))), tz)
    if backend == "arrow-odbc":
        return SqlCdcClient(ArrowOdbcBackend(conn), tz)
    raise ValueError(f"Unknown backend {backend!r} (use mssql-python, arrow-odbc or fake)")
