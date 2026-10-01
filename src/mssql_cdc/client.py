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
from collections.abc import Iterator, Sequence
from datetime import datetime
from typing import NamedTuple

import pyarrow as pa

from . import lsn as _lsn

_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")
_TZ_RE = re.compile(r"^[A-Za-z0-9 ._+\-/()]+$")
# int, decimal(18,2), varchar(20) COLLATE Greek_CI_AS
_TYPE_RE = re.compile(r"^[a-z0-9]+(\((max|\d+(,\d+)?)\))?( COLLATE [A-Za-z0-9_]+)?$")


class DataLossError(RuntimeError):
    """Raised when requested change data was already purged by CDC cleanup."""


class SchemaChangedError(RuntimeError):
    """Raised when the source's schema changed in a way the running query cannot absorb:
    restart it to re-infer the schema (ADR 0023)."""


class SourceTable(NamedTuple):
    """The table a capture instance tracks (``sys.sp_cdc_help_change_data_capture``)."""

    schema: str
    table: str
    keys: list[str]  # columns of the unique index CDC identifies rows by; [] without one
    start_lsn: str | None  # the instance's low endpoint; known before capture reaches it


class CaptureInstance(NamedTuple):
    """One capture instance of a source table (SQL Server allows two per table)."""

    name: str
    start_lsn: str | None  # its low endpoint, as sys.fn_cdc_get_min_lsn once capture reaches it
    columns: list[str]  # captured columns, in capture order; [] when unknown (the fake)
    column_types: list[str | None]  # their default Spark types; None: no default mapping


class DdlChange(NamedTuple):
    """A DDL statement on the tracked table (``sys.sp_cdc_get_ddl_history``)."""

    lsn: str
    commit_ts: str | None  # commit time (UTC, ms) of the last commit at or before ``lsn``
    command: str


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


def _sql_type(col: dict) -> str | None:
    """Declared type of a captured column (a ``sys.sp_cdc_get_captured_columns`` row), to CAST
    a bound key value to. None when a bound cannot round-trip: ``time`` comes back as Arrow
    time64[ns], which has no Python value, and a type without a Spark mapping is untested."""
    t = (col["data_type"] or "").lower()
    if t in ("decimal", "numeric"):
        return f"{t}({int(col['numeric_precision'])},{int(col['numeric_scale'])})"
    if t == "time" or t not in _SPARK_TYPES:
        return None
    if t in ("char", "varchar", "nchar", "nvarchar", "binary", "varbinary"):
        n = int(col["character_maximum_length"])
        return f"{t}({'max' if n == -1 else n})"
    if t in ("datetime2", "datetimeoffset"):
        return f"{t}({int(col['datetime_precision'])})"
    return t


def _check_type(sql_type: str) -> str:
    if not isinstance(sql_type, str) or not _TYPE_RE.match(sql_type):
        raise ValueError(f"Invalid SQL type: {sql_type!r}")
    return sql_type


def _key_select(select: str, keys: Sequence[str], types, lo, hi) -> tuple[str, list]:
    """``select`` (no WHERE) over the rows with ``lo <= (keys) < hi`` in the order ORDER BY
    sorts them: column by column, NULL first. A None bound is open. Returns the query and
    its parameters, in the order of the ``?`` marks.

    T-SQL has no row-value comparison, and a seek takes equalities on leading key columns
    plus a range on the next one; anything else it filters row by row. So the range is cut
    into disjoint pieces of that shape, one SELECT each, joined by UNION ALL. The leading
    values both bounds share become equalities, then from (x, y) to (u, v) the pieces are
    ``a = x AND b >= y``, ``a > x AND a < u`` and ``a = u AND b < v``: a range inside one
    leading value is one seek, not a read of the whole value (``tests/integration``).
    x and u can differ in Python and be equal in SQL (``'n'`` and ``'N'`` under a
    case-insensitive collation): the first piece also checks ``(a, b) < (u, v)`` and the
    last ``a > x``, which empties it, so every row still comes once.

    ``types`` None: integer bounds, inlined. Otherwise each bound is a parameter CAST to its
    column's declared type (and collation), so a varchar key is never compared as nvarchar.
    """
    ks = [f"[{_check_column(k)}]" for k in keys]
    n = len(ks)

    def cmp(i: int, op: str, v) -> tuple[str, list]:  # one column against one bound value
        k = ks[i]
        if v is None:  # NULL sorts first: nothing is below it, everything is at or above it
            nulls = {"=": f"{k} IS NULL", ">": f"{k} IS NOT NULL", ">=": "1 = 1", "<": "1 = 0"}
            return nulls[op], []
        if types is None:
            x, params = str(int(v)), []
        else:
            t, _, coll = _check_type(types[i]).partition(" COLLATE ")
            x, params = f"CAST(? {f'COLLATE {coll} ' if coll else ''}AS {t})", [v]
        return (f"({k} {op} {x} OR {k} IS NULL)" if op == "<" else f"{k} {op} {x}"), params

    def conj(parts, wrap=False) -> tuple[str, list]:
        sql = " AND ".join(s for s, _ in parts)
        return (f"({sql})" if wrap and len(parts) > 1 else sql), [v for _, ps in parts for v in ps]

    p = 0  # leading values both bounds share
    while lo is not None and hi is not None and p < n - 1 and lo[p] == hi[p]:
        p += 1
    eq = [cmp(i, "=", lo[i]) for i in range(p)]  # p > 0 only with both bounds

    def chain(bound, j: int, op: str) -> list:  # keys p..j-1 equal to bound, key j op bound
        return [cmp(i, "=", bound[i]) for i in range(p, j)] + [cmp(j, op, bound[j])]

    def below(bound) -> tuple[str, list]:  # (keys p..) < bound, row by row
        terms = [conj(chain(bound, j, "<"), wrap=True) for j in range(p, n)]
        return "(" + " OR ".join(s for s, _ in terms) + ")", [v for _, t in terms for v in t]

    pieces = []  # key p: equal to lo's, between lo's and hi's, equal to hi's
    if lo is not None:
        pieces += [
            eq + chain(lo, j, ">" if j < n - 1 else ">=") + ([below(hi)] if hi is not None else [])
            for j in range(n - 1, p, -1)
        ]
    middle = [cmp(p, ">" if p < n - 1 else ">=", lo[p])] if lo is not None else []
    pieces.append(eq + middle + ([cmp(p, "<", hi[p])] if hi is not None else []))
    if hi is not None:
        pieces += [
            eq + chain(hi, j, "<") + ([cmp(p, ">", lo[p])] if lo is not None else [])
            for j in range(p + 1, n)
        ]
    sql, params = [], []
    for piece in pieces:
        where, ps = conj(piece)
        sql.append(f"{select} WHERE {where}" if where else select)
        params += ps
    return " UNION ALL ".join(sql), params


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

    # -- snapshot of the tracked table (ADR 0016) ------------------------------
    @abstractmethod
    def source_table(self, capture_instance: str) -> SourceTable:
        """The table ``capture_instance`` tracks, its key and the instance's first LSN."""

    @abstractmethod
    def key_range(self, schema: str, table: str, key: str) -> tuple:
        """(MIN, MAX) of ``key`` in the table; (None, None) when it is empty."""

    @abstractmethod
    def key_types(self, capture_instance: str, keys: Sequence[str]) -> list[str | None]:
        """Declared SQL type of each key column, to CAST bounds to; None where it cannot."""

    @abstractmethod
    def key_tiles(self, schema: str, table: str, keys: Sequence[str], n: int) -> list[tuple]:
        """The first key of tiles 2..n of the table's rows ordered by ``keys``
        (``NTILE(n)``, NULL first): the lower bounds that split it into ranges of about the
        same number of rows. Fewer when the table has fewer than ``n`` rows."""

    @abstractmethod
    def iter_table(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
        keys: Sequence[str],
        types: Sequence[str] | None,
        lo: tuple | None,
        hi: tuple | None,
        batch_size: int,
    ) -> Iterator[pa.RecordBatch]:
        """Current rows of the table (``columns`` only) with ``lo <= (keys) < hi``, compared
        column by column with NULL first, like ORDER BY. A None bound is open, so the range
        open below also holds the rows whose leading key is NULL. ``types``: the key columns'
        SQL types for the bounds (``key_types``); None for integer bounds."""

    # -- schema changes and capture instance switches (ADR 0023) ----------------
    @abstractmethod
    def capture_instances(self, capture_instance: str) -> list[CaptureInstance]:
        """Every capture instance of the table ``capture_instance`` tracks, oldest first. A
        dropped ``capture_instance`` is followed to its table when that can be told."""

    @abstractmethod
    def ddl_history(self, capture_instance: str, from_lsn: str, to_lsn: str) -> list[DdlChange]:
        """DDL on the tracked table recorded by ``capture_instance`` with LSN in (from, to]."""

    def present_columns(self, capture_instance: str, columns: Sequence[str]) -> list[str]:
        """Which of ``columns`` the source table still has, for a snapshot to read (the rest
        it fills with NULL)."""
        return list(columns)

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
    def batches(
        self, sql: str, params: Sequence[str], batch_size: int
    ) -> Iterator[pa.RecordBatch]: ...

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
                self._offset_min = int(
                    self._b.scalar("SELECT DATEPART(TZOFFSET, SYSDATETIMEOFFSET())")
                )
        if self._tz is not None:
            return self._tz
        assert self._offset_min is not None  # the fallback above set it
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
        # Style 126 drops ".000" on whole seconds; the offset contract always carries ms.
        # Checkpoints written without them still resume: fromisoformat reads both forms.
        return datetime.fromisoformat(value).isoformat(timespec="milliseconds") if value else None

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
                "WHERE session_id = @@SPID AND wait_type = 'ASYNC_NETWORK_IO'"
            )
        except Exception:  # noqa: BLE001 - a metric must never fail a read
            return None
        return int(value or 0)

    def _captured_rows(self, ci: str) -> list[dict]:
        # The documented API, not cdc.captured_columns: it needs only what the query
        # functions need (SELECT on the source columns, gating role if any).
        ci = _check_ident(ci, "capture instance")
        not_found = (
            f"Capture instance {ci!r} not found, or the login lacks SELECT on its source "
            "columns (or membership in its gating role). Pass 'columns' explicitly."
        )
        try:
            rows = [
                r
                for batch in self._b.batches(
                    "EXEC sys.sp_cdc_get_captured_columns @capture_instance = ?", (ci,), 1000
                )
                for r in batch.to_pylist()
            ]
        except Exception as exc:  # Error 22981, driver-specific type
            raise ValueError(not_found) from exc
        if not rows:
            raise ValueError(not_found)
        return sorted(rows, key=lambda r: r["column_ordinal"])

    def captured_columns(self, capture_instance):
        ddl = []
        for r in self._captured_rows(capture_instance):
            name = _check_column(r["column_name"])
            try:
                typ = _spark_type(r["data_type"], r["numeric_precision"], r["numeric_scale"])
            except ValueError as e:
                raise ValueError(
                    f"{capture_instance}.{name}: {e}. Pass 'columns' explicitly."
                ) from None
            ddl.append(f"`{name.replace('`', '``')}` {typ}")
        return ", ".join(ddl)

    def capture_instances(self, capture_instance):
        # ponytail: one sp_cdc_get_captured_columns per instance per call (every planning);
        # cache by (name, create_date) if planning time shows up in profiles.
        out = []
        for r in self._resolve(capture_instance)[1]:
            cols = self._captured_rows(r["capture_instance"])
            types = []
            for c in cols:
                try:
                    types.append(
                        _spark_type(c["data_type"], c["numeric_precision"], c["numeric_scale"])
                    )
                except ValueError:
                    types.append(None)  # fine unless the query reads it; load() says so
            out.append(
                CaptureInstance(
                    r["capture_instance"],
                    self._hex(r["start_lsn"]),
                    [_check_column(c["column_name"]) for c in cols],
                    types,
                )
            )
        return out

    def ddl_history(self, capture_instance, from_lsn, to_lsn):
        # The documented API, not cdc.ddl_history (invariant 11): it needs what
        # sp_cdc_get_captured_columns needs. Its ddl_lsn comes back binary, like start_lsn in
        # source_table; the few rows (one per DDL) are filtered here.
        ci = _check_ident(capture_instance, "capture instance")
        rows = [
            r
            for batch in self._b.batches(
                "EXEC sys.sp_cdc_get_ddl_history @capture_instance = ?", (ci,), 1000
            )
            for r in batch.to_pylist()
        ]
        out = []
        for r in rows:
            lsn = _lsn.normalize(r["ddl_lsn"])
            if from_lsn < lsn <= to_lsn:
                out.append(DdlChange(lsn, self._commit_time_at_or_before(lsn), r["ddl_command"]))
        return sorted(out)

    def _commit_time_at_or_before(self, lsn: str) -> str | None:
        # A DDL's LSN is no commit's: sys.fn_cdc_map_lsn_to_time returns NULL for it (SQL
        # Server 2022). The last commit before it, one seek on the mapping's key.
        value = self._b.scalar(
            "SELECT TOP (1) CONVERT(varchar(23), "
            + self._utc("tran_end_time")
            + ", 126) FROM cdc.lsn_time_mapping WHERE start_lsn <= CONVERT(binary(10), ?, 1) "
            "ORDER BY start_lsn DESC",
            (lsn,),
        )
        return datetime.fromisoformat(value).isoformat(timespec="milliseconds") if value else None

    def present_columns(self, capture_instance, columns):
        # A dropped captured column stays in the capture instance; one added back under the
        # same name is another column (a new column_id), so match by column_id, not by name.
        # A column no instance captures is not read either: its change rows could only be NULL.
        rows = self._resolve(capture_instance)[1]
        captured = {}
        for r in rows:  # oldest first: the newest instance capturing a column wins
            for c in self._captured_rows(r["capture_instance"]):
                captured[c["column_name"].lower()] = c["column_id"]
        sql = (
            "SELECT name, column_id FROM sys.columns "
            "WHERE object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))"
        )
        live = {
            r["name"].lower(): r["column_id"]
            for batch in self._b.batches(
                sql, (rows[0]["source_schema"], rows[0]["source_table"]), 1000
            )
            for r in batch.to_pylist()
        }
        return [
            c for c in columns if c.lower() in live and captured.get(c.lower()) == live[c.lower()]
        ]

    # -- data -----------------------------------------------------------------
    def iter_changes(
        self, capture_instance, from_lsn, to_lsn, columns, include_command_id, batch_size
    ):
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

    # -- snapshot (ADR 0016) ----------------------------------------------------
    def _resolve(self, capture_instance: str) -> tuple[dict | None, list[dict]]:
        """The ``sys.sp_cdc_help_change_data_capture`` row of ``capture_instance`` (None when
        it is gone) and the rows of every instance of its table, oldest first.

        The documented API, not cdc.change_tables (invariant 11). Called without arguments
        it lists the capture instances whose captured columns the login can SELECT, which
        the query functions already require. @source_schema/@source_name applies the same
        check to one table, so this one listing already holds every instance of it.
        """
        ci = _check_ident(capture_instance, "capture instance")
        listed = [
            r
            for batch in self._b.batches("EXEC sys.sp_cdc_help_change_data_capture", (), 1000)
            for r in batch.to_pylist()
        ]
        # The CDC functions and the change table resolve the name case-insensitively under
        # the default collation, so a config may not match the stored case: exact first.
        rows = [r for r in listed if r["capture_instance"] == ci] or [
            r for r in listed if r["capture_instance"].lower() == ci.lower()
        ]
        if len(rows) > 1:
            names = ", ".join(repr(r["capture_instance"]) for r in rows)
            raise ValueError(
                f"Capture instance {ci!r} matches {names} ignoring case: pass the exact name."
            )

        def table(r: dict) -> tuple[str, str]:
            return r["source_schema"], r["source_table"]

        if rows:
            found, key = rows[0], table(rows[0])
        else:
            # Gone, e.g. disabled after a newer instance took over (ADR 0023). A default
            # name (<schema>_<table>) still tells its table.
            tables = {table(r) for r in listed if "_".join(table(r)).lower() == ci.lower()}
            if len(tables) != 1:
                similar = [
                    r["capture_instance"]
                    for r in listed
                    if r["capture_instance"].lower().startswith(ci.lower())
                    or ci.lower().startswith("_".join(table(r)).lower() + "_")
                ]
                hint = (
                    f" Capture instances of what may be its table: {', '.join(map(repr, similar))}"
                    f"; if {ci!r} was disabled, set captureInstance to the newer one."
                    if similar
                    else ""
                )
                raise ValueError(
                    f"Capture instance {ci!r} not found, or the login lacks SELECT on its source "
                    f"columns (or membership in its gating role).{hint}"
                )
            found, key = None, tables.pop()
        same = sorted(
            (r for r in listed if table(r) == key),
            key=lambda r: (str(r.get("create_date") or ""), self._hex(r["start_lsn"]) or ""),
        )
        return found, same

    def source_table(self, capture_instance):
        found, same = self._resolve(capture_instance)
        r = found or same[-1]  # gone: the table's newest instance
        keys = re.findall(r"\[([^\]]+)\]", r["index_column_list"] or "")  # "[a], [b]"
        return SourceTable(
            _check_column(r["source_schema"]),
            _check_column(r["source_table"]),
            [_check_column(k) for k in keys],
            self._hex(r["start_lsn"]),
        )

    def key_range(self, schema, table, key):
        t, k = f"[{_check_column(schema)}].[{_check_column(table)}]", f"[{_check_column(key)}]"
        # two scalar subqueries: each is one seek on an index led by the key
        for batch in self._b.batches(
            f"SELECT (SELECT MIN({k}) FROM {t}) AS lo, (SELECT MAX({k}) FROM {t}) AS hi", (), 1
        ):
            if batch.num_rows:
                row = batch.to_pylist()[0]
                return row["lo"], row["hi"]
        return None, None

    def key_types(self, capture_instance, keys):
        rows = self._captured_rows(capture_instance)
        by_name = {r["column_name"]: r for r in rows}
        types = [_sql_type(by_name[k]) if k in by_name else None for k in keys]
        strings = [i for i, t in enumerate(types) if t and t.startswith(("char(", "varchar("))]
        if strings:
            # A string parameter is nvarchar, and CAST to varchar converts it with the code
            # page of the database's default collation: a key in another code page loses
            # characters and its bounds their order. Converted in the column's own collation
            # they keep both. sys.columns shows the columns of a table the login can SELECT.
            sql = (
                "SELECT name, collation_name FROM sys.columns "
                "WHERE object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))"
            )
            params = (rows[0]["source_schema"], rows[0]["source_table"])
            coll = {
                r["name"]: r["collation_name"]
                for batch in self._b.batches(sql, params, 1000)
                for r in batch.to_pylist()
            }
            for i in strings:
                c = coll.get(keys[i])
                types[i] = f"{types[i]} COLLATE {_check_ident(c, 'collation')}" if c else None
        # A datetime2(7) or datetimeoffset(7) bound comes back truncated to microseconds.
        # That keeps its own column's order, but ahead of another key column two bounds can
        # swap: (t, 5) < (t + 100 ns, 3) come back as (T, 5) > (T, 3), and the rows between
        # them would land in two ranges. Only the last key column may be truncated.
        for i in range(len(types) - 1):
            if types[i] in ("datetime2(7)", "datetimeoffset(7)"):
                types[i] = None
        return types

    def key_tiles(self, schema, table, keys, n):
        # NTILE over the table's own rows, as split_points over the change table's (ADR 0015):
        # one ordered pass over the key; only the first key of each later tile comes back.
        # The helper columns take CDC's own __$ prefix, so no key column can shadow them.
        t = f"[{_check_column(schema)}].[{_check_column(table)}]"
        k = ", ".join(f"[{_check_column(c)}]" for c in keys)
        sql = (
            f"SELECT {k} FROM (SELECT {k}, [__$tile], "
            f"LAG([__$tile]) OVER (ORDER BY {k}) AS [__$prev] FROM ("
            f"SELECT {k}, NTILE({int(n)}) OVER (ORDER BY {k}) AS [__$tile] FROM {t}) a"
            ") b WHERE [__$tile] <> [__$prev] ORDER BY [__$tile]"
        )
        return [
            tuple(row)
            for batch in self._b.batches(sql, (), 1000)
            for row in zip(*(c.to_pylist() for c in batch.columns))
        ]

    def iter_table(self, schema, table, columns, keys, types, lo, hi, batch_size):
        # READ COMMITTED, never NOLOCK: a dirty read can keep a row that a rollback then
        # removes, and no change row would ever correct it downstream.
        cols = ", ".join(f"[{_check_column(c)}]" for c in columns)
        sql, params = _key_select(
            f"SELECT {cols} FROM [{_check_column(schema)}].[{_check_column(table)}]",
            keys,
            types,
            lo,
            hi,
        )
        yield from self._b.batches(sql, params, batch_size)

    def _change_table_batches(self, ci: str, sql: str, params, batch_size: int):
        """Batches of a query on cdc.[<ci>_CT]; a denied read names the grant it needs."""
        try:
            yield from self._b.batches(sql, params, batch_size)
        except Exception as exc:
            if "denied" in str(exc) and f"{ci}_CT".lower() in str(exc).lower():
                raise PermissionError(
                    f"The login cannot read the change table cdc.[{ci}_CT]. Beyond what the CDC "
                    f"query functions need, the reader needs: GRANT SELECT ON cdc.[{ci}_CT] "
                    "TO <user> (ADR 0009). Each capture instance has its own change table: a "
                    "new instance of the table needs its own grant."
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
