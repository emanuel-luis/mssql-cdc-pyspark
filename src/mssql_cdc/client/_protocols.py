"""What the data source needs from SQL Server (``CdcClient``) and from a driver
(``Backend``), and the records they pass."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any, NamedTuple, Protocol, runtime_checkable

import pyarrow as pa

from ..lsn import Lsn


class SourceTable(NamedTuple):
    """The table a capture instance tracks (``sys.sp_cdc_help_change_data_capture``)."""

    schema: str
    table: str
    keys: list[str]  # columns of the unique index CDC identifies rows by; [] without one
    start_lsn: Lsn | None  # the instance's low endpoint; known before capture reaches it


class CaptureInstance(NamedTuple):
    """One capture instance of a source table (SQL Server allows two per table)."""

    name: str
    start_lsn: Lsn | None  # its low endpoint, as sys.fn_cdc_get_min_lsn once capture reaches it
    columns: list[str]  # captured columns, in capture order; [] when unknown (the fake)
    column_types: list[str | None]  # their default Spark types; None: no default mapping
    # captured computed columns, not in ``columns``: CDC stores NULL for them in every change row
    computed: tuple[str, ...] = ()


class DdlChange(NamedTuple):
    """A DDL statement on the tracked table (``sys.sp_cdc_get_ddl_history``)."""

    lsn: Lsn
    commit_ts: str | None  # commit time (UTC, ms) of the last commit at or before ``lsn``
    command: str


# --------------------------------------------------------------------------- #
# Interface
# --------------------------------------------------------------------------- #
@runtime_checkable
class CdcClient(Protocol):
    """What the data source needs from SQL Server: ``SqlCdcClient``, or the ``fake`` backend's
    client. A ``typing.Protocol`` (ADR 0030): any class with these methods is one, and
    ``isinstance`` checks that it has them (by name; a type checker checks their signatures).
    A subclass inherits the methods that have a body here, and must define the abstract ones.

    LSNs are hex strings (invariant 6): returned as ``Lsn``, taken as any ``str`` in that form.
    """

    @abstractmethod
    def max_lsn(self) -> Lsn | None:
        """``sys.fn_cdc_get_max_lsn()``: the last LSN capture has processed; None (NULL) on a
        database capture has not written to yet."""

    @abstractmethod
    def min_lsn(self, capture_instance: str) -> Lsn:
        """``sys.fn_cdc_get_min_lsn``: the oldest LSN the instance's change table holds."""

    @abstractmethod
    def increment_lsn(self, lsn: str) -> Lsn:
        """``sys.fn_cdc_increment_lsn``: the next LSN after ``lsn``."""

    @abstractmethod
    def decrement_lsn(self, lsn: str) -> Lsn:
        """``sys.fn_cdc_decrement_lsn``: the LSN before ``lsn``."""

    @abstractmethod
    def lsn_to_time(self, lsn: str) -> str | None:
        """Commit time of an LSN as ISO-8601 UTC string (millisecond precision)."""

    @abstractmethod
    def time_to_lsn(self, ts_utc: datetime) -> Lsn | None:
        """The last LSN in cdc.lsn_time_mapping committed at or before ``ts_utc`` (naive,
        UTC), as ``sys.fn_cdc_map_time_to_lsn('largest less than or equal', ...)``; None
        when there is none."""

    @abstractmethod
    def nth_commit_after(self, lsn: str, n: int) -> Lsn | None:
        """The n-th commit LSN strictly after ``lsn`` in cdc.lsn_time_mapping."""

    @abstractmethod
    def split_points(
        self, capture_instance: str, from_lsn: str, to_lsn: str, n: int
    ) -> list[tuple[Lsn, Lsn, int]]:
        """Up to ``n`` commit-aligned upper bounds that split [from, to] into ranges holding
        about the same number of change rows of ``capture_instance``, ascending, each with
        the LSN after it (``increment_lsn``), where the next range starts, and the rows of
        its tile."""

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
    def key_range(
        self,
        schema: str,
        table: str,
        key: str,
        lo: int | None = None,
        hi: int | None = None,
        isolation: str | None = None,
    ) -> tuple[Any, Any]:
        """(MIN, MAX) of ``key`` in the table; (None, None) when it is empty. ``lo``/``hi``:
        an integer key's rows in ``[lo, hi)`` only (None: open). ``isolation``: as
        ``iter_table``'s."""

    @abstractmethod
    def key_types(self, capture_instance: str, keys: Sequence[str]) -> list[str | None]:
        """Declared SQL type of each key column, to CAST bounds to; None where it cannot."""

    @abstractmethod
    def key_tiles(
        self, schema: str, table: str, keys: Sequence[str], n: int
    ) -> list[tuple[Any, ...]]:
        """The first key of tiles 2..n of the table's rows ordered by ``keys``
        (``NTILE(n)``, NULL first): the lower bounds that split it into ranges of about the
        same number of rows. Fewer when the table has fewer than ``n`` rows."""

    @abstractmethod
    def key_buckets(
        self,
        schema: str,
        table: str,
        key: str | None,
        kind: str | None,
        width: int,
        lo: int | None = None,
        hi: int | None = None,
        isolation: str | None = None,
    ) -> list[tuple[int, int, Decimal | None]]:
        """``(bucket, rows, key_sum)`` of the table's rows grouped by ``floor(o / width)``,
        ``o`` the key's ordinal: an integer key itself (``kind`` "int"), a date its day number
        from 1970-01-01 ("date"); ``key_sum`` sums ``o``. A NULL key is in no bucket. ``key``
        None: ``[(0, rows, None)]``, the whole table. ``lo``/``hi``: an integer key's rows in
        ``[lo, hi)`` only (None: open). ``isolation``: as ``iter_table``'s. Tier 1 of
        ``reconcile()``; a chunked snapshot's integer plan (``plan_chunks``)."""

    @abstractmethod
    def iter_table(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
        keys: Sequence[str],
        types: Sequence[str] | None,
        lo: tuple[Any, ...] | None,
        hi: tuple[Any, ...] | None,
        batch_size: int,
        isolation: str | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Current rows of the table (``columns`` only) with ``lo <= (keys) < hi``, compared
        column by column with NULL first, like ORDER BY. A None bound is open, so the range
        open below also holds the rows whose leading key is NULL. ``types``: the key columns'
        SQL types for the bounds (``key_types``); None for integer bounds. ``isolation``:
        None (READ COMMITTED) or ``"snapshot"``; never READ UNCOMMITTED."""

    # -- chunked snapshots (ADR 0028) -------------------------------------------
    @abstractmethod
    def row_estimate(self, schema: str, table: str) -> int:
        """About how many rows the table has, from metadata, not a count."""

    @abstractmethod
    def key_max(self, schema: str, table: str, keys: Sequence[str]) -> tuple[Any, ...] | None:
        """The last key of the table in ORDER BY's order; None when it is empty."""

    @abstractmethod
    def key_bound(
        self,
        schema: str,
        table: str,
        keys: Sequence[str],
        types: Sequence[str] | None,
        lo: tuple[Any, ...] | None,
        hi: tuple[Any, ...] | None,
        n: int,
        isolation: str | None = None,
    ) -> tuple[Any, ...] | None:
        """The key that leaves ``n`` rows in ``[lo, key)``: the (n + 1)-th of the rows with
        ``lo <= (keys) < hi`` in ORDER BY's order. None when they are ``n`` or fewer.
        ``isolation``: as ``iter_table``'s."""

    # -- schema changes and capture instance switches (ADR 0023) ----------------
    @abstractmethod
    def capture_instances(self, capture_instance: str) -> list[CaptureInstance]:
        """Every capture instance of the table ``capture_instance`` tracks, oldest first. A
        dropped ``capture_instance`` is followed to its table when that can be told. Its
        computed columns are in ``computed``, not in ``columns``."""

    def forget_columns(self) -> None:
        """Make the next ``capture_instances`` read the captured columns again: a client
        may cache them, and ALTER COLUMN changes their types."""
        return  # a body, not a docstring only: a default, not abstract (ADR 0030)

    @abstractmethod
    def ddl_history(self, capture_instance: str, from_lsn: str, to_lsn: str) -> list[DdlChange]:
        """DDL on the tracked table recorded by ``capture_instance`` with LSN in (from, to]."""

    def present_columns(self, capture_instance: str, columns: Sequence[str]) -> list[str]:
        """Which of ``columns`` the source table still has, for a snapshot to read (the rest
        it fills with NULL). Not a computed column: its change rows are all NULL."""
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

    def clock(self) -> tuple[str | None, int | None]:
        """How commit times are converted to UTC: (zone, None), or (None, a fixed UTC offset
        in minutes) on a server that names no zone; (None, None) when there is nothing to
        convert. Planned ranges carry it, so every task converts as the driver does."""
        return None, None

    def set_clock(self, zone: str | None, offset_min: int | None) -> None:
        """Take ``clock()`` of the driver's client instead of detecting it again."""
        return

    def refresh_clock(self) -> None:
        """Read a fixed UTC offset (``clock()``'s second) again: the driver, once per batch."""
        return

    def close(self) -> None:
        """Release the connection, if any."""
        return


# --------------------------------------------------------------------------- #
# Backends: "run this SQL, give me Arrow"
# --------------------------------------------------------------------------- #
@runtime_checkable
class Backend(Protocol):
    """What ``SqlCdcClient`` needs from a database driver: run a query, return Apache Arrow.
    A ``typing.Protocol`` (ADR 0030): a class with ``batches``, ``scalar`` and ``close`` is
    one, inheriting or not; a subclass inherits ``scalar`` and ``close``. Every parameter is
    text, which the T-SQL converts on the server (ADR 0003)."""

    @abstractmethod
    def batches(self, sql: str, params: Sequence[str], batch_size: int) -> Iterator[pa.RecordBatch]:
        """The rows of ``sql``, ``params`` bound to its ``?`` marks, in Arrow record batches
        of at most ``batch_size`` rows."""

    def scalar(self, sql: str, params: Sequence[str] = ()) -> Any:
        """The first column of the first row of ``sql``; None when it returns no row."""
        for batch in self.batches(sql, params, 1):
            if batch.num_rows:
                return batch.column(0)[0].as_py()
        return None

    def close(self) -> None:
        """Close the connection."""
        return  # a body, not a docstring only: a default, not abstract (ADR 0030)
