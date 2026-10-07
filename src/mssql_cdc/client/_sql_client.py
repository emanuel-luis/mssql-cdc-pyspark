"""``SqlCdcClient``: ``CdcClient`` in plain T-SQL over a ``Backend``."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any, cast

import pyarrow as pa

from .. import lsn as _lsn
from ..lsn import Lsn
from ._protocols import Backend, CaptureInstance, CdcClient, DdlChange, SourceTable
from ._sql import _int_range, _isolated, _key_select, _spark_type, _sql_type
from ._validators import _check_capture_instance, _check_column, _check_ident, _check_tz

_log = logging.getLogger("mssql_cdc.client")  # the package's: what logging configs name

_SHIFT_HOURS = 3  # no zone moves its clock by more, or twice within them


class SqlCdcClient(CdcClient):
    def __init__(
        self, backend: Backend, source_timezone: str = "auto", lock_timeout_ms: int | None = None
    ) -> None:
        self._b = backend
        self._lock_timeout_ms = lock_timeout_ms  # on every read of the source table (ADR 0029)
        self._tz = None if source_timezone.lower() == "auto" else _check_tz(source_timezone)
        self._offset_min: int | None = None  # set instead of _tz by the pre-2022 fallback
        self._resolved_table: tuple[str, str] | None = None  # (schema, table) _resolve found
        # capture_instances' cache: the kept captured column rows, the computed column names
        self._columns: dict[tuple[str, str], tuple[list[dict[str, Any]], tuple[str, ...]]] = {}

    # -- helpers --------------------------------------------------------------
    @property
    def timezone(self) -> str:
        """Time zone of the server clock; with ``auto``, detected once per client (the driver's
        reader hands its clock to the tasks: ``clock``/``set_clock``).

        SQL Server 2022+ and Azure SQL name it (``CURRENT_TIMEZONE_ID()``), and ``AT TIME
        ZONE`` applies the daylight-saving rules in force at each commit. Older versions
        have no such function; then the server's current UTC offset
        (``SYSDATETIMEOFFSET()``) is applied to every commit, returned as ``UTC-03:00``, and
        read again for each batch (``refresh_clock``). That is exact for zones without
        daylight saving; elsewhere, name the zone.
        """
        if self._tz is None and self._offset_min is None:
            # The version, not a failed call: the name fails to compile before 2022 even in a
            # CASE branch never taken, and any other error must not pass for an old server.
            named = self._b.scalar(
                "SELECT CASE WHEN CAST(SERVERPROPERTY('ProductMajorVersion') AS int) >= 16 "
                "OR CAST(SERVERPROPERTY('EngineEdition') AS int) IN (5, 8) THEN 1 ELSE 0 END"
            )
            if named == 1:
                self._tz = _check_tz(self._b.scalar("SELECT CURRENT_TIMEZONE_ID()"))
            else:
                self._offset_min = self._server_offset()
                _log.warning(
                    "mssql_cdc: SQL Server before 2022 names no time zone: commit times are "
                    "converted with its current UTC offset (%+d minutes). In a zone with "
                    "daylight saving set sourceTimeZone to its name, or finalized_until can run "
                    "ahead of the data around a transition, even across runs.",
                    self._offset_min,
                )
        if self._tz is not None:
            return self._tz
        assert self._offset_min is not None  # the fallback above set it
        sign, minutes = ("+" if self._offset_min >= 0 else "-"), abs(self._offset_min)
        return f"UTC{sign}{minutes // 60:02d}:{minutes % 60:02d}"

    def clock(self) -> tuple[str | None, int | None]:
        _ = self.timezone  # resolves the zone, or the fallback's offset
        return self._tz, self._offset_min

    def set_clock(self, zone: str | None, offset_min: int | None) -> None:
        if zone is not None:
            self._tz = _check_tz(zone)  # inlined into the T-SQL like a configured one
        if offset_min is not None:
            self._offset_min = int(offset_min)

    def _server_offset(self) -> int:
        return int(self._b.scalar("SELECT DATEPART(TZOFFSET, SYSDATETIMEOFFSET())"))

    def refresh_clock(self) -> None:
        # The pre-2022 fallback's offset only: it is the server's current one, so a run that
        # kept the first would convert with it past a daylight-saving change (ADR 0008).
        if self._tz is None and self._offset_min is not None:
            offset = self._server_offset()
            if offset != self._offset_min:
                _log.warning(
                    "mssql_cdc: the server's UTC offset changed from %+d to %+d minutes: "
                    "batches from now on convert commit times with it",
                    self._offset_min,
                    offset,
                )
            self._offset_min = offset

    def _utc(
        self,
        t: str,
        lsn: str | None = None,
        offset_min: int | None = None,
        drops: str | None = None,
    ) -> str:
        """UTC time of the commit at LSN ``lsn`` that reads ``t`` (``tran_end_time``: a
        timezone-less datetime in the server's clock).

        ``offset_min``: a UTC offset known to hold for every value of ``t``; a plain
        ``DATEADD`` then replaces ``AT TIME ZONE``, which costs ~2.7x the read (lab t8).

        A fall-back repeats an hour of a named zone's clock, and ``AT TIME ZONE`` reads it the
        first time, with the offset before the change. A commit after the clock went back
        (``_drops``; ``drops``: a query of those it can follow, None: its own) reads it the
        second time, with the offset 3 hours on. So commit times follow LSN order across the
        change (ADR 0008). Without ``lsn``: the first reading.
        """
        zone = self.timezone
        offset_min = self._offset_min if offset_min is None else offset_min
        if offset_min is not None:
            return f"CAST(DATEADD(minute, {-int(offset_min)}, {t}) AS datetime2(3))"
        if zone.upper() == "UTC":
            return f"CAST({t} AS datetime2(3))"
        first = f"CAST(({t} AT TIME ZONE N'{zone}') AT TIME ZONE 'UTC' AS datetime2(3))"
        if lsn is None:
            return first
        if drops is None:  # none unless t is a time a fall-back repeats: an empty range then
            start = f"CASE WHEN {self._repeated(t)} THEN {self._before(t)} ELSE {lsn} END"
            drops = self._drops(start, lsn)
        later = self._zone_offset(f"DATEADD(hour, {_SHIFT_HOURS}, {t})")
        return (
            f"CASE WHEN EXISTS (SELECT 1 FROM ({drops}) x WHERE x.start_lsn <= {lsn} "
            f"AND x.t > DATEADD(hour, -{_SHIFT_HOURS}, {t})) "
            f"THEN CAST(DATEADD(minute, -{later}, {t}) AS datetime2(3)) ELSE {first} END"
        )

    def _zone_offset(self, t: str) -> str:
        """Minutes east of UTC of the named zone at local time ``t``; in the hour a fall-back
        repeats, the offset before the change (tests/integration)."""
        return f"DATEPART(TZOFFSET, {t} AT TIME ZONE N'{self.timezone}')"

    def _repeated(self, t: str) -> str:
        """``t`` is a time a fall-back repeats: the offset 3 hours on is smaller, and ``t``
        moved on by the difference is past the repeated hour."""
        now = self._zone_offset(t)
        later = self._zone_offset(f"DATEADD(hour, {_SHIFT_HOURS}, {t})")
        moved = self._zone_offset(f"DATEADD(minute, {now} - {later}, {t})")
        return f"({later} < {now} AND {moved} = {later})"

    def _drops(self, start: str, end: str) -> str:
        """The commits with LSN in (``start``, ``end``] the server clock went back to at a
        fall-back, columns start_lsn and t (its time): each reads more than a minute before
        the commit before it (less is jitter), at a time the fall-back repeats. A commit that
        follows a drop in the hours after it reads the repeated hour the second time."""
        # start as a column: a seek on the key, where in the WHERE it filtered a full scan
        return (
            f"SELECT d.start_lsn, d.t FROM (SELECT {start} AS lo) b CROSS APPLY ("
            "SELECT start_lsn, tran_end_time AS t, LAG(tran_end_time) OVER (ORDER BY start_lsn) "
            "AS prev FROM cdc.lsn_time_mapping "
            f"WHERE start_lsn > b.lo AND start_lsn <= {end}) d "
            f"WHERE d.t < DATEADD(minute, -1, d.prev) AND {self._repeated('d.t')}"
        )

    def _before(self, t: str) -> str:
        """An LSN below every commit that reads less than 3 hours before ``t``, a drop and the
        commit before it included: the last that reads earlier (the time index's seek)."""
        return (
            "COALESCE((SELECT TOP (1) start_lsn FROM cdc.lsn_time_mapping "
            f"WHERE tran_end_time <= DATEADD(hour, -{_SHIFT_HOURS}, {t}) "
            "ORDER BY tran_end_time DESC), 0x00000000000000000000)"
        )

    def _range_utc(self, from_lsn: str, to_lsn: str) -> tuple[str, list[str]]:
        """_commit_ts of the change rows of [from_lsn, to_lsn] (``m``, the mapping) and its
        parameters: one DATEADD when one offset holds for the range, else row by row, and
        with the fall-back drops a commit of the range can follow (two queries here, run
        only near a transition)."""
        zone = self.timezone
        if self._offset_min is not None or zone.upper() == "UTC":
            return self._utc("m.tran_end_time"), []
        offset = self._range_offset(from_lsn, to_lsn)
        if offset is not None:
            return self._utc("m.tran_end_time", offset_min=offset), []
        drops: list[str] = []
        for batch in self._b.batches(
            "SELECT CONVERT(varchar(22), x.start_lsn, 1) AS l, CONVERT(varchar(23), x.t, 126) AS t "
            "FROM (SELECT MIN(tran_end_time) AS t0 FROM cdc.lsn_time_mapping WHERE start_lsn "
            "BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1) "
            "HAVING MIN(tran_end_time) IS NOT NULL) r "
            f"CROSS APPLY ({self._drops(self._before('r.t0'), 'CONVERT(binary(10), ?, 1)')}) x",
            (from_lsn, to_lsn, to_lsn),
            1000,
        ):
            drops += [v for row in batch.to_pylist() for v in (row["l"], row["t"])]
        if not drops:
            return self._utc("m.tran_end_time"), []
        values = ", ".join(
            ["(CONVERT(binary(10), ?, 1), CONVERT(datetime, ?, 126))"] * (len(drops) // 2)
        )
        given = f"SELECT * FROM (VALUES {values}) v(start_lsn, t)"
        return self._utc("m.tran_end_time", "m.start_lsn", drops=given), drops

    def _range_offset(self, from_lsn: str, to_lsn: str) -> int | None:
        """The named zone's UTC offset over a range of commits, when it is one offset.

        Equal offsets 3 hours before its first commit and 3 hours after its last, less than
        7 days apart, mean no daylight-saving change in between (no zone changes twice
        within a week), nor a fall-back's repeated hour near it, which ``AT TIME ZONE``
        reads with the offset before the change. None otherwise, or when the range has no
        commits: the caller then converts row by row.
        """
        first = self._zone_offset(f"DATEADD(hour, -{_SHIFT_HOURS}, MIN(tran_end_time))")
        last = self._zone_offset(f"DATEADD(hour, {_SHIFT_HOURS}, MAX(tran_end_time))")
        value = self._b.scalar(
            "SELECT CASE WHEN DATEDIFF(day, MIN(tran_end_time), MAX(tran_end_time)) < 7 "
            f"AND {first} = {last} THEN {last} END "
            "FROM cdc.lsn_time_mapping "
            "WHERE start_lsn BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)",
            (from_lsn, to_lsn),
        )
        return None if value is None else int(value)

    def _hex(self, value: str | bytes | None) -> Lsn | None:
        return None if value is None else _lsn.normalize(value)

    # -- metadata -------------------------------------------------------------
    def max_lsn(self) -> Lsn | None:
        return self._hex(self._b.scalar("SELECT CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1)"))

    def min_lsn(self, capture_instance: str) -> Lsn:
        value = self._hex(
            self._b.scalar(
                "SELECT CONVERT(varchar(22), sys.fn_cdc_get_min_lsn(?), 1)",
                (_check_capture_instance(capture_instance),),
            )
        )
        if value is None or value == _lsn.ZERO_LSN:
            raise ValueError(
                f"Capture instance {capture_instance!r} not found, the login lacks "
                "permission to read it, or capture has not processed its creation yet "
                "(sys.fn_cdc_get_min_lsn returned 0x00...). Right after "
                "sys.sp_cdc_enable_table, retry once the capture job has run."
            )
        return value

    def increment_lsn(self, lsn: str) -> Lsn:
        return cast(  # NULL only for a NULL lsn
            Lsn,
            self._hex(
                self._b.scalar(
                    "SELECT CONVERT(varchar(22), "
                    "sys.fn_cdc_increment_lsn(CONVERT(binary(10), ?, 1)), 1)",
                    (lsn,),
                )
            ),
        )

    def decrement_lsn(self, lsn: str) -> Lsn:
        return cast(  # NULL only for a NULL lsn
            Lsn,
            self._hex(
                self._b.scalar(
                    "SELECT CONVERT(varchar(22), "
                    "sys.fn_cdc_decrement_lsn(CONVERT(binary(10), ?, 1)), 1)",
                    (lsn,),
                )
            ),
        )

    def lsn_to_time(self, lsn: str) -> str | None:
        # The mapping's own row, as sys.fn_cdc_map_lsn_to_time reads it: None for an LSN that
        # is no commit's. Its LSN orders it in a fall-back's repeated hour (_utc).
        return self._commit_time("=", lsn)

    def _commit_time(self, op: str, lsn: str) -> str | None:
        """UTC commit time of the last commit with an LSN ``op`` ``lsn``, one seek on the
        mapping's key."""
        value = self._b.scalar(
            "SELECT TOP (1) CONVERT(varchar(23), "
            + self._utc("m.tran_end_time", "m.start_lsn")
            + f", 126) FROM cdc.lsn_time_mapping m WHERE m.start_lsn {op} "
            "CONVERT(binary(10), ?, 1) ORDER BY m.start_lsn DESC",
            (lsn,),
        )
        # Style 126 drops ".000" on whole seconds; the offset contract always carries ms.
        # Checkpoints written without them still resume: fromisoformat reads both forms.
        return datetime.fromisoformat(value).isoformat(timespec="milliseconds") if value else None

    def time_to_lsn(self, ts_utc: datetime) -> Lsn | None:
        # tran_end_time is in the server's clock: convert UTC to it, the inverse of _utc.
        # Whole seconds: the function takes datetime, which rounds milliseconds to 1/300 s,
        # upwards too; a later LSN would skip a commit the copy lacks, an earlier one replays.
        at = self._server_clock("CONVERT(datetime2(0), ?, 126)")
        lsn = self._hex(
            self._b.scalar(
                "SELECT CONVERT(varchar(22), sys.fn_cdc_map_time_to_lsn("
                f"N'largest less than or equal', {at}), 1)",
                (ts_utc.replace(microsecond=0).isoformat(),) * at.count("?"),
            )
        )
        return None if lsn == _lsn.ZERO_LSN else lsn

    def _server_clock(self, utc: str) -> str:
        """The server-clock time no commit after ``utc`` (a datetime2 expression) reads at or
        before. A fall-back repeats an hour of a named zone's clock, so a commit up to an hour
        after ``utc`` can read earlier than it: take the earlier of ``utc``'s time and the next
        hour's less that hour. Anywhere else that is ``utc``'s time."""
        zone = self.timezone
        if self._offset_min is not None:
            return f"DATEADD(minute, {int(self._offset_min)}, {utc})"
        if zone.upper() == "UTC":
            return utc

        def local(t: str) -> str:
            return f"CONVERT(datetime2(0), ({t} AT TIME ZONE 'UTC') AT TIME ZONE N'{zone}')"

        hour_after = f"DATEADD(hour, -1, {local(f'DATEADD(hour, 1, {utc})')})"
        return f"(SELECT MIN(v) FROM (VALUES ({local(utc)}), ({hour_after})) x(v))"

    def nth_commit_after(self, lsn: str, n: int) -> Lsn | None:
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

    def split_points(
        self, capture_instance: str, from_lsn: str, to_lsn: str, n: int
    ) -> list[tuple[Lsn, Lsn, int]]:
        # Tiles of the change table's own rows, not of cdc.lsn_time_mapping's commits: those
        # are database-wide, and on a real table they left the largest range with ~2x the
        # mean rows (ADR 0015). Each bound is the last commit LSN of its tile, so a commit
        # whose rows straddle two tiles stays whole in the first range. The LSN after each
        # comes along, where the next range starts: no round trip per bound. So does the
        # tile's row count, free once grouped, by which the reader merges small tiles.
        ci = _check_capture_instance(capture_instance)
        n = int(n)
        sql = (
            "SELECT CONVERT(varchar(22), MAX(__$start_lsn), 1) AS b, "
            "CONVERT(varchar(22), sys.fn_cdc_increment_lsn(MAX(__$start_lsn)), 1) AS n, "
            "COUNT_BIG(*) AS r FROM ("
            f"SELECT __$start_lsn, NTILE({n}) OVER (ORDER BY __$start_lsn) AS g "
            f"FROM cdc.[{ci}_CT] "
            "WHERE __$start_lsn BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)"
            ") x GROUP BY g ORDER BY b"
        )
        points: list[tuple[Lsn, Lsn, int]] = []
        for batch in self._change_table_batches(ci, sql, (from_lsn, to_lsn), 1000):
            for b, after, rows in zip(*(c.to_pylist() for c in batch.columns)):
                points.append((_lsn.normalize(b), _lsn.normalize(after), int(rows)))
        return points

    def ping(self, samples: int = 3) -> list[float]:
        import time

        times: list[float] = []
        for _ in range(samples):
            t0 = time.perf_counter()
            self._b.scalar("SELECT 1")
            times.append((time.perf_counter() - t0) * 1000)
        return times

    def network_wait_ms(self) -> int | None:
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

    def _captured_rows(self, ci: str) -> list[dict[str, Any]]:
        # The documented API, not cdc.captured_columns: it needs only what the query
        # functions need (SELECT on the source columns, gating role if any).
        ci = _check_capture_instance(ci)
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
            # the driver's own words too: a dropped connection is no missing instance
            cause = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
            raise ValueError(f"{not_found} Cause: {cause}") from exc
        if not rows:
            raise ValueError(not_found)
        return sorted(rows, key=lambda r: r["column_ordinal"])

    def captured_columns(self, capture_instance: str) -> str:
        ddl: list[str] = []
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

    def capture_instances(self, capture_instance: str) -> list[CaptureInstance]:
        rows = self._resolve(capture_instance)[1]
        computed: set[int] | None = None
        out: list[CaptureInstance] = []
        for r in rows:
            # An instance's columns never change, their types do (ALTER COLUMN): cached per
            # instance and creation, so not refetched every planning; forget_columns() drops
            # them when a batch holds DDL, before its types are checked (ADR 0023's D1).
            key = (r["capture_instance"], str(r.get("create_date")))
            if key not in self._columns:
                if computed is None:
                    # CDC stores NULL for a computed column in every change row: left out (by
                    # column_id, which a column dropped and added back does not keep)
                    computed = {
                        c["column_id"] for c in self._table_columns(rows[0]) if c["is_computed"]
                    }
                cols = self._captured_rows(r["capture_instance"])
                self._columns[key] = (
                    [c for c in cols if c["column_id"] not in computed],
                    tuple(c["column_name"] for c in cols if c["column_id"] in computed),
                )
            kept, skipped = self._columns[key]
            types: list[str | None] = []
            for c in kept:
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
                    [_check_column(c["column_name"]) for c in kept],
                    types,
                    skipped,
                )
            )
        return out

    def _table_columns(self, r: Mapping[str, Any]) -> list[dict[str, Any]]:
        """``sys.columns`` rows (name, column_id, is_computed) of the source table of ``r``, a
        ``_resolve`` row. sys.columns shows the columns of a table the login can SELECT."""
        sql = (
            "SELECT name, column_id, is_computed FROM sys.columns "
            "WHERE object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))"
        )
        params = (r["source_schema"], r["source_table"])
        return [c for batch in self._b.batches(sql, params, 1000) for c in batch.to_pylist()]

    def forget_columns(self) -> None:
        self._columns.clear()

    def ddl_history(self, capture_instance: str, from_lsn: str, to_lsn: str) -> list[DdlChange]:
        # The documented API, not cdc.ddl_history (invariant 11): it needs what
        # sp_cdc_get_captured_columns needs. Its ddl_lsn comes back binary, like start_lsn in
        # source_table; the few rows (one per DDL) are filtered here.
        ci = _check_capture_instance(capture_instance)
        rows = [
            r
            for batch in self._b.batches(
                "EXEC sys.sp_cdc_get_ddl_history @capture_instance = ?", (ci,), 1000
            )
            for r in batch.to_pylist()
        ]
        out: list[DdlChange] = []
        for r in rows:
            lsn = _lsn.normalize(r["ddl_lsn"])
            if from_lsn < lsn <= to_lsn:
                out.append(DdlChange(lsn, self._commit_time_at_or_before(lsn), r["ddl_command"]))
        return sorted(out)

    def _commit_time_at_or_before(self, lsn: str) -> str | None:
        # A DDL's LSN is no commit's: sys.fn_cdc_map_lsn_to_time returns NULL for it (SQL
        # Server 2022). The last commit before it.
        return self._commit_time("<=", lsn)

    def present_columns(self, capture_instance: str, columns: Sequence[str]) -> list[str]:
        # A dropped captured column stays in the capture instance; one added back under the
        # same name is another column (a new column_id), so match by column_id, not by name.
        # A column no instance captures is not read either: its change rows could only be NULL;
        # nor is a computed column, which CDC stores as NULL in every change row.
        rows = self._resolve(capture_instance)[1]
        captured: dict[str, int] = {}
        for r in rows:  # oldest first: the newest instance capturing a column wins
            for c in self._captured_rows(r["capture_instance"]):
                captured[c["column_name"].lower()] = c["column_id"]
        live = {
            c["name"].lower(): c["column_id"]
            for c in self._table_columns(rows[0])
            if not c["is_computed"]
        }
        return [
            c for c in columns if c.lower() in live and captured.get(c.lower()) == live[c.lower()]
        ]

    # -- data -----------------------------------------------------------------
    def iter_changes(
        self,
        capture_instance: str,
        from_lsn: str,
        to_lsn: str,
        columns: Sequence[str],
        include_command_id: bool,
        batch_size: int,
    ) -> Iterator[pa.RecordBatch]:
        # The change table itself, not cdc.fn_cdc_get_all_changes_<ci>: the function does
        # not return __$command_id (ADR 0009). Unlike the function, the table does not
        # reject a range that cleanup purged; the reader re-checks min_lsn after reading.
        ci = _check_capture_instance(capture_instance)
        cols = ", ".join(f"c.[{_check_column(c)}]" for c in columns)
        cmd_select = "c.[__$command_id] AS _command_id, " if include_command_id else ""
        cmd_order = "c.[__$command_id], " if include_command_id else ""
        commit_ts, params = self._range_utc(from_lsn, to_lsn)
        sql = (
            "SELECT "
            "CONVERT(varchar(22), c.[__$start_lsn], 1) AS _start_lsn, "
            "CONVERT(varchar(22), c.[__$seqval], 1) AS _seqval, "
            "c.[__$operation] AS _operation, "
            f"{cmd_select}"
            f"{commit_ts} AS _commit_ts"
            f"{', ' + cols if cols else ''} "
            f"FROM cdc.[{ci}_CT] c "
            "JOIN cdc.lsn_time_mapping m ON m.start_lsn = c.[__$start_lsn] "
            "WHERE c.[__$start_lsn] BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1) "
            f"ORDER BY c.[__$start_lsn], {cmd_order}c.[__$seqval], c.[__$operation]"
        )
        yield from self._change_table_batches(ci, sql, (*params, from_lsn, to_lsn), batch_size)

    # -- snapshot (ADR 0016) ----------------------------------------------------
    def _resolve(self, capture_instance: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """The ``sys.sp_cdc_help_change_data_capture`` row of ``capture_instance`` (None when
        it is gone) and the rows of every instance of its table, oldest first.

        The documented API, not cdc.change_tables (invariant 11). Called without arguments
        it lists the capture instances whose captured columns the login can SELECT, which
        the query functions already require. @source_schema/@source_name applies the same
        check to one table, so this one listing already holds every instance of it.

        Every planning resolves the name again: after the first, only the table it resolved
        to is listed, and the whole database only when that misses the name (the instance
        gone, the table renamed).
        """
        ci = _check_capture_instance(capture_instance)

        def listing(sql: str, params: Sequence[str] = ()) -> list[dict[str, Any]]:
            return [r for b in self._b.batches(sql, params, 1000) for r in b.to_pylist()]

        listed: list[dict[str, Any]] | None = None
        if self._resolved_table is not None:
            try:
                one = listing(
                    "EXEC sys.sp_cdc_help_change_data_capture @source_schema = ?, @source_name = ?",
                    self._resolved_table,
                )
            except Exception:  # noqa: BLE001 - renamed or dropped; the full listing tells
                one = []
            if any(r["capture_instance"].lower() == ci.lower() for r in one):
                listed = one
        if listed is None:
            listed = listing("EXEC sys.sp_cdc_help_change_data_capture")
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

        def table(r: Mapping[str, Any]) -> tuple[str, str]:
            return r["source_schema"], r["source_table"]

        found: dict[str, Any] | None
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
        self._resolved_table = key
        return found, same

    def source_table(self, capture_instance: str) -> SourceTable:
        found, same = self._resolve(capture_instance)
        r = found or same[-1]  # gone: the table's newest instance
        keys = re.findall(r"\[([^\]]+)\]", r["index_column_list"] or "")  # "[a], [b]"
        return SourceTable(
            _check_column(r["source_schema"]),
            _check_column(r["source_table"]),
            [_check_column(k) for k in keys],
            self._hex(r["start_lsn"]),
        )

    def _source_read(self, sql: str, isolation: str | None = None) -> str:
        """A read of the source table: a snapshot's, a plan's or reconcile's (``_isolated``)."""
        return _isolated(sql, isolation, self._lock_timeout_ms)

    def key_range(
        self,
        schema: str,
        table: str,
        key: str,
        lo: int | None = None,
        hi: int | None = None,
        isolation: str | None = None,
    ) -> tuple[Any, Any]:
        t, k = f"[{_check_column(schema)}].[{_check_column(table)}]", f"[{_check_column(key)}]"
        t += _int_range(k, lo, hi, "WHERE")
        # two scalar subqueries: each is one seek on an index led by the key
        sql = f"SELECT (SELECT MIN({k}) FROM {t}) AS lo, (SELECT MAX({k}) FROM {t}) AS hi"
        for batch in self._b.batches(self._source_read(sql, isolation), (), 1):
            if batch.num_rows:
                row = batch.to_pylist()[0]
                return row["lo"], row["hi"]
        return None, None

    def key_types(self, capture_instance: str, keys: Sequence[str]) -> list[str | None]:
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

    def key_tiles(
        self, schema: str, table: str, keys: Sequence[str], n: int
    ) -> list[tuple[Any, ...]]:
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
            for batch in self._b.batches(self._source_read(sql), (), 1000)
            for row in zip(*(c.to_pylist() for c in batch.columns))
        ]

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
        # One scan (a seek with lo/hi), aggregated on the server: one row per bucket crosses
        # the network. T-SQL's integer division truncates toward zero; the CASE floors a
        # negative ordinal, so a bucket is the same range Spark computes. The __$ names cannot
        # be a column's.
        t = f"[{_check_column(schema)}].[{_check_column(table)}]"
        if key is None:
            sql = (
                "SELECT CAST(0 AS bigint) AS b, COUNT_BIG(*) AS n, "
                f"CAST(NULL AS decimal(38,0)) AS s FROM {t}"
            )
        else:
            k = f"[{_check_column(key)}]"
            ordinals: dict[str | None, str] = {
                "int": f"CAST({k} AS bigint)",
                "date": f"CAST(DATEDIFF(day, CAST('19700101' AS date), {k}) AS bigint)",
            }
            o = ordinals[kind]
            w = f"CAST({int(width)} AS bigint)"
            sql = (
                "SELECT [__$b] AS b, COUNT_BIG(*) AS n, SUM(CAST([__$o] AS decimal(38,0))) AS s "
                f"FROM (SELECT {o} AS [__$o], CASE WHEN {o} >= 0 THEN {o} / {w} "
                f"ELSE ({o} + 1) / {w} - 1 END AS [__$b] FROM {t} WHERE {k} IS NOT NULL"
                f"{_int_range(k, lo, hi, 'AND')}) x GROUP BY [__$b]"
            )
        return [
            tuple(row)
            for batch in self._b.batches(self._source_read(sql, isolation), (), 1000)
            for row in zip(*(c.to_pylist() for c in batch.columns))
        ]

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
        yield from self._b.batches(self._source_read(sql, isolation), params, batch_size)

    def row_estimate(self, schema: str, table: str) -> int:
        # sys.sp_spaceused: public, and a lookup of the partitions' row counts, not a scan
        name = f"[{_check_column(schema)}].[{_check_column(table)}]"
        for batch in self._b.batches("EXEC sys.sp_spaceused @objname = ?", (name,), 1):
            if batch.num_rows:
                return int(str(batch.column("rows")[0].as_py()).strip() or 0)
        return 0

    def key_max(self, schema: str, table: str, keys: Sequence[str]) -> tuple[Any, ...] | None:
        t = f"[{_check_column(schema)}].[{_check_column(table)}]"
        k = ", ".join(f"[{_check_column(c)}]" for c in keys)
        desc = ", ".join(f"[{c}] DESC" for c in keys)
        sql = self._source_read(f"SELECT TOP (1) {k} FROM {t} ORDER BY {desc}")
        for batch in self._b.batches(sql, (), 1):
            if batch.num_rows:
                return tuple(c[0].as_py() for c in batch.columns)
        return None

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
        # Each seekable piece of [lo, hi) takes its first n + 1 keys (TOP ends its seek there)
        # and the (n + 1)-th of their union is the bound: at most a few times n keys read,
        # never the range itself.
        t = f"[{_check_column(schema)}].[{_check_column(table)}]"
        k = ", ".join(f"[{_check_column(c)}]" for c in keys)
        n = int(n)
        union, params = _key_select(
            f"SELECT TOP ({n + 1}) {k} FROM {t}",
            keys,
            types,
            lo,
            hi,
            lambda sql: f"SELECT * FROM ({sql} ORDER BY {k}) p",
        )
        sql = f"SELECT {k} FROM ({union}) u ORDER BY {k} OFFSET {n} ROWS FETCH NEXT 1 ROWS ONLY"
        for batch in self._b.batches(self._source_read(sql, isolation), params, 1):
            if batch.num_rows:
                return tuple(c[0].as_py() for c in batch.columns)
        return None

    def _change_table_batches(
        self, ci: str, sql: str, params: Sequence[str], batch_size: int
    ) -> Iterator[pa.RecordBatch]:
        """Batches of a query on cdc.[<ci>_CT]; a denied read names the grant it needs."""
        try:
            yield from self._b.batches(sql, params, batch_size)
        except Exception as exc:
            # an error naming the change table, then SQL Server asked whether the login may
            # read it: the message's language and each driver's layout of it do not matter
            if f"{ci}_CT".lower() in str(exc).lower() and self._denied(ci):
                raise PermissionError(
                    f"The login cannot read the change table cdc.[{ci}_CT]. Beyond what the CDC "
                    f"query functions need, the reader needs: GRANT SELECT ON cdc.[{ci}_CT] "
                    "TO <user> (ADR 0009). Each capture instance has its own change table: a "
                    "new instance of the table needs its own grant."
                ) from exc
            raise

    def _denied(self, ci: str) -> bool:
        """Whether the login lacks SELECT on the change table of ``ci``, an instance that still
        exists: HAS_PERMS_BY_NAME is 0 for any object the login cannot see, a dropped one too,
        and sys.fn_cdc_get_min_lsn is 0x00 once the instance is gone."""
        try:
            perms: int | None = self._b.scalar(
                "SELECT CASE WHEN sys.fn_cdc_get_min_lsn(?) > 0x00000000000000000000 "
                "THEN HAS_PERMS_BY_NAME(?, 'OBJECT', 'SELECT') END",
                (ci, f"cdc.[{ci}_CT]"),
            )
        except Exception:  # noqa: BLE001 - the read's own error is the one to raise
            return False
        return perms == 0

    def close(self) -> None:
        self._b.close()
