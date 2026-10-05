"""A file-backed stand-in for SQL Server CDC, for tests and local experiments.

It reproduces the parts of CDC the data source relies on:

* a database-wide commit timeline (``cdc.lsn_time_mapping``), including
  "dummy" entries written while the database is idle;
* per-capture-instance change rows ordered by commit LSN;
* a per-instance low watermark (``sys.fn_cdc_get_min_lsn``) that cleanup moves
  before it deletes the change rows below it;
* for instances given a key (one column or several), the source table's current rows,
  which a snapshot reads (tiled like NTILE, or in a chunked snapshot's chunks: an exact row
  estimate, key bounds, rows per slice of a key range) and cleanup does not touch;
  ``commit_before_read`` queues a transaction the next read commits just before it reads
  them, as a writer does between a chunk's stamp and its SELECT (ADR 0028);
* up to two capture instances per source table (ADR 0023): a newer one starts at the next
  commit, and from there every commit lands in both, each with only its own captured
  columns and its own ``__$command_id`` (an update that changes none of an instance's
  columns writes no row there); DDL rows (``sys.sp_cdc_get_ddl_history``), a DROP
  COLUMN also removing the column from the source table, which a snapshot then cannot select.

An instance's table is named after the first instance of it (the one the constructor
declares). Captured columns (Spark DDL, per instance) are optional; without them the
instance keeps every column of every change and ``columns`` must be passed to the source.

State lives in plain files so that the Spark driver and every executor process
see the same data (Python workers are separate processes, even locally).
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa

from . import lsn as _lsn
from .client import CaptureInstance, CdcClient, DdlChange, SourceTable

_MAPPING = "lsn_time_mapping.jsonl"
_MIN = "min_lsn.json"
_KEYS = "keys.json"
_INSTANCES = "instances.json"  # name -> {"table", "created", "columns": [[name, type]] | None}
_DROPPED = "dropped.json"  # table -> lower names of the columns DROP COLUMN removed from it
_QUEUED = "before_read.jsonl"  # transactions the next table read commits first (tests)


def _read_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _read_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _write_json(path: str, value) -> None:
    # whole or not at all: another process may read it meanwhile (commit_before_read)
    with open(path + ".tmp", "w", encoding="utf-8") as fh:
        json.dump(value, fh)
    os.replace(path + ".tmp", path)


def _key_columns(key) -> list[str]:
    """``keys`` maps an instance to one column or a list of them."""
    return [key] if isinstance(key, str) else list(key or [])


def _sort_key(values) -> tuple:
    """A key tuple in SQL Server's ORDER BY order: column by column, NULL first."""
    return tuple((v is not None, v) for v in values)


def _parse_columns(columns) -> list[list[str]] | None:
    """Spark DDL (``"id INT, amount DECIMAL(18,2)"``), or a list of names (typed STRING) or
    of (name, type) pairs."""
    if columns is None:
        return None
    if isinstance(columns, str):
        out = []
        for part in re.split(r",(?![^()]*\))", columns):  # commas outside parentheses
            name, _, typ = part.strip().partition(" ")
            out.append([name.strip("`"), typ.strip().upper() or "STRING"])
        return out
    return [[c, "STRING"] if isinstance(c, str) else [c[0], c[1]] for c in columns]


def _instances(path: str) -> dict:
    """Every capture instance; ones created before instances.json existed are their own table."""
    known = _read_json(os.path.join(path, _INSTANCES))
    for name in _read_json(os.path.join(path, _MIN)):
        known.setdefault(name, {"table": name, "created": None, "columns": None})
    return known


def _resolve(path: str, ci: str) -> tuple[str | None, list[tuple[str, dict]]]:
    """The instance named ``ci`` (exact first, else ignoring case; None when gone) and every
    instance of its table, oldest first. A gone ``ci`` still names its table when the table
    is named after it, as ``SqlCdcClient`` follows a default instance name."""
    known = _instances(path)
    name = ci if ci in known else next((n for n in known if n.lower() == ci.lower()), None)
    if name is not None:
        table = known[name]["table"]
    else:
        table = next((m["table"] for m in known.values() if m["table"].lower() == ci.lower()), None)
        if table is None:
            raise ValueError(f"Capture instance {ci!r} not found")
    mins = _read_json(os.path.join(path, _MIN))
    same = sorted(
        ((n, m) for n, m in known.items() if m["table"] == table),
        key=lambda nm: (nm[1]["created"] or "", mins.get(nm[0], "")),
    )
    return name, same


class FakeCdcClient(CdcClient):
    def __init__(self, path: str):
        self.path = path

    # -- state ----------------------------------------------------------------
    def _mapping(self) -> list[dict]:
        return _read_jsonl(os.path.join(self.path, _MAPPING))

    def _changes(self, ci: str) -> list[dict]:
        return _read_jsonl(os.path.join(self.path, "changes", f"{self._name(ci)}.jsonl"))

    def _mins(self) -> dict:
        return _read_json(os.path.join(self.path, _MIN))

    def _name(self, ci: str) -> str:
        """The instance as created: the exact name first, else ignoring case (SQL Server's
        default collation), as ``SqlCdcClient.source_table``."""
        mins = self._mins()
        return ci if ci in mins else next((n for n in mins if n.lower() == ci.lower()), ci)

    # -- CdcClient ------------------------------------------------------------
    def max_lsn(self):
        m = self._mapping()
        return m[-1]["start_lsn"] if m else None  # NULL before capture's first entry, as there

    def min_lsn(self, capture_instance):
        mins, name = self._mins(), self._name(capture_instance)
        if name not in mins:
            raise ValueError(f"Capture instance {capture_instance!r} not found")
        return mins[name]

    def increment_lsn(self, lsn):
        return _lsn.from_int(_lsn.to_int(lsn) + 1)

    def decrement_lsn(self, lsn):
        return _lsn.from_int(max(_lsn.to_int(lsn) - 1, 0))

    def lsn_to_time(self, lsn):
        # like sys.fn_cdc_map_lsn_to_time: an entry's own LSN only, None for any other
        return next((r["tran_end_time"] for r in self._mapping() if r["start_lsn"] == lsn), None)

    def _time_at_or_before(self, lsn: str) -> str | None:
        """The commit time of the last entry at or before ``lsn``, as
        ``SqlCdcClient._commit_time_at_or_before``."""
        return max(
            (
                (r["start_lsn"], r["tran_end_time"])
                for r in self._mapping()
                if r["start_lsn"] <= lsn
            ),
            default=(None, None),
        )[1]

    def time_to_lsn(self, ts_utc):
        at = ts_utc.isoformat(timespec="milliseconds")  # the mapping's times are UTC here
        return max(
            (r["start_lsn"] for r in self._mapping() if r["tran_end_time"] <= at), default=None
        )

    def nth_commit_after(self, lsn, n):
        after = [r["start_lsn"] for r in self._mapping() if r["start_lsn"] > lsn]
        return after[: int(n)][-1] if after else None

    def split_points(self, capture_instance, from_lsn, to_lsn, n):
        # like SqlCdcClient: tiles of the capture instance's change rows, bound = last LSN
        lsns = sorted(
            r["start_lsn"]
            for r in self._changes(capture_instance)
            if from_lsn <= r["start_lsn"] <= to_lsn
        )
        if not lsns:
            return []
        n = max(1, min(int(n), len(lsns)))
        size, rem = divmod(len(lsns), n)
        points, idx = [], 0
        for i in range(n):
            idx += size + (1 if i < rem else 0)
            points.append((lsns[idx - 1], self.increment_lsn(lsns[idx - 1])))
        return points

    def _keys(self) -> dict:
        return _read_json(os.path.join(self.path, _KEYS))

    def source_table(self, capture_instance):
        name, same = _resolve(self.path, capture_instance)  # not found -> ValueError
        name = name or same[-1][0]  # gone: the table's newest instance
        table = same[0][1]["table"]
        return SourceTable("dbo", table, _key_columns(self._keys().get(table)), self.min_lsn(name))

    def capture_instances(self, capture_instance):
        mins = self._mins()
        out = []
        for name, meta in _resolve(self.path, capture_instance)[1]:
            cols = meta.get("columns") or []
            out.append(
                CaptureInstance(name, mins.get(name), [c for c, _ in cols], [t for _, t in cols])
            )
        return out

    def ddl_history(self, capture_instance, from_lsn, to_lsn):
        rows = _read_jsonl(os.path.join(self.path, "ddl", f"{self._name(capture_instance)}.jsonl"))
        return [
            DdlChange(r["lsn"], self._time_at_or_before(r["lsn"]), r["command"])
            for r in rows
            if from_lsn < r["lsn"] <= to_lsn
        ]

    def captured_columns(self, capture_instance):
        cols = _instances(self.path).get(self._name(capture_instance), {}).get("columns")
        if not cols:
            return super().captured_columns(capture_instance)  # no metadata: 'columns' required
        return ", ".join(f"`{c}` {t}" for c, t in cols)

    def _dropped(self, table: str) -> set[str]:
        return set(_read_json(os.path.join(self.path, _DROPPED)).get(table, []))

    def present_columns(self, capture_instance, columns):
        # ponytail: by name, so a column added back after its DROP counts as present; SQL
        # Server matches by column_id (another column). Model column ids if a test needs it.
        dropped = self._dropped(self.source_table(capture_instance).table)
        return [c for c in columns if c.lower() not in dropped]

    def _table(self, table: str) -> list[dict]:
        return list(_read_json(os.path.join(self.path, "tables", f"{table}.json")).values())

    def _key_values(self, table, key, lo=None, hi=None) -> list:
        """The table's non-NULL values of ``key`` in ``[lo, hi)`` (None: open)."""
        return [
            v
            for v in (r.get(key) for r in self._table(table))
            if v is not None and (lo is None or v >= lo) and (hi is None or v < hi)
        ]

    def key_range(self, schema, table, key, lo=None, hi=None, isolation=None):
        keys = self._key_values(table, key, lo, hi)
        return (min(keys), max(keys)) if keys else (None, None)

    def key_types(self, capture_instance, keys):
        return ["sql_variant"] * len(keys)  # the fake compares Python values; nothing to CAST

    def key_tiles(self, schema, table, keys, n):
        # NTILE(n): the first (rows % n) tiles hold one row more; bound = first row of a tile
        rows = sorted((tuple(r.get(k) for k in keys) for r in self._table(table)), key=_sort_key)
        n = min(int(n), len(rows))
        if n <= 1:
            return []
        size, rem = divmod(len(rows), n)
        starts, idx = [], 0
        for i in range(n - 1):
            idx += size + (1 if i < rem else 0)
            starts.append(rows[idx])
        return starts

    def _keys_in(self, table, keys, lo, hi) -> list[tuple]:
        """The table's keys with ``lo <= key < hi``, in ORDER BY's order."""
        found = sorted((tuple(r.get(k) for k in keys) for r in self._table(table)), key=_sort_key)
        return [
            k
            for k in found
            if (lo is None or _sort_key(k) >= _sort_key(lo))
            and (hi is None or _sort_key(k) < _sort_key(hi))
        ]

    def row_estimate(self, schema, table):
        return len(self._table(table))

    def key_max(self, schema, table, keys):
        found = self._keys_in(table, keys, None, None)
        return found[-1] if found else None

    def key_bound(self, schema, table, keys, types, lo, hi, n, isolation=None):
        found = self._keys_in(table, keys, lo, hi)
        return found[int(n)] if len(found) > int(n) else None

    def _commit_queued(self) -> None:
        """Commit what ``FakeCdcDatabase.commit_before_read`` queued; the read that renames the
        queue first takes it, so one of a snapshot's parallel reads commits it."""
        path = os.path.join(self.path, _QUEUED)
        claimed = f"{path}.{os.getpid()}.{uuid.uuid4().hex}"
        try:
            os.replace(path, claimed)
        except FileNotFoundError:
            return
        db = FakeCdcDatabase(self.path, [])
        for tx in _read_jsonl(claimed):
            at = datetime.fromisoformat(tx["at"]) if tx["at"] else None
            db.commit(tx["capture_instance"], [(op, row) for op, row in tx["changes"]], at)
        os.remove(claimed)

    def key_buckets(self, schema, table, key, kind, width, lo=None, hi=None, isolation=None):
        # ponytail: integer keys only; the table's JSON rows keep no date type
        if key is None:
            return [(0, len(self._table(table)), None)]
        out: dict[int, tuple[int, int]] = {}  # bucket -> (rows, key sum)
        for o in self._key_values(table, key, lo, hi):
            n, s = out.get(o // int(width), (0, 0))
            out[o // int(width)] = (n + 1, s + o)
        return [(b, n, Decimal(s)) for b, (n, s) in sorted(out.items())]

    def iter_table(self, schema, table, columns, keys, types, lo, hi, batch_size, isolation=None):
        def inside(row):
            k = _sort_key(row.get(c) for c in keys)
            return (lo is None or k >= _sort_key(lo)) and (hi is None or k < _sort_key(hi))

        if isolation not in (None, "snapshot"):
            raise ValueError(f"isolation must be None or 'snapshot', not {isolation!r}")
        gone = [c for c in columns if c.lower() in self._dropped(table)]
        if gone:  # as SQL Server refuses to select it
            raise ValueError(f"Invalid column name {gone[0]!r}")
        self._commit_queued()  # after the snapshot's stamp, before its read (tests)
        rows = [r for r in self._table(table) if inside(r)]
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            yield pa.RecordBatch.from_pydict({c: [r.get(c) for r in chunk] for c in columns})

    def iter_changes(
        self, capture_instance, from_lsn, to_lsn, columns, include_command_id, batch_size
    ):
        times = {r["start_lsn"]: r["tran_end_time"] for r in self._mapping()}
        rows = [r for r in self._changes(capture_instance) if from_lsn <= r["start_lsn"] <= to_lsn]
        rows.sort(
            key=lambda r: (r["start_lsn"], r.get("command_id", 0), r["seqval"], r["operation"])
        )
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            data = {
                "_start_lsn": [r["start_lsn"] for r in chunk],
                "_seqval": [r["seqval"] for r in chunk],
                "_operation": pa.array([r["operation"] for r in chunk], pa.int32()),
            }
            if include_command_id:
                data["_command_id"] = pa.array([r.get("command_id") for r in chunk], pa.int32())
            data["_commit_ts"] = pa.array(
                [datetime.fromisoformat(times[r["start_lsn"]]) for r in chunk], pa.timestamp("us")
            )
            for col in columns:
                data[col] = [r["row"].get(col) for r in chunk]
            yield pa.RecordBatch.from_pydict(data)


class FakeCdcDatabase:
    """Writer side of the fake: simulates transactions, idle time and cleanup."""

    def __init__(
        self,
        path: str,
        capture_instances: Iterable[str],
        start_lsn: int = 0x2A_0000_0100_0001,
        keys: dict[str, str | list[str]] | None = None,
        columns: dict[str, str] | None = None,
    ):
        """``keys``: capture instance -> key column, or a list of them. Instances with a key
        also keep the source table's current rows (what a snapshot reads), updated by every
        commit. ``columns``: capture instance -> its captured columns as Spark DDL, what
        ``sp_cdc_get_captured_columns`` would report (the source then infers its schema)."""
        self.path = path
        os.makedirs(os.path.join(path, "changes"), exist_ok=True)
        os.makedirs(os.path.join(path, "tables"), exist_ok=True)
        os.makedirs(os.path.join(path, "ddl"), exist_ok=True)
        if keys:
            _write_json(os.path.join(path, _KEYS), keys)
        self._keys = FakeCdcClient(path)._keys()
        self._next = start_lsn
        existing = _read_jsonl(os.path.join(path, _MAPPING))
        if existing:
            self._next = _lsn.to_int(existing[-1]["start_lsn"]) + 16
        mins_path = os.path.join(path, _MIN)
        if not os.path.exists(mins_path):
            first = _lsn.from_int(self._next)
            names = list(capture_instances)
            _write_json(mins_path, {ci: first for ci in names})
            _write_json(
                os.path.join(path, _INSTANCES),
                {
                    ci: {
                        "table": ci,
                        "created": None,
                        "columns": _parse_columns((columns or {}).get(ci)),
                    }
                    for ci in names
                },
            )

    def _append(self, rel: str, row: dict) -> None:
        with open(os.path.join(self.path, rel), "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")

    def _new_lsn(self) -> str:
        # after any commit another writer made (commit_before_read's, in a Python worker)
        mapping = _read_jsonl(os.path.join(self.path, _MAPPING))
        if mapping:
            self._next = max(self._next, _lsn.to_int(mapping[-1]["start_lsn"]) + 16)
        value = _lsn.from_int(self._next)
        self._next += 16  # leave gaps, like real LSNs
        return value

    @staticmethod
    def _ts(at: datetime | None) -> str:
        at = at or datetime.now(timezone.utc).replace(tzinfo=None)
        return at.isoformat(timespec="milliseconds")

    def _same_table(self, capture_instance: str) -> list[tuple[str, dict]]:
        """Every instance of ``capture_instance``'s table; an undeclared name is its own."""
        try:
            return _resolve(self.path, capture_instance)[1]
        except ValueError:
            return [(capture_instance, {"table": capture_instance, "columns": None})]

    def commit(
        self, capture_instance: str, changes: Sequence[tuple[int, dict]], at: datetime | None = None
    ) -> str:
        """One transaction on the table ``capture_instance`` tracks (an instance of it, or the
        table's name). ``changes`` is a list of (operation, row) with CDC codes 1-4. Every
        instance of the table captures it, with its own columns and command ids."""
        start = self._new_lsn()
        seq = _lsn.to_int(start)
        same = self._same_table(capture_instance)
        for k, (name, meta) in enumerate(same):
            keep = {c.lower() for c, _ in meta["columns"]} if meta.get("columns") else None
            captured = [
                row if keep is None else {c: v for c, v in row.items() if c.lower() in keep}
                for _, row in changes
            ]
            for cmd, (op, _) in enumerate(changes, start=1):
                row, pair = captured[cmd - 1], {3: cmd, 4: cmd - 2}.get(op, -1)  # other image
                # an update of columns this instance does not capture writes no change row
                # (SQL Server 2022, ADR 0023); compared by value, SQL Server by column
                if (
                    keep is not None
                    and 0 <= pair < len(captured)
                    and changes[pair][0] == 7 - op
                    and captured[pair] == row
                ):
                    continue
                # an update's 3 and 4 share __$seqval and __$command_id, as on SQL Server:
                # only __$operation orders them
                n = cmd - 1 if op == 4 and cmd > 1 and changes[cmd - 2][0] == 3 else cmd
                self._append(
                    os.path.join("changes", f"{name}.jsonl"),
                    {
                        "start_lsn": start,
                        "seqval": _lsn.from_int(seq + n),  # the same in every instance
                        # differs per instance, as on SQL Server (ADR 0023); order kept
                        "command_id": n + k,
                        "operation": op,
                        "row": row,
                    },
                )
        self._append(_MAPPING, {"start_lsn": start, "tran_end_time": self._ts(at)})
        table = same[0][1]["table"]
        keys = _key_columns(self._keys.get(table))
        if keys:  # the source table: after-images replace, deletes remove, before-images do nothing
            p = os.path.join(self.path, "tables", f"{table}.json")
            rows = _read_json(p)
            for op, row in changes:
                ident = json.dumps([row[k] for k in keys])
                if op in (2, 4):
                    rows[ident] = row
                elif op == 1:
                    rows.pop(ident, None)
            _write_json(p, rows)
        return start

    def commit_before_read(
        self, capture_instance: str, changes: Sequence[tuple[int, dict]], at: datetime | None = None
    ) -> None:
        """Queue ``commit(capture_instance, changes, at)`` for the next snapshot read to make
        just before it reads the table: after its stamp (a chunk's L), before its SELECT."""
        at_text = at.isoformat() if at else None
        tx = {"capture_instance": capture_instance, "changes": list(changes), "at": at_text}
        self._append(_QUEUED, tx)

    def idle(self, at: datetime | None = None) -> str:
        """A dummy lsn_time_mapping entry with no change rows."""
        start = self._new_lsn()
        self._append(_MAPPING, {"start_lsn": start, "tran_end_time": self._ts(at)})
        return start

    def cleanup(self, capture_instance: str, low_water_mark: str) -> None:
        """Like sys.sp_cdc_cleanup_change_table: move the low watermark, then delete the
        change rows below it, and the cdc.lsn_time_mapping rows below every instance's."""
        p = os.path.join(self.path, _MIN)
        mins = _read_json(p)
        mins[capture_instance] = low_water_mark
        _write_json(p, mins)
        low = min(mins.values())
        for path, lsn_from in (
            (os.path.join(self.path, "changes", f"{capture_instance}.jsonl"), low_water_mark),
            (os.path.join(self.path, _MAPPING), low),
        ):
            kept = [r for r in _read_jsonl(path) if r["start_lsn"] >= lsn_from]
            with open(path, "w", encoding="utf-8") as fh:
                fh.writelines(json.dumps(r) + "\n" for r in kept)

    # -- schema changes (ADR 0023) ---------------------------------------------
    def add_capture_instance(
        self, table_ci_name: str, columns, at: datetime | None = None, name: str | None = None
    ) -> str:
        """Like sys.sp_cdc_enable_table with a new @capture_instance on the table that
        ``table_ci_name`` (an instance of it, or its name) tracks. It starts at the next
        commit's LSN, captures ``columns`` (Spark DDL, a list of names, or (name, type)
        pairs) and is named ``name``, by default ``<table>_v2`` (or the next free number).
        Returns the name."""
        same = _resolve(self.path, table_ci_name)[1]
        if len(same) >= 2:
            raise ValueError("A table can have at most two capture instances")
        table = same[0][1]["table"]
        known = _instances(self.path)
        if name is None:
            n = 2
            while f"{table}_v{n}" in known:
                n += 1
            name = f"{table}_v{n}"
        known[name] = {"table": table, "created": self._ts(at), "columns": _parse_columns(columns)}
        _write_json(os.path.join(self.path, _INSTANCES), known)
        mins = _read_json(os.path.join(self.path, _MIN))
        mins[name] = _lsn.from_int(self._next)  # the next commit is its first
        _write_json(os.path.join(self.path, _MIN), mins)
        return name

    def drop_capture_instance(self, name: str) -> None:
        """Like sys.sp_cdc_disable_table: the instance, its change rows and its DDL history go."""
        name = _resolve(self.path, name)[0] or name
        for rel in (_INSTANCES, _MIN):
            state = _read_json(os.path.join(self.path, rel))
            state.pop(name, None)
            _write_json(os.path.join(self.path, rel), state)
        for rel in (os.path.join("changes", f"{name}.jsonl"), os.path.join("ddl", f"{name}.jsonl")):
            if os.path.exists(os.path.join(self.path, rel)):
                os.remove(os.path.join(self.path, rel))

    def ddl(
        self,
        capture_instance: str,
        column: str | None,
        command: str,
        new_type: str | None = None,
    ) -> str:
        """A DDL statement on the table, recorded by each of its instances at a new LSN
        (after the last commit; the next commit's is larger). ``column``: the column it adds,
        alters or drops, if any. ``new_type``: a captured column's new Spark type (ALTER
        COLUMN), which the instances then report. Returns the LSN."""
        lsn = self._new_lsn()
        known = _instances(self.path)
        for name, _ in self._same_table(capture_instance):
            self._append(
                os.path.join("ddl", f"{name}.jsonl"),
                {"lsn": lsn, "command": command},
            )
            for col in known.get(name, {}).get("columns") or []:
                if new_type and column and col[0].lower() == column.lower():
                    col[1] = new_type.upper()
        _write_json(os.path.join(self.path, _INSTANCES), known)
        if column and re.search(r"\b(ADD|DROP\s+COLUMN)\b", command, re.IGNORECASE):
            # the source table loses the column (a snapshot may no longer select it) or has it
            table = self._same_table(capture_instance)[0][1]["table"]
            dropped = _read_json(os.path.join(self.path, _DROPPED))
            names = set(dropped.get(table, [])) - {column.lower()}
            if re.search(r"\bDROP\s+COLUMN\b", command, re.IGNORECASE):
                names.add(column.lower())
                p = os.path.join(self.path, "tables", f"{table}.json")
                rows = {
                    k: {c: v for c, v in r.items() if c.lower() != column.lower()}
                    for k, r in _read_json(p).items()
                }
                _write_json(p, rows)
            _write_json(os.path.join(self.path, _DROPPED), {**dropped, table: sorted(names)})
        return lsn
