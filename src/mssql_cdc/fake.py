"""A file-backed stand-in for SQL Server CDC, for tests and local experiments.

It reproduces the parts of CDC the data source relies on:

* a database-wide commit timeline (``cdc.lsn_time_mapping``), including
  "dummy" entries written while the database is idle;
* per-capture-instance change rows ordered by commit LSN;
* a per-instance low watermark (``sys.fn_cdc_get_min_lsn``) that cleanup moves
  before it deletes the change rows below it;
* for instances given a key (one column or several), the source table's current rows,
  which a snapshot reads (tiled like NTILE) and cleanup does not touch.

State lives in plain files so that the Spark driver and every executor process
see the same data (Python workers are separate processes, even locally).
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone

import pyarrow as pa

from . import lsn as _lsn
from .client import CdcClient, SourceTable

_MAPPING = "lsn_time_mapping.jsonl"
_MIN = "min_lsn.json"
_KEYS = "keys.json"


def _read_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _key_columns(key) -> list[str]:
    """``keys`` maps an instance to one column or a list of them."""
    return [key] if isinstance(key, str) else list(key or [])


def _sort_key(values) -> tuple:
    """A key tuple in SQL Server's ORDER BY order: column by column, NULL first."""
    return tuple((v is not None, v) for v in values)


class FakeCdcClient(CdcClient):
    def __init__(self, path: str):
        self.path = path

    # -- state ----------------------------------------------------------------
    def _mapping(self) -> list[dict]:
        return _read_jsonl(os.path.join(self.path, _MAPPING))

    def _changes(self, ci: str) -> list[dict]:
        return _read_jsonl(os.path.join(self.path, "changes", f"{self._name(ci)}.jsonl"))

    def _mins(self) -> dict:
        p = os.path.join(self.path, _MIN)
        if not os.path.exists(p):
            return {}
        with open(p, encoding="utf-8") as fh:
            return json.load(fh)

    def _name(self, ci: str) -> str:
        """The instance as created: the exact name first, else ignoring case (SQL Server's
        default collation), as ``SqlCdcClient.source_table``."""
        mins = self._mins()
        return ci if ci in mins else next((n for n in mins if n.lower() == ci.lower()), ci)

    # -- CdcClient ------------------------------------------------------------
    def max_lsn(self):
        m = self._mapping()
        return m[-1]["start_lsn"] if m else _lsn.ZERO_LSN

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
        # "largest less than or equal" semantics, like sys.fn_cdc_map_lsn_to_time
        best = None
        for row in self._mapping():
            if row["start_lsn"] <= lsn:
                best = row["tran_end_time"]
            else:
                break
        return best

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
            points.append(lsns[idx - 1])
        return points

    def _keys(self) -> dict:
        p = os.path.join(self.path, _KEYS)
        if not os.path.exists(p):
            return {}
        with open(p, encoding="utf-8") as fh:
            return json.load(fh)

    def source_table(self, capture_instance):
        start = self.min_lsn(capture_instance)  # not found -> ValueError, like the real one
        name = self._name(capture_instance)
        return SourceTable("dbo", name, _key_columns(self._keys().get(name)), start)

    def _table(self, table: str) -> list[dict]:
        p = os.path.join(self.path, "tables", f"{table}.json")
        if not os.path.exists(p):
            return []
        with open(p, encoding="utf-8") as fh:
            return list(json.load(fh).values())

    def key_range(self, schema, table, key):
        keys = [r[key] for r in self._table(table) if r.get(key) is not None]
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

    def iter_table(self, schema, table, columns, keys, types, lo, hi, batch_size):
        def inside(row):
            k = _sort_key(row.get(c) for c in keys)
            return (lo is None or k >= _sort_key(lo)) and (hi is None or k < _sort_key(hi))

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
    ):
        """``keys``: capture instance -> key column, or a list of them. Instances with a key
        also keep the source table's current rows (what a snapshot reads), updated by every
        commit."""
        self.path = path
        os.makedirs(os.path.join(path, "changes"), exist_ok=True)
        os.makedirs(os.path.join(path, "tables"), exist_ok=True)
        if keys:
            with open(os.path.join(path, _KEYS), "w", encoding="utf-8") as fh:
                json.dump(keys, fh)
        self._keys = FakeCdcClient(path)._keys()
        self._next = start_lsn
        existing = _read_jsonl(os.path.join(path, _MAPPING))
        if existing:
            self._next = _lsn.to_int(existing[-1]["start_lsn"]) + 16
        mins_path = os.path.join(path, _MIN)
        if not os.path.exists(mins_path):
            first = _lsn.from_int(self._next)
            with open(mins_path, "w", encoding="utf-8") as fh:
                json.dump({ci: first for ci in capture_instances}, fh)

    def _append(self, rel: str, row: dict) -> None:
        with open(os.path.join(self.path, rel), "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")

    def _new_lsn(self) -> str:
        value = _lsn.from_int(self._next)
        self._next += 16  # leave gaps, like real LSNs
        return value

    @staticmethod
    def _ts(at: datetime | None) -> str:
        at = at or datetime.now(timezone.utc).replace(tzinfo=None)
        return at.isoformat(timespec="milliseconds")

    def commit(
        self, capture_instance: str, changes: Sequence[tuple[int, dict]], at: datetime | None = None
    ) -> str:
        """One transaction. ``changes`` is a list of (operation, row) with CDC codes 1-4."""
        start = self._new_lsn()
        seq = _lsn.to_int(start)
        for cmd, (op, row) in enumerate(changes, start=1):
            self._append(
                os.path.join("changes", f"{capture_instance}.jsonl"),
                {
                    "start_lsn": start,
                    "seqval": _lsn.from_int(seq + cmd),
                    "operation": op,
                    "command_id": cmd,
                    "row": row,
                },
            )
        self._append(_MAPPING, {"start_lsn": start, "tran_end_time": self._ts(at)})
        keys = _key_columns(self._keys.get(capture_instance))
        if keys:  # the source table: after-images replace, deletes remove, before-images do nothing
            p = os.path.join(self.path, "tables", f"{capture_instance}.json")
            table = {}
            if os.path.exists(p):
                with open(p, encoding="utf-8") as fh:
                    table = json.load(fh)
            for op, row in changes:
                ident = json.dumps([row[k] for k in keys])
                if op in (2, 4):
                    table[ident] = row
                elif op == 1:
                    table.pop(ident, None)
            with open(p, "w", encoding="utf-8") as fh:
                json.dump(table, fh)
        return start

    def idle(self, at: datetime | None = None) -> str:
        """A dummy lsn_time_mapping entry with no change rows."""
        start = self._new_lsn()
        self._append(_MAPPING, {"start_lsn": start, "tran_end_time": self._ts(at)})
        return start

    def cleanup(self, capture_instance: str, low_water_mark: str) -> None:
        """Like sys.sp_cdc_cleanup_change_table: move the low watermark, then delete the
        change rows below it."""
        p = os.path.join(self.path, _MIN)
        with open(p, encoding="utf-8") as fh:
            mins = json.load(fh)
        mins[capture_instance] = low_water_mark
        with open(p, "w", encoding="utf-8") as fh:
            json.dump(mins, fh)
        path = os.path.join(self.path, "changes", f"{capture_instance}.jsonl")
        kept = [r for r in _read_jsonl(path) if r["start_lsn"] >= low_water_mark]
        with open(path, "w", encoding="utf-8") as fh:
            fh.writelines(json.dumps(r) + "\n" for r in kept)
