"""SQL Server types to Spark types, and the T-SQL builders the client's queries share."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any

from ._validators import _check_column, _check_type

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


def _sql_type(col: Mapping[str, Any]) -> str | None:
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


def _text(v: object, sql_type: str) -> str:
    """A key bound as text that ``CAST(? AS sql_type)`` reads back as ``v``. ISO 8601 reads the
    same under any DATEFORMAT and language; ``datetime`` takes at most 3 fractional digits."""
    if isinstance(v, datetime):
        ms = sql_type in ("datetime", "smalldatetime")  # 1/300 s ticks: ms is within half one
        return v.isoformat(timespec="milliseconds" if ms else "microseconds")
    if isinstance(v, Decimal):
        return format(v, "f")  # str() writes 0E-10
    return str(v)  # str, int, bool (CAST reads 'True'), float, date, UUID


def _key_select(
    select: str,
    keys: Sequence[str],
    types: Sequence[str] | None,
    lo: Sequence[Any] | None,
    hi: Sequence[Any] | None,
    piece: Callable[[str], str] = lambda sql: sql,
) -> tuple[str, list[str]]:
    """``select`` (no WHERE) over the rows with ``lo <= (keys) < hi`` in the order ORDER BY
    sorts them: column by column, NULL first. A None bound is open. Returns the query and
    its parameters, in the order of the ``?`` marks. ``piece`` wraps each SELECT of the
    UNION ALL (``key_bound`` gives each its own TOP and ORDER BY).

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
    Bound as text with either backend (arrow-odbc binds nothing else, ADR 0003): ``_text``.
    """
    ks = [f"[{_check_column(k)}]" for k in keys]
    n = len(ks)

    def cmp(i: int, op: str, v: Any) -> tuple[str, list[str]]:  # one column against one bound
        k = ks[i]
        if v is None:  # NULL sorts first: nothing is below it, everything is at or above it
            nulls = {"=": f"{k} IS NULL", ">": f"{k} IS NOT NULL", ">=": "1 = 1", "<": "1 = 0"}
            return nulls[op], []
        if types is None:
            x, params = str(int(v)), []
        else:
            t, _, coll = _check_type(types[i]).partition(" COLLATE ")
            # hex, as LSNs are (invariant 6); a chunk bound arrives as that text (_json_key)
            if isinstance(v, (bytes, bytearray)) or t.startswith(("binary(", "varbinary(")):
                hexed = v if isinstance(v, str) else "0x" + bytes(v).hex()
                x, params = f"CONVERT({t}, ?, 1)", [hexed]
            else:
                x, params = f"CAST(? {f'COLLATE {coll} ' if coll else ''}AS {t})", [_text(v, t)]
        return (f"({k} {op} {x} OR {k} IS NULL)" if op == "<" else f"{k} {op} {x}"), params

    def conj(parts: list[tuple[str, list[str]]], wrap: bool = False) -> tuple[str, list[str]]:
        sql = " AND ".join(s for s, _ in parts)
        return (f"({sql})" if wrap and len(parts) > 1 else sql), [v for _, ps in parts for v in ps]

    p = 0  # leading values both bounds share
    while lo is not None and hi is not None and p < n - 1 and lo[p] == hi[p]:
        p += 1
    eq = [cmp(i, "=", lo[i]) for i in range(p)] if lo is not None else []  # p > 0: both bounds

    # keys p..j-1 equal to bound, key j op bound
    def chain(bound: Sequence[Any], j: int, op: str) -> list[tuple[str, list[str]]]:
        return [cmp(i, "=", bound[i]) for i in range(p, j)] + [cmp(j, op, bound[j])]

    def below(bound: Sequence[Any]) -> tuple[str, list[str]]:  # (keys p..) < bound, row by row
        terms = [conj(chain(bound, j, "<"), wrap=True) for j in range(p, n)]
        return "(" + " OR ".join(s for s, _ in terms) + ")", [v for _, t in terms for v in t]

    pieces: list[list[tuple[str, list[str]]]] = []  # key p: = lo's, between lo's and hi's, = hi's
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
    sql: list[str] = []
    params: list[str] = []
    for part in pieces:
        where, ps = conj(part)
        sql.append(piece(f"{select} WHERE {where}" if where else select))
        params += ps
    return " UNION ALL ".join(sql), params


def _int_range(k: str, lo: int | None, hi: int | None, joiner: str) -> str:
    """`` <joiner> k >= lo AND k < hi`` for integer bounds, inlined (None: open); '' for none."""
    parts = [f"{k} {op} {int(v)}" for op, v in ((">=", lo), ("<", hi)) if v is not None]
    return f" {joiner} " + " AND ".join(parts) if parts else ""


def _isolated(sql: str, isolation: str | None, lock_timeout_ms: int | None = None) -> str:
    """``sql`` read under ``isolation``: None (READ COMMITTED) or ``"snapshot"``; never READ
    UNCOMMITTED, whose dirty reads a rollback can leave in a snapshot or a plan. With
    ``lock_timeout_ms`` (the ``lockTimeoutMs`` option), a lock it waits for longer fails it
    with error 1222 instead: Spark retries a task, a hang it cannot (ADR 0029)."""
    if isolation not in (None, "snapshot"):
        raise ValueError(f"isolation must be None or 'snapshot', not {isolation!r}")
    # SNAPSHOT reads the versions committed when the SELECT starts, without the writers' locks;
    # SQL Server refuses it unless the DBA set ALLOW_SNAPSHOT_ISOLATION. Sent without
    # parameters, the batch leaves the session at SNAPSHOT (tests/integration): after an
    # integer plan, backfill's client only reads CDC metadata (max_lsn, commit times) and seeks
    # the first key after MAX under SNAPSHOT anyway. LOCK_TIMEOUT stays the same way, on a
    # client that sends it before every read of the source table.
    if isolation:
        sql = "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; " + sql
    return sql if lock_timeout_ms is None else f"SET LOCK_TIMEOUT {int(lock_timeout_ms)}; " + sql
