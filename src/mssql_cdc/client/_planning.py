"""A chunked snapshot's chunks, planned once per snapshot (ADR 0028)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from ._protocols import CdcClient, SourceTable
from ._sql import _text

if TYPE_CHECKING:
    from ..payloads import IntExtent, SnapshotExtent


def _json_key(values: Sequence[Any], types: Sequence[str | None] | None) -> object:
    """A key as JSON, for a chunk bound (``snapshotChunks``, the facts' detail): a scalar for
    one column, a list for several. A value JSON has no type for becomes the text ``CAST``
    reads back (``_text``, typed by ``types``), binary its hex."""
    out: list[object] = []
    for v, t in zip(values, types or [None] * len(values)):
        if not (v is None or isinstance(v, (bool, int, float, str))):
            v = "0x" + bytes(v).hex() if isinstance(v, (bytes, bytearray)) else _text(v, t or "")
        out.append(v)
    return out[0] if len(out) == 1 else out


def _key_tuple(bound: object) -> tuple[Any, ...] | None:
    """A chunk bound from JSON back to the key tuple ``iter_table`` takes; None: open."""
    if bound is None:
        return None
    return tuple(bound) if isinstance(bound, list) else (bound,)


def snapshot_plan(client: CdcClient, capture_instance: str, source: SourceTable) -> SnapshotExtent:
    """What a chunked snapshot's chunks tile, read after its LSN S was recorded (ADR 0028): a
    key a row lacks below MIN or above MAX was inserted after S, so the stream has it.

    One integer key: ``{"kind": "int", "lo": MIN, "hi": MAX, "rows": estimate}``. Other keys:
    ``{"kind": "keyset", "max": MAX}``; ``max`` None (an empty table, no unique index, a key
    type no bound can be bound as): one chunk, the whole table. ``plan_chunks`` cuts it."""
    s, t, keys = source.schema, source.table, source.keys
    top: tuple[Any, ...] | None = None
    if len(keys) == 1:
        lo, hi = client.key_range(s, t, keys[0])
        if lo is not None and all(isinstance(v, int) and not isinstance(v, bool) for v in (lo, hi)):
            return {"kind": "int", "lo": lo, "hi": hi, "rows": client.row_estimate(s, t)}
        top = None if hi is None else (hi,)
    elif keys:
        top = client.key_max(s, t, keys)
    types = client.key_types(capture_instance, keys) if top is not None else [None]
    if top is None or None in types:
        return {"kind": "keyset", "max": None}
    return {"kind": "keyset", "max": _json_key(top, types)}


# ponytail: an integer key is counted in about this many slices per chunk, and one count query
# returns at most _MAX_SLICES of them; a chunk then holds chunk_rows less at most one slice.
_SLICES = 16
_MAX_SLICES = 100_000


def plan_chunks(
    client: CdcClient,
    capture_instance: str,
    source: SourceTable,
    extent: SnapshotExtent,
    chunk_rows: int,
    isolation: str | None = None,
) -> list[list[Any]]:
    """Every chunk ``[lo, hi)`` (JSON bounds) of a chunked snapshot whose ``snapshot_plan`` is
    ``extent``, of at most about ``chunk_rows`` rows each, planned once before its first wave
    (ADR 0028). They tile the key space up to MAX: the first is open below (NULL first), each
    starts where the previous ends, and the last ends just above MAX, at MAX + 1 or at the
    first key after it (``last_bound``; None, open, when there is none or it reads back as
    MAX, a truncated datetime2(7) in MAX's microsecond, and then sought again before each
    wave): the keys after MAX were inserted after S and come from the stream, so a table
    written while it is read does not pile them into the last chunk.

    One integer key: rows counted per slice of a fixed grid (``_int_chunks``), so a sparse
    region or a sentinel far above the ids neither empties nor overfills a chunk. Other keys:
    the key ``chunk_rows`` rows after the previous bound (``key_bound``), walked up front.
    ``isolation``: the counts and seeks read as ``iter_table``'s, as the chunks will be."""
    s, t, keys = source.schema, source.table, source.keys
    if extent["kind"] == "int":
        return _int_chunks(client, s, t, keys[0], extent, chunk_rows, isolation)
    if extent["max"] is None:
        return [[None, None]]
    types = cast("list[str]", client.key_types(capture_instance, keys))  # all set: a MAX
    top = _key_tuple(extent["max"])
    out: list[list[Any]] = []
    lo = None
    while True:
        bound = client.key_bound(s, t, keys, types, _key_tuple(lo), top, chunk_rows, isolation)
        if bound is None:
            break
        out.append([lo, _json_key(bound, types)])
        lo = out[-1][1]
    end = last_bound(client, capture_instance, source, extent["max"], isolation)
    return [*out, [lo, end]]


def last_bound(
    client: CdcClient,
    capture_instance: str,
    source: SourceTable,
    top: object,
    isolation: str | None = None,
) -> object:
    """The end of a keyset plan's last chunk: the first key after MAX (``top``, as JSON) now,
    as JSON; None (open) when there is none. ``isolation``: as ``iter_table``'s."""
    types = cast("list[str]", client.key_types(capture_instance, source.keys))  # all set: a MAX
    after = client.key_bound(
        source.schema, source.table, source.keys, types, _key_tuple(top), None, 1, isolation
    )
    end = None if after is None else _json_key(after, types)
    # A datetime2(7) last key column comes back truncated to the microsecond (key_types): when
    # the key after MAX shares MAX's microsecond, its bound reads back as MAX's, at or below
    # MAX, and would leave the keys of that microsecond in no chunk. Then the last is open.
    return None if end == top else end


def _int_chunks(
    client: CdcClient,
    schema: str,
    table: str,
    key: str,
    extent: IntExtent,
    chunk_rows: int,
    isolation: str | None = None,
) -> list[list[Any]]:
    """An integer key's chunks: its rows counted per slice of a fixed grid in one GROUP BY
    (``key_buckets``, about ``_SLICES`` slices per ``chunk_rows``), then consecutive slices
    packed into chunks of at most ``chunk_rows`` rows, so empty and sparse slices join their
    neighbours. A slice of more rows is counted again on a finer grid over its own keys (two
    seeks for its MIN and MAX, then a GROUP BY of its rows), down to one value a slice. Every
    bound but the last is the start of a slice holding rows: no chunk starts empty."""

    def cut(a: int | None, b: int | None, lo: int, hi: int, rows: int) -> list[tuple[int, int]]:
        # (start, rows) of the slices of [a, b) holding rows, on a grid of width w over lo..hi
        n = min(_MAX_SLICES, max(1, -(-_SLICES * rows // chunk_rows)))
        w = max(1, -(-(hi - lo + 1) // n))
        out: list[tuple[int, int]] = []
        buckets = client.key_buckets(schema, table, key, "int", w, a, b, isolation)
        for i, count, _ in sorted(buckets):
            x = i * w if a is None else max(i * w, a)
            y = (i + 1) * w if b is None else min((i + 1) * w, b)
            big = count > chunk_rows and w > 1
            inner: tuple[Any, ...] = (
                client.key_range(schema, table, key, x, y, isolation) if big else (None,)
            )
            if inner[0] is not None:
                out += cut(x, y, inner[0], inner[1], count)
            else:
                out.append((x, count))
        return out

    # keys above MAX are the stream's; below MIN, the first chunk's (it is open below)
    end = extent["hi"] + 1
    bounds: list[list[Any]] = []
    start: int | None = None
    rows = 0
    for x, count in cut(None, end, extent["lo"], extent["hi"], extent["rows"]):
        if rows and rows + count > chunk_rows:
            bounds.append([start, x])
            start, rows = x, 0
        rows += count
    return [*bounds, [start, end]]
