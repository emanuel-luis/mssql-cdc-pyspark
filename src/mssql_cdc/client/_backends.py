"""The two ``Backend``s: mssql-python (the default) and arrow-odbc (ADR 0003)."""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from typing import Any

import pyarrow as pa

from ._protocols import Backend

_MAX_BATCH_BYTES = 64 * 1024 * 1024  # about the most an Arrow batch from either backend holds
_VARIABLE = (
    pa.types.is_string,
    pa.types.is_large_string,
    pa.types.is_binary,
    pa.types.is_large_binary,
)


def _widest_row(batch: pa.RecordBatch) -> int:
    """At least the bytes of ``batch``'s widest row: the longest value of each text or binary
    column, plus every other column's bytes per row."""
    import pyarrow.compute as pc

    width = 0
    for col in batch.columns:
        if any(is_type(col.type) for is_type in _VARIABLE):
            width += pc.max(pc.binary_length(col)).as_py() or 0
        else:
            width += col.nbytes // len(col)  # a batch has rows
    return width


class MssqlPythonBackend(Backend):
    """Microsoft ``mssql-python`` driver with native Arrow fetch (>= 1.5.0)."""

    def __init__(
        self,
        connection_string: str,
        timeout: int = 30,
        max_bytes_per_batch: int = _MAX_BATCH_BYTES,
    ) -> None:
        import mssql_python  # imported lazily: this runs on driver and executors

        self._conn = mssql_python.connect(connection_string, autocommit=True, timeout=timeout)
        self._max_bytes = max_bytes_per_batch

    def batches(self, sql: str, params: Sequence[str], batch_size: int) -> Iterator[pa.RecordBatch]:
        # batch_size is a row count; the bytes are bounded as ArrowOdbcBackend's are, so a
        # table of (max) columns does not build gigabyte batches in a Python worker. Nothing
        # tells a row's size before it is fetched: the first batch is one row, and each next
        # one as many as fit at the widest row of the last (not its mean: one long value among
        # short ones is what the next batch may hold several of), at most twice its rows.
        cur = self._conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            size = 1
            while True:
                batch = cur.arrow_batch(size)
                if batch.num_rows == 0:
                    break
                # ponytail: a run of rows far wider than the batch before still overshoots, by
                # how much wider; a hard bound needs a cap per (max) value, as arrow-odbc's
                # (which fails a longer value)
                fit = self._max_bytes // max(1, _widest_row(batch))
                size = max(1, min(batch_size, 2 * batch.num_rows, fit))
                yield batch
        finally:
            cur.close()

    def scalar(self, sql: str, params: Sequence[str] = ()) -> Any:
        cur = self._conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
            return None if row is None else row[0]
        finally:
            cur.close()

    def close(self) -> None:
        self._conn.close()


def _timestamps_in_us(schema: pa.Schema) -> pa.Schema:
    """arrow-odbc reads ``datetime2(7)`` as nanoseconds, which end in 2262 (a 9999-12-31
    sentinel fails) and which Spark truncates anyway: fetch microseconds, like mssql-python."""
    return pa.schema(
        [f.with_type(pa.timestamp("us")) if pa.types.is_timestamp(f.type) else f for f in schema]
    )


class ArrowOdbcBackend(Backend):
    """``arrow-odbc``. Requires unixODBC + Microsoft ODBC Driver 18 on the worker.

    It binds every parameter as text, which the T-SQL here already does (ADR 0003).
    """

    # ponytail: one bound for every (max) column, in characters or bytes; a longer value fails
    # the read (arrow-odbc refuses to truncate). An option when a table needs more.
    MAX_VALUE_SIZE = 64 * 1024

    def __init__(
        self,
        connection_string: str,
        timeout: int = 30,
        max_bytes_per_batch: int = _MAX_BATCH_BYTES,
    ) -> None:
        import arrow_odbc

        # The same connection string as mssql-python's, which names no driver.
        if not re.search(r"(^|;)\s*driver\s*=", connection_string, re.IGNORECASE):
            connection_string = "Driver={ODBC Driver 18 for SQL Server};" + connection_string
        self._conn = arrow_odbc.connect(connection_string, login_timeout_sec=timeout)
        self._max_bytes = max_bytes_per_batch

    def batches(self, sql: str, params: Sequence[str], batch_size: int) -> Iterator[pa.RecordBatch]:
        from arrow_odbc import TextEncoding

        reader = self._conn.read_arrow_batches(
            sql,
            batch_size=batch_size,
            parameters=list(params),
            max_bytes_per_batch=self._max_bytes,
            max_text_size=self.MAX_VALUE_SIZE,
            max_binary_size=self.MAX_VALUE_SIZE,
            map_schema=_timestamps_in_us,
            fetch_concurrently=False,
            # UTF-16 both ways: parameters bind as nvarchar, as mssql-python's do (narrow ones
            # are varchar in the database's code page), and text does not depend on the locale
            payload_text_encoding=TextEncoding.UTF16,
        )
        for batch in reader:
            if batch.num_rows:
                yield batch

    def close(self) -> None:
        self._conn = None  # arrow_odbc.Connection has no close(): its __del__ disconnects
