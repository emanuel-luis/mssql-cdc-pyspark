"""Access to SQL Server CDC metadata and change tables.

``CdcClient`` is the interface the Spark data source depends on. ``SqlCdcClient``
implements it with plain T-SQL on top of a small ``Backend`` that knows how to run a
query and return Apache Arrow record batches. Both are ``typing.Protocol``s (ADR 0030): a
class with their methods is one, inheriting or not. Two backends ship:

* ``mssql-python`` (default): Microsoft's official driver. ``pip`` only; it bundles
  the ODBC driver and fetches natively into Arrow via ``cursor.arrow_batch()``.
* ``arrow-odbc``: needs unixODBC and msodbcsql18 on every worker.

Every parameter is text, all arrow-odbc binds, so both backends bind the same way: LSNs
cross the driver boundary as hex strings converted server-side with
``CONVERT(binary(10), ?, 1)``, snapshot key bounds are CAST to their column's type.
Changes are read from the change table ``cdc.<capture_instance>_CT`` (ADR 0009).
A chunked snapshot's chunks are planned here too (``snapshot_plan``, ``plan_chunks``,
ADR 0028), once per snapshot: an integer key from per-slice row counts aggregated on the
server, other keys from seeks on the table's key; no row crosses the network.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# Every name the module this package replaced defined stays importable from here, the
# private ones (tests, the data source) too.
from ._backends import _MAX_BATCH_BYTES as _MAX_BATCH_BYTES
from ._backends import ArrowOdbcBackend, MssqlPythonBackend
from ._backends import _timestamps_in_us as _timestamps_in_us
from ._planning import _MAX_SLICES as _MAX_SLICES
from ._planning import _SLICES as _SLICES
from ._planning import _int_chunks as _int_chunks
from ._planning import _json_key as _json_key
from ._planning import _key_tuple as _key_tuple
from ._planning import last_bound, plan_chunks, snapshot_plan
from ._protocols import Backend, CaptureInstance, CdcClient, DdlChange, SourceTable
from ._sql import _SPARK_TYPES as _SPARK_TYPES
from ._sql import _int_range as _int_range
from ._sql import _isolated as _isolated
from ._sql import _key_select as _key_select
from ._sql import _spark_type as _spark_type
from ._sql import _sql_type as _sql_type
from ._sql import _text as _text
from ._sql_client import _SHIFT_HOURS as _SHIFT_HOURS
from ._sql_client import SqlCdcClient
from ._sql_client import _log as _log
from ._validators import _check_capture_instance as _check_capture_instance
from ._validators import _check_column as _check_column
from ._validators import _check_ident as _check_ident
from ._validators import _check_type as _check_type
from ._validators import _check_tz as _check_tz

__all__ = [
    "ArrowOdbcBackend",
    "Backend",
    "CaptureInstance",
    "CdcClient",
    "DataLossError",
    "DdlChange",
    "MssqlPythonBackend",
    "SchemaChangedError",
    "SourceTable",
    "SqlCdcClient",
    "is_data_loss",
    "is_schema_changed",
    "last_bound",
    "make_client",
    "plan_chunks",
    "snapshot_plan",
]


class DataLossError(RuntimeError):
    """Raised when requested change data was already purged by CDC cleanup."""


class SchemaChangedError(RuntimeError):
    """Raised when the source's schema changed in a way the running query cannot absorb:
    restart it to re-infer the schema (ADR 0023)."""


def _raised(exc: BaseException, error: type[BaseException]) -> bool:
    # Raised in the data source, it reaches the caller as Spark's exception, whose text holds
    # the Python worker's traceback, ending in "mssql_cdc.client.<error>: <message>".
    return isinstance(exc, error) or f"{error.__module__}.{error.__qualname__}:" in str(exc)


def is_data_loss(exc: BaseException) -> bool:
    """Whether ``exc`` is a ``DataLossError``, or the exception Spark stopped a query with
    because of one: what ``awaitTermination()`` raises (``StreamingQueryException``),
    ``query.exception()`` or ``await_all`` returns. Recover with a re-snapshot
    (``to_delta(on_data_loss="resnapshot")``)."""
    return _raised(exc, DataLossError)


def is_schema_changed(exc: BaseException) -> bool:
    """Whether ``exc`` is a ``SchemaChangedError``, or the exception Spark stopped a query
    with because of one (as ``is_data_loss``). Restart the query, or change the target first
    when the message asks to."""
    return _raised(exc, SchemaChangedError)


# --------------------------------------------------------------------------- #
# Factory (called on the driver and inside every executor task)
# --------------------------------------------------------------------------- #
def make_client(options: Mapping[str, Any]) -> CdcClient:
    opts = {k.lower(): v for k, v in dict(options).items()}
    backend = opts.get("backend", "mssql-python").lower()
    tz = opts.get("sourcetimezone", "auto")
    if backend == "fake":
        from ..fake import FakeCdcClient

        return FakeCdcClient(opts["fakepath"])
    conn = opts.get("connectionstring")
    if not conn:
        raise ValueError("Option 'connectionString' is required")
    timeout = _non_negative(opts, "connectTimeout", "seconds")
    timeout = 30 if timeout is None else timeout
    lock_timeout = _non_negative(opts, "lockTimeoutMs", "milliseconds")  # None: wait forever
    if backend == "mssql-python":
        return SqlCdcClient(MssqlPythonBackend(conn, timeout), tz, lock_timeout)
    if backend == "arrow-odbc":
        return SqlCdcClient(ArrowOdbcBackend(conn, timeout), tz, lock_timeout)
    raise ValueError(f"Unknown backend {backend!r} (use mssql-python, arrow-odbc or fake)")


def _non_negative(opts: Mapping[str, Any], name: str, unit: str) -> int | None:
    """Option ``name`` (``opts`` keyed in lower case) as an integer of at least 0; None when
    absent. Anything else is a ValueError naming it."""
    raw = opts.get(name.lower())
    if raw is None:
        return None
    try:
        n = int(str(raw).strip())
    except ValueError:
        n = -1
    if n < 0:
        raise ValueError(f"{name} must be a non-negative integer ({unit}), not {raw!r}")
    return n
