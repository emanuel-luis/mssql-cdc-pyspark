"""Type checks of the pluggable seams (ADR 0030), run by mypy (``files`` in pyproject.toml),
never by pytest: a backend of one's own fits without inheriting, both clients are
``CdcClient``s, and the client's LSNs are ``Lsn``s. ``tests/test_client_sql.py`` checks the
same with ``isinstance`` at run time."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

import pyarrow as pa
from typing_extensions import assert_type

from mssql_cdc import Backend, CdcClient, Lsn, make_client
from mssql_cdc.client import ArrowOdbcBackend, MssqlPythonBackend, SqlCdcClient
from mssql_cdc.fake import FakeCdcClient
from mssql_cdc.lsn import ZERO_LSN, from_int, normalize


class OwnBackend:  # no base class
    def batches(self, sql: str, params: Sequence[str], batch_size: int) -> Iterator[pa.RecordBatch]:
        return iter(())

    def scalar(self, sql: str, params: Sequence[str] = ()) -> Any:
        return None

    def close(self) -> None:
        return None


class NoScalar:
    def batches(self, sql: str, params: Sequence[str], batch_size: int) -> Iterator[pa.RecordBatch]:
        return iter(())

    def close(self) -> None:
        return None


class Subclassed(Backend):  # 0.3's form: scalar and close inherited
    def batches(self, sql: str, params: Sequence[str], batch_size: int) -> Iterator[pa.RecordBatch]:
        return iter(())


def seams(path: str) -> None:
    backends: list[Backend] = [OwnBackend(), Subclassed()]
    backends += [MssqlPythonBackend("Server=x"), ArrowOdbcBackend("Server=x")]
    clients: list[CdcClient] = [SqlCdcClient(OwnBackend()), FakeCdcClient(path)]
    clients.append(SqlCdcClient(Subclassed(), "UTC", lock_timeout_ms=5000))
    assert_type(make_client({"backend": "fake", "fakePath": path}), CdcClient)
    SqlCdcClient(NoScalar())  # type: ignore[arg-type]
    SqlCdcClient(object())  # type: ignore[arg-type]
    Backend()  # type: ignore[misc]


def canonical(lsn: Lsn) -> None: ...


def lsns(client: CdcClient, text: str) -> None:
    assert_type(normalize(text), Lsn)
    assert_type(from_int(42), Lsn)
    assert_type(ZERO_LSN, Lsn)
    assert_type(client.max_lsn(), Lsn | None)  # NULL before capture's first write
    assert_type(client.increment_lsn(text), Lsn)  # any str in: an offset's, the facts'
    assert_type(client.nth_commit_after(ZERO_LSN, 1), Lsn | None)
    assert_type(client.split_points("dbo_t", text, text, 4)[0][0], Lsn)
    assert_type(client.source_table("dbo_t").start_lsn, Lsn | None)
    plain: str = client.min_lsn("dbo_t")  # an Lsn is a str
    canonical(client.min_lsn("dbo_t"))
    canonical(plain)  # type: ignore[arg-type]
