"""Helpers every part of the pipeline uses: option lookup, facts timestamps, a table's
capture instances, the retention test, a stream's sink app_ids, and ``_StreamBase``: the
session and options each part of ``CdcStream`` reads."""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from contextlib import closing
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .. import _metricsfs

if TYPE_CHECKING:
    from ..client import CdcClient
    from ..types import SparkSessionLike

# the name of the module this package replaced: callers filter its records by it
_log = logging.getLogger("mssql_cdc.pipeline")
_URI = _metricsfs.URI  # a scheme; a Windows drive has one letter


def _opt(options: Mapping[str, Any], key: str) -> Any:
    return next((v for k, v in options.items() if k.lower() == key.lower()), None)


def _ts(iso: str | None) -> datetime | None:
    """A commit time (UTC ISO-8601, as in offsets) for a TIMESTAMP_NTZ facts column."""
    return datetime.fromisoformat(iso) if iso else None


def _iso(ts: datetime | None) -> str:
    """A TIMESTAMP_NTZ facts value as an offset's commit_ts; '' when NULL."""
    return ts.isoformat(timespec="milliseconds") if ts else ""


def _instances(options: Mapping[str, Any], ci: str) -> list[str]:
    """``ci`` and the other capture instances of its source table, lower-cased (SQL Server
    resolves names ignoring case): after a switch the target holds rows of each (ADR 0023)."""
    from ..client import make_client

    with closing(make_client(options)) as client:
        return sorted({ci.lower(), *(i.name.lower() for i in client.capture_instances(ci))})


def _lost(client: CdcClient, ci: str, lsn: str) -> str | None:
    """When CDC no longer holds the changes right after ``lsn``, the ``min_lsn`` of the
    capture instance that should: the one the source reads them from (ADR 0023), the newest
    instance of the table starting at or before them, else the oldest. Once an older instance
    is dropped, what only it held is gone too. None when they are all there."""
    nxt = client.increment_lsn(lsn)
    instances = client.capture_instances(ci)
    name = instances[0].name if instances else ci
    for i in instances[1:]:
        if i.start_lsn and i.start_lsn <= nxt:
            name = i.name
    low = client.min_lsn(name)
    return low if nxt < low else None


def _family(app_id: str) -> re.Pattern[str]:
    """The sink app_ids of ``app_id``'s stream: its generations (ADR 0018)."""
    return re.compile(re.escape(app_id) + r"(\.g\d+)?")


def _version(spark: SparkSessionLike, target: str) -> int:
    from ..tables import version

    return version(spark, target)


class _StreamBase:
    """The session and options ``CdcStream.__init__`` sets, which every part reads."""

    spark: SparkSessionLike
    options: dict[str, Any]

    def _capture_instance(self) -> str:
        ci: str | None = _opt(self.options, "captureInstance")
        if not ci:
            raise ValueError("Option 'captureInstance' is required (e.g. 'dbo_orders')")
        return ci
