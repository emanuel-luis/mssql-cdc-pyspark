"""SQL Server CDC for PySpark: a DataSource V2 streaming source plus a
completeness ("partition finalization") signal for downstream consumers."""

from __future__ import annotations

import importlib
import importlib.metadata
import importlib.util
import sys
import warnings
from typing import TYPE_CHECKING

try:
    from .client import (
        Backend,
        CdcClient,
        DataLossError,
        SchemaChangedError,
        is_data_loss,
        is_schema_changed,
    )
    from .fanout import await_all, start_many, stop_all
    from .lsn import Lsn
    from .payloads import (
        BatchDetail,
        DataSkippedDetail,
        SnapshotChunkDetail,
        SnapshotCompletionDetail,
        SnapshotOpenDetail,
        SnapshotPlanDetail,
        WaveMetadata,
    )
    from .pipeline import stream
    from .reconcile import reconcile
    from .silver import apply_changes
    from .source import SourceOptions
    from .types import (
        ApplyResult,
        BackfillState,
        BackfillStatus,
        DataFrameLike,
        Freshness,
        Granularity,
        Isolation,
        Offset,
        OnDataLoss,
        ReconcileResult,
        SnapshotMode,
        SparkSessionLike,
        StreamingQueryLike,
    )
except ModuleNotFoundError as _missing:
    # "pyspark", or "pyspark.sql" when the parent is blocked; a PySpark that is present but
    # lacks a module (too old) keeps its own error.
    if (_missing.name or "").partition(".")[0] != "pyspark" or importlib.util.find_spec("pyspark"):
        raise
    raise ImportError(
        "mssql_cdc needs PySpark. Run it on a Spark platform (Databricks, EMR, Dataproc, "
        'Fabric...), which ships its own, or pip install "mssql-cdc-pyspark[spark]" for a '
        "local Spark."
    ) from _missing

__all__ = [
    "ApplyResult",
    "Backend",
    "BackfillState",
    "BackfillStatus",
    "BatchDetail",
    "CdcClient",
    "DataFrameLike",
    "DataLossError",
    "DataSkippedDetail",
    "Freshness",
    "Granularity",
    "Isolation",
    "Lsn",
    "Offset",
    "OnDataLoss",
    "ReconcileResult",
    "SchemaChangedError",
    "SnapshotChunkDetail",
    "SnapshotCompletionDetail",
    "SnapshotMode",
    "SnapshotOpenDetail",
    "SnapshotPlanDetail",
    "SourceOptions",
    "SparkSessionLike",
    "StreamingQueryLike",
    "WaveMetadata",
    "apply_changes",
    "await_all",
    "is_data_loss",
    "is_schema_changed",
    "reconcile",
    "register",
    "start_many",
    "stop_all",
    "stream",
]
try:
    __version__ = importlib.metadata.version("mssql-cdc-pyspark")
except importlib.metadata.PackageNotFoundError:  # a source tree on sys.path, not installed
    __version__ = "0+unknown"


def register(spark: SparkSessionLike) -> None:
    """Register ``format("mssql_cdc")`` (the stream) and ``format("mssql_cdc_snapshot")``
    (the tracked table's current rows) on a SparkSession.

    ``numPartitions`` defaults to the cores of this session's compute. The data source
    plans in a Python worker that has no session, so the count is taken here and rides
    along as an attribute of a registered subclass (Spark pickles it by value).
    """
    from .source import MssqlCdcDataSource, MssqlCdcSnapshotDataSource
    from .spark import available_cores

    cores = available_cores(spark)
    for base in (MssqlCdcDataSource, MssqlCdcSnapshotDataSource):
        spark.dataSource.register(
            type(base.__name__, (base,), {"default_num_partitions": cores or None})
        )


# Exported before 1.0 though the API reference never listed them, so internal (ADR 0021): until
# 2.0 they still import from here, with a DeprecationWarning; their modules keep them, and may
# change them in any release. Hidden from type checkers, which flag the import instead.
_DEPRECATED = {
    "HAS_ADMISSION_CONTROL": "mssql_cdc.source",
    "OPERATIONS": "mssql_cdc.source",
    "MssqlCdcDataSource": "mssql_cdc.source",
    "make_client": "mssql_cdc.client",
}

if not TYPE_CHECKING:

    def __getattr__(name: str) -> object:
        home = _DEPRECATED.get(name)
        if home is None:
            raise AttributeError(f"module 'mssql_cdc' has no attribute {name!r}")
        # `from mssql_cdc import x` asks twice: importlib's fromlist probe, then the import
        # itself; only the second warns.
        if sys._getframe(1).f_code.co_name != "_handle_fromlist":
            warnings.warn(
                f"mssql_cdc.{name} is internal and leaves the mssql_cdc namespace in 2.0: "
                f"import it from {home}, which may change it in any release",
                DeprecationWarning,
                stacklevel=2,
            )
        return getattr(importlib.import_module(home), name)
