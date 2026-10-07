"""SQL Server CDC for PySpark: a DataSource V2 streaming source plus a
completeness ("partition finalization") signal for downstream consumers."""

import importlib.metadata
import importlib.util

try:
    from .client import (
        Backend,
        CdcClient,
        DataLossError,
        SchemaChangedError,
        is_data_loss,
        is_schema_changed,
        make_client,
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
    from .source import HAS_ADMISSION_CONTROL, OPERATIONS, MssqlCdcDataSource, SourceOptions
    from .types import (
        ApplyResult,
        BackfillState,
        BackfillStatus,
        Granularity,
        Isolation,
        Offset,
        OnDataLoss,
        ReconcileResult,
        SnapshotMode,
    )
except ModuleNotFoundError as e:
    # "pyspark", or "pyspark.sql" when the parent is blocked; a PySpark that is present but
    # lacks a module (too old) keeps its own error.
    if (e.name or "").partition(".")[0] != "pyspark" or importlib.util.find_spec("pyspark"):
        raise
    raise ImportError(
        "mssql_cdc needs PySpark. Run it on a Spark platform (Databricks, EMR, Dataproc, "
        'Fabric...), which ships its own, or pip install "mssql-cdc-pyspark[spark]" for a '
        "local Spark."
    ) from e

__all__ = [
    "HAS_ADMISSION_CONTROL",
    "OPERATIONS",
    "ApplyResult",
    "Backend",
    "BackfillState",
    "BackfillStatus",
    "BatchDetail",
    "CdcClient",
    "DataLossError",
    "DataSkippedDetail",
    "Granularity",
    "Isolation",
    "Lsn",
    "MssqlCdcDataSource",
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
    "WaveMetadata",
    "apply_changes",
    "await_all",
    "is_data_loss",
    "is_schema_changed",
    "make_client",
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


def register(spark) -> None:
    """Register ``format("mssql_cdc")`` (the stream) and ``format("mssql_cdc_snapshot")``
    (the tracked table's current rows) on a SparkSession.

    ``numPartitions`` defaults to the cores of this session's compute. The data source
    plans in a Python worker that has no session, so the count is taken here and rides
    along as an attribute of a registered subclass (Spark pickles it by value).
    """
    from .source import MssqlCdcSnapshotDataSource
    from .spark import available_cores

    cores = available_cores(spark)
    for base in (MssqlCdcDataSource, MssqlCdcSnapshotDataSource):
        spark.dataSource.register(
            type(base.__name__, (base,), {"default_num_partitions": cores or None})
        )
