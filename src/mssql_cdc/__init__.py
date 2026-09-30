"""SQL Server CDC for PySpark: a DataSource V2 streaming source plus a
completeness ("partition finalization") signal for downstream consumers."""

import importlib.metadata
import importlib.util

try:
    from .client import DataLossError, make_client
    from .pipeline import stream
    from .silver import apply_changes
    from .source import HAS_ADMISSION_CONTROL, OPERATIONS, MssqlCdcDataSource
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
    "DataLossError",
    "MssqlCdcDataSource",
    "apply_changes",
    "make_client",
    "register",
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
