"""SQL Server CDC for PySpark: a DataSource V2 streaming source plus a
completeness ("partition finalization") signal for downstream consumers."""

from .client import DataLossError, make_client
from .pipeline import stream
from .source import HAS_ADMISSION_CONTROL, OPERATIONS, MssqlCdcDataSource

__all__ = [
    "HAS_ADMISSION_CONTROL",
    "OPERATIONS",
    "DataLossError",
    "MssqlCdcDataSource",
    "make_client",
    "register",
    "stream",
]
__version__ = "0.1.0"


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
        spark.dataSource.register(type(base.__name__, (base,), {"default_num_partitions": cores or None}))
