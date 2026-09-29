"""SQL Server CDC for PySpark: a DataSource V2 streaming source plus a
completeness ("partition finalization") signal for downstream consumers."""

from .client import DataLossError, make_client
from .pipeline import stream
from .source import HAS_ADMISSION_CONTROL, OPERATIONS, MssqlCdcDataSource

__all__ = [
    "DataLossError",
    "HAS_ADMISSION_CONTROL",
    "MssqlCdcDataSource",
    "OPERATIONS",
    "make_client",
    "register",
    "stream",
]
__version__ = "0.1.0"


def register(spark) -> None:
    """Register ``format("mssql_cdc")`` on a SparkSession.

    ``numPartitions`` defaults to the cores of this session's compute. The data source
    plans in a Python worker that has no session, so the count is taken here and rides
    along as an attribute of a registered subclass (Spark pickles it by value).
    """
    from .spark import available_cores

    cores = available_cores(spark)
    source = type(MssqlCdcDataSource.__name__, (MssqlCdcDataSource,),
                  {"default_num_partitions": cores or None})
    spark.dataSource.register(source)
