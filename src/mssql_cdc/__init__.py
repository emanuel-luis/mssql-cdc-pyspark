"""SQL Server CDC for PySpark: a DataSource V2 streaming source plus a
completeness ("partition finalization") signal for downstream consumers."""

from .client import DataLossError, make_client
from .source import HAS_ADMISSION_CONTROL, OPERATIONS, MssqlCdcDataSource

__all__ = [
    "DataLossError",
    "HAS_ADMISSION_CONTROL",
    "MssqlCdcDataSource",
    "OPERATIONS",
    "make_client",
    "register",
]
__version__ = "0.1.0"


def register(spark) -> None:
    """Register ``format("mssql_cdc")`` on a SparkSession."""
    spark.dataSource.register(MssqlCdcDataSource)
