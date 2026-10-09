"""Test only: ``connect_server`` (tests/conftest.py) puts this directory on its Python
workers' path. In the server's ``foreachBatch`` worker, where a Connect client's sink runs,
the DataFrame cache API raises while the file ``MSSQL_CDC_TEST_REFUSE_CACHING`` names
exists (``refuse_caching`` creates it), as on Databricks serverless (ADR 0032)."""

import os
import sys

_FLAG = os.environ.get("MSSQL_CDC_TEST_REFUSE_CACHING", "")


def _refusing(name, original):
    def refused(self, *args, **kwargs):
        if os.path.exists(_FLAG):
            raise RuntimeError(f"[NOT_SUPPORTED_WITH_SERVERLESS] {name} is not supported")
        return original(self, *args, **kwargs)

    return refused


if _FLAG and any(a.endswith("foreach_batch_worker") for a in getattr(sys, "orig_argv", ())):
    from pyspark.sql.connect.dataframe import DataFrame

    for _name in ("persist", "cache", "unpersist", "localCheckpoint", "checkpoint"):
        setattr(DataFrame, _name, _refusing(_name, getattr(DataFrame, _name)))
