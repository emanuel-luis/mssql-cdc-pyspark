"""Credentials apart from the connection string (ADR 0034): user and password, a fixed access
token, a token provider called per connection, and no value of theirs in an error message, a
log, the facts, the metrics files or userMetadata. The drivers' connect functions are specced
mocks: no SQL Server here (tests/integration logs in with user and password; an Entra ID token
needs Azure, so its path ends at the driver call)."""

import json
import logging
import os
import pickle
import re
import sys
import traceback
import types
from pathlib import Path
from unittest.mock import create_autospec

import pytest
import token_provider

from mssql_cdc.client import make_client
from mssql_cdc.client._credentials import SQL_COPT_SS_ACCESS_TOKEN, token_struct

CONN = "Server=host,1433;Database=sales;Encrypt=yes"
PASSWORD = "p;a}ss w{o=rd"  # what a connection string would need braces and }} for
TOKEN = "eyJ0eXAi.fixed"
PROVIDER = "token_provider:token"


@pytest.fixture
def connect(monkeypatch):
    """mssql_python.connect, specced: a call it would refuse fails here too."""
    import mssql_python

    spy = create_autospec(mssql_python.connect)
    monkeypatch.setattr(mssql_python, "connect", spy)
    monkeypatch.setattr(token_provider, "calls", 0)
    return spy


def _arrow_odbc_connect(
    connection_string,
    user=None,
    password=None,
    login_timeout_sec=None,
    packet_size=None,
    autocommit=True,
):  # arrow_odbc.connect's signature (10.x): its module needs unixODBC to import
    raise AssertionError("specced, never called")


@pytest.fixture
def arrow_connect(monkeypatch):
    spy = create_autospec(_arrow_odbc_connect)
    monkeypatch.setitem(sys.modules, "arrow_odbc", types.SimpleNamespace(connect=spy))
    monkeypatch.setattr(token_provider, "calls", 0)
    return spy


def test_the_token_struct_is_its_byte_length_then_the_token_in_utf16le():
    assert token_struct("ab") == b"\x04\x00\x00\x00a\x00b\x00"
    long = "x" * 70_000  # a length past 2 bytes: little-endian 32 bits
    assert token_struct(long)[:4] == (140_000).to_bytes(4, "little")


def test_user_and_password_go_to_the_driver_apart_from_the_connection_string(connect):
    options = {"connectionString": CONN, "user": "cdc_reader", "password": PASSWORD}
    make_client(options)
    # the driver escapes them into the string it builds; the options keep neither
    connect.assert_called_once_with(
        CONN, autocommit=True, timeout=30, UID="cdc_reader", PWD=PASSWORD
    )
    assert options["connectionString"] == CONN
    make_client({"connectionString": CONN + ";UID=u", "PASSWORD": PASSWORD})  # either alone
    assert connect.call_args.kwargs == {"autocommit": True, "timeout": 30, "PWD": PASSWORD}


def test_arrow_odbc_takes_user_and_password_as_its_own_parameters(arrow_connect):
    make_client(
        {"connectionString": CONN, "backend": "arrow-odbc", "user": "u", "password": PASSWORD}
    )
    arrow_connect.assert_called_once_with(
        "Driver={ODBC Driver 18 for SQL Server};" + CONN,
        user="u",
        password=PASSWORD,
        login_timeout_sec=30,
    )


def test_an_access_token_goes_as_the_pre_connect_attribute(connect):
    make_client({"connectionString": CONN, "accessToken": TOKEN})
    connect.assert_called_once_with(
        CONN,
        autocommit=True,
        timeout=30,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct(TOKEN)},
    )
    assert SQL_COPT_SS_ACCESS_TOKEN == 1256  # msodbcsql.h's SQL_COPT_SS_ACCESS_TOKEN


def _token_of(call) -> str:
    raw = call.kwargs["attrs_before"][SQL_COPT_SS_ACCESS_TOKEN]
    return raw[4:].decode("utf-16-le")


def test_the_token_provider_is_called_for_every_connection(connect):
    options = {"connectionString": CONN, "tokenProvider": PROVIDER}
    make_client(options)
    make_client(options)
    assert [_token_of(c) for c in connect.call_args_list] == [
        "eyJ0eXAi.token-1",
        "eyJ0eXAi.token-2",
    ]


def test_only_the_providers_name_crosses_to_the_executors(connect):
    from pyspark.sql.types import IntegerType, StructField, StructType

    from mssql_cdc.source import MssqlCdcStreamReader

    options = {"connectionString": CONN, "captureInstance": "dbo_orders", "tokenProvider": PROVIDER}
    reader = MssqlCdcStreamReader(options, StructType([StructField("id", IntegerType())]))
    assert reader.client is reader.client  # the driver's connection, kept: one token
    shipped = pickle.dumps(reader)  # what Spark sends with each task
    assert b"token-1" not in shipped and PROVIDER.encode() in shipped
    assert pickle.loads(shipped).client  # a task's own connection: the provider again
    assert [_token_of(c) for c in connect.call_args_list] == [
        "eyJ0eXAi.token-1",
        "eyJ0eXAi.token-2",
    ]


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (
            {"accessToken": TOKEN, "tokenProvider": PROVIDER},
            "Options 'accessToken' and 'tokenProvider' exclude each other: give one",
        ),
        (
            {"accessToken": TOKEN, "user": "u", "password": PASSWORD},
            (
                "Option 'accessToken' logs in with an access token, which excludes option "
                "'user', option 'password': drop them"
            ),
        ),
        (
            {"tokenProvider": PROVIDER, "connectionString": CONN + ";uid = u;PWD={x;y}"},
            (
                "Option 'tokenProvider' logs in with an access token, which excludes UID in "
                "connectionString, PWD in connectionString: drop them"
            ),
        ),
        (
            {"accessToken": TOKEN, "connectionString": "Authentication=ActiveDirectoryMsi;" + CONN},
            "excludes Authentication in connectionString",
        ),
        (
            {"accessToken": TOKEN, "connectionString": CONN + ";Trusted_Connection=yes"},
            "excludes Trusted_Connection in connectionString",
        ),
        (
            {"accessToken": TOKEN, "backend": "arrow-odbc"},
            "Option 'accessToken' needs backend=mssql-python",
        ),
        (
            {"tokenProvider": PROVIDER, "backend": "arrow-odbc"},
            "Option 'tokenProvider' needs backend=mssql-python",
        ),
        (
            {"user": "u", "connectionString": CONN + ";UID=v"},
            "Option 'user' and UID in connectionString both name the login: give it once",
        ),
        (
            {"password": PASSWORD, "connectionString": CONN + ";PWD={a;b}}c}"},
            "Option 'password' and PWD in connectionString both give its password",
        ),
    ],
)
def test_combinations_that_leave_the_login_ambiguous_fail_before_connecting(
    connect, arrow_connect, options, message
):
    with pytest.raises(ValueError, match=re.escape(message)) as info:
        make_client({"connectionString": CONN, **options})
    assert not connect.called and not arrow_connect.called
    assert token_provider.calls == 0  # refused before the provider ran
    for value in (TOKEN, PASSWORD, "eyJ0eXAi"):
        assert value not in str(info.value)


def test_a_braced_value_is_no_keyword(connect):
    """``;UID=`` inside a braced value is text, not a second login."""
    make_client({"connectionString": "Server={h;UID=x};Database=d", "user": "u"})
    assert connect.call_args.kwargs["UID"] == "u"


@pytest.mark.parametrize(
    ("provider", "error", "message"),
    [
        ("token provider", ValueError, "tokenProvider 'token provider' names no function"),
        ("token_provider:nowhere", ValueError, "tokenProvider 'token_provider:nowhere' names no"),
        ("no_such_module:token", ValueError, "('package.module:function', installed on every"),
        ("token_provider:not_a_function", TypeError, "is not a function"),
        (
            "token_provider:failing",
            RuntimeError,
            (
                "tokenProvider 'token_provider:failing' failed: OSError: no route to "
                "login.microsoftonline.com"
            ),
        ),
        (
            "token_provider:access_token",
            TypeError,
            (
                "tokenProvider 'token_provider:access_token' returned a SimpleNamespace, not a "
                "token string (with azure-identity: credential.get_token(scope).token)"
            ),
        ),
    ],
)
def test_a_provider_that_gives_no_token_says_which_and_why(connect, provider, error, message):
    with pytest.raises(error, match=re.escape(message)) as info:
        make_client({"connectionString": CONN, "tokenProvider": provider})
    assert "eyJ0eXAi" not in str(info.value)  # neither what it returned nor what it holds
    assert not connect.called


def test_a_driver_error_holding_a_credential_is_raised_with_it_masked(connect, caplog):
    import mssql_python

    quoted = "{" + PASSWORD.replace("}", "}}") + "}"  # as a connection string has it
    connect.side_effect = mssql_python.OperationalError(
        driver_error="Login failed for user 'cdc_reader'.",
        ddbc_error=f"[ODBC Driver 18] UID=cdc_reader;PWD={quoted}",
    )
    caplog.set_level(logging.DEBUG)
    options = {"connectionString": CONN, "user": "cdc_reader", "password": PASSWORD}
    with pytest.raises(ConnectionError) as info:
        make_client(options)
    shown = "".join(traceback.format_exception(info.value))  # what a log or Spark would show
    assert "OperationalError: Driver Error: Login failed for user '***'." in shown
    assert "PWD={***}" in shown
    assert PASSWORD not in shown and "cdc_reader" not in shown
    assert info.value.__cause__ is None and info.value.__suppress_context__
    assert PASSWORD not in caplog.text and "cdc_reader" not in caplog.text

    connect.side_effect = mssql_python.OperationalError(
        driver_error="Timeout expired", ddbc_error=f"the access token {TOKEN} expired"
    )
    with pytest.raises(ConnectionError, match=r"the access token \*\*\* expired"):
        make_client({"connectionString": CONN, "accessToken": TOKEN})


def test_a_driver_error_without_one_is_raised_unchanged(connect):
    """Its type tells a transient error, which the driver's reader retries (ADR 0029)."""
    import mssql_python

    error = mssql_python.OperationalError(
        driver_error="Communication link failure", ddbc_error="Usage of the connection failed"
    )
    connect.side_effect = error
    with pytest.raises(mssql_python.OperationalError) as info:  # 'sa' only as a whole word
        make_client({"connectionString": CONN, "user": "sa", "password": "pw-1"})
    assert info.value is error


@pytest.mark.parametrize("option", ["password", "accessToken", "tokenProvider"])
def test_spark_redacts_the_secret_options_in_a_plan(spark, workdir, capsys, option):
    """Spark's default spark.redaction.regex covers the keys 'password' and 'accessToken'
    (and 'tokenProvider'), where it never covers 'connectionString' and its PWD=
    (docs/reference/options.md)."""
    from mssql_cdc.fake import FakeCdcDatabase

    FakeCdcDatabase(os.path.join(workdir, "src"), ["dbo_orders"])
    df = (
        spark.readStream.format("mssql_cdc")
        .options(backend="fake", fakePath=os.path.join(workdir, "src"))
        .options(captureInstance="dbo_orders", columns="id INT", user="cdc_reader")
        .option("connectionString", "Server=h;PWD=shown-value-0")
        .option(option, "hidden-value-1")
        .load()
    )
    df.explain(extended=True)
    plan = capsys.readouterr().out
    assert "cdc_reader" in plan and "PWD=shown-value-0" in plan  # the plan shows options
    assert "hidden-value-1" not in plan


def test_no_credential_reaches_the_facts_metrics_or_commit_metadata(delta_spark, workdir, caplog):
    from mssql_cdc import stream
    from mssql_cdc.fake import FakeCdcDatabase

    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), ["dbo_orders"])
    db.commit("dbo_orders", [(2, {"id": 1})])
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))
    options = {
        "backend": "fake",  # it ignores credentials: what is checked is where options go
        "fakePath": os.path.join(workdir, "src"),
        "captureInstance": "dbo_orders",
        "columns": "id INT",
        "user": "cdc_reader",
        "password": "hidden-value-2",
    }
    caplog.set_level(logging.DEBUG, logger="mssql_cdc")  # py4j's DEBUG log has every call
    q = stream(spark, options).to_delta(
        target, "cred-v1", ckpt, facts, trigger={"availableNow": True}
    )
    q.awaitTermination()
    rows = spark.read.format("delta").load(facts).collect()
    assert rows and all(r["detail"] is None for r in rows)  # known options: no warning
    history = spark.sql(f"DESCRIBE HISTORY delta.`{target}`").select("userMetadata").collect()
    written = [json.dumps([r.asDict() for r in rows], default=str), str(history)]
    written += [json.dumps(q.recentProgress, default=str), caplog.text]
    # the checkpoint, which holds the metrics directory
    written += [p.read_text("utf-8", "replace") for p in Path(ckpt).rglob("*") if p.is_file()]
    assert not [w for w in written if "hidden-value-2" in w or "cdc_reader" in w]
