"""Validators of everything inlined into T-SQL (invariant 13)."""

from __future__ import annotations

import re
import unicodedata

_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")
_TZ_RE = re.compile(r"^[A-Za-z0-9 ._+\-/()]+$")
# int, decimal(18,2), varchar(20) COLLATE Greek_CI_AS
_TYPE_RE = re.compile(r"^[a-z0-9]+(\((max|\d+(,\d+)?)\))?( COLLATE [A-Za-z0-9_]+)?$")


def _check_ident(name: str, what: str) -> str:
    if not _IDENT_RE.fullmatch(name):
        raise ValueError(f"Invalid {what}: {name!r}")
    return name


def _check_capture_instance(name: str) -> str:
    """A capture instance name: a sysname of at most 100 characters, which SQL Server derives
    as ``<schema>_<table>`` and so may hold any letter (``dbo_Situação``). It is bound as a
    parameter, or bracket-quoted in ``cdc.[<name>_CT]``: no ``]``, no control character."""
    if (
        not isinstance(name, str)
        or not name
        or len(name) > 100
        or "]" in name
        or any(unicodedata.category(c)[0] == "C" for c in name)
    ):
        raise ValueError(f"Invalid capture instance: {name!r}")
    return name


def _check_column(name: str) -> str:
    if "]" in name or not name:
        raise ValueError(f"Invalid column name: {name!r}")
    return name


def _check_type(sql_type: str) -> str:
    if not isinstance(sql_type, str) or not _TYPE_RE.fullmatch(sql_type):
        raise ValueError(f"Invalid SQL type: {sql_type!r}")
    return sql_type


def _check_tz(name: object) -> str:
    if not isinstance(name, str) or not _TZ_RE.fullmatch(name):
        raise ValueError(f"Invalid sourceTimeZone: {name!r}")
    return name
