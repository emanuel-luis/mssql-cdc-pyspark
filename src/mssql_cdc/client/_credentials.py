"""Credentials given apart from the connection string (ADR 0034).

``user`` and ``password`` go to the driver as arguments of their own, which it escapes; an
Entra ID access token, fixed (``accessToken``) or returned by the function ``tokenProvider``
names, goes as the ``SQL_COPT_SS_ACCESS_TOKEN`` pre-connect attribute. ``make_client`` runs
for every connection, on the driver and in every executor task, so the provider is called
for each: only its name crosses to the executors, in the options, and a long stream gets a
fresh token whenever it opens a connection. No value is logged or put in a message of the
library's; a driver error that holds one is raised again with it masked (``masked``).
"""

from __future__ import annotations

import pkgutil
import re
import struct
from collections.abc import Mapping
from typing import Any, TypedDict

SQL_COPT_SS_ACCESS_TOKEN = 1256  # msodbcsql.h: the access token, set before connecting
# key=value pairs of an ODBC connection string; a {braced} value may hold ; and = (}} is a })
_PAIR = re.compile(r"([^=;]+)=(?:\{(?:[^}]|\}\})*\}|[^;]*)")
# the login keywords an access token replaces: ODBC Driver 18 refuses a token next to them
_LOGIN_KEYS = ("UID", "PWD", "Authentication", "Trusted_Connection")


def token_struct(token: str) -> bytes:
    """The ``SQL_COPT_SS_ACCESS_TOKEN`` value (msodbcsql's ``ACCESSTOKEN``): the token's
    length in bytes as a little-endian 32-bit integer, then the token in UTF-16-LE."""
    data = token.encode("utf-16-le")
    return struct.pack("<I", len(data)) + data


class Login(TypedDict, total=False):
    """A backend's credential arguments: what ``credentials`` returns."""

    user: str
    password: str
    access_token: str


def credentials(opts: Mapping[str, Any], conn: str, backend: str) -> Login:
    """The backend's credential arguments for the options in ``opts`` (keyed in lower case),
    checked before anything connects; ``tokenProvider`` is called here. An error names
    options, never a value."""
    user, password = opts.get("user"), opts.get("password")
    token, provider = opts.get("accesstoken"), opts.get("tokenprovider")
    keys = {k.strip().lower() for k in _PAIR.findall(conn)}
    if token and provider:
        raise ValueError("Options 'accessToken' and 'tokenProvider' exclude each other: give one")
    if token or provider:
        kind = "accessToken" if token else "tokenProvider"
        clash = [f"option {o!r}" for o, v in (("user", user), ("password", password)) if v]
        clash += [f"{k} in connectionString" for k in _LOGIN_KEYS if k.lower() in keys]
        if clash:
            raise ValueError(
                f"Option {kind!r} logs in with an access token, which excludes "
                f"{', '.join(clash)}: drop them"
            )
        if backend != "mssql-python":
            raise ValueError(
                f"Option {kind!r} needs backend=mssql-python: arrow-odbc cannot set the access "
                "token attribute before it connects. With arrow-odbc, name an Entra ID method "
                "of ODBC Driver 18 in connectionString instead (Authentication=ActiveDirectoryMsi"
                " for a managed identity)."
            )
        return {"access_token": str(token) if token else _token(str(provider))}
    login: Login = {}
    for option, key, what in (
        ("user", "UID", "name the login"),
        ("password", "PWD", "give its password"),
    ):
        if opts.get(option) and key.lower() in keys:
            raise ValueError(
                f"Option {option!r} and {key} in connectionString both {what}: give it once, "
                "in the option, which keeps it out of the connection string"
            )
    if user:
        login["user"] = str(user)
    if password:
        login["password"] = str(password)
    return login


def _token(name: str) -> str:
    """What the function ``name`` (``package.module:function``) returns, called with no
    arguments: imported here, so on whichever node connects."""
    try:
        function = pkgutil.resolve_name(name)
    except (ValueError, ImportError, AttributeError) as exc:
        raise ValueError(
            f"tokenProvider {name!r} names no function this Python can import "
            f"('package.module:function', installed on every node): {exc}"
        ) from exc
    if not callable(function):
        raise TypeError(f"tokenProvider {name!r} is not a function")
    try:
        token = function()
    except Exception as exc:
        raise RuntimeError(f"tokenProvider {name!r} failed: {type(exc).__name__}: {exc}") from exc
    if not isinstance(token, str) or not token:
        raise TypeError(
            f"tokenProvider {name!r} returned a {type(token).__name__}, not a token string "
            "(with azure-identity: credential.get_token(scope).token)"
        )
    return token


def _mask(text: str, creds: Login) -> str:
    for name, value in creds.items():
        # as given, and as a connection string quotes it ({p}}w} for p}w), longer first
        for shown in sorted({str(value).replace("}", "}}"), str(value)}, key=len, reverse=True):
            found = re.escape(shown)  # a user name only as a whole word: 'sa' in "Usage" is none
            text = re.sub(rf"(?<!\w){found}(?!\w)" if name == "user" else found, "***", text)
    return text


def masked(exc: BaseException, creds: Login) -> Exception | None:
    """A ConnectionError for ``exc`` with the values of ``creds`` masked, when its message or
    one down its chain holds one (a driver echoing what it was given, SQL Server naming the
    login); raise it ``from None``, dropping ``exc``. None when none does: raise ``exc``."""
    texts: list[str] = []
    seen: BaseException | None = exc
    while seen is not None:
        texts.append(f"{type(seen).__name__}: {seen}")
        seen = seen.__cause__ or seen.__context__
    if all(_mask(t, creds) == t for t in texts):
        return None
    return ConnectionError(_mask(texts[0], creds))
