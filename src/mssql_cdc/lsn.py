"""Helpers for SQL Server log sequence numbers (LSNs).

An LSN is a 10-byte binary value. Everywhere in this project it is carried as a
fixed-width, uppercase hex string with a ``0x`` prefix (22 characters), which is
what ``CONVERT(varchar(22), <lsn>, 1)`` returns on SQL Server. Fixed width means
plain string comparison matches LSN order, and the value is JSON-serializable, so
it can live inside a Spark streaming offset.
"""

from __future__ import annotations

import re
from typing import NewType

Lsn = NewType("Lsn", str)
"""An LSN in its canonical form, ``0x`` and 20 uppercase hex digits (ADR 0030), as
``normalize``, ``from_int`` and the client's methods return it. A ``str`` at run time, so it
fits anywhere a ``str`` does; parameters that take an LSN accept any ``str`` in that form."""

LSN_BYTES = 10
_HEX_RE = re.compile(r"^0x[0-9A-F]{20}$")
ZERO_LSN = Lsn("0x" + "0" * 20)


def normalize(value: str | bytes | bytearray) -> Lsn:
    """Return the canonical ``0x`` + 20 uppercase hex form of an LSN."""
    if isinstance(value, (bytes, bytearray)):
        if len(value) != LSN_BYTES:
            raise ValueError(f"LSN must be {LSN_BYTES} bytes, got {len(value)}")
        return Lsn("0x" + bytes(value).hex().upper())
    text = value.strip()
    if text[:2].lower() == "0x":
        text = text[2:]
    text = text.upper().rjust(20, "0")
    out = "0x" + text
    if not _HEX_RE.match(out):
        raise ValueError(f"Invalid LSN: {value!r}")
    return Lsn(out)


def to_int(lsn: str) -> int:
    return int(normalize(lsn), 16)


def from_int(value: int) -> Lsn:
    if value < 0 or value >= 1 << (8 * LSN_BYTES):
        raise ValueError(f"LSN integer out of range: {value}")
    return Lsn("0x" + format(value, "020X"))
