import pytest

from mssql_cdc import lsn


def test_normalize_forms():
    assert lsn.normalize("0x0000002a000001f00003") == "0x0000002A000001F00003"
    assert lsn.normalize(bytes.fromhex("0000002A000001F00003")) == "0x0000002A000001F00003"
    assert lsn.normalize("2A") == "0x" + "0" * 18 + "2A"


def test_roundtrip_and_ordering():
    a, b = lsn.from_int(0x2A_0000_01F0_0003), lsn.from_int(0x2A_0000_01F0_0004)
    assert lsn.to_int(a) + 1 == lsn.to_int(b)
    assert a < b  # fixed width: string order == LSN order


def test_invalid():
    with pytest.raises(ValueError):
        lsn.normalize("0xZZ")
    with pytest.raises(ValueError):
        lsn.normalize(b"\x00" * 9)
