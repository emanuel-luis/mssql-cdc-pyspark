"""FakeCdcClient against SQL Server on one scripted history: the CDC semantics the unit tests
run the engine on, checked where they come from (CLAUDE.md, Testing strategy).

Both sides are compared by position in ``cdc.lsn_time_mapping`` (an entry's ordinal), never
by LSN or time value, which differ by construction.
"""

from __future__ import annotations

import time
from bisect import bisect_left
from contextlib import closing

import pytest

from mssql_cdc.client import make_client
from mssql_cdc.fake import FakeCdcClient, FakeCdcDatabase
from mssql_cdc.lsn import normalize

pytestmark = pytest.mark.sqlserver

# One transaction per line: its T-SQL, and the change rows CDC writes for it (operation, row)
SCRIPT = [
    ("INSERT INTO dbo.parity VALUES (1, 'a')", [(2, {"id": 1, "v": "a"})]),
    (
        (
            "BEGIN TRAN; INSERT INTO dbo.parity VALUES (2, 'b'); "
            "INSERT INTO dbo.parity VALUES (3, 'c'); COMMIT"
        ),
        [(2, {"id": 2, "v": "b"}), (2, {"id": 3, "v": "c"})],
    ),
    (
        "UPDATE dbo.parity SET v = 'a2' WHERE id = 1",
        [(3, {"id": 1, "v": "a"}), (4, {"id": 1, "v": "a2"})],
    ),
    ("DELETE FROM dbo.parity WHERE id = 2", [(1, {"id": 2, "v": "b"})]),
    ("INSERT INTO dbo.parity VALUES (4, 'd')", [(2, {"id": 4, "v": "d"})]),
]
ROWS = sum(len(changes) for _, changes in SCRIPT)


def _changes(client, ci: str, entries: list[str]) -> list[tuple]:
    """Every change row from the first entry on: (entry ordinal, rank of its (command_id,
    seqval) in its commit, operation, id, v). An update's 3 and 4 share one rank."""
    out, seen = [], {}
    for batch in client.iter_changes(ci, entries[0], entries[-1], ["id", "v"], True, 1000):
        for r in batch.to_pylist():
            ordinal = entries.index(normalize(r["_start_lsn"]))
            ranks = seen.setdefault(ordinal, [])
            if (r["_command_id"], r["_seqval"]) not in ranks:  # rows come in that order
                ranks.append((r["_command_id"], r["_seqval"]))
            rank = ranks.index((r["_command_id"], r["_seqval"]))
            out.append((ordinal, rank, r["_operation"], r["id"], r["v"]))
    return out


def _observe(client, ci: str, entries: list[str]) -> dict:
    """What the source asks of CDC over the history, by entry ordinal."""

    def at(lsn):  # an LSN that is no entry stays as it is, to show in the diff
        return entries.index(lsn) if lsn in entries else lsn

    first, last = entries[0], entries[-1]
    return {
        "changes": _changes(client, ci, entries),
        "nth_commit_after": [
            at(client.nth_commit_after(client.decrement_lsn(first), k))
            for k in range(1, len(entries) + 1)
        ],
        # the LSN after an entry: past it, at or before the next one, and no commit's
        "increment_lsn": [bisect_left(entries, client.increment_lsn(e)) for e in entries],
        "lsn_to_time": [
            (client.lsn_to_time(e) is not None, client.lsn_to_time(client.increment_lsn(e)))
            for e in entries
        ],
        "split_points": {
            n: sorted((at(b), rows) for b, _, rows in client.split_points(ci, first, last, n))
            for n in (1, 2, 3, ROWS, ROWS + 3)
        },
    }


def test_the_fake_answers_as_sql_server_on_one_scripted_history(sqlserver, tmp_path):
    ci = sqlserver.cdc_table("parity", "id INT NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL")
    for sql, _ in SCRIPT:
        sqlserver.run(sql)
        # a commit time of its own: cleanup lowers a low water mark to the first
        # cdc.lsn_time_mapping entry sharing its tran_end_time (datetime, 1/300 s)
        time.sleep(0.01)
    sqlserver.wait_for_changes(ci, ROWS)
    commits = {
        normalize(lsn)
        for (lsn,) in sqlserver.run(
            f"SELECT DISTINCT CONVERT(varchar(22), __$start_lsn, 1) FROM cdc.[{ci}_CT]"
        )
    }
    assert len(commits) == len(SCRIPT)  # one commit, one entry, per transaction
    entries = [
        normalize(lsn)
        for (lsn,) in sqlserver.run(
            "SELECT CONVERT(varchar(22), start_lsn, 1) FROM cdc.lsn_time_mapping "
            "WHERE start_lsn BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1) "
            "ORDER BY start_lsn",
            (min(commits), max(commits)),
        )
    ]
    # The fake replays the same timeline: an entry that is no commit of this table (an idle
    # one capture wrote meanwhile) is an idle entry there too.
    db, script = FakeCdcDatabase(str(tmp_path), [ci]), iter(SCRIPT)
    fake_entries = [db.commit(ci, next(script)[1]) if e in commits else db.idle() for e in entries]
    fake = FakeCdcClient(str(tmp_path))
    k = entries.index(sorted(commits)[2])  # the update's commit

    with closing(make_client({"connectionString": sqlserver.connection_string})) as client:
        observed = _observe(client, ci, entries)
        assert _observe(fake, ci, fake_entries) == observed
        # its 3 and 4 share one rank on both sides: only the operation orders them
        assert [c for c in observed["changes"] if c[0] == k] == [
            (k, 0, 3, 1, "a"),
            (k, 0, 4, 1, "a2"),
        ]

        # cleanup up to the update's commit: the low watermark moves there, rows below it go
        sqlserver.run(
            "DECLARE @lw binary(10) = CONVERT(binary(10), ?, 1); "
            "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = ?, "
            "@low_water_mark = @lw, @threshold = 5000",
            (entries[k], ci),
        )
        db.cleanup(ci, fake_entries[k])
        assert entries.index(client.min_lsn(ci)) == fake_entries.index(fake.min_lsn(ci)) == k
        kept = [c for c in observed["changes"] if c[0] >= k]
        assert _changes(client, ci, entries) == _changes(fake, ci, fake_entries) == kept
