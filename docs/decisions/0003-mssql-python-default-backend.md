# 0003: `mssql-python` as default driver, `arrow-odbc` as fallback

**Status:** accepted. Survey in `docs/CONNECTORS.md`.  
**Date:** 2026-09-28T16:13:29-03:00 (recorded when the repository was first committed; decided before)  
**Amended:** 2026-09-28T21:44:31-03:00, the `(max)` caveat measured (lab t8)  
**Amended:** 2026-09-30T16:14:54-03:00, installed with the package instead of the `[mssql]` extra  
**Amended:** 2026-10-01T17:55:59-03:00, `arrow-odbc` tested in CI; what it took and what still differs (see Amendment 2)  
**Amended:** 2026-10-05T00:15:23-03:00, the default ships closed binaries too: ADBC's rejection corrected (see Amendment 3)  
**Amended:** 2026-10-08T14:28:19-03:00, `arrow-odbc`'s concurrent fetch measured (lab t8, see Amendment 4)

## Context
`read()` yields Arrow record batches from executors. The driver should fetch natively into
Arrow with bounded memory, install with pip on managed platforms, preserve binary and
decimal types, and bind parameters.

## Decision
Default to Microsoft's `mssql-python` (`cursor.arrow_batch`, since 1.5.0; bundles ODBC
Driver 18). Support `arrow-odbc` where msodbcsql18 is already installed. Reject
ConnectorX (no parameters, query executed twice, decimal forced to (38,10)), Columnar
ADBC (closed binary, separate installer) and turbodbc (no binary type support).

## Consequences
* LSNs travel as hex strings in both directions (`CONVERT(..., 1)`), so neither backend
  binds or decodes binary values.
* `mssql-python` was said to fall back to row-by-row fetch when a result has `(max)`
  columns. Measured with lab t8 (one connection, 91 columns, 200k rows): two `(max)`
  columns cost about a quarter of the throughput (26k vs 34k rows/s), and the Arrow fetch
  stays 2–3x faster than `fetchall()` either way. A cost, not a cliff.
* On Linux, `mssql-python` needs `libltdl7`, `libkrb5-3` and `libgssapi-krb5-2`.

## Amendment: `mssql-python` is installed with the package
`mssql-python` moves from the `[mssql]` extra into `[project].dependencies`: the default
backend should work after `pip install mssql-cdc-pyspark`, without an extra the user has to
know about. The extra is removed rather than kept as an alias, since nothing has been
released with it. `[spark]` and `[arrow-odbc]` stay.

* The import stays lazy inside `MssqlPythonBackend`, so importing `mssql_cdc` on a node
  without the driver's system libraries still works, and `arrow-odbc` and `fake` users are
  unaffected.
* Costs: every install pulls `azure-identity` (with `msal` and `cryptography`)
  transitively, and `arrow-odbc` users carry a driver they do not use.
* The Linux system libraries (`libltdl7`, `libkrb5-3`, `libgssapi-krb5-2`) are still not
  installed by pip; platforms whose image lacks them need an init script
  (`docs/DATABRICKS.md`).

## Amendment 2: `arrow-odbc` is tested
CI's `integration` job installs unixODBC and `msodbcsql18` from Microsoft's repository, then
runs the integration tests a second time with `MSSQL_CDC_TEST_BACKEND=arrow-odbc`. That run
keeps the tests that take the `backend` fixture: commit times on a non-UTC server, every
mapped type inferred and read back, a stream resumed from its checkpoint, the least-privilege
login, a purged range, split points, the bootstrap, snapshot tiles over composite, string
and other typed keys, and the move to a newer capture instance with its DDL. Both backends
pass the same assertions (14 tests, locally against SQL Server 2022 with msodbcsql18 18.7).
Making them pass took these changes, which are also what still differs:

* The connection string is the same for both: without a `Driver` keyword the backend adds
  `Driver={ODBC Driver 18 for SQL Server}`. `connectTimeout` now applies to both.
* arrow-odbc binds parameters only as text. LSNs already crossed as hex strings
  (invariant 6); snapshot key bounds now do too, with either backend: `CAST(? AS <type>)`
  from ISO 8601 or plain text, binary as hex through `CONVERT(<type>, ?, 1)`. A `datetime`
  bound keeps three fractional digits, which its 1/300 s ticks round back to the same value.
  The seek tests on mssql-python still read only their own rows, without `CONVERT_IMPLICIT`.
* Text travels as UTF-16 both ways. Narrow parameters arrive as varchar in the database's
  code page (a Greek key became `?`), and narrow results depend on the worker's locale.
* A column with no upper bound (the `(max)` types, `text`, `ntext`, `xml`, `image`) needs one
  for arrow-odbc's fetch buffers: 64 KiB per value, in characters or bytes. A longer value
  fails the read, since arrow-odbc refuses to truncate (checked). mssql-python has no such
  limit, so tables with larger values should stay on it.
* `datetime2(7)` is fetched in microseconds, not nanoseconds: truncated as mssql-python
  truncates it, and a 9999-12-31 sentinel no longer overflows.
* `datetimeoffset` arrives as SQL Server's text form, `2026-09-28 13:50:01.1234567 -03:00`,
  which pyarrow does not parse; the reader turns it into its UTC instant before the cast to
  the Spark schema (invariant 7). Asked for a timestamp, arrow-odbc returns the wall-clock
  time without the offset, and asked for one with a time zone it panics.
* `close()` drops the connection: arrow-odbc's `Connection` has no `close()` and disconnects
  when it is freed.
* Not run with arrow-odbc: the heartbeat, facts metrics, the re-snapshot, silver, type
  changes and dropped columns. Throughput is not measured.

## Amendment 3: the default ships closed binaries too
The Decision rejected Columnar ADBC partly as a "closed binary". The default is no
different: `mssql-python` depends on `mssql-python-odbc`, whose wheel holds Microsoft's ODBC
Driver 18 binaries under Microsoft's licenses (its metadata says "Other/Proprietary
License"), so every default install brings in proprietary binaries, while the package
itself is MIT. What still sets ADBC apart is its separate installer: `mssql-python` installs
with pip on every managed platform. The choice stands; the trade-off is now stated:

* The README's License section and the installation page say so, as license scanners in
  enterprise pipelines flag the dependency.
* A license-sensitive install leaves it out (`pip install --no-deps mssql-cdc-pyspark`, then
  `pyarrow` and `arrow-odbc`) and uses `backend=arrow-odbc` with ODBC Driver 18 installed
  separately, where its EULA is accepted explicitly (`ACCEPT_EULA=Y`). The import of
  `mssql-python` is lazy, so nothing else needs it.

## Amendment 4: `arrow-odbc`'s concurrent fetch, measured
`ArrowOdbcBackend` passes `fetch_concurrently=False`, turning off arrow-odbc's default: a
second set of buffers filled on a thread of its own while the caller takes the previous
batch. Lab t8 `--concurrency` timed it on and off, next to mssql-python, over
`dbo.fetch_bench` (400k change rows) split by `split_points` into 1, 2 and 4 ranges, each
read by a process with a connection of its own, as Spark's tasks read them. Median of 5 runs,
rows/s and Arrow MB/s:

| Columns | Partitions | mssql-python | arrow-odbc | arrow-odbc, `fetch_concurrently` |
|---|---|---|---|---|
| 91, two `(max)` | 1 | 30.5k, 54 | 27.0k, 42 | 40.1k, 63 |
| 91, two `(max)` | 2 | 56.3k, 99 | 51.3k, 80 | 65.8k, 103 |
| 91, two `(max)` | 4 | 89.5k, 157 | 97.2k, 152 | 84.4k, 132 |
| 89, no `(max)` | 1 | 49.3k, 71 | 44.9k, 57 | 53.5k, 68 |
| 89, no `(max)` | 2 | 82.0k, 118 | 77.6k, 98 | 92.3k, 116 |
| 89, no `(max)` | 4 | 127.7k, 184 | 120.4k, 152 | 121.7k, 153 |

SQL Server 2022 CU27 from the compose lab; the client in a Debian 12 container on the same
Docker VM (16 CPUs), with unixODBC 2.3.11, msodbcsql18 18.7.1, arrow-odbc 10.4.2,
mssql-python 1.15.0 and pyarrow 25.0.1. mssql-python's batches hold 12–14% more Arrow
bytes for the same rows, so rows/s is the number to compare.

* Partitions scale both backends alike, 2.6x to 3.6x at 4: each is a connection of its
  own, so arrow-odbc needs nothing more for them.
* The concurrent fetch is a clear win at 1 and 2 partitions: +48% and +28% with the `(max)`
  columns, +19% without, and ahead of mssql-python. At 4 it gains nothing, and loses 13%
  with the `(max)` columns, here where client and server share the VM's CPUs. t8 does
  nothing with a batch; the reader also casts it and hands it to Spark, more work for the
  fetch thread to overlap.
* It costs a second set of buffers: up to `max_bytes_per_batch`, 64 MiB, more per task.
* No option: a later release stops turning it off, once a run with client and server on
  separate machines settles the 4-partition result. The code is unchanged for now.
