# 0003: `mssql-python` as default driver, `arrow-odbc` as fallback

**Status:** accepted. Survey in `docs/CONNECTORS.md`.  
**Date:** 2026-09-28T16:13:29-03:00 (recorded when the repository was first committed; decided before)  
**Amended:** 2026-09-28T21:44:31-03:00, the `(max)` caveat measured (lab t8)  
**Amended:** 2026-09-30T16:14:54-03:00, installed with the package instead of the `[mssql]` extra  
**Amended:** 2026-10-01T17:55:59-03:00, `arrow-odbc` tested in CI; what it took and what still differs (see Amendment 2)  
**Amended:** 2026-10-05T00:15:23-03:00, the default ships closed binaries too: ADBC's rejection corrected (see Amendment 3)

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
