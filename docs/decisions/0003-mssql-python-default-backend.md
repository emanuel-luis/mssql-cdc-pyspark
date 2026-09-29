# 0003: `mssql-python` as default driver, `arrow-odbc` as fallback

**Status:** accepted. Survey in `docs/CONNECTORS.md`.  
**Date:** 2026-09-28T16:13:29-03:00 (recorded when the repository was first committed; decided before)  
**Amended:** 2026-09-28T21:44:31-03:00, the `(max)` caveat measured (lab t8)

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
