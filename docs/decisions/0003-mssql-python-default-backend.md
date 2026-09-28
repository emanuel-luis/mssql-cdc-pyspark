# 0003: `mssql-python` as default driver, `arrow-odbc` as fallback

**Status:** accepted (2026-09). Survey in `docs/CONNECTORS.md`.

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
* `mssql-python` falls back to row-by-row fetch when a result has `(max)` columns.
* On Linux, `mssql-python` needs `libltdl7`, `libkrb5-3` and `libgssapi-krb5-2`.
