# SQL Server drivers with native Arrow output

`read()` runs inside Spark executors and yields `pyarrow.RecordBatch`, so the
driver's Arrow support and install footprint matter. Survey as of September 2026
(versions and behaviour from PyPI, project docs and source code):

| | mssql-python | arrow-odbc | ConnectorX | ADBC (Columnar) | turbodbc |
|---|---|---|---|---|---|
| Streaming Arrow API | `cursor.arrow_batch()` / `arrow_reader()` | `read_arrow_batches` (row + byte caps) | `arrow_stream` (unbounded channel) | `fetch_record_batch` | `fetcharrowbatches` |
| pip-only | yes (bundles ODBC 18; needs libltdl7, libkrb5 system libs) | no: unixODBC + msodbcsql18 | yes | no: `dbc` installer | no: build / conda |
| binary(10) | large_binary | fixed_size_binary(10) | large_binary | binary | unsupported |
| decimal(18,2) | decimal128(18,2) | decimal128(18,2) | decimal128(38,10) | decimal128 | float64 |
| Query parameters | yes | strings only | none | yes | yes |
| License | MIT (+ Microsoft license for the ODBC binaries) | MIT | MIT | closed binary | MIT |

**Default: `mssql-python`** (Arrow fetch since 1.5.0). **Fallback: `arrow-odbc`**
where msodbcsql18 is already installed on workers.

Design choices that make the two backends interchangeable:

* LSNs are bound as hex strings and converted server-side with
  `CONVERT(binary(10), ?, 1)`, and returned as `CONVERT(varchar(22), lsn, 1)`, so no
  backend has to bind or decode binary values.
* Every batch is cast to the Spark schema before it is yielded
  (`large_string` -> `string`, etc.).

Known caveats: mssql-python falls back to row-by-row fetch when a result contains
`(max)` columns; arrow-odbc maps `datetime2(7)` to nanoseconds, which overflows
after year 2262 (sentinel dates like 9999-12-31).
