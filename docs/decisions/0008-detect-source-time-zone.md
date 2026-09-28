# 0008: Detect the server time zone by name

**Status:** accepted (2026-09)

## Context
`cdc.lsn_time_mapping.tran_end_time` is a timezone-less `datetime` in the server clock.
The offset's `commit_ts` and `finalized_until` are UTC, so the source converts it with
`AT TIME ZONE`, which needs the server's Windows time zone name. `sourceTimeZone`
defaulted to `UTC`, silently wrong on any server whose clock is not UTC.

Reading the current offset (`SYSDATETIMEOFFSET()`, `DATEPART(TZOFFSET, ...)`) is not
enough: it is the offset *now*. In a zone with daylight saving, older commits carry the
other offset, and a fixed offset shifts them by an hour.

## Decision
`sourceTimeZone` defaults to `auto`: the client reads `CURRENT_TIMEZONE_ID()` (SQL Server
2022+, Azure SQL Database and Managed Instance), which returns the zone *name*, e.g.
`E. South America Standard Time`. `AT TIME ZONE` then applies the rules in force at each
commit. On older versions the function does not exist and `auto` fails with a message
to set `sourceTimeZone` explicitly. The detected name goes through the same validator as
an explicit one before it is inlined.

## Consequences
* Correct commit times on non-UTC servers with no configuration, on 2022+.
* SQL Server 2016–2019 must set `sourceTimeZone`; there is no silent fallback to a fixed
  offset.
* One extra scalar query per client (driver, and each task on executors).
