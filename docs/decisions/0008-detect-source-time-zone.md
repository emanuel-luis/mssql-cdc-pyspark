# 0008: Detect the server time zone by name

**Status:** accepted  
**Date:** 2026-09-28T17:05:54-03:00  
**Amended:** 2026-09-28T20:19:40-03:00, current-offset fallback for SQL Server 2016–2019
(see Amendment)  
**Amended:** 2026-09-28T21:44:31-03:00, a named zone is converted per range, not per row (see Amendment 2)

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
* One extra scalar query per client (driver, and each task on executors).

## Amendment: fallback to the current offset before 2022
The first source this ran against in production is SQL Server 2016 SP3, where `auto` just
failed. A common convention for such servers, and the one this team already uses, is to
read the server's current UTC offset once per run from `SYSDATETIMEOFFSET()` and apply it
to every commit time. `auto` now does the same when `CURRENT_TIMEZONE_ID()` does not
exist: it reads `DATEPART(TZOFFSET, SYSDATETIMEOFFSET())` and converts with
`DATEADD(minute, -offset, tran_end_time)`; `timezone` reports it as `UTC-03:00`.

* Exact for zones without daylight saving (Brazil since 2019, UTC servers).
* In a zone with daylight saving, commits from the other half of the year come out an
  hour off, and a run that spans a transition applies one offset to both sides. Such
  servers should set `sourceTimeZone` to the zone name, which `AT TIME ZONE` resolves per
  commit (available since SQL Server 2016).
* `tests/integration` forces the fallback against a real server (clock in
  `America/Sao_Paulo`) and checks it converts exactly like the zone name.

## Amendment 2: convert per range, not per row
`AT TIME ZONE` is expensive: converting every change row with it cut one connection's
read from ~28k to ~10.5k rows/s (lab t8, 91 columns). For a named zone the reader now asks
once per partition for the zone's UTC offset at the first and last commit of the range.
When both ends have the same offset and are less than 7 days apart, no daylight-saving
change happened in between (no zone changes twice within a week), so a plain `DATEADD`
with that offset is exact for every row; otherwise it converts row by row as before.
Reads with a named zone now run at the speed of UTC ones (~27k rows/s in t8), and the
integration tests (server clock in `America/Sao_Paulo`) still get UTC commit times.
