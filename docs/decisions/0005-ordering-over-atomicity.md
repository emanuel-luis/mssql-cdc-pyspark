# 0005: Data first, verdict after, monotonic

**Status:** accepted (2026-09)

## Context
Committing data and the verdict atomically would need multi-table transactions (DBR 18+,
catalog-managed tables, staging tables: temp views are not valid transaction sources)
and would tie the core to one platform.

## Decision
Advance `finalized_until` only after the data commit, and never backwards. Periods
strictly before `finalized_until` are complete.

## Consequences
* If the job dies between the two steps, the verdict lags and consumers wait longer.
  It can never run ahead of the data, which is the failure that matters.
* Works on any Delta runtime.
