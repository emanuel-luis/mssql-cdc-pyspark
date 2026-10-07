# 0030: `typing.Protocol` for the pluggable seams

**Status:** accepted  
**Date:** 2026-10-07T09:05:52-03:00

## Context
The library has two seams. `CdcClient` is what the data source needs from SQL Server:
`SqlCdcClient` implements it in T-SQL, the `fake` backend's `FakeCdcClient` from files
([ADR 0006](0006-file-backed-fake-for-engine-tests.md)). `Backend` is what `SqlCdcClient` needs
from a driver: `mssql-python` and `arrow-odbc` ([ADR 0003](0003-mssql-python-default-backend.md)).
Through 0.3 both were ABCs, so a class was one only by inheriting, and a type checker rejected
a backend of one's own that did not. Neither was exported.

LSNs cross every boundary as hex strings (invariant 6) and were typed `str`, the same type as
a capture instance or a commit time.

## Decision
* `CdcClient` and `Backend` are `typing.Protocol`s, `runtime_checkable`, exported from
  `mssql_cdc` and listed in the API reference. A class with their methods is one, inheriting
  or not; `isinstance` checks that it has them, by name (a type checker checks the
  signatures). `SqlCdcClient(backend)` takes any `Backend`; `make_client` returns a
  `CdcClient`.
* The protocols are the 0.3 classes themselves, not new ones beside them. A class that
  subclasses a protocol inherits its method bodies, and Python enforces its abstract methods
  as it did the ABC's (a protocol's metaclass derives from `ABCMeta`): `class X(Backend)`
  that defines `batches` still gets `scalar` and `close`, and one that does not still fails
  to instantiate. So subclassing stays supported, with nothing to deprecate.
* A method with a body is a default for subclasses and still a member a class that does not
  inherit must define. The body is never only `pass`, `...` or a docstring, which a type
  checker reads as abstract in a subclass of a protocol: a no-op default is a bare `return`.
* `Lsn = NewType("Lsn", str)` in `mssql_cdc.lsn`, exported: `normalize`, `from_int` and
  `ZERO_LSN` are `Lsn`s, and so is every LSN the client returns, its `SourceTable`,
  `CaptureInstance` and `DdlChange` included. At run time it is the same `str`.
* Parameters still take any `str`: the LSNs a caller holds come from the offsets' JSON and
  the facts table, where they are plain strings, and requiring `Lsn` would make every such
  call wrap them in a check that does nothing at run time. So `Lsn` marks what is canonical
  on the way out, and a function of one's own can ask for it. `Offset["lsn"]` stays `str`:
  the offset is JSON (invariant 1).
* A member added to a protocol breaks a class that implements it without inheriting: a
  minor's change, listed under "Breaking" ([ADR 0021](0021-compatibility-policy-for-0x.md)).
  A body for it keeps subclasses working.
* The data source still builds its client from its options in every task: the `backend`
  option names a built-in backend only, and a backend of one's own runs under
  `SqlCdcClient` outside a stream.

Considered:

* Protocols beside the ABCs, the ABCs kept one release as bases and removed in 0.5 (the
  roadmap's first plan). The names are taken: `client.Backend` would be either the ABC, and
  `mssql_cdc.Backend` another class of the same name, or the protocol, whose bodiless
  `scalar` a 0.3 subclass would inherit and silently get `None` from. Explicit subclassing
  of a protocol is part of PEP 544, so one class serves both.
* A backend named by an option (`backend=package.module:Class`), so the data source builds a
  user's backend in every task. A new behaviour, not a type: a later release, when someone
  needs it.
* `Lsn` on parameters too. It flags a non-canonical string only where every caller already
  normalizes, and costs a wrapper at each of them.

## Consequences
* A backend of one's own type-checks and passes `isinstance` without a base class
  (`tests/typing_protocols.py`, `tests/test_client_sql.py`).
* Code that subclassed `client.Backend` or `client.CdcClient` runs unchanged. A subclass
  that annotated an LSN return as `str` is now flagged by a type checker, as `Lsn` is
  narrower; at run time nothing changed.
* The protocols' methods are public API: renaming or removing one, or changing its
  signature, is a break listed in the changelog.
