# 0034: Credentials outside the connection string

**Status:** accepted  
**Date:** 2026-10-10T17:59:29-03:00

## Context
The connection string was the only way to log in: `UID` and `PWD` inside it, a password with
`;` or `}` quoted by the caller in braces with every `}` doubled (the Databricks example
carried a helper for it). Spark shows a streaming query's options in its plan, and its
default redaction (`spark.redaction.regex`, `(?i)secret|password|token|access[.]?key`) matches
neither the key `connectionString` nor `PWD=`: the analyzed plan shows the password whole
(`tests/test_credentials.py`). The way out was a session setting the operator had to know.

Entra ID logins had the driver's own `Authentication=` methods (a managed identity, a service
principal, `DefaultAzureCredential`). They cover the identities the driver can reach by
itself, not a token obtained some other way: a federated workload identity, a platform's
token broker, a sovereign cloud (`mssql-python`'s `token_provider=` fixes the scope to the
commercial cloud). ODBC Driver 18 takes a token as the pre-connect attribute
`SQL_COPT_SS_ACCESS_TOKEN` (1256): its length in bytes as a little-endian 32-bit integer, then
the token in UTF-16-LE. A token expires; a stream runs for days and every executor task opens
a connection of its own. Options are strings, pickled with the reader to the executors, and
nothing live may cross (invariant 5).

## Decision
* Options `user` and `password`, passed to the driver as arguments of their own:
  `mssql_python.connect(..., UID=user, PWD=password)`, whose keyword arguments it escapes into
  the connection string it builds, and `arrow_odbc.connect(..., user=, password=)`, which does
  the same. The library never writes them into the connection string or back into the
  options.
* `accessToken`: an Entra ID access token, which `mssql-python` gets as
  `attrs_before={1256: <struct>}`.
* `tokenProvider`: `package.module:function`, resolved by `pkgutil.resolve_name` and called
  with no arguments in `make_client`, which runs for every connection: on the driver when the
  reader first needs one and again after a retry reconnects (ADR 0029), in every executor task.
  It returns the token as a string. Only the name crosses to the executors, in the options;
  no token does. No scope argument: the function knows its cloud and scope, and
  `azure-identity` stays the user's dependency, never the library's. Not `mssql-python`'s
  `token_provider=`: it takes a credential object, which options cannot carry and which each
  node would have to build anyway, which is what the named function does.
* One source per value, checked before anything connects and before the provider runs:
  `accessToken` or `tokenProvider`, not both; a token excludes `user`, `password`, and `UID`,
  `PWD`, `Authentication` or `Trusted_Connection` in the connection string (ODBC Driver 18
  refuses a token next to them, and `mssql-python` acquires its own token for
  `Authentication=`); `user` excludes `UID` in the string and `password` excludes `PWD`. A
  precedence rule would differ by driver (`mssql-python`'s keyword arguments override the
  string, arrow-odbc appends to it). The string's keywords are read with braced values
  honoured. Each `ValueError` names options, never a value.
* `backend=arrow-odbc` refuses a token: `arrow_odbc.connect` (10.x) sets no attribute before
  it connects. Its users keep the driver's `Authentication=` methods.
* Masking. A message of the library names options, never their values. A driver error raised
  while connecting whose message, or one down its chain, holds the value of `user`,
  `password` or the token (SQL Server's "Login failed for user '...'" names the user) is
  raised again as a `ConnectionError` with each value replaced by `***`, as given and as a
  connection string quotes it (`}` doubled), the user only as a whole word, `from None` so the
  original is not chained. Any other error passes unchanged,
  so ADR 0029's test of a transient error still sees the driver's type. The facts, the
  metrics files and `userMetadata` never held options; a test runs a stream with these
  options and finds neither value there, in its progress or in the library's log.
* Spark's default redaction covers the keys `password`, `accessToken` and `tokenProvider`.
  `user` it shows, as any option: a name, not a secret. Error messages mask it all the same,
  since they get pasted into tickets and chats.

## Consequences
* A password needs no quoting, and Spark hides it by default where it shows options. The
  `connectionString` option works as before, `UID` and `PWD` included.
* A stream can log in with a token it renews by itself: each connection calls the provider.
  The driver keeps its connection for the query's life (ADR 0029) and calls the provider again
  only when it reconnects; every executor task calls it once. A provider should cache its
  token (`azure-identity`'s credentials do, per Python process) rather than ask Entra ID
  each time.
* The provider's module must be importable on every node, installed with the job's
  libraries. Importing a module the options name runs code, which whoever sets the options
  runs anyway: they write the job.
* No Entra ID in the tests: CI has no Azure. `tests/test_credentials.py` checks the token
  struct's bytes and the call path to `mssql_python.connect` with a specced mock, the
  provider called per connection on the driver and after the reader is pickled, and the
  masking; `tests/integration` logs in with `user` and `password` (a password with `;`, `}`,
  `{`, `=` and a space) with each backend.
* A refused login whose message names the user is a `ConnectionError` now, not the driver's
  `OperationalError`. A refused login was never transient, so the retries of ADR 0029 do not
  change.
