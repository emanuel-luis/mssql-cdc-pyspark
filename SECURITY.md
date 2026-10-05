# Security policy

## Supported versions

Only the latest 0.x release gets fixes, in the next patch or minor release; older releases
get no backports
([ADR 0021](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0021-compatibility-policy-for-0x/)).

## Reporting a vulnerability

Report it privately, through GitHub's private vulnerability reporting: the repository's
Security tab, "Report a vulnerability". Do not open a public issue, which discloses the
problem before a fix exists.

## Scope

In scope:

- SQL injection into the T-SQL the library sends, through its options (capture instance,
  columns, time zone, snapshot keys and chunks) or through metadata read from SQL Server
  (schema, table, column and capture instance names).
- Credential leaks: a connection string or password written by the library to logs, error
  messages, the facts or control tables, or Delta commit metadata.

Out of scope: vulnerabilities in SQL Server, Spark, Delta Lake or the drivers themselves
(report those to their maintainers), and exposure that follows from how the operator
configures them, such as Spark's redaction settings or table access policies.
