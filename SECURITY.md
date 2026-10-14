# Security policy

## Supported versions

Only the latest release gets fixes, in the next patch or minor release; older releases get
no backports
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
- Credential leaks: a connection string, password or access token written by the library to
  logs, error messages, the facts or control tables, metrics files, or Delta commit metadata.

Out of scope: vulnerabilities in SQL Server, Spark, Delta Lake or the drivers themselves
(report those to their maintainers), and exposure that follows from how the operator
configures them, such as Spark's redaction settings or table access policies.

## Verifying a release

The workflow that builds and uploads a release, `.github/workflows/release.yml`, attests
each sdist and wheel it publishes, signed through Sigstore with its identity and bound to
the file's SHA-256:

- a PEP 740 attestation, which PyPI stores with the file (every file since 0.1.0;
  `https://pypi.org/integrity/mssql-cdc-pyspark/<version>/<file>/provenance` serves it);
- from 1.0.0rc1 on, a GitHub artifact attestation (SLSA build provenance), stored with the
  repository.

To check PyPI's (the tool downloads the file from PyPI; the sdist works the same way):

```bash
uvx pypi-attestations verify pypi --repository https://github.com/emanuel-luis/mssql-cdc-pyspark \
  pypi:mssql_cdc_pyspark-X.Y.Z-py3-none-any.whl
```

To check GitHub's, for a file you have, with the GitHub CLI (logged in):

```bash
pip download --no-deps mssql-cdc-pyspark==X.Y.Z -d dist/
gh attestation verify dist/mssql_cdc_pyspark-X.Y.Z-py3-none-any.whl \
  --repo emanuel-luis/mssql-cdc-pyspark \
  --signer-workflow emanuel-luis/mssql-cdc-pyspark/.github/workflows/release.yml \
  --source-ref refs/tags/vX.Y.Z
```

`--source-ref` asks for the build of the release tag: the TestPyPI dry runs attest builds of
a branch too. A pass means the file is what that workflow built from the tagged commit,
unchanged since; it says nothing about what the code does. A file that fails a check that
applies to it did not come from this repository's release: do not install it, and report it
as above. Before 1.0.0rc1 only PyPI's check applies (those releases have no GitHub
attestation); from 1.0.0rc1 on, a file must pass both.
