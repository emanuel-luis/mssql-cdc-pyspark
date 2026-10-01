# Releasing

The version is static in `pyproject.toml`. `.github/workflows/release.yml` builds the sdist
and the wheel, checks them (`twine check --strict`, the tag against the version, an import
of each distribution) and publishes with Trusted Publishing, so no token is stored
anywhere:

* pushing a tag `vX.Y.Z` (or `vX.Y.ZrcN`) publishes to PyPI once a reviewer approves the
  `pypi` environment;
* running the workflow by hand (workflow_dispatch) publishes to TestPyPI: the dry run.

What a version number may change is set by the 0.x policy
([ADR 0021](decisions/0021-compatibility-policy-for-0x.md)); every release is recorded in
[`CHANGELOG.md`](https://github.com/emanuel-luis/mssql-cdc-pyspark/blob/main/CHANGELOG.md).

## One-time setup

1. Accounts on [PyPI](https://pypi.org) and [TestPyPI](https://test.pypi.org) (separate
   sites, separate accounts), both with two-factor authentication.
2. A pending publisher on each (Account settings, Publishing, "Add a new pending
   publisher", GitHub): project `mssql-cdc-pyspark`, owner `emanuel-luis`, repository
   `mssql-cdc-pyspark`, workflow `release.yml`, environment `pypi` on PyPI and `testpypi`
   on TestPyPI. The first upload creates the project and turns the pending publisher
   into a normal one.
3. GitHub environments (repository Settings, Environments):
   * `pypi`: a required reviewer (the maintainer; leave "Prevent self-review" off when the
     maintainer is the only reviewer), and deployment branches and tags limited to
     selected ones with the tag rule `v*`;
   * `testpypi`: no reviewer; the manual run comes from a branch, so do not limit it to
     `v*` tags.
4. Optional: a tag ruleset (Settings, Rules, Rulesets) targeting `v*` that restricts
   creation, update and deletion to maintainers, so nobody else can push or move a release
   tag.

## Dry run (TestPyPI)

1. Actions, `release`, "Run workflow" on `main`. The `testpypi` job publishes what `build`
   built.
2. In a clean virtual environment:

   ```bash
   python -m venv /tmp/try-mssql-cdc && . /tmp/try-mssql-cdc/bin/activate
   pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ mssql-cdc-pyspark
   pip install "pyspark>=4.2"   # platforms ship their own; a plain venv needs it to import
   python -c "import mssql_cdc; print(mssql_cdc.__version__)"
   ```

TestPyPI, like PyPI, accepts a file name only once, even after a delete. A second dry run
of the same version fails at the upload (the build checks still run); to repeat the
whole thing, run it from a branch whose version is a pre-release (`uv version 0.1.0rc1`).

## Release

1. Set the version: `uv version --bump patch` for fixes, `uv version --bump minor`
   otherwise (ADR 0021); it updates `uv.lock` too. For 0.1.0 skip this: `pyproject.toml`
   already says `0.1.0`.
2. In `CHANGELOG.md`, turn `## [Unreleased]` into `## [X.Y.Z] - YYYY-MM-DD` with the date
   of the tag (for 0.1.0, replace `unreleased` in `## [0.1.0] - unreleased` with it), keep
   an empty `## [Unreleased]` above it, check the "State compatibility" line, and update
   the links at the bottom.
3. Commit (`chore(release): X.Y.Z`), push to `main` and wait for CI.
4. Tag and push the tag:

   ```bash
   git tag -a vX.Y.Z -m "X.Y.Z"
   git push origin vX.Y.Z
   ```

5. In the `release` run, approve the `pypi` deployment ("Review deployments"). Then
   install from PyPI in a clean environment, as in the dry run without the TestPyPI index.

If `build` fails on the tag (a version mismatch, a failed check), nothing was published:
delete the tag (`git push --delete origin vX.Y.Z` and `git tag -d vX.Y.Z`), fix, and tag
again. Once PyPI has a version, it cannot be uploaded again: fix forward with the next
patch.

A release candidate follows the same steps with a version like `0.2.0rc1`
(`uv version 0.2.0rc1`, tag `v0.2.0rc1`); pip installs it only with `--pre` or an exact
pin.

## After the first release

Switch the install in [`DATABRICKS.md`](DATABRICKS.md) from the requirements file with a
git reference to the `pypi` library type, pinned to the release, for example
`{"pypi": {"package": "mssql-cdc-pyspark==0.1.0"}}`; confirm on a cluster that it installs
(with `mssql-python`) before replacing the old instructions.
