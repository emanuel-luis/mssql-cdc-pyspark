# Releasing

The version is static in `pyproject.toml`. `.github/workflows/release.yml` builds the sdist
and the wheel, checks them (`twine check --strict`, the tag against the version, an import
of each distribution) and publishes with Trusted Publishing, so no token is stored
anywhere:

* pushing a tag `vX.Y.Z` (or `vX.Y.ZrcN`) publishes to PyPI once a reviewer approves the
  `pypi` environment;
* running the workflow by hand (workflow_dispatch) publishes to TestPyPI: the dry run.

The job that publishes attests each file first, signed through Sigstore with the workflow's
identity: a GitHub artifact attestation (`actions/attest`, SLSA build provenance; nothing to
set up in a public repository) and, with the upload, a PEP 740 attestation the index stores
with the file (`gh-action-pypi-publish`'s default). The OIDC token that signs is the
publish job's, never `build`'s, which runs code installed from PyPI. How to check them:
[SECURITY.md](https://github.com/emanuel-luis/mssql-cdc-pyspark/blob/main/SECURITY.md#verifying-a-release).

What a version number may change is set by the compatibility policy
([ADR 0021](decisions/0021-compatibility-policy-for-0x.md), amendment 11 from 1.0.0); every
release is recorded in
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
4. Optional while there is one maintainer: a tag ruleset (Settings, Rules, Rulesets)
   targeting `v*` that restricts creation, update and deletion to maintainers, so nobody
   else can push or move a release tag.
5. The documentation site, published per version by `.github/workflows/docs.yml`
   ([ADR 0024](decisions/0024-documentation-site.md) amendment 1): Settings, Pages,
   Source "GitHub Actions"; and in Settings, Environments, `github-pages`, under
   "Deployment branches and tags", a tag rule `v*` next to the `main` branch rule, or a
   tag's deployment is refused. The first run creates the `gh-pages` branch, which holds
   every version: do not delete it or make it a Pages source.

## Dry run (TestPyPI)

1. Actions, `release`, "Run workflow" on `main`. The `testpypi` job attests and publishes
   what `build` built.
2. In a clean virtual environment:

   ```bash
   python -m venv /tmp/try-mssql-cdc && . /tmp/try-mssql-cdc/bin/activate
   pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ mssql-cdc-pyspark
   pip install "pyspark>=4.2"   # platforms ship their own; a plain venv needs it to import
   python -c "import mssql_cdc; print(mssql_cdc.__version__)"
   ```

3. Check the run's GitHub attestation on the files it attested, as
   [SECURITY.md](https://github.com/emanuel-luis/mssql-cdc-pyspark/blob/main/SECURITY.md#verifying-a-release)
   does, with the branch the run built (`refs/heads/main` here) as `--source-ref`:

   ```bash
   gh run download <run id> -n dist -D /tmp/dry-dist
   for f in /tmp/dry-dist/*; do
     gh attestation verify "$f" --repo emanuel-luis/mssql-cdc-pyspark \
       --signer-workflow emanuel-luis/mssql-cdc-pyspark/.github/workflows/release.yml \
       --source-ref refs/heads/main
   done
   ```

TestPyPI, like PyPI, accepts a file name only once, even after a delete. A second dry run
of the same version fails at the upload (the build checks still run); to repeat the
whole thing, run it from a branch whose version is a pre-release (`uv version 0.1.0rc1`).

## Release

1. Set the version (ADR 0021 amendment 11): `uv version --bump patch` for fixes only,
   `uv version --bump minor` for anything added or deprecated, `uv version --bump major`
   for a break; it updates `uv.lock` too.
2. In `CHANGELOG.md`, turn `## [Unreleased]` into `## [X.Y.Z] - YYYY-MM-DD` with the date
   of the tag (for 0.1.0, replace `unreleased` in `## [0.1.0] - unreleased` with it), keep
   an empty `## [Unreleased]` above it, check the "State compatibility" line, and update
   the links at the bottom. Bump the two install pins to the new version: the `%pip
   install mssql-cdc-pyspark==...` line of `examples/databricks_notebook.py`, the
   `pypi` library and `%pip` line in [`DATABRICKS.md`](DATABRICKS.md) and the pin in
   [`index.md`](index.md).
3. Commit (`chore(release): X.Y.Z`), push to `main` and wait for CI.
4. Tag and push the tag:

   ```bash
   git tag -a vX.Y.Z -m "X.Y.Z"
   git push origin vX.Y.Z
   ```

5. The tag starts the `release` run. Approve its `pypi` deployment ("Review
   deployments") only once the `ci` run of the release commit on `main` (step 3) is green:
   the tag points at that commit, so it gets no `ci` run of its own (a workflow a tag
   triggers counts as publishing, and `ci` restores caches; zizmor's cache-poisoning
   audit). Then install from PyPI in a clean
   environment, as in the dry run without the TestPyPI index, and check both attestations
   of the wheel and the sdist as
   [SECURITY.md](https://github.com/emanuel-luis/mssql-cdc-pyspark/blob/main/SECURITY.md#verifying-a-release)
   says (1.0.0rc1 is the first release with the GitHub one).

   The tag also starts a `docs` run, which publishes the site as version `X.Y.Z` and moves
   `stable` (the site's root) to it. Check that it went green and that the version
   selector lists `X.Y.Z` with `stable`. Runs share one concurrency group and a newer one
   cancels one still waiting, so if a push to `main` right after the tag left it cancelled,
   run `docs` again from the tag (Actions, `docs`, "Run workflow", the tag as the ref).
6. Keep the state the release writes, so later versions are tested against it
   ([ADR 0021](decisions/0021-compatibility-policy-for-0x.md) amendment 3). On Linux (or
   WSL), in a fresh virtual environment holding the wheel from PyPI and the `pyspark` and
   `delta-spark` versions of the tag's `uv.lock`, from the repository root:

   ```bash
   python -m venv /tmp/compat-X.Y.Z && . /tmp/compat-X.Y.Z/bin/activate
   pip install mssql-cdc-pyspark==X.Y.Z pyspark==<uv.lock> delta-spark==<uv.lock>
   python tests/compat/generate.py tests/compat/X.Y.Z
   ```

   On the fake backend it runs a bootstrap, batches, a re-snapshot into generation 1 and
   batches in it, then opens a chunked snapshot for a second stream and reads one wave of
   it, and writes the checkpoints, the bronze, silver, facts and control tables, the second
   stream's bronze and a `manifest.json` (about 500 KiB). `tests/compat/test_compat.py`
   copies every `tests/compat/<version>` and resumes it with the current code: the open
   chunked snapshot finished, no duplicates, offsets carry on, every table migrated, silver
   applied, the next re-snapshot. Commit the
   directory to `main` (`test(compat): X.Y.Z state`). If a "Breaking" change of the release
   stops `generate.py` from running on it, adapt the script; the directories already
   committed keep their own `manifest.json`. A release candidate gets none.

`build` refuses a tag whose commit is not on `main`, and a version without its
`## [X.Y.Z]` heading in `CHANGELOG.md`; the `docs` run refuses the same tags. If `build`
fails on the tag (a version mismatch, a failed check), nothing was published:
delete the tag (`git push --delete origin vX.Y.Z` and `git tag -d vX.Y.Z`), fix, and tag
again; the `docs` run of the new tag publishes over the version the first one published.
If the version is abandoned instead, for the next number, its pages stay in the selector
(and in `stable`, for a final one): remove them as below. For a final version, point
`stable` back at the release before it first, before the `mike delete`:
`uv run --group docs mike alias --update-aliases <previous X.Y.Z> stable`.
If `pypi` fails at the attestation, nothing was published either: re-run the failed job,
or, if the workflow needs the fix, do as above. Once PyPI has a version, it cannot be
uploaded again: fix forward with the next patch.

A release candidate follows the same steps with a version like `1.0.0rc1`
(`uv version 1.0.0rc1`, tag `v1.0.0rc1`); pip installs it only with `--pre` or an exact
pin. Its `docs` run publishes version `1.0.0rc1` and leaves `stable` alone.

A published version stays in the selector. To remove one (a release candidate once its
final release is out, say), from an up-to-date clone with the docs group:

```bash
git fetch origin gh-pages:refs/remotes/origin/gh-pages
uv run --group docs mike delete 1.0.0rc1   # commits to the local gh-pages branch
git push origin gh-pages
```

then run `docs` from `main` ("Run workflow") to deploy the branch.
