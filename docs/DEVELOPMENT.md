# Development

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| uv | 0.9+ | manages Python, the venv and `uv.lock` |
| Python | 3.10–3.13 | CI uses 3.11; `uv python install 3.11` if none is present |
| Java | 17 or 21 | required by Spark 4.x; `java` on `PATH` or `JAVA_HOME` set |
| Docker | Engine (Linux) or Desktop (Windows) | SQL Server 2022 image is linux/amd64 |
| Git | any | |

Pick your platform below, then continue with [Lab database](#lab-database).

## Linux (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk-headless                  # Spark
sudo apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2      # mssql-python runtime
curl -LsSf https://astral.sh/uv/install.sh | sh                  # uv
# Docker Engine + compose plugin: https://docs.docker.com/engine/install/

git clone https://github.com/emanuel-luis/mssql-cdc-pyspark.git && cd mssql-cdc-pyspark
uv sync                                  # .venv + dev group, pinned by uv.lock
uv run pytest -q                         # engine tests, no SQL Server needed
```

Run commands with `uv run <cmd>`, or activate the venv once with `. .venv/bin/activate`.
The `Makefile` wraps the common steps:

```bash
make install | up | down | setup | seed | stream | test | lab-sql | lab-spark | lab
```

## Windows

Two ways. WSL2 is the closer match to CI; native works with a few extra variables.

### Windows with WSL2 (recommended)

1. `wsl --install -d Ubuntu` (PowerShell as administrator), then reboot.
2. Install Docker Desktop, keep the WSL2 backend, and enable integration for the
   Ubuntu distro (Settings > Resources > WSL integration).
3. Open an Ubuntu shell and follow the [Linux](#linux-ubuntudebian) steps, skipping
   Docker Engine (Docker Desktop provides `docker` inside WSL).

Clone into the Linux filesystem (`~/...`) rather than `/mnt/c/...`: file access across
the boundary is much slower.

### Windows native (PowerShell)

```powershell
winget install Microsoft.OpenJDK.17      # puts java on PATH; open a new shell afterwards
winget install astral-sh.uv
winget install Docker.DockerDesktop

git clone https://github.com/emanuel-luis/mssql-cdc-pyspark.git; cd mssql-cdc-pyspark
uv sync
```

Spark on Windows needs two more things before any streaming test runs:

1. **Hadoop native helpers.** Download `winutils.exe` and `hadoop.dll` built for
   Hadoop 3.x, put both in `C:\hadoop\bin`, then:

   ```powershell
   $env:HADOOP_HOME = "C:\hadoop"
   $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
   ```

   Without them Spark fails with `HADOOP_HOME and hadoop.home.dir are unset`.
2. **Python workers.** Point Spark at the venv interpreter; otherwise workers start
   whatever `python` is on `PATH` (often the Microsoft Store alias, which fails):

   ```powershell
   $env:PYSPARK_PYTHON = "$PWD\.venv\Scripts\python.exe"
   $env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
   ```

Use `setx` (or System Properties > Environment Variables) to make these permanent.
Then:

```powershell
uv run pytest -q
```

`mssql-python` Windows wheels bundle the ODBC driver; no system libraries needed.
There is no `make`: `scripts\lab.ps1` has the same targets.

```powershell
.\scripts\lab.ps1 install | up | down | setup | seed | stream | test | lab-sql | lab-spark | lab
```

## Lab database

Same on every platform (on Windows native, `scripts\lab.ps1 <target>` instead of `make`):

```bash
cp .env.example .env                 # set MSSQL_SA_PASSWORD
docker compose up -d
docker compose ps                    # wait for "healthy"
uv run python -m lab.workload setup
```

The first Spark session with Delta downloads `io.delta:delta-spark_4.2_2.13` from
Maven Central into `~/.ivy2*`.

## Output locations

`examples/local_pipeline.py` writes Delta tables and checkpoints under
`MSSQL_CDC_WORK` (default: the system temp directory); the lab checks write to the
system temp directory. Keep both on a local disk: network and cloud-synced folders can
break the file renames Spark relies on.

## Tests

```bash
uv run pytest -q                            # everything; Delta tests skip if jars can't resolve
MSSQL_CDC_TEST_DELTA=0 uv run pytest -q     # don't even try Delta
uv run pytest -q -k idle                    # one topic
uv run pytest -q -m sqlserver               # integration: SQL Server 2022 in Docker
```

* `tests/test_source_fake.py` is the main safety net: real Spark streaming,
  simulated SQL Server.
* `tests/test_client_sql.py` pins generated T-SQL.
* `tests/test_delta_sink.py` needs Delta.
* `tests/integration` starts a throwaway SQL Server 2022 with CDC and SQL Server Agent
  through [testcontainers](https://testcontainers-python.readthedocs.io/), with the
  server clock in `America/Sao_Paulo`, and runs the source against it. It needs a
  running Docker daemon (Docker Engine, or Docker Desktop on Windows) and skips without
  one; the first run pulls the SQL Server image. The default `pytest` run leaves
  these tests out.

## Lab

See `LAB.md`. Each check writes `lab/results/<check>-<utc>.json` (gitignored).

## CI

`.github/workflows/ci.yml`:

* `unit`: pytest with Delta on Ubuntu, Java 17, Python 3.11.
* `integration`: `pytest -m sqlserver` (testcontainers on the runner's Docker).
* `lab`: a SQL Server 2022 service container with Agent, then workload, checks t2–t7 and
  `examples/local_pipeline.py`. t1 and the destructive t7 run only on the weekly schedule
  or a manual dispatch ("Run workflow"), and are `continue-on-error`. Results are uploaded
  as the `lab-results` artifact.

## Releasing (later)

Not published to PyPI yet. Install from Git:
`pip install "mssql-cdc-pyspark[mssql] @ git+https://github.com/emanuel-luis/mssql-cdc-pyspark.git"`.
