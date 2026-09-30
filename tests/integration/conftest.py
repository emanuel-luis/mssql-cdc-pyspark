"""A throwaway SQL Server 2022 with CDC (testcontainers), for tests marked ``sqlserver``.

Needs Docker. The default ``pytest`` run deselects these; run them with
``uv run pytest -m sqlserver``. Without a reachable Docker daemon they skip; once the
daemon answers, any container or setup failure fails the run.
"""

from __future__ import annotations

import time

import pytest

IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
PASSWORD = "It_Str0ng_Passw0rd!"  # throwaway container on a random port
DATABASE = "cdc_it"
# A non-UTC server clock on purpose: commit times only come out right if
# sourceTimeZone=auto detects the zone and AT TIME ZONE converts them.
SERVER_TZ = "America/Sao_Paulo"
SERVER_TZ_WINDOWS = "E. South America Standard Time"


class SqlServer:
    timezone_name = SERVER_TZ_WINDOWS

    def __init__(self, host: str, port: int):
        self.base = (
            f"Server={host},{port};UID=sa;PWD={PASSWORD};Encrypt=yes;TrustServerCertificate=yes;"
        )
        self.connection_string = self.base + f"Database={DATABASE};"
        self._conn = None

    def connect(self, database: str = DATABASE, timeout: float = 120):
        import mssql_python

        deadline = time.time() + timeout
        while True:  # the "ready" log line can come before logins are accepted
            try:
                return mssql_python.connect(self.base + f"Database={database};", autocommit=True)
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(2)

    def run(self, sql: str, params=()) -> list:
        if self._conn is None:
            self._conn = self.connect()
        cur = self._conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            return cur.fetchall() if cur.description else []
        finally:
            cur.close()

    def run_enabling_cdc(self, sql: str, params=()) -> list:
        """``run`` for ``sp_cdc_enable_db``/``_table``, rerun when chosen as a deadlock victim.

        Each rolls back whole on error 1205, and at startup the Agent still boots alongside
        the fixture, so SQL Server's own advice holds: rerun the transaction.
        """
        for _ in range(4):
            try:
                return self.run(sql, params)
            except Exception as exc:
                if "deadlock victim" not in str(exc):
                    raise
                time.sleep(1)
        return self.run(sql, params)

    def login(self, name: str, *grants: str) -> str:
        """A login whose database user has only ``grants``; returns its connection string."""
        self.run(f"CREATE LOGIN [{name}] WITH PASSWORD = '{PASSWORD}', CHECK_POLICY = OFF")
        self.run(f"CREATE USER [{name}] FOR LOGIN [{name}]")
        for grant in grants:
            self.run(grant)
        return self.connection_string.replace("UID=sa;", f"UID={name};")

    def cdc_table(self, name: str, columns_ddl: str) -> str:
        """Create ``dbo.<name>`` with CDC on; returns the capture instance."""
        self.run(f"CREATE TABLE dbo.[{name}] ({columns_ddl})")
        self.run_enabling_cdc(
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = ?, "
            "@role_name = NULL, @supports_net_changes = 0",
            (name,),
        )
        return f"dbo_{name}"

    def wait_for_changes(self, capture_instance: str, rows: int, timeout: float = 120) -> None:
        deadline = time.time() + timeout
        while self.run(f"SELECT COUNT(*) FROM cdc.[{capture_instance}_CT]")[0][0] < rows:
            if time.time() > deadline:
                raise TimeoutError(f"CDC capture did not reach {rows} rows in {capture_instance}")
            time.sleep(1)


@pytest.fixture(scope="session")
def sqlserver():
    docker = pytest.importorskip("docker")
    try:
        docker.from_env().ping()
    except Exception as exc:  # noqa: BLE001 - any Docker error means skip
        pytest.skip(f"Docker daemon unavailable: {exc}")

    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    container = (
        DockerContainer(
            IMAGE,
            platform="linux/amd64",
            env={
                "ACCEPT_EULA": "Y",
                "MSSQL_PID": "Developer",
                "MSSQL_AGENT_ENABLED": "true",  # CDC capture runs as an Agent job
                "MSSQL_SA_PASSWORD": PASSWORD,
                "TZ": SERVER_TZ,
            },
        )
        .with_exposed_ports(1433)
        .waiting_for(
            LogMessageWaitStrategy(
                "SQL Server is now ready for client connections"
            ).with_startup_timeout(300)
        )
    )
    with container:
        server = SqlServer(container.get_container_host_ip(), int(container.get_exposed_port(1433)))
        master = server.connect("master")
        cur = master.cursor()
        cur.execute(f"CREATE DATABASE [{DATABASE}]")
        cur.close()
        master.close()
        server.run_enabling_cdc("EXEC sys.sp_cdc_enable_db")
        yield server
