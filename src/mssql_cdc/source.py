"""PySpark Python Data Source (DataSource V2) for SQL Server Change Data Capture.

Streaming offsets are commit LSNs. Each micro-batch reads the closed interval
``[fn_cdc_increment_lsn(start), end]`` from the change table ``cdc.<ci>_CT``.
``end`` never exceeds ``sys.fn_cdc_get_max_lsn()``, the last LSN the capture
process has processed, so every batch is a prefix of the source commit history.

The offset also carries ``commit_ts``, the UTC commit time of the end LSN. The end
LSN can be a CDC "dummy" entry with no change rows (written while the database is
idle), so the batch rows alone cannot tell how far capture progressed; the offset
can. That is what the finalization layer uses.

Usage::

    from mssql_cdc import register
    register(spark)
    df = (spark.readStream.format("mssql_cdc")
            .option("connectionString", "Server=...;Database=...;UID=...;PWD=...;Encrypt=yes")
            .option("captureInstance", "dbo_orders")
            .load())  # columns inferred from CDC metadata; "columns" (DDL) overrides

When ``columns`` is omitted, the captured columns and their types come from
``sys.sp_cdc_get_captured_columns`` at ``load()`` time, on the driver: the union of every
capture instance of the table, since a batch may read two (ADR 0023).

Schema changes (ADR 0023). Every planning lists the table's capture instances and the DDL
each recorded inside the batch (``sys.sp_cdc_get_ddl_history``). A captured column whose
type no longer fits the query's fails the batch before anything is read
(``SchemaChangedError``); other DDL is logged and, with ``metricsPath``, left there as an
event file for the sink (``schemaChangePolicy=fail`` fails on any DDL). A newer instance of
the same table takes over at its ``start_lsn`` S: a batch reads the older one below S and
the newer one from S, and a column an instance lacks reads NULL. Offsets do not change.
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import pairwise
from typing import TYPE_CHECKING

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    InputPartition,
)

from .lsn import ZERO_LSN

try:  # Spark 4.2+ (and runtimes that backported SPARK-55304)
    from pyspark.sql.streaming.datasource import (
        ReadAllAvailable,
        ReadMaxRows,
        SupportsTriggerAvailableNow,
    )

    HAS_ADMISSION_CONTROL = True
except ImportError:  # pragma: no cover - older Spark
    HAS_ADMISSION_CONTROL = False

    class SupportsTriggerAvailableNow:  # type: ignore[no-redef]
        pass


if TYPE_CHECKING:
    from .client import CdcClient

METADATA_COLUMNS = [
    ("_capture_instance", "STRING"),
    ("_start_lsn", "STRING"),
    ("_seqval", "STRING"),
    ("_operation", "INT"),
    ("_command_id", "INT"),
    ("_commit_ts", "TIMESTAMP_NTZ"),
]
# 1-4 are SQL Server's __$operation codes; 0 marks a snapshot row (ADR 0016)
OPERATIONS = {0: "snapshot", 1: "delete", 2: "insert", 3: "update_before", 4: "update_after"}
SCHEMA_CHANGE_POLICIES = ("classify", "fail")

_log = logging.getLogger(__name__)


def _opt(options, key: str, default=None):
    lowered = {k.lower(): v for k, v in dict(options).items()}
    return lowered.get(key.lower(), default)


def _truthy(value) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def _positive_int(options, name: str, default=None, allowed="a positive integer", hint="") -> int:
    """Option ``name`` as an integer of at least 1; a ValueError names it and its value."""
    value = _opt(options, name, default)
    try:
        n = int(str(value).strip())
    except ValueError:
        n = 0
    if n < 1:
        raise ValueError(f"{name} must be {allowed}, not {value!r}{hint}")
    return n


# Every option the source reads, in lower case (Spark's option names ignore case); any other
# is warned about, since a misspelt one would silently leave its default in force.
KNOWN_OPTIONS = frozenset(
    {
        "captureinstance",
        "connectionstring",
        "backend",
        "sourcetimezone",
        "connecttimeout",
        "startinglsn",
        "maxcommitsperbatch",
        "numpartitions",
        "failondataloss",
        "includecommandid",
        "columns",
        "arrowbatchsize",
        "metricspath",
        "schemachangepolicy",
        "isolationlevel",
        "snapshotchunks",
        "snapshotkeys",
        "snapshotlsn",
        "fakepath",
    }
)


# -- types across schema changes (ADR 0023) ------------------------------------
_INTS = ["tinyint", "smallint", "int", "bigint"]
_DIGITS = {"tinyint": 3, "smallint": 5, "int": 10, "bigint": 20}
_DECIMAL = re.compile(r"^decimal\((\d+),(\d+)\)$")


def _fits(old: str, new: str) -> bool:
    """Whether every value of Spark type ``old`` fits ``new`` unchanged: the same type, or
    a widening Delta type widening also accepts."""
    old, new = (re.sub(r"\s+", "", t).lower() for t in (old, new))
    if old == new:
        return True
    if old in _INTS and new in _INTS:
        return _INTS.index(old) < _INTS.index(new)
    if (old, new) in (("float", "double"), ("date", "timestamp_ntz")):
        return True
    if old in _INTS and new == "double":
        return old != "bigint"
    n, o = _DECIMAL.match(new), _DECIMAL.match(old)
    if n and old in _INTS:
        return int(n[1]) - int(n[2]) >= _DIGITS[old]
    if n and o:
        return int(n[2]) >= int(o[2]) and int(n[1]) - int(n[2]) >= int(o[1]) - int(o[2])
    return False


def union_columns(instances) -> str:
    """Spark DDL of the columns of every capture instance (``CaptureInstance``, oldest first),
    by name in capture order, older first. A column two instances type differently takes
    the newer's type when it holds the older's values; otherwise ``SchemaChangedError``."""
    from .client import SchemaChangedError

    cols: dict[str, tuple[str, str, str]] = {}  # lower name -> (name, type, instance)
    for inst in instances:
        for name, typ in zip(inst.columns, inst.column_types):
            seen = cols.get(name.lower())
            if seen and not _fits(seen[1], typ):
                raise SchemaChangedError(
                    f"Capture instances {seen[2]!r} and {inst.name!r} of the same table capture "
                    f"{name!r} as {seen[1]} and {typ}, and the newer type does not hold the older's "
                    "values. Pass 'columns' with a type that holds both, or disable the older "
                    f"instance once the stream has read past {inst.name!r}'s start "
                    f"({inst.start_lsn})."
                )
            cols[name.lower()] = (seen[0] if seen else name, typ, inst.name)
    return ", ".join(f"`{n.replace('`', '``')}` {t}" for n, t, _ in cols.values())


def _write_metrics(path: str, name: str, metrics: dict) -> None:
    """One JSON per partition, ``<name>.json``; a task retry overwrites its file. Best
    effort: a metric must never fail a read."""
    import json

    try:
        os.makedirs(path, exist_ok=True)
        name = os.path.join(path, f"{name}.json")
        with open(name + ".tmp", "w", encoding="utf-8") as fh:
            json.dump(metrics, fh)
        os.replace(name + ".tmp", name)
    except OSError:
        pass


def _write_event(path: str, kind: str, ci: str, lsn: str, commit_ts, detail: str) -> None:
    """One JSON per event, for the sink to fold into the facts (ADR 0023): named by its kind
    and LSN, so a replanned batch rewrites the same file. Best effort, like the metrics."""
    import json

    try:
        os.makedirs(path, exist_ok=True)
        name = os.path.join(path, f"event-{kind}-{lsn}.json")
        body = {"event": kind, "capture_instance": ci, "lsn": lsn, "commit_ts": commit_ts}
        with open(name + ".tmp", "w", encoding="utf-8") as fh:
            json.dump({**body, "detail": detail}, fh)
        os.replace(name + ".tmp", name)
    except OSError as exc:
        _log.warning("mssql_cdc: could not write the %s event file in %s: %s", kind, path, exc)


_DTO = re.compile(r"(.{19})(?:\.(\d{1,7}))? ([+-]\d\d:\d\d)")


def _to_schema(table, target):
    """Invariant 7: ``table`` cast to the Spark schema. arrow-odbc reads a ``datetimeoffset``
    as text, ``2026-09-28 13:50:01.1234567 -03:00``, which pyarrow does not parse: a TIMESTAMP
    column whose text is all in that form becomes its UTC instant first. Other text (a varchar
    the ``columns`` option reads as TIMESTAMP) is left to the cast."""
    import pyarrow as pa

    for i, field in enumerate(target):
        col = table.column(i)
        if pa.types.is_timestamp(field.type) and field.type.tz and pa.types.is_string(col.type):
            # ponytail: a Python loop over the column; vectorise if a profile shows it
            texts = col.to_pylist()
            found = [None if v is None else _DTO.fullmatch(v) for v in texts]
            if all(m or v is None for m, v in zip(found, texts)):
                values = [
                    m and datetime.fromisoformat(f"{m[1]}.{(m[2] or '').ljust(6, '0')[:6]}{m[3]}")
                    for m in found
                ]
                table = table.set_column(i, field.name, pa.array(values, field.type))
    return table.cast(target)


@dataclass
class LsnRange(InputPartition):
    capture_instance: str  # the instance this range reads: batches can span two (ADR 0023)
    from_lsn: str  # inclusive
    to_lsn: str  # inclusive
    columns: list[str] | None = None  # the source columns it has; None: all. The rest read NULL
    # the driver client's clock(), so a task converts commit times as the offsets were
    zone: str | None = None
    offset_min: int | None = None


class MssqlCdcDataSource(DataSource):
    """``spark.readStream.format("mssql_cdc")``."""

    default_num_partitions: int | None = None  # set by register() from the session's cores

    @classmethod
    def name(cls) -> str:
        return "mssql_cdc"

    def schema(self) -> str:
        columns = _opt(self.options, "columns") or self._captured_columns()
        include_cmd = _truthy(_opt(self.options, "includeCommandId", "true"))
        meta = [f"{n} {t}" for n, t in METADATA_COLUMNS if include_cmd or n != "_command_id"]
        return ", ".join(meta) + ", " + columns

    def _captured_columns(self) -> str:
        """Without ``columns``, the union of the captured columns of every capture instance
        of the table, from CDC metadata (driver side)."""
        from .client import make_client

        ci = _opt(self.options, "captureInstance")
        if not ci:
            raise ValueError("Option 'captureInstance' is required (e.g. 'dbo_orders')")
        client = make_client(self.options)
        try:
            instances = client.capture_instances(ci)
            for inst in instances:
                if not inst.columns or None in inst.column_types:
                    return client.captured_columns(inst.name)  # raises, naming what is missing
            return union_columns(instances)
        finally:
            client.close()

    def streamReader(self, schema):
        cls = MssqlCdcStreamReader if HAS_ADMISSION_CONTROL else MssqlCdcLegacyStreamReader
        return cls(dict(self.options), schema, self.default_num_partitions)


class MssqlCdcSnapshotDataSource(MssqlCdcDataSource):
    """``spark.read.format("mssql_cdc_snapshot")``: the tracked table's current rows in the
    stream's schema, as operation 0 at one LSN (ADR 0016). ``CdcStream.snapshot`` writes
    them to Delta and returns the offset the stream starts from.

    With ``snapshotChunks``, a JSON list of ``[chunk, lo, hi]`` (key bounds as
    ``client.plan_chunks`` plans them), only those chunks, one partition each, stamped with
    ``snapshotLsn`` and numbered in an extra ``_chunk INT`` column; with ``metricsPath``,
    each leaves ``chunk-<chunk>.json`` there (ADR 0028). ``snapshotKeys``, a JSON list of
    columns, cuts ranges on those instead of the unique index. ``isolationLevel=snapshot`` reads
    under SNAPSHOT isolation instead of READ COMMITTED, where the DBA allows it."""

    @classmethod
    def name(cls) -> str:
        return "mssql_cdc_snapshot"

    def schema(self) -> str:
        chunked = _opt(self.options, "snapshotChunks")
        return super().schema() + (", _chunk INT" if chunked else "")

    def reader(self, schema):
        return MssqlCdcSnapshotReader(dict(self.options), schema, self.default_num_partitions)


class _Common:
    """What the stream and snapshot readers share: options, the schema, a lazy client."""

    def __init__(self, options: dict, schema, default_num_partitions: int | None = None):
        self.options = options
        unknown = sorted(str(k) for k in options if str(k).lower() not in KNOWN_OPTIONS)
        if unknown:
            _log.warning(
                "mssql_cdc: unknown option(s) %s ignored; see docs/reference/options.md",
                ", ".join(unknown),
            )
        self.capture_instance = _opt(options, "captureInstance")
        if not self.capture_instance:
            raise ValueError("Option 'captureInstance' is required (e.g. 'dbo_orders')")
        self.include_command_id = _truthy(_opt(options, "includeCommandId", "true"))
        self.fail_on_data_loss = _truthy(_opt(options, "failOnDataLoss", "true"))
        num_partitions = str(_opt(options, "numPartitions", "auto")).strip().lower()
        # auto: the session's cores (register()), else this driver node's CPUs. Each
        # partition opens its own connection to SQL Server.
        self.num_partitions = (
            max(1, default_num_partitions or os.cpu_count() or 1)
            if num_partitions == "auto"
            else _positive_int(options, "numPartitions", allowed="'auto' or a positive integer")
        )
        # at least 1: with 0, mssql-python can return an empty first batch and read nothing
        self.batch_size = _positive_int(options, "arrowBatchSize", "10000")
        # optional: a directory (local or FUSE, e.g. /Volumes/...) where each partition leaves
        # its metrics for delta_sink(metrics_path=...) to fold into the batch facts
        self.metrics_path = _opt(options, "metricsPath")
        policy = str(_opt(options, "schemaChangePolicy", "classify")).strip().lower()
        if policy not in SCHEMA_CHANGE_POLICIES:
            raise ValueError(f"schemaChangePolicy must be 'classify' or 'fail', not {policy!r}")
        self.schema_change_policy = policy
        self.explicit_columns = bool(_opt(options, "columns"))
        meta_names = {n for n, _ in METADATA_COLUMNS}
        self.field_names = list(schema.fieldNames())
        self.source_columns = [f for f in self.field_names if f not in meta_names]
        self.schema = schema
        self._client: CdcClient | None = None
        # driver side (ADR 0023): the capture instance names seen this run, gone ones first,
        # then oldest to newest; the type each source column is expected to have; and the
        # instances whose columns the query's schema was checked against (None: not planned yet)
        self._names = [self.capture_instance]
        self._expected: dict[str, str] | None = None
        self._checked: set[str] | None = None

    # A live DB connection must never be pickled to executors.
    def __getstate__(self):
        state = self.__dict__.copy()
        state["_client"] = None
        return state

    @property
    def client(self):
        if self._client is None:
            from .client import make_client

            self._client = make_client(self.options)
        return self._client

    def _instances(self, client) -> list:
        """Every capture instance of the source table, oldest first (``CaptureInstance``).
        Looked up by the newest name seen this run first, so an instance disabled after a
        newer one took over is followed (ADR 0023)."""
        error: ValueError | None = None
        for name in reversed(self._names):
            try:
                found = client.capture_instances(name)
                break
            except ValueError as exc:
                error = error or exc
        else:
            assert error is not None  # _names is never empty
            raise error
        current = {i.name.lower() for i in found}
        self._names = [n for n in self._names if n.lower() not in current]
        self._names += [i.name for i in found]
        return found

    def _gone(self, instances) -> list[str]:
        """Instance names seen this run (or configured) that the table no longer has."""
        current = {i.name.lower() for i in instances}
        return [n for n in self._names if n.lower() not in current]

    def _check_declared(self, instances) -> None:
        """A source column no capture instance of the table captures would read NULL in every
        change row: fail instead (a typo in 'columns', or a column CDC does not capture)."""
        if any(not i.columns for i in instances):  # unknown (the fake without metadata)
            return
        captured = {c.lower() for i in instances for c in i.columns}
        missing = [c for c in self.source_columns if c.lower() not in captured]
        if missing:
            raise ValueError(
                f"No capture instance of the table ({', '.join(i.name for i in instances)}) "
                f"captures {', '.join(missing)}: fix 'columns', or capture them with a new "
                "capture instance (sql/switch_capture_instance.sql)."
            )


class _BaseReader(_Common, DataSourceStreamReader):
    # -- offsets --------------------------------------------------------------
    def _offset(self, lsn: str) -> dict:
        return {"lsn": lsn, "commit_ts": self.client.lsn_to_time(lsn) or ""}

    def _max_lsn(self) -> str:
        # sys.fn_cdc_get_max_lsn() is NULL on a database capture has not written to yet
        return self.client.max_lsn() or ZERO_LSN

    def initialOffset(self) -> dict:
        start = (_opt(self.options, "startingLsn", "earliest") or "earliest").strip()
        if start.lower() == "earliest":
            # offsets hold the last *processed* LSN, so start just before min_lsn (of the
            # table's oldest capture instance: a batch reads it below the newer's start)
            oldest = self._instances(self.client)[0].name
            lsn = self.client.decrement_lsn(self.client.min_lsn(oldest))
        elif start.lower() == "latest":
            lsn = self._max_lsn()
        else:
            from .lsn import normalize

            lsn = normalize(start)
        return self._offset(lsn)

    def partitions(self, start: dict, end: dict):
        if end["lsn"] <= start["lsn"]:
            return []
        client = self.client
        instances = self._instances(client)
        if self._checked is None:  # the run's first planning: load() took the schema from these
            self._check_declared(instances)
            self._checked = {instances[0].name.lower()}
        expected = self._expected_types(instances)
        from_lsn = client.increment_lsn(start["lsn"])
        to_lsn = end["lsn"]
        pieces = self._pieces(client, instances, from_lsn, to_lsn)
        # schema checks first: a batch that fails them reads nothing (ADR 0023)
        events = self._check_ddl(client, [i for i, _, _ in pieces], start["lsn"], to_lsn, expected)
        wanted = {c.lower() for c in self.source_columns}
        for older, inst in pairwise(instances):
            if any(p[0] is inst and p[1] == inst.start_lsn for p in pieces):  # crosses its start
                detail = f"{older.name} -> {inst.name}"
                kept = {c.lower() for c in inst.columns}
                lost = [c for c in older.columns if kept and c.lower() in wanted - kept]
                if lost:  # NULL from S on: the warning and the facts say so
                    detail += f"; no longer captured, read as NULL: {', '.join(lost)}"
                ts = client.lsn_to_time(inst.start_lsn)
                events.append(("capture_instance_switched", inst.name, inst.start_lsn, ts, detail))
        ranges = []
        gone = self._gone(instances)
        for inst, lo, hi in pieces:
            # failOnDataLoss=false: skip ahead to what cleanup left
            oldest = inst is instances[0]
            lo = max(lo, self._guard_retention(client, inst.name, lo, gone if oldest else []))
            if lo <= hi:  # invariant 3: cleanup may have left nothing up to hi
                if inst.name.lower() not in self._checked:  # the run's first read of it (D2)
                    self._check_switch(inst)
                    self._checked.add(inst.name.lower())
                ranges += self._split(client, inst, lo, hi)
        for kind, ci, lsn, ts, detail in events:
            _log.warning("mssql_cdc: %s on %s at %s: %s", kind, ci, lsn, detail)
            if self.metrics_path:  # for the sink to fold into the facts
                _write_event(self.metrics_path, kind, ci, lsn, ts, detail)
        return ranges

    def _pieces(self, client, instances, lo: str, hi: str) -> list[tuple]:
        """[lo, hi] cut at each newer instance's start S: (instance, from, to) with the
        older instance up to S - 1 and the newer one from S. Never an empty piece."""
        usable = [instances[0], *(i for i in instances[1:] if i.start_lsn)]
        pieces = []
        for inst, nxt in zip(usable, [*usable[1:], None]):
            s = nxt.start_lsn if nxt is not None else None
            if s is None or s > hi:
                pieces.append((inst, lo, hi))
                break
            if lo < s:
                pieces.append((inst, lo, client.decrement_lsn(s)))
            lo = max(lo, s)
        return pieces

    def _split(self, client, inst, lo: str, hi: str) -> list[LsnRange]:
        """[lo, hi] of one instance in up to numPartitions ranges of about the same rows."""
        cols, clock = self._columns_of(inst), client.clock()
        ranges = []
        if self.num_partitions > 1:
            for b, after in client.split_points(inst.name, lo, hi, self.num_partitions):
                if lo <= b < hi:  # a bound two tiles share comes twice: once
                    ranges.append(LsnRange(inst.name, lo, b, cols, *clock))
                    lo = after
        return [*ranges, LsnRange(inst.name, lo, hi, cols, *clock)]

    def _columns_of(self, inst) -> list[str] | None:
        """The query's source columns ``inst`` captures; None when it has them all (or its
        columns are unknown)."""
        if not inst.columns:
            return None
        have = {c.lower() for c in inst.columns}
        cols = [c for c in self.source_columns if c.lower() in have]
        return None if len(cols) == len(self.source_columns) else cols

    def _expected_types(self, instances) -> dict[str, str]:
        """lower name -> the Spark type the query reads a source column as, to compare the
        captured types with: the schema's when inferred; with 'columns', the captured types
        at this run's first planning (the declared ones are the user's own conversions)."""
        if self._expected is None:
            if self.explicit_columns:
                self._expected = {
                    c.lower(): t
                    for i in instances
                    for c, t in zip(i.columns, i.column_types)
                    if t is not None
                }
            else:
                wanted = {c.lower() for c in self.source_columns}
                self._expected = {
                    f.name.lower(): f.dataType.simpleString()
                    for f in self.schema.fields
                    if f.name.lower() in wanted
                }
        return self._expected

    def _check_ddl(self, client, used, start: str, end: str, expected) -> list[tuple]:
        """D1 of ADR 0023: the DDL the instances read by this batch recorded in (start, end].
        A captured column whose type no longer fits the query's, or with
        schemaChangePolicy=fail any DDL, raises ``SchemaChangedError``; the rest become
        'schema_change' events."""
        from .client import SchemaChangedError

        found: dict = {}
        for inst in {i.name: i for i in used}.values():  # one call per instance
            for d in client.ddl_history(inst.name, start, end):
                found.setdefault(d.lsn, (inst, d))  # recorded by both instances: once
        if not found:
            return []
        changes = [found[lsn] for lsn in sorted(found)]
        if self.schema_change_policy == "fail":
            inst, d = changes[0]
            raise SchemaChangedError(
                f"{inst.name}: DDL at {d.lsn} ({d.command!r}) inside the batch, and "
                "schemaChangePolicy=fail. Restart the query to re-infer the schema (and enable "
                "delta.enableTypeWidening on bronze for a widening); the replayed batch holds "
                "the same DDL, so restart with schemaChangePolicy=classify to go past it."
            )
        wanted = {c.lower() for c in self.source_columns}
        changed = {
            f"{c} {t} (read as {expected[c.lower()]})"
            for inst in used
            for c, t in zip(inst.columns, inst.column_types)
            if c.lower() in wanted
            and c.lower() in expected
            and not (t and _fits(t, expected[c.lower()]))
        }
        if changed:
            commands = "; ".join(f"{d.lsn}: {d.command}" for _, d in changes)
            fix = "update 'columns'" if self.explicit_columns else "re-infer the schema"
            raise SchemaChangedError(
                f"{used[0].name}: the type of captured column(s) {', '.join(sorted(changed))} "
                f"changed inside the batch ({commands}). Restart the query to {fix} (and "
                "enable delta.enableTypeWidening on bronze for a widening)."
            )
        return [
            ("schema_change", inst.name, d.lsn, d.commit_ts, d.command[:500]) for inst, d in changes
        ]

    def _check_switch(self, newer) -> None:
        """D2 of ADR 0023: read a capture instance the query's schema was not checked against
        (a newer one, at its start or skipped ahead to) only when the schema holds what it
        captures; otherwise fail before reading it, so the next load() infers the new columns.
        With 'columns' the declared list decides."""
        new = [
            c for c in newer.columns if c.lower() not in {s.lower() for s in self.source_columns}
        ]
        if self.explicit_columns:
            if new:
                _log.warning(
                    "mssql_cdc: %s captures %s, which 'columns' does not list: not read",
                    newer.name,
                    ", ".join(new),
                )
            return
        from .client import SchemaChangedError

        query = {f.name.lower(): f.dataType.simpleString() for f in self.schema.fields}
        changed = [
            f"{c} {t} (read as {query[c.lower()]})"
            for c, t in zip(newer.columns, newer.column_types)
            if c.lower() in query and not (t and _fits(t, query[c.lower()]))
        ]
        what = []
        if new:
            what.append(f"new column(s) {', '.join(new)}")
        if changed:
            what.append(f"other type(s) {', '.join(changed)}")
        if what:
            raise SchemaChangedError(
                f"The stream reaches capture instance {newer.name!r} of the table (from "
                f"{newer.start_lsn}), with {'; '.join(what)}. Restart the query to re-infer the "
                "schema (and enable delta.enableTypeWidening on bronze for a widening); it "
                "resumes there."
            )

    def _guard_retention(self, client, capture_instance: str, from_lsn: str, gone=()) -> str:
        """Invariant 4: fail when change data at or after ``from_lsn`` is gone from
        ``capture_instance``. ``gone``: older instances of the table disabled meanwhile."""
        min_lsn = client.min_lsn(capture_instance)
        if from_lsn < min_lsn and self.fail_on_data_loss:
            from .client import DataLossError

            why = "purged by CDC cleanup"
            if gone:  # where the gap lies does not tell the two apart
                why += (
                    f", or held only by capture instance {', '.join(map(repr, gone))}, disabled "
                    "before the stream read them"
                )
            raise DataLossError(
                f"{capture_instance}: change data from {from_lsn} is gone (min_lsn is now "
                f"{min_lsn}): {why}. A re-snapshot is required. "
                "Set failOnDataLoss=false to skip ahead (loses changes)."
            )
        if from_lsn < min_lsn:  # failOnDataLoss=false: skipped, but never without a trace
            try:
                at = client.lsn_to_time(min_lsn)
            except Exception:  # noqa: BLE001 - a log line must never fail the read
                at = None
            _log.warning(
                "mssql_cdc: %s: change data from %s up to min_lsn %s (committed at %s) is "
                "gone; failOnDataLoss=false skips it, and those changes are lost",
                capture_instance,
                from_lsn,
                min_lsn,
                f"{at} UTC" if at else "an unknown time",
            )
        return min_lsn

    # -- data (runs on executors) ---------------------------------------------
    def read(self, partition: LsnRange) -> Iterator:  # type: ignore[override]  # partitions() only plans LsnRange
        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_schema

        target = to_arrow_schema(self.schema, timezone="UTC")  # TIMESTAMP columns are UTC instants
        cols = self.source_columns if partition.columns is None else partition.columns
        absent = [c for c in self.source_columns if c not in cols]  # read as typed NULL
        client = self.client
        started, rows, nbytes = time.perf_counter(), 0, 0
        try:
            client.set_clock(partition.zone, partition.offset_min)  # the driver's, not detected
            if self.metrics_path:  # one round trip and the session's wait so far, before reading
                rtt = client.ping(1)
                wait_before = client.network_wait_ms()
            for batch in client.iter_changes(
                partition.capture_instance,
                partition.from_lsn,
                partition.to_lsn,
                cols,
                self.include_command_id,
                self.batch_size,
            ):
                if batch.num_rows == 0:
                    continue
                table = pa.Table.from_batches([batch])
                table = table.append_column(
                    "_capture_instance",
                    pa.array([partition.capture_instance] * table.num_rows, pa.string()),
                )
                for name in absent:
                    table = table.append_column(
                        name, pa.nulls(table.num_rows, target.field(name).type)
                    )
                table = _to_schema(table.select(self.field_names), target)
                rows, nbytes = rows + table.num_rows, nbytes + table.nbytes
                yield from table.to_batches()
            # Cleanup may have run since partitions() checked. It moves min_lsn before it
            # deletes rows, so min_lsn past from_lsn now means rows may be missing.
            min_lsn = self._guard_retention(client, partition.capture_instance, partition.from_lsn)
            if self.metrics_path:  # rows or not: a batch that read none writes its facts row too
                wait_after = client.network_wait_ms()
                try:
                    watermark = client.lsn_to_time(min_lsn)
                except Exception:  # noqa: BLE001 - a metric must never fail a read
                    watermark = None
                try:  # how far capture had got, and how old that was when seen here (ADR 0020)
                    source_max = client.lsn_to_time(client.max_lsn())
                    seen = datetime.now(timezone.utc).replace(tzinfo=None)  # commit times are UTC
                    capture_lag = (
                        (seen - datetime.fromisoformat(source_max)).total_seconds()
                        if source_max
                        else None
                    )
                except Exception:  # noqa: BLE001 - a metric must never fail a read
                    source_max = capture_lag = None
                try:  # the batch's last partition ends at its end offset: where the stream is
                    to_commit_ts = client.lsn_to_time(partition.to_lsn)
                except Exception:  # noqa: BLE001 - a metric must never fail a read
                    to_commit_ts = None
                _write_metrics(
                    self.metrics_path,
                    f"{partition.from_lsn}-{partition.to_lsn}",
                    {
                        "from_lsn": partition.from_lsn,
                        "to_lsn": partition.to_lsn,
                        "rows": rows,
                        "bytes": nbytes,
                        "seconds": time.perf_counter() - started,
                        "rtt_ms": rtt[0] if rtt else None,
                        "network_wait_ms": (
                            None
                            if wait_before is None or wait_after is None
                            else wait_after - wait_before
                        ),
                        # what cleanup has deleted up to, as a commit time (ADR 0017)
                        "retention_watermark_ts": watermark,
                        "source_max_commit_ts": source_max,
                        "capture_lag_seconds": capture_lag,
                        "to_commit_ts": to_commit_ts,
                    },
                )
        finally:
            client.close()
            self._client = None

    def commit(self, end: dict) -> None:
        # SQL Server CDC retention is time-based: there is nothing to acknowledge.
        pass


class MssqlCdcStreamReader(_BaseReader, SupportsTriggerAvailableNow):
    """Spark 4.2+: admission control (maxCommitsPerBatch) and Trigger.AvailableNow."""

    def __init__(self, options, schema, default_num_partitions=None):
        super().__init__(options, schema, default_num_partitions)
        self._target = None
        # Spark polls latestOffset and reportLatestOffset back to back, about every 10 ms on
        # an idle stream: the max_lsn latestOffset read last, and the offset last reported
        self._seen_max: str | None = None
        self._reported: dict | None = None
        max_commits = _opt(options, "maxCommitsPerBatch")  # 0 used to mean unlimited, silently
        self._max_commits = (
            _positive_int(options, "maxCommitsPerBatch", hint="; omit it to read up to max_lsn")
            if max_commits
            else None
        )

    def getDefaultReadLimit(self):
        # ReadMaxRows is reused with "commits" semantics: custom ReadLimits are not
        # supported for Python sources, and a batch must end on a commit boundary.
        return ReadMaxRows(self._max_commits) if self._max_commits else ReadAllAvailable()

    def prepareForTriggerAvailableNow(self) -> None:
        self._target = self._max_lsn()

    def latestOffset(self, start: dict, limit) -> dict:
        if self._target:  # Trigger.AvailableNow: up to the max_lsn it started with
            upper = self._target
        else:
            upper = self._seen_max = self._max_lsn()
        if upper <= start["lsn"]:  # idle: nth_commit_after could only lower upper
            return start
        if isinstance(limit, ReadMaxRows):
            nth = self.client.nth_commit_after(start["lsn"], limit.max_rows)
            if nth is not None and nth < upper:
                upper = nth
        return self._offset(upper)

    def reportLatestOffset(self):
        # Surfaces capture progress (max_lsn) as latestOffset in query progress: the one
        # latestOffset just read, and its commit time once per LSN
        lsn = self._seen_max or self._max_lsn()
        if self._reported is None or self._reported["lsn"] != lsn:
            self._reported = self._offset(lsn)
        return self._reported


class MssqlCdcLegacyStreamReader(_BaseReader):  # pragma: no cover - Spark < 4.2
    """Spark 4.0/4.1 without admission control: every batch reads up to max_lsn."""

    def latestOffset(self) -> dict:  # type: ignore[override]  # Spark < 4.2 signature; stubs are 4.2
        return self._offset(self._max_lsn())


# --------------------------------------------------------------------------- #
# Snapshot of the tracked table (ADR 0016)
# --------------------------------------------------------------------------- #
def snapshot_lsn(client, source) -> str:
    """The LSN a snapshot is stamped with, recorded *before* the table is read.

    Every commit up to ``max_lsn`` is already in the table when the read starts; a commit the
    read also sees comes later and has a larger LSN, so the stream from here replays it and
    the downstream MERGE absorbs the overlap. A capture instance that capture has not reached
    yet (``max_lsn`` below its first LSN: a quiet database, just after the enable) starts
    the stream at its first LSN instead; ``fn_cdc_get_min_lsn`` is NULL until then, the
    instance's ``start_lsn`` in ``source`` (a ``SourceTable``) is not. ``max_lsn`` itself is
    NULL on a database capture has not written to yet.
    """
    max_lsn = client.max_lsn() or ZERO_LSN
    if source.start_lsn is None:
        return max_lsn  # no low endpoint yet; the stream's retention guard still checks it
    return max(max_lsn, client.decrement_lsn(source.start_lsn))


@dataclass
class KeyRange(InputPartition):
    capture_instance: str
    lsn: str  # the snapshot's LSN and commit time, stamped on every row
    commit_ts: str | None
    schema: str
    table: str
    keys: list[str]  # []: the whole table in one partition
    types: list[str] | None  # the keys' SQL types, bounds bound as CAST(? AS type); None: integers
    lo: tuple | None  # inclusive, one value per key; None: open, plus the rows that sort first
    hi: tuple | None  # exclusive; None: open
    columns: list[str] | None = None  # the source columns the table still has; None: all
    chunk: int | None = None  # its chunk of a chunked snapshot (snapshotChunks), for _chunk


ISOLATION_LEVELS = {"readcommitted": None, "snapshot": "snapshot"}  # never READ UNCOMMITTED


class MssqlCdcSnapshotReader(_Common, DataSourceReader):
    def __init__(self, options: dict, schema, default_num_partitions: int | None = None):
        super().__init__(options, schema, default_num_partitions)
        self.chunks = _opt(options, "snapshotChunks")
        if self.chunks:  # the chunk number, not a source column
            self.source_columns = [c for c in self.source_columns if c != "_chunk"]
        level = str(_opt(options, "isolationLevel", "readCommitted")).strip().lower()
        if level not in ISOLATION_LEVELS:
            raise ValueError(
                f"isolationLevel must be 'readCommitted' or 'snapshot', not {level!r}: a "
                "snapshot never reads uncommitted rows (NOLOCK)"
            )
        self.isolation = ISOLATION_LEVELS[level]

    def partitions(self):
        client = self.client
        from .lsn import normalize

        # the configured instance, or the newest of its table once it is disabled (ADR 0023)
        instances = self._instances(client)
        self._check_declared(instances)  # else the snapshot has values its change rows lack
        ci = self.capture_instance
        if ci.lower() not in {i.name.lower() for i in instances}:
            ci = instances[-1].name
        source = client.source_table(ci)
        given = _opt(self.options, "snapshotLsn")
        lsn = normalize(given) if given else snapshot_lsn(client, source)
        commit_ts = client.lsn_to_time(lsn)
        schema, table, keys = source.schema, source.table, source.keys
        given_keys = _opt(self.options, "snapshotKeys")
        if given_keys:  # reconcile()'s keys, which bound its ranges: not always the index's
            import json

            keys = [str(k) for k in json.loads(given_keys)]
        # a captured column the table no longer has reads NULL, like its later change rows
        present = client.present_columns(ci, self.source_columns)
        columns = None if len(present) == len(self.source_columns) else present

        def ranges(keys, types, bounds):
            return [
                KeyRange(ci, lsn, commit_ts, schema, table, keys, types, a, b, columns)
                for a, b in pairwise([None, *bounds, None])
            ]

        if self.chunks:
            import json

            from .client import _key_tuple

            plan = json.loads(self.chunks)
            values = [
                v for _, *ends in plan for b in ends for v in _key_tuple(b) or () if v is not None
            ]
            types = None  # integer bounds are inlined, as an integer key's plan has them
            if not all(isinstance(v, int) and not isinstance(v, bool) for v in values):
                types = client.key_types(ci, keys)
            if values and (not keys or not set(keys) <= set(present) or None in (types or [])):
                raise ValueError(
                    f"snapshotChunks has key bounds, but {schema}.{table}'s key {keys} cannot "
                    "be read in ranges (no unique index, a column it no longer has, or a type "
                    "a bound cannot be bound as)"
                )
            return [
                KeyRange(ci, lsn, commit_ts, schema, table, keys, types, lo, hi, columns, int(i))
                for i, lo, hi in ((i, _key_tuple(a), _key_tuple(b)) for i, a, b in plan)
            ]
        if self.num_partitions <= 1 or not keys or not set(keys) <= set(present):
            return ranges([], None, [])
        if len(keys) == 1:
            # One integer key: uniform ranges over MIN..MAX, two seeks. NTILE would read and
            # spool the whole key to count and tile it; sparse or skewed keys give uneven
            # ranges instead. ponytail: a single non-integer key pays these two seeks too.
            lo, hi = client.key_range(schema, table, keys[0])
            if lo is None or lo == hi:  # empty, or one row
                return ranges([], None, [])
            if all(isinstance(v, int) and not isinstance(v, bool) for v in (lo, hi)):
                n = min(self.num_partitions, hi - lo + 1)
                return ranges(keys, None, [(lo + (hi - lo + 1) * i // n,) for i in range(1, n)])
        # Composite or non-integer key: tiles of the rows by NTILE, bounds typed per column.
        types = client.key_types(ci, keys)
        if None in types:
            return ranges([], None, [])
        return ranges(keys, types, client.key_tiles(schema, table, keys, self.num_partitions))

    def read(self, partition: KeyRange) -> Iterator:  # type: ignore[override]  # partitions() only plans KeyRange
        from datetime import datetime

        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_schema

        target = to_arrow_schema(self.schema, timezone="UTC")
        commit_ts = datetime.fromisoformat(partition.commit_ts) if partition.commit_ts else None
        meta = {  # constant per snapshot; _seqval and _command_id have no meaning here
            "_capture_instance": (partition.capture_instance, pa.string()),
            "_start_lsn": (partition.lsn, pa.string()),
            "_seqval": (None, pa.string()),
            "_operation": (0, pa.int32()),
            "_command_id": (None, pa.int32()),
            "_commit_ts": (commit_ts, pa.timestamp("us")),
            "_chunk": (partition.chunk, pa.int32()),  # in the schema with snapshotChunks only
        }
        cols = self.source_columns if partition.columns is None else partition.columns
        client = self.client
        started, rows, nbytes = time.perf_counter(), 0, 0
        try:
            for batch in client.iter_table(
                partition.schema,
                partition.table,
                cols,
                partition.keys,
                partition.types,
                partition.lo,
                partition.hi,
                self.batch_size,
                self.isolation,
            ):
                if batch.num_rows == 0:
                    continue
                table = pa.Table.from_batches([batch])
                for name in self.field_names:
                    if name in meta:
                        value, typ = meta[name]
                        table = table.append_column(name, pa.array([value] * table.num_rows, typ))
                    elif name not in cols:  # dropped from the source table: typed NULL
                        table = table.append_column(
                            name, pa.nulls(table.num_rows, target.field(name).type)
                        )
                table = _to_schema(table.select(self.field_names), target)
                rows, nbytes = rows + table.num_rows, nbytes + table.nbytes
                yield from table.to_batches()
            if self.metrics_path and partition.chunk is not None:
                try:  # how far capture had got once the chunk was read
                    high = client.max_lsn()
                except Exception:  # noqa: BLE001 - a metric must never fail a read
                    high = None
                _write_metrics(
                    self.metrics_path,
                    f"chunk-{partition.chunk}",
                    {
                        "chunk": partition.chunk,
                        "rows": rows,
                        "bytes": nbytes,
                        "seconds": time.perf_counter() - started,
                        "high_lsn": high,
                    },
                )
        finally:
            client.close()
            self._client = None
