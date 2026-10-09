"""One declaration for the common pipeline: SQL Server CDC -> Delta, with facts and metrics.

    from mssql_cdc import stream
    query = stream(spark, options).to_delta("bronze.orders", "orders-v1",
                                            checkpoint="/Volumes/cat/sch/vol/ckpt/orders",
                                            facts_table="ops.ingestion_facts",
                                            bootstrap=True, on_data_loss="resnapshot")

The options go to ``readStream`` once, and the sink gets what it needs from them. With a
facts table, per-partition metrics default to ``<checkpoint>/_mssql_cdc_metrics`` (of the
live generation, see below) when the
checkpoint is a path Python can write on every node (local, or FUSE such as a Volume);
with a URI checkpoint (``dbfs:/``, ``abfss://``...) set ``metricsPath`` yourself (a Volume, or
a URI ``pyarrow.fs`` opens such as ``s3://``): the files then go to
``<metricsPath>/<sink app_id>``, so streams may share one ``metricsPath``.

``bootstrap=True`` first writes a snapshot of the tracked table into the target (once) and
starts a new checkpoint from its LSN, so the target holds the whole table, not only what CDC
retention still has (ADR 0016). For a table too big to snapshot within the CDC retention,
``seed(target, df, as_of)`` writes a copy you already have as that snapshot (ADR 0025).

``on_data_loss="resnapshot"`` checks, before the query starts, whether CDC cleanup already
deleted changes the checkpoint has not read. If so it snapshots the table into the target
again and moves the stream to a new generation ``n``: Spark checkpoint
``<checkpoint>/_generations/<n>`` and sink app_id ``<app_id>.g<n>`` (metrics then default
under that checkpoint), recorded in ``<checkpoint>/_mssql_cdc_generation.json`` (so it needs
a checkpoint path Python and Spark resolve alike: local, or a Volume) and as an event row in
the facts table, which it requires. At most once per ``resnapshot_interval_days``. A purge
while the query runs still fails it with ``DataLossError``; the next run recovers. Run one
job per stream (ADR 0018).

``snapshot="chunked"`` (ADR 0028) takes either snapshot next to the stream instead of before
it, for a table the link cannot read within the CDC retention: ``to_delta`` only opens it at
an LSN S (a 'snapshot_open' facts row with the key's extent) and starts the stream generation
at S at once; ``backfill()``, called repeatedly in its own task, plans the chunks once (a
'snapshot_plan' row), reads the key space in them,
each stamped with an LSN at or after S, appends them to the target (``_snapshot`` S,
``_chunk``) and records them as 'snapshot_chunk' rows, then writes the snapshot's
'bootstrap' or 'resnapshot' row. Downstream rebuilds from S: its rows and the changes after
S. A loss while one is open opens a newer one, which abandons it.

A run (one ``to_delta`` call, its bootstrap and its re-snapshot) takes snapshots in one
mode, and the mode may change between runs, but not while a snapshot of the other mode is
open: both modes write 'snapshot_open' before reading the table (a chunked one once per
generation, ``<app_id>#snapshots``; a full one per run, at its own S), the snapshot's
'bootstrap' or 'resnapshot' row closes it, and a run, ``snapshot()``, ``seed()`` or
``backfill()`` that finds one of the other mode still open raises. A full one CDC cleanup
has passed can never complete and stops counting.

The stream follows a newer capture instance of its table (ADR 0023): snapshots in the target
are found under any capture instance of the table, and ``snapshot_on_switch=True`` appends a
snapshot after the batch that first reads the newer one, so that rows unchanged since then
carry the columns only it captures instead of NULL.
"""

from __future__ import annotations

# Every name the module this package replaced defined stays importable from here, the
# private ones (tests, silver) too; the event helpers it had moved to mssql_cdc.events.
from .. import events as _events
from ..types import BackfillState as BackfillState
from ..types import BackfillStatus as BackfillStatus
from ..types import Isolation as Isolation
from ..types import Offset as Offset
from ..types import OnDataLoss as OnDataLoss
from ..types import SnapshotMode as SnapshotMode
from ._chunked import _SNAPSHOT_FACTS as _SNAPSHOT_FACTS
from ._chunked import _ChunkRead as _ChunkRead
from ._chunked import _earlier_wave as _earlier_wave
from ._common import _URI as _URI
from ._common import _family as _family
from ._common import _instances as _instances
from ._common import _iso as _iso
from ._common import _log as _log
from ._common import _lost as _lost
from ._common import _opt as _opt
from ._common import _ts as _ts
from ._common import _version as _version
from ._lock import _Opened as _Opened
from ._lock import _unfinished as _unfinished
from ._recovery import _STATE as _STATE
from ._recovery import _generation as _generation
from ._recovery import _last_offset as _last_offset
from ._recovery import _read_state as _read_state
from ._recovery import _write_state as _write_state
from ._stream import CdcStream, stream
from ._stream import _snapshot_after_switch as _snapshot_after_switch

__all__ = [
    "BackfillState",
    "BackfillStatus",
    "CdcStream",
    "Isolation",
    "Offset",
    "OnDataLoss",
    "SnapshotMode",
    "stream",
]

_chunk_detail = _events.chunk_detail
_mode = _events.mode
_plan_of = _events.plan_of
