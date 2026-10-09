"""The metrics directory (``metricsPath``): the JSON files the reader and its partitions
leave there and the sink and ``backfill()`` fold. A local or FUSE path (``/Volumes/...``)
goes through Python's file functions; a URI (``s3://``, ``gs://``, ``abfss://``, ``hdfs://``,
``file://``) through ``pyarrow.fs``, its credentials from pyarrow's defaults (environment,
instance profile...) or the URI's query (``?region=...``). The layout is the same: one
directory per stream, one file per partition or event.

A file is written whole or not at all: under a temporary name moved over its own, or, on a
filesystem that cannot move (pyarrow raises ``NotImplementedError``), under its own name at
once, which an object store shows only once the upload completes. A retry rewrites the same
name, as before.
"""

from __future__ import annotations

import functools
import glob
import json
import logging
import os
import re
from collections.abc import Iterable
from typing import Any

_log = logging.getLogger(__name__)
URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]+:")  # a scheme; a Windows drive has one letter


@functools.cache
def _open(uri: str) -> tuple[Any, str]:
    """``uri``'s ``pyarrow.fs`` filesystem and its path there, once per directory and process."""
    from pyarrow import fs

    found: tuple[Any, str] = fs.FileSystem.from_uri(uri)
    return found


def _locate(name: str) -> tuple[Any, str]:
    """The filesystem of a file ``list_json`` named, and its path there."""
    head, q, query = name.partition("?")
    parent, _, base = head.rpartition("/")
    filesystem, path = _open(parent + q + query)
    return filesystem, f"{path}/{base}"


def join(path: str, name: str) -> str:
    """``name`` inside directory ``path``; a URI keeps its query (pyarrow's options) last."""
    if not URI.match(path):
        return os.path.join(path, name)
    head, q, query = path.partition("?")
    return f"{head.rstrip('/')}/{name}{q}{query}"


def check(path: str) -> None:
    """``ValueError`` unless ``path`` is local or a URI ``pyarrow.fs`` opens here."""
    if URI.match(path):
        try:
            _open(path)
        except Exception as exc:  # any failure: the option is unusable here
            raise ValueError(
                f"metricsPath {path!r}: pyarrow.fs cannot open it ({exc}). Use a URI pyarrow.fs "
                "opens with the credentials every node has (s3://, gs://, abfss://, hdfs://), or "
                "a local or FUSE path every node sees (e.g. /Volumes/...)"
            ) from exc


def write_json(path: str, name: str, value: object) -> None:
    """``value`` as ``name`` in directory ``path``, created when missing."""
    if not URI.match(path):
        os.makedirs(path, exist_ok=True)
        target = os.path.join(path, name)
        with open(target + ".tmp", "w", encoding="utf-8") as fh:
            json.dump(value, fh)
        os.replace(target + ".tmp", target)
        return
    filesystem, base = _open(path)
    filesystem.create_dir(base, recursive=True)
    target, data = f"{base}/{name}", json.dumps(value).encode()
    with filesystem.open_output_stream(target + ".tmp") as out:
        out.write(data)
    try:
        filesystem.move(target + ".tmp", target)
    except NotImplementedError:  # an object store without rename: one upload, whole or none
        with filesystem.open_output_stream(target) as out:
            out.write(data)
        filesystem.delete_file(target + ".tmp")


def list_json(path: str, events: bool = False) -> list[str]:
    """The partitions' files in ``path`` or, with ``events``, the reader's event files
    (``event-<kind>-<key>.json``); none when it cannot be listed, as ``glob`` does."""
    if not URI.match(path):
        return [
            name
            for name in glob.glob(os.path.join(path, "*.json"))
            if os.path.basename(name).startswith("event-") == events
        ]
    from pyarrow import fs

    try:
        filesystem, base = _open(path)
        infos = filesystem.get_file_info(fs.FileSelector(base, allow_not_found=True))
    except Exception as exc:  # noqa: BLE001 - metrics never fail a batch
        _log.warning("mssql_cdc: could not list the metrics files in %s: %s", path, exc)
        return []
    return [
        join(path, i.base_name)
        for i in infos
        if i.is_file
        and i.base_name.endswith(".json")
        and i.base_name.startswith("event-") == events
    ]


def read_json(name: str) -> Any:
    """The content of a file ``list_json`` named; ``OSError`` or ``ValueError`` if unreadable."""
    if not URI.match(name):
        with open(name, encoding="utf-8") as fh:
            return json.load(fh)
    filesystem, path = _locate(name)
    with filesystem.open_input_stream(path) as stream:
        return json.loads(stream.read())


def remove(names: Iterable[str]) -> None:
    """Remove the files ``list_json`` named; one already gone or not removable is skipped."""
    for name in names:
        try:
            if URI.match(name):
                filesystem, path = _locate(name)
                filesystem.delete_file(path)
            else:
                os.remove(name)
        except (OSError, ValueError):
            pass
