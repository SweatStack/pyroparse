"""Raw FIT message access — the escape hatch."""

from __future__ import annotations

import os

from pyroparse._core import dump_fit_messages as _dump_path
from pyroparse._core import dump_fit_messages_bytes as _dump_bytes
from pyroparse._types import Source


def all_messages(source: Source) -> list[dict]:
    """Return every message in a FIT file as an ordered list of dicts.

    Each dict has ``"kind"`` (message type string) and ``"fields"`` (list of
    field dicts with name, number, developer_data_index, value, units).

    No pyroparse opinions are applied — field names, values, and units come
    straight from the FIT profile as decoded by ``fitparser``.
    """
    if isinstance(source, (str, os.PathLike)):
        return _dump_path(str(os.fspath(source)))
    if isinstance(source, bytes):
        return _dump_bytes(source)
    return _dump_bytes(source.read())
