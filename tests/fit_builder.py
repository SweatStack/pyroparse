"""Minimal FIT file builder for synthetic test fixtures.

Real device recordings pin down real-world behaviour; this builder pins down
*spec* behaviour on exact byte layouts (developer field base types, message
ordering, sentinel values) that no recorded fixture happens to exercise.
Little-endian, single section, no compressed timestamps. Framing follows
docs/FIT-FORMAT.md.
"""

from __future__ import annotations

import struct
from datetime import datetime, timezone

FIT_EPOCH = datetime(1989, 12, 31, tzinfo=timezone.utc)

# Base type bytes (FIT-FORMAT.md section 7).
ENUM = 0x00
SINT8 = 0x01
UINT8 = 0x02
STRING = 0x07
UINT8Z = 0x0A
BYTE = 0x0D
SINT16 = 0x83
UINT16 = 0x84
SINT32 = 0x85
UINT32 = 0x86
FLOAT32 = 0x88
FLOAT64 = 0x89

# Global message numbers.
FILE_ID = 0
SESSION = 18
RECORD = 20
ACTIVITY = 34
FIELD_DESCRIPTION = 206
DEVELOPER_DATA_ID = 207

# struct format and invalid sentinel per numeric base type (section 8).
_NUMERIC = {
    ENUM: ("B", 0xFF),
    SINT8: ("b", 0x7F),
    UINT8: ("B", 0xFF),
    UINT8Z: ("B", 0x00),
    BYTE: ("B", 0xFF),
    SINT16: ("h", 0x7FFF),
    UINT16: ("H", 0xFFFF),
    SINT32: ("i", 0x7FFFFFFF),
    UINT32: ("I", 0xFFFFFFFF),
    FLOAT32: ("f", None),
    FLOAT64: ("d", None),
}

_CRC_TABLE = [
    0x0000, 0xCC01, 0xD801, 0x1400, 0xF001, 0x3C00, 0x2800, 0xE401,
    0xA001, 0x6C00, 0x7800, 0xB401, 0x5000, 0x9C01, 0x8801, 0x4400,
]


def crc16(data: bytes) -> int:
    """FIT CRC-16 (section 14)."""
    crc = 0
    for byte in data:
        for nibble in (byte & 0xF, byte >> 4):
            tmp = _CRC_TABLE[crc & 0xF]
            crc = (crc >> 4) & 0x0FFF
            crc ^= tmp ^ _CRC_TABLE[nibble]
    return crc


def fit_time(dt: datetime) -> int:
    """Seconds since the FIT epoch, i.e. a ``date_time`` value."""
    return int((dt - FIT_EPOCH).total_seconds())


def encode(value, base_type: int, size: int) -> bytes:
    """Encode one field value. ``None`` encodes the base type's invalid sentinel."""
    if isinstance(value, bytes):
        if len(value) != size:
            raise ValueError(f"expected {size} raw bytes, got {len(value)}")
        return value
    if base_type == STRING:
        raw = b"" if value is None else value.encode()
        if len(raw) >= size:
            raise ValueError(f"{value!r} does not fit in {size} bytes with a NUL")
        return raw.ljust(size, b"\0")
    fmt, sentinel = _NUMERIC[base_type]
    if struct.calcsize(fmt) != size:
        raise ValueError(
            f"base type 0x{base_type:02x} is {struct.calcsize(fmt)} bytes, got size {size}"
        )
    if value is None:
        return b"\xff" * size if sentinel is None else struct.pack("<" + fmt, sentinel)
    return struct.pack("<" + fmt, value)


class FitBuilder:
    """Assemble a FIT file from explicit definition and data messages.

    ``fields`` are ``(field_number, base_type, size)`` triples and
    ``dev_fields`` are ``(field_number, base_type, size, developer_data_index)``.
    A developer field's base type here only encodes the test value; the file
    declares it through a FieldDescription message, as the spec requires.
    """

    def __init__(self) -> None:
        self._body = bytearray()
        self._defs: dict[int, tuple[list, list]] = {}

    def define(self, local: int, mesg: int, fields, dev_fields=()) -> FitBuilder:
        fields, dev_fields = list(fields), list(dev_fields)
        header = 0x40 | local | (0x20 if dev_fields else 0)
        out = bytearray([header, 0, 0]) + struct.pack("<H", mesg) + bytes([len(fields)])
        for number, base_type, size in fields:
            out += bytes([number, size, base_type])
        if dev_fields:
            out += bytes([len(dev_fields)])
            for number, _base_type, size, dev_index in dev_fields:
                out += bytes([number, size, dev_index])
        self._defs[local] = (fields, dev_fields)
        self._body += out
        return self

    def data(self, local: int, values, dev_values=()) -> FitBuilder:
        fields, dev_fields = self._defs[local]
        values, dev_values = list(values), list(dev_values)
        if len(values) != len(fields) or len(dev_values) != len(dev_fields):
            raise ValueError("value count does not match the definition")
        out = bytearray([local])
        for (_, base_type, size), value in zip(fields, values):
            out += encode(value, base_type, size)
        for (_, base_type, size, _), value in zip(dev_fields, dev_values):
            out += encode(value, base_type, size)
        self._body += out
        return self

    def build(self) -> bytes:
        body = bytes(self._body)
        header = struct.pack("<BBHI4s", 14, 0x20, 2171, len(body), b".FIT")
        header += struct.pack("<H", crc16(header))
        return header + body + struct.pack("<H", crc16(body))


# ---------------------------------------------------------------------------
# Message helpers — the minimum a valid activity file needs
# ---------------------------------------------------------------------------

def file_id(b: FitBuilder, local: int) -> None:
    """file_id: type=activity (4), manufacturer=garmin (1)."""
    b.define(local, FILE_ID, [(0, ENUM, 1), (1, UINT16, 2)]).data(local, [4, 1])


def session(b: FitBuilder, local: int, start: datetime, elapsed_s: int, sport: int = 1) -> None:
    """Session with end ``timestamp``, ``start_time``, ``sport`` (1 = running),
    and ``total_elapsed_time``."""
    b.define(local, SESSION, [(253, UINT32, 4), (2, UINT32, 4), (5, ENUM, 1), (7, UINT32, 4)])
    b.data(local, [fit_time(start) + elapsed_s, fit_time(start), sport, elapsed_s * 1000])


def activity(b: FitBuilder, local: int, end: datetime, utc_offset_s: int) -> None:
    """Activity: ``timestamp`` (end of activity, UTC) and ``local_timestamp``
    (the same instant in local time), plus ``num_sessions``."""
    b.define(local, ACTIVITY, [(253, UINT32, 4), (5, UINT32, 4), (1, UINT16, 2)])
    b.data(local, [fit_time(end), fit_time(end) + utc_offset_s, 1])


def developer_app(b: FitBuilder, local: int, dev_index: int, app_id: bytes) -> None:
    """developer_data_id: registers a CIQ app UUID under a developer_data_index."""
    b.define(local, DEVELOPER_DATA_ID, [(1, BYTE, 16), (3, UINT8, 1)])
    b.data(local, [app_id, dev_index])


def field_description(
    b: FitBuilder,
    local: int,
    dev_index: int,
    field_number: int,
    base_type: int,
    name: str,
    *,
    scale: int | None = None,
    offset: int | None = None,
    units: str | None = None,
) -> None:
    """field_description: declares how a developer field's bytes decode."""
    fields = [
        (0, UINT8, 1),    # developer_data_index
        (1, UINT8, 1),    # field_definition_number
        (2, UINT8, 1),    # fit_base_type_id
        (3, STRING, 64),  # field_name (spec maximum)
        (6, UINT8, 1),    # scale
        (7, SINT8, 1),    # offset
        (8, STRING, 16),  # units
    ]
    b.define(local, FIELD_DESCRIPTION, fields)
    b.data(local, [dev_index, field_number, base_type, name, scale, offset, units])
