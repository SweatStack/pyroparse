# FIT Binary File Format Reference

Reference for implementing a FIT file parser. Derived from the
[Garmin FIT SDK](https://developer.garmin.com/fit/protocol/),
[Profile.xlsx](https://github.com/garmin/fit-sdk-tools), and verified
against the [fitparser](https://github.com/stadelmanma/fitparse-rs) Rust
implementation (v0.10.0, SDK 21.171.00).

---

## Table of contents

1. [Overview](#1-overview)
2. [File structure](#2-file-structure)
3. [File header](#3-file-header)
4. [Record headers](#4-record-headers)
5. [Definition messages](#5-definition-messages)
6. [Data messages](#6-data-messages)
7. [Base types](#7-base-types)
8. [Invalid / sentinel values](#8-invalid--sentinel-values)
9. [Endianness](#9-endianness)
10. [String fields](#10-string-fields)
11. [Array fields](#11-array-fields)
12. [Timestamps](#12-timestamps)
13. [Compressed timestamps](#13-compressed-timestamps)
14. [CRC-16](#14-crc-16)
15. [Developer fields](#15-developer-fields)
16. [Profile: messages and fields](#16-profile-messages-and-fields)
17. [Scale and offset](#17-scale-and-offset)
18. [Component fields](#18-component-fields)
19. [Accumulated fields](#19-accumulated-fields)
20. [Subfields (dynamic fields)](#20-subfields-dynamic-fields)
21. [Chained FIT files](#21-chained-fit-files)
22. [Activity file structure](#22-activity-file-structure)
23. [Truncated and corrupt files](#23-truncated-and-corrupt-files)
24. [Key message types for activity parsing](#24-key-message-types-for-activity-parsing)

---

## 1. Overview

FIT (Flexible and Interoperable Data Transfer) is a binary format designed
for compact storage on embedded fitness devices. A FIT file is a sequential
stream of messages — no random access, no index, no compression.

The format separates **framing** (how bytes are laid out) from **profile**
(what messages and fields mean). The framing is fixed and compact. The
profile is defined in an external spreadsheet (Profile.xlsx) distributed
with the FIT SDK and can grow across SDK versions without changing the
binary format.

A parser needs two things:
1. A binary reader that understands the framing (headers, definitions, data)
2. A profile that maps message numbers and field numbers to names and types

---

## 2. File structure

```
┌──────────────────────────────────────────────┐
│ File Header              (12 or 14 bytes)    │
├──────────────────────────────────────────────┤
│ Data Records             (variable length)   │
│   Definition Message                         │
│   Data Message                               │
│   Data Message                               │
│   Definition Message                         │
│   Data Message (compressed timestamp)        │
│   ...                                        │
├──────────────────────────────────────────────┤
│ File CRC                 (2 bytes)           │
└──────────────────────────────────────────────┘
```

A file may contain multiple FIT files concatenated (chained). Each has its
own header, data section, and CRC. See [section 21](#21-chained-fit-files).

---

## 3. File header

The header is always at least 12 bytes. A 14-byte header adds a header CRC.

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 1 | `header_size` | Total header length in bytes (12 or 14) |
| 1 | 1 | `protocol_version` | High nibble = major, low nibble = minor |
| 2 | 2 | `profile_version` | Little-endian u16. `value / 100` = major, `value % 100` = minor |
| 4 | 4 | `data_size` | Little-endian u32. Number of bytes between header and file CRC |
| 8 | 4 | `data_type` | ASCII `.FIT` (bytes `2E 46 49 54`) |
| 12 | 2 | `header_crc` | Little-endian u16. Only present if `header_size >= 14` |

### Header CRC

If `header_size >= 14` and `header_crc != 0`: the CRC covers bytes 0–11
(the first 12 bytes). Validate using the CRC-16 algorithm in
[section 14](#14-crc-16).

If `header_crc == 0` or `header_size == 12`: the header has no CRC. In
this case, the header bytes are included in the data CRC calculation
(see [section 14](#14-crc-16)).

### Derived values

```
data_start = header_size
data_end   = header_size + data_size
file_crc   = bytes at offset data_end..data_end+2
```

---

## 4. Record headers

Every data record (definition or data message) starts with a 1-byte record
header. The top two bits determine the type:

### Normal header (bit 7 = 0)

```
Bit:  7  6  5  4  3  2  1  0
      0  T  D  0  ├─ local ─┤
```

| Bits | Field | Description |
|------|-------|-------------|
| 7 | `normal` | Always `0` |
| 6 | `message_type` | `0` = data message, `1` = definition message |
| 5 | `developer_data` | `1` = definition includes developer fields (only valid when bit 6 = 1; reserved/ignored for data messages) |
| 4 | reserved | Always `0` |
| 3–0 | `local_message_number` | 0–15. Maps to a definition. |

### Compressed timestamp header (bit 7 = 1)

```
Bit:  7  6  5  4  3  2  1  0
      1  ├local─┤  ├offset──┤
```

| Bits | Field | Description |
|------|-------|-------------|
| 7 | `compressed` | Always `1` |
| 6–5 | `local_message_number` | 0–3 (only 4 slots for compressed timestamps) |
| 4–0 | `time_offset` | 5-bit value (0–31). See [section 13](#13-compressed-timestamps) |

Compressed timestamp headers always indicate a **data message** (never a
definition). Developer data flag is always false.

---

## 5. Definition messages

A definition message tells the parser how to read subsequent data messages
with the same local message number. It follows immediately after its
1-byte record header.

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| +0 | 1 | reserved | Ignored |
| +1 | 1 | `architecture` | `0` = little-endian, `1` = big-endian |
| +2 | 2 | `global_message_number` | u16, read with the endianness just declared |
| +4 | 1 | `num_fields` | Number of field definitions that follow |
| +5 | 3×N | field definitions | 3 bytes per field (see below) |

### Field definition (3 bytes each)

| Byte | Field | Description |
|------|-------|-------------|
| 0 | `field_definition_number` | Identifies the field within the message (maps to profile) |
| 1 | `size` | Total bytes for this field in data messages |
| 2 | `base_type` | Base type ID (see [section 7](#7-base-types)). Mask with `& 0x1F` to get the type. Bit 7 indicates endian-ability (set for multi-byte types). |

### Local message type redefinition

Local message types can be redefined within a single FIT file. A new
definition message with the same local number replaces the previous
definition. All subsequent data messages with that local number use the
new layout. This allows devices with limited RAM to reuse local message
slots (some embedded devices only support local message type 0).

Care must be taken: if a definition is redefined, any data messages between
the old definition and new definition use the old layout. Data messages
after the new definition use the new layout.

### Developer field definitions

Only present if the `developer_data` bit (bit 5) was set in the record
header.

| Offset | Size | Field |
|--------|------|-------|
| +0 | 1 | `num_dev_fields` |
| +1 | 3×M | developer field definitions |

Each developer field definition (3 bytes):

| Byte | Field | Description |
|------|-------|-------------|
| 0 | `field_number` | Developer-defined field number |
| 1 | `size` | Bytes in data messages |
| 2 | `developer_data_index` | Identifies which developer (CIQ app) owns this field |

### Total data message size

After parsing a definition, you know the exact size of corresponding data
messages:

```
data_size = sum(field.size for field in fields) + sum(dev_field.size for dev_field in dev_fields)
```

This is crucial for skipping messages you don't care about.

---

## 6. Data messages

A data message is a sequence of raw bytes whose layout is determined by the
most recent definition message with the same local message number.

There is no framing within a data message — it's just the concatenation of
all field bytes in definition order, followed by developer field bytes in
definition order:

```
┌─────────────────────────────────────────┐
│ field_0 bytes  (size from definition)   │
│ field_1 bytes                           │
│ ...                                     │
│ field_N bytes                           │
│ dev_field_0 bytes (if developer data)   │
│ dev_field_1 bytes                       │
│ ...                                     │
└─────────────────────────────────────────┘
```

To read a data message:
1. Look up the definition by `local_message_number`
2. For each field in the definition, read `field.size` bytes
3. Interpret the bytes according to `field.base_type` and the definition's
   endianness

### Skipping data messages

If you don't need a message (e.g., Record messages during a metadata scan),
you can skip it entirely by advancing `sum(field sizes) + sum(dev field sizes)`
bytes. No decoding needed.

---

## 7. Base types

The base type byte in field definitions identifies the binary encoding.
Mask the raw byte with `& 0x1F` to get the type ID (bits 5–6 are reserved,
bit 7 indicates endian-ability).

| Base Type # | Endian | Base Type Field | Name | Size (bytes) | C equivalent |
|-------------|--------|-----------------|---------|-------------|--------------|
| 0 | 0 | 0x00 | enum | 1 | u8 |
| 1 | 0 | 0x01 | sint8 | 1 | i8 |
| 2 | 0 | 0x02 | uint8 | 1 | u8 |
| 3 | 1 | 0x83 | sint16 | 2 | i16 |
| 4 | 1 | 0x84 | uint16 | 2 | u16 |
| 5 | 1 | 0x85 | sint32 | 4 | i32 |
| 6 | 1 | 0x86 | uint32 | 4 | u32 |
| 7 | 0 | 0x07 | string | 1 | UTF-8, NUL-terminated |
| 8 | 1 | 0x88 | float32 | 4 | f32 (IEEE 754) |
| 9 | 1 | 0x89 | float64 | 8 | f64 (IEEE 754) |
| 10 | 0 | 0x0A | uint8z | 1 | u8 |
| 11 | 1 | 0x8B | uint16z | 2 | u16 |
| 12 | 1 | 0x8C | uint32z | 4 | u32 |
| 13 | 0 | 0x0D | byte | 1 | u8 (opaque) |
| 14 | 1 | 0x8E | sint64 | 8 | i64 |
| 15 | 1 | 0x8F | uint64 | 8 | u64 |
| 16 | 1 | 0x90 | uint64z | 8 | u64 |

### Base type field byte layout

```
Bit:  7        6-5        4-0
      Endian   Reserved   Base Type Number
```

- **Bit 7 (Endian Ability):** `0` = single-byte type, `1` = multi-byte type
  that uses the definition message's endianness
- **Bits 6–5:** Reserved
- **Bits 4–0:** Base type number (0–16)

To extract the base type number: `base_type_field & 0x1F`. The full byte
value (with endian bit set) is shown in the Base Type Field column above.

### Size validation

If `field.size % base_type.size() != 0`, treat the field as `byte` type to
prevent reading past field boundaries. This happens when a file has
mismatched sizes (e.g., profile version differences).

---

## 8. Invalid / sentinel values

Each base type has a sentinel value meaning "no data". Fields with invalid
values should be treated as absent (null).

| Type | Invalid value | Notes |
|------|--------------|-------|
| enum | `0xFF` | |
| sint8 | `0x7F` | |
| uint8 | `0xFF` | |
| sint16 | `0x7FFF` | |
| uint16 | `0xFFFF` | |
| sint32 | `0x7FFFFFFF` | |
| uint32 | `0xFFFFFFFF` | |
| sint64 | `0x7FFFFFFFFFFFFFFF` | |
| uint64 | `0xFFFFFFFFFFFFFFFF` | |
| float32 | `0xFFFFFFFF` | Per SDK spec. fitparser also treats non-finite as invalid. |
| float64 | `0xFFFFFFFFFFFFFFFF` | Per SDK spec. fitparser also treats non-finite as invalid. |
| uint8z | `0x00` | |
| uint16z | `0x0000` | |
| uint32z | `0x00000000` | |
| uint64z | `0x0000000000000000` | |
| string | `0x00` | Empty or all NUL |
| byte | `0xFF` | **Field** is invalid only if **all** bytes are invalid |

For `z` types ("zero-invalid"), the sentinel is `0` instead of `0xFF...`.
This is used for fields where zero is not a valid value (e.g., serial
numbers).

For `byte` type: individual bytes with value `0xFF` are invalid, but the
field as a whole is only invalid if every byte is `0xFF`. This differs from
all other types where a single invalid value makes the field invalid.

---

## 9. Endianness

Endianness is declared **per definition message** in the `architecture`
byte:
- `0` = little-endian (the vast majority of real-world files)
- `1` = big-endian

The endianness applies to:
- The `global_message_number` in the definition itself
- All multi-byte field values in corresponding data messages

Single-byte fields and the file header are always little-endian.

Different local message numbers can have different endianness within the
same file (theoretically — in practice, all are little-endian).

### Manufacturer-specific messages

Global message numbers `0xFF00–0xFFFE` are reserved for manufacturer-specific
messages. These are not defined in the public FIT profile and will not be
interoperable between different vendors. A parser should treat them as
unknown messages (skip their data bytes based on the definition).

---

## 10. String fields

String fields are `base_type = 7`. The `size` in the field definition is
the total byte count (including padding/NUL).

To decode:
1. Read `size` bytes
2. Find the first NUL byte (`0x00`)
3. Take everything before the NUL as the string value
4. Interpret as UTF-8 (if invalid UTF-8, the field should be treated as
   absent)

Strings are always exactly `size` bytes in the data message — remaining
bytes after the NUL terminator are padding and must be consumed.

---

## 11. Array fields

When a field's `size` in the definition is larger than its base type's
natural size, the field contains multiple values:

```
num_elements = field.size / base_type.size()
```

Read `num_elements` values of the base type sequentially. Individual
elements that are invalid (sentinel value) should be marked as such but
kept in the array.

If only one element, return it as a scalar. If multiple, return as an array.

---

## 12. Timestamps

### FIT epoch

The FIT epoch is **1989-12-31T00:00:00 UTC** (December 31, 1989).

To convert to Unix timestamp:

```
FIT_EPOCH_OFFSET = 631065600  # seconds between Unix epoch and FIT epoch
unix_timestamp = fit_timestamp + FIT_EPOCH_OFFSET
```

### Absolute vs relative timestamps

The `date_time` type has a special threshold: **`0x10000000`** (268435456).

- If `value >= 0x10000000`: absolute timestamp (seconds since FIT epoch)
- If `value < 0x10000000`: relative timestamp (elapsed seconds, not tied
  to the FIT epoch)

The constant `0x10000000` is defined as `date_time.min` in the FIT profile.
The same threshold applies to `local_date_time` values.

In practice, all activity timestamps from real devices are absolute. Relative
timestamps appear in synthetic or testing files.

### Timestamp fields

Field definition number **253** is always a `date_time` timestamp across all
message types. It's encoded as `uint32` — seconds since the FIT epoch (UTC).

### Common fields

Two field numbers are reserved across all FIT messages:

- **Field 253** (`timestamp`, `date_time`) — UTC timestamp, as described above
- **Field 254** (`message_index`, `message_index`) — sequence number for
  multi-record data. Starts at 0, increments by 1. Used to reference records
  across messages (e.g., a `blood_pressure` message may reference a
  `user_profile` by `message_index`). The high bit (0x8000) is a "selected"
  flag. Not relevant for most activity parsing.

### Local timestamps

The `local_date_time` type represents seconds since midnight December 31,
1989 **local time** (not UTC). When both `timestamp` (UTC) and
`local_timestamp` (local) are present (e.g., in the Activity message), the
timezone offset can be derived:

```
tz_offset_seconds = local_timestamp - timestamp
```

### Timestamp resolution

FIT timestamps have **1-second resolution** in the base format. Sub-second
precision comes from compressed timestamps (5-bit offset within a second)
or from `enhanced_*` fields in newer SDK versions.

---

## 13. Compressed timestamps

Compressed timestamps save space by encoding a 5-bit time offset in the
record header instead of a full 4-byte timestamp field.

### How it works

The device maintains a **base timestamp** (the most recent full timestamp
from field 253 of any message). When a compressed timestamp header is
encountered:

```python
offset = header & 0x1F                    # 5-bit value (0-31)
mask = 0x1F                               # low 5 bits
timestamp = offset + (base_timestamp & ~mask)  # replace low 5 bits

# Rollover: if offset < current low 5 bits, a wrap occurred
if offset < (base_timestamp & mask):
    timestamp += 32

base_timestamp = timestamp                # update for next message
```

The 5-bit offset replaces the low 5 bits of the base timestamp. If the new
offset is numerically less than the current low 5 bits, a rollover has
occurred and 32 seconds are added.

### Setting the base timestamp

The base timestamp is set whenever a data message contains field 253
(timestamp). It must be set before the first compressed timestamp message
appears.

### Range

The 5-bit offset allows at most 31 seconds between messages before a full
timestamp is needed. Devices typically emit a full timestamp every few
seconds and use compressed timestamps in between.

---

## 14. CRC-16

FIT uses a **CRC-16** with nibble-based processing (4 bits at a time).

### Lookup table

```
CRC_TABLE = [
    0x0000, 0xCC01, 0xD801, 0x1400,
    0xF001, 0x3C00, 0x2800, 0xE401,
    0xA001, 0x6C00, 0x7800, 0xB401,
    0x5000, 0x9C01, 0x8801, 0x4400,
]
```

### Algorithm

Initial CRC value: **0x0000**

For each byte:

```python
def update_crc(crc: int, byte: int) -> int:
    # Process low nibble
    tmp = CRC_TABLE[crc & 0xF]
    crc = (crc >> 4) & 0x0FFF
    crc = crc ^ tmp ^ CRC_TABLE[byte & 0xF]

    # Process high nibble
    tmp = CRC_TABLE[crc & 0xF]
    crc = (crc >> 4) & 0x0FFF
    crc = crc ^ tmp ^ CRC_TABLE[(byte >> 4) & 0xF]

    return crc
```

### What bytes are CRC'd

**Header CRC** (optional, at offset 12–13):
- Covers bytes 0–11 of the header

**File CRC** (at offset `data_end`):
- If the header has its own valid CRC: covers only the data section
  (bytes `header_size` to `data_end - 1`)
- If the header has no CRC (12-byte header or `header_crc == 0`): covers
  the header + data section (bytes 0 to `data_end - 1`)

The 2-byte CRC value itself is NOT included in either calculation.

---

## 15. Developer fields

Developer fields allow CIQ (Connect IQ) apps to add custom data to FIT
messages. They use a two-phase mechanism.

### Phase 1: Registration

Two message types register developer fields:

**DeveloperDataId** (global message number 207):
- Field 1: `application_id` — 16-byte UUID identifying the CIQ app
- Field 3: `developer_data_index` — u8 index used in subsequent references

**FieldDescription** (global message number 206):
- Field 0: `developer_data_index` — links to the DeveloperDataId
- Field 1: `field_definition_number` — the field number used in data
  messages
- Field 2: `fit_base_type_id` — base type for decoding the raw bytes
- Field 3: `field_name` — string name of the field (max 64 bytes)
- Field 6: `units` — string units (max 16 bytes)
- Field 8: `native_field_num` — if set, this developer field is considered
  equivalent to the corresponding native field number

**native_field_num caveat:** When a developer field overrides a native field,
it must preserve the native field's units. However, **scale and offset from
Profile.xlsx must NOT be applied** to the developer field. The developer
field is logged at full precision using the appropriate base data type.
For example, if overriding `total_hemoglobin_conc` (native scale=100), the
developer field should be logged as a float, not as a scaled integer.

### Phase 2: Data

Developer field definitions appear in definition messages (when bit 5 of
the record header is set). Data messages then include the developer field
bytes after the regular field bytes.

To decode a developer field:
1. Look up the `FieldDescription` by `(developer_data_index, field_number)`
2. Use `fit_base_type_id` from the description (not from the definition
   message) to interpret the bytes
3. Apply scale, offset, and units from the description

### Parser requirement

The parser must maintain a map of `(developer_data_index, field_number)` →
`FieldDescription` accumulated during parsing. Developer field descriptions
can appear at any point in the file and apply to all subsequent data
messages.

---

## 16. Profile: messages and fields

The FIT profile (Profile.xlsx) defines what messages and fields exist. It's
separate from the binary format — a parser can read any FIT file without a
profile, but can't assign meaning to fields without one.

### Where to get it

Profile.xlsx is distributed with the FIT SDK and also available at:
https://github.com/garmin/fit-sdk-tools/blob/main/Profile.xlsx

The repo also contains `FitCSVTool.jar` (converts binary FIT ↔ readable
CSV, useful for debugging) and example FIT files with their CSV decodes.

### Profile.xlsx structure

Two key sheets:

**Types sheet** — defines all enum types and their value mappings:

| Column | Description |
|--------|-------------|
| Type Name | Enum name (e.g., `sport`, `manufacturer`, `mesg_num`) |
| Base Type | Underlying binary type (e.g., `enum`, `uint16`) |
| Value | Numeric value |
| Name | String name for this value (e.g., `cycling`, `garmin`) |

**Messages sheet** — defines all messages and their fields:

| Column | Description |
|--------|-------------|
| Message Name | Message type (e.g., `record`, `session`) |
| Field Def # | Field definition number (the `field_definition_number` in binary) |
| Field Name | Human-readable name |
| Field Type | Data type — either a base type or an enum name from the Types sheet |
| Array | `[N]` if the field is a fixed-size array |
| Components | Comma-separated destination field names for component expansion |
| Scale | Scale factor (see [section 17](#17-scale-and-offset)) |
| Offset | Offset value |
| Units | Unit string |
| Bits | Comma-separated bit widths for component extraction |
| Accumulate | Comma-separated `0`/`1` per component — whether to accumulate |
| Ref Field Name | Reference field for dynamic/subfield resolution |
| Ref Field Value | Value of the reference field that activates this subfield |
| Comment | Human-readable notes |

Rows without a Field Def # are **subfields** — they share the field number
of the preceding main field and are activated by the Ref Field Name/Value
columns. See [section 20](#20-subfields-dynamic-fields).

### Key message numbers

| Global # | Message | Description |
|----------|---------|-------------|
| 0 | file_id | File type, manufacturer, serial number |
| 18 | session | Session summary (sport, duration, distance) |
| 19 | lap | Lap boundary and summary |
| 20 | record | Per-second timeseries data (the bulk of the file) |
| 21 | event | Timer events, gear changes, recovery HR |
| 23 | device_info | Device metadata (manufacturer, product, serial) |
| 49 | file_creator | Software version that created the file |
| 206 | field_description | Developer field metadata |
| 207 | developer_data_id | CIQ app identification |

---

## 17. Scale and offset

Some fields store values at a different scale than their natural units.
The profile defines `scale` and `offset` per field.

### Formula

```
decoded_value = raw_value / scale - offset
```

**Note the order:** divide by scale first, then subtract offset. This is
NOT `(raw_value - offset) / scale`.

### When to apply

- If `scale == 1.0` and `offset == 0.0`: no conversion needed, return the
  raw integer value as-is
- Otherwise: convert to float and apply the formula, return as float

### Examples from the profile

| Field | Scale | Offset | Raw → Decoded |
|-------|-------|--------|---------------|
| `speed` | 1000 | 0 | 2345 → 2.345 m/s |
| `altitude` | 5 | 500 | 3000 → 100.0 m |
| `position_lat` | 1 | 0 | 623456789 → 623456789 semicircles |
| `total_timer_time` | 1000 | 0 | 3842700 → 3842.7 s |
| `total_distance` | 100 | 0 | 4523050 → 45230.5 m |

### GPS coordinates

Position fields (`position_lat`, `position_long`) are stored as signed
32-bit integers in **semicircles**. To convert to degrees:

```
degrees = semicircles × (180 / 2^31)
```

This conversion is NOT part of the scale/offset system — it's a separate
unit conversion that the parser applies.

---

## 18. Component fields

Some fields pack multiple sub-values into a single binary value using
bit-level extraction. The profile defines these in the `Components` and
`Bits` columns.

### Example: `compressed_speed_distance` (field 0 in record message)

This is a 3-byte field that contains:
- Bits 0–11: `speed` (12 bits, scale 1000)
- Bits 12–23: `distance` (12 bits, scale 100, accumulated)

### Extraction algorithm

Bits are extracted **LSB-first** from the raw bytes:

```python
def extract_component(data: bytes, bit_offset: int, num_bits: int) -> int:
    """Extract num_bits starting at bit_offset within the byte array."""
    result = 0
    byte_idx = bit_offset // 8
    bit_idx = bit_offset % 8

    for i in range(num_bits):
        if data[byte_idx] & (1 << bit_idx):
            result |= (1 << i)
        bit_idx += 1
        if bit_idx == 8:
            bit_idx = 0
            byte_idx += 1

    return result
```

After extraction, the sub-value may need:
1. Scale/offset applied
2. Insertion into the field map as if it were a standalone field (e.g.,
   extracted `speed` becomes field 6)
3. Further component expansion (cascaded components)
4. Accumulation (if marked in the profile)

### Implementation note

Component expansion is profile-driven. For each field that has components,
the profile specifies: which sub-fields, how many bits each, and what field
numbers they map to. A selective parser only needs to handle components for
fields it actually decodes.

---

## 19. Accumulated fields

Some fields represent **cumulative totals** that increase across messages.
The profile marks these with `accumulate = true`.

### How it works

The device may reset the raw value at boundaries. The parser maintains a
running total per (message_type, field_number) pair:

```python
accumulators = {}  # (mesg_num, field_num) -> running_total

def accumulate(mesg_num, field_num, raw_value):
    key = (mesg_num, field_num)
    if key not in accumulators:
        accumulators[key] = raw_value
        return raw_value
    accumulators[key] += raw_value
    return accumulators[key]
```

### Common accumulated fields

- `distance` when extracted from `compressed_speed_distance` components

### Reset

Accumulators are reset when the file CRC is reached (end of a FIT file
section). For chained files, each section starts fresh.

---

## 20. Subfields (dynamic fields)

Some fields have conditional interpretation based on the value of another
field in the same message. The official SDK calls these **dynamic fields**
(the profile calls them subfields).

### How they work

Subfields have no field definition number of their own — they share the
field number of the main field. The interpretation changes based on a
**reference field** value:

- Subfields must have one or more reference field/value combinations
- Reference fields must be integer type (no floats)
- If the reference field matches, interpret using the subfield's name,
  type, scale, offset
- If no reference matches, interpret as the main field

### Example

Field #3 (`data`) in the `event` message is a dynamic field:
- If `event == timer` → `data` is interpreted as `timer_trigger`
- If `event == battery` → `data` is interpreted as `battery_level`
- If `event == fitness_equipment` → `data` is interpreted as
  `fitness_equipment_state`
- If `event == front_gear_change` or `rear_gear_change` → `data` is
  interpreted as `gear_change_data`, which itself has component expansion
  into `rear_gear_num`, `rear_gear`, `front_gear_num`, `front_gear`

### Subfields with components

Subfields may themselves contain components (bit-packed sub-values).
Components can also be nested — the SDK calls these "nested components."

### Implementation note

Subfields only affect how the value is named and typed. The raw binary
value is the same regardless. For a parser that uses the main field name
(e.g., always calls it `data`), subfields can be ignored. They matter
primarily for enum resolution and component expansion.

---

## 21. Chained FIT files

A single `.fit` file on disk can contain multiple FIT files concatenated:

```
┌─ FIT file 1 ─────────────────────────┐
│ Header₁ │ Data₁ │ CRC₁              │
├─ FIT file 2 ─────────────────────────┤
│ Header₂ │ Data₂ │ CRC₂              │
├─ FIT file 3 ─────────────────────────┤
│ Header₃ │ Data₃ │ CRC₃              │
└──────────────────────────────────────┘
```

Each section is independent:
- Definition message mappings reset at each header
- Compressed timestamp base resets
- Accumulated field totals reset
- Developer field descriptions reset

### Detection

After reading a file CRC, check if there are remaining bytes. If so, parse
the next header. The parser loop is:

```
while bytes_remaining > 0:
    parse_header()
    parse_data_records()
    parse_crc()
    reset_state()
```

### Multi-activity files

Multi-activity FIT files (triathlon, multisport) are typically structured as
chained FIT files, each containing one Session message. Alternatively, a
single FIT section may contain multiple Session messages separated by Lap
and Event messages.

---

## 22. Activity file structure

Activity files are the most common FIT file type. They store sensor data,
GPS tracks, laps, and events from an active session.

### Required messages

Every valid Activity file must contain:

| Message | Purpose |
|---------|---------|
| File Id | First message. Type property = 4 (activity). Contains manufacturer, product, serial number. |
| Activity | Usually last message. Contains local_timestamp and num_sessions. May be missing in truncated files. |
| Session | One or more. Summary: sport, duration, distance. Multi-sport files have one per leg + transitions. |
| Lap | At least one per session. Lap boundaries and triggers. Sequential, non-overlapping. |
| Record | Per-second timeseries. Timestamp + at least one data value required. |

### Optional messages

| Message | Purpose |
|---------|---------|
| Device Info | Device/sensor metadata. device_index=0 is the creator. |
| Event | Timer start/stop, gear changes, workout step triggers. |
| Length | Pool swim / track lap summaries. |
| Segment Lap | Live segment results. |
| User Profile | Age, weight, height, resting HR at recording time. |
| HRV | RR interval data from HR monitors. |
| Workout / Workout Step | Structured workout definitions. |
| Zones Target | HR/power zone configuration. |

### Message ordering patterns

Files may use two encoding patterns:

**Summary last** (most common): Record messages appear first, followed by
Lap and Session summaries at the end. The Activity message is last.

**Summary first**: Summary messages appear before their Record messages.
Used by some platforms when streaming data.

A robust parser should handle both patterns. The Garmin cookbook recommends
reading the entire file, grouping by message type, then processing by
start_time rather than file order.

### Field definition order

Field definitions within a definition message do NOT need to be in order
of increasing field number. However, the field data in data messages MUST
follow the exact same order as defined in the definition message. This is
a critical implementation detail — you read fields in definition order, not
field-number order.

---

## 23. Truncated and corrupt files

Activity files may be truncated if the device lost power or the sync was
interrupted during transfer. The official SDK guidance:

- **IsFIT()**: Quick check — bytes 8–11 must be ASCII `.FIT`
- **CheckIntegrity()**: Full CRC check. Reads the entire file twice (once
  for CRC, once for decode). **Not recommended for Activity files** — it's
  inefficient and prevents recovery of partial data.
- **Read()**: Parse the file and recover as much data as possible. Use
  try/catch to handle errors gracefully.

For Activity files, the recommended approach is to **skip integrity checking
and parse with error recovery**. A truncated file may still contain valid
Record, Lap, and Session data up to the point of corruption. The Activity
message (usually last) may be missing — dependencies on it should be
minimized.

The file CRC will not match for truncated files. A parser should treat CRC
failure as a warning, not a fatal error, for Activity files.

---

## 24. Key message types for activity parsing

The messages most relevant to parsing fitness activity data. Field numbers
reference FIT SDK 21.171.00.

### record (global #20)

Per-second timeseries data. The bulk of any activity file. Devices may use
**Smart Recording**, which only writes a Record when values change
significantly, resulting in irregular time intervals (not always 1-second).
Parsers should not assume uniform spacing between records.

| Field # | Name | Type | Scale | Units | Notes |
|---------|------|------|-------|-------|-------|
| 253 | timestamp | uint32 | 1 | s | Seconds since FIT epoch |
| 0 | position_lat | sint32 | 1 | semicircles | × (180/2³¹) for degrees |
| 1 | position_long | sint32 | 1 | semicircles | × (180/2³¹) for degrees |
| 2 | altitude | uint16 | 5 | m | offset 500 |
| 3 | heart_rate | uint8 | 1 | bpm | |
| 4 | cadence | uint8 | 1 | rpm | |
| 5 | distance | uint32 | 100 | m | Cumulative |
| 6 | speed | uint16 | 1000 | m/s | |
| 7 | power | uint16 | 1 | watts | |
| 13 | temperature | sint8 | 1 | °C | |
| 73 | enhanced_speed | uint32 | 1000 | m/s | Preferred over field 6 |
| 78 | enhanced_altitude | uint32 | 5 | m | offset 500, preferred over field 2 |

**Component field:** Field 0 may also appear as `compressed_speed_distance`
(byte[3]) in older devices/profiles. It packs speed (12 bits, scale 1000)
and distance (12 bits, scale 100, accumulated) into 3 bytes. When present,
it expands into fields 5 (distance) and 6 (speed). Modern devices use
fields 5, 6, 73, and 78 directly instead.

### session (global #18)

One per activity (or per sport in multi-sport files).

| Field # | Name | Type | Scale | Units |
|---------|------|------|-------|-------|
| 253 | timestamp | uint32 | 1 | s |
| 2 | start_time | uint32 | 1 | s |
| 5 | sport | enum | — | — |
| 6 | sub_sport | enum | — | — |
| 7 | total_elapsed_time | uint32 | 1000 | s |
| 8 | total_timer_time | uint32 | 1000 | s |
| 9 | total_distance | uint32 | 100 | m |
| 253 | local_timestamp | uint32 | 1 | s |

### device_info (global #23)

Emitted per device (head unit, sensors, CIQ apps). Often re-emitted per
session.

| Field # | Name | Type | Scale | Units |
|---------|------|------|-------|-------|
| 253 | timestamp | uint32 | 1 | s |
| 0 | device_index | uint8 | — | — |
| 1 | device_type | uint8 | — | — |
| 2 | manufacturer | uint16 | — | — |
| 3 | serial_number | uint32z | — | — |
| 4 | product | uint16 | — | — |
| 27 | product_name | string | — | — |
| 25 | ant_device_number | uint16z | — | — |

`device_index == 0` identifies the creator device (head unit).

### lap (global #19)

One per lap. Marks boundaries for per-lap analysis.

| Field # | Name | Type | Scale | Units |
|---------|------|------|-------|-------|
| 253 | timestamp | uint32 | 1 | s |
| 2 | start_time | uint32 | 1 | s |
| 24 | lap_trigger | enum | — | — |

`lap_trigger` values: 0=manual, 1=time, 2=distance, 3=position_start,
4=position_lap, 5=position_waypoint, 6=position_marked, 7=session_end,
8=fitness_equipment.

### event (global #21)

Timer start/stop, gear changes, recovery HR measurements.

| Field # | Name | Type | Scale | Units |
|---------|------|------|-------|-------|
| 253 | timestamp | uint32 | 1 | s |
| 0 | event | enum | — | — |
| 1 | event_type | enum | — | — |
| 3 | data | uint32 | — | — |

### activity (global #34)

Top-level container. One per file, summarizes session count.

| Field # | Name | Type | Scale | Units |
|---------|------|------|-------|-------|
| 253 | timestamp | uint32 | 1 | s |
| 5 | local_timestamp | uint32 | 1 | s |
| 1 | num_sessions | uint16 | — | — |
