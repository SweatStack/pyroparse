//! FIT message decoder.
//!
//! Decodes raw binary events from [`FitReader`] into typed structures using
//! the generated profile definitions.
//!
//! Two modes:
//! - [`scan_metadata`]: metadata-only scan (skips Record data)
//! - [`full_parse`]: complete parse producing `ParseResult` with Record
//!   data, metadata, laps, and extra columns

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::fit::binary::{FitEvent, FitReader, MessageDef};
use crate::fit::profile::{self, BaseType};
use crate::reference::classify_developer_field;
use crate::fields::{normalize_field_name, is_canonical_column, is_handled_field};
use crate::types::{TypedColumn, promote_type, base_type_to_arrow, field_arrow_type, read_raw_f64};
use crate::summary::{Interval, Kind, Value, SUMMARY_FIELDS};
use crate::{
    AthleteMeta, SessionMeta, DeviceMeta, ScanResult, ParseResult,
    RecordRow, LapBoundary, LengthInterval, SEMICIRCLE_TO_DEGREES,
    CourseResult, CoursePoint, CourseMeta,
    classify_developer_sensors, bytes_to_uuid,
    column_for_developer_field,
};

// ---------------------------------------------------------------------------
// Byte-level field reading
// ---------------------------------------------------------------------------

/// Read a u8 from a field's bytes. Returns None if invalid (0xFF).
#[inline]
fn read_u8_valid(data: &[u8]) -> Option<u8> {
    let v = *data.first()?;
    if v == 0xFF { None } else { Some(v) }
}

/// Read a sint8 from a field's bytes. Returns None if invalid (0x7F).
#[inline]
fn read_i8_valid(data: &[u8]) -> Option<i8> {
    let v = *data.first()? as i8;
    if v == 0x7F { None } else { Some(v) }
}

/// Read a u16 from field bytes with endianness. Returns None if invalid (0xFFFF).
#[inline]
fn read_u16(data: &[u8], big_endian: bool) -> Option<u16> {
    if data.len() < 2 { return None; }
    let v = if big_endian {
        u16::from_be_bytes([data[0], data[1]])
    } else {
        u16::from_le_bytes([data[0], data[1]])
    };
    if v == 0xFFFF { None } else { Some(v) }
}

/// Read a u32 from field bytes with endianness. Returns None if invalid (0xFFFFFFFF).
#[inline]
fn read_u32(data: &[u8], big_endian: bool) -> Option<u32> {
    if data.len() < 4 { return None; }
    let v = if big_endian {
        u32::from_be_bytes([data[0], data[1], data[2], data[3]])
    } else {
        u32::from_le_bytes([data[0], data[1], data[2], data[3]])
    };
    if v == 0xFFFFFFFF { None } else { Some(v) }
}

/// Read a u32z from field bytes. Returns None if invalid (0x00000000).
#[inline]
fn read_u32z(data: &[u8], big_endian: bool) -> Option<u32> {
    if data.len() < 4 { return None; }
    let v = if big_endian {
        u32::from_be_bytes([data[0], data[1], data[2], data[3]])
    } else {
        u32::from_le_bytes([data[0], data[1], data[2], data[3]])
    };
    if v == 0 { None } else { Some(v) }
}

/// Read a NUL-terminated string from field bytes.
#[inline]
fn read_string(data: &[u8]) -> Option<String> {
    let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
    if end == 0 { return None; }
    String::from_utf8(data[..end].to_vec()).ok()
}

// ---------------------------------------------------------------------------
// Field extraction from raw bytes
// ---------------------------------------------------------------------------

/// Helper to iterate over fields in a data message, yielding (field_number,
/// field_bytes) pairs using the definition's field layout.
struct FieldIter<'a> {
    def: &'a MessageDef,
    field_bytes: &'a [u8],
    index: usize,
    offset: usize,
}

impl<'a> FieldIter<'a> {
    fn new(def: &'a MessageDef, field_bytes: &'a [u8]) -> Self {
        Self { def, field_bytes, index: 0, offset: 0 }
    }
}

impl<'a> Iterator for FieldIter<'a> {
    type Item = (u8, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let field = self.def.fields.get(self.index)?;
        let size = field.size as usize;
        let end = self.offset + size;
        if end > self.field_bytes.len() {
            return None;
        }
        let data = &self.field_bytes[self.offset..end];
        self.offset = end;
        self.index += 1;
        Some((field.number, data))
    }
}

/// Read a data message's field by its profile name, decoded as the profile
/// says: the bytes with the base type the file's definition declares (that
/// type's invalid value reads `None`), then `raw / scale - offset`. `None`
/// when the message does not carry the field. No field number or scale is
/// restated in code (AGENTS.md, "Resolve fields from the profile").
fn read_field(
    def: &MessageDef,
    field_bytes: &[u8],
    table: &[profile::FieldDef],
    name: &str,
) -> Option<f64> {
    let pf = table.iter().find(|f| f.name == name)?;
    let mut offset = 0usize;
    for field in &def.fields {
        let end = offset + field.size as usize;
        if field.number == pf.number {
            let data = field_bytes.get(offset..end)?;
            let raw = read_raw_f64(data, BaseType::from_byte(field.base_type), def.big_endian)?;
            return Some(raw / pf.scale - pf.offset);
        }
        offset = end;
    }
    None
}

// ---------------------------------------------------------------------------
// Metadata scanner
// ---------------------------------------------------------------------------

/// Scan a FIT file for metadata only, skipping Record data.
///
/// This is the replacement for the hand-written `FitScanner`. It uses the
/// binary reader and generated profile to decode Session, DeviceInfo,
/// Activity, DeveloperDataId, and FieldDescription messages.
/// Decode file_id type field (field 0, enum) → file type string.
fn decode_file_type(def: &MessageDef, field_bytes: &[u8]) -> Option<String> {
    for (num, data) in FieldIter::new(def, field_bytes) {
        if num == 0 {
            if let Some(v) = read_u8_valid(data) {
                return Some(profile::file_name(v).to_string());
            }
        }
    }
    None
}

pub fn scan_metadata(data: &[u8]) -> Result<ScanResult, String> {
    let mut reader = FitReader::new(data)
        .map_err(|e| e.to_string())?;

    let mut result = ScanResult::default();
    let mut metric_set = HashSet::new();
    let mut current_app_for_idx: BTreeMap<u8, String> = BTreeMap::new();
    let mut dev_field_owners: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut utc_offsets: Vec<UtcOffset> = Vec::new();
    let mut zones: Vec<SessionZones> = Vec::new();
    let mut section_base = 0usize;

    while let Some(event) = reader.next().map_err(|e| e.to_string())? {
        match event {
            FitEvent::Definition { local, global_message_number } => {
                // Detect available metrics from Record definitions.
                if global_message_number == profile::MESG_RECORD {
                    if let Some(def) = reader.def(local) {
                        detect_metrics_from_def(def, &mut metric_set);
                    }
                }
            }

            FitEvent::Data { local, field_bytes, .. }
            | FitEvent::CompressedData { local, field_bytes, .. } => {
                let def = reader.def(local)
                    .ok_or("data message without preceding definition")?;
                let global = def.global_message_number;

                match global {
                    profile::MESG_FILE_ID => {
                        if result.file_type.is_none() {
                            result.file_type = decode_file_type(def, field_bytes);
                        }
                    }
                    profile::MESG_RECORD => {
                        // Skip — we only need metadata.
                    }
                    profile::MESG_SESSION => {
                        result.sessions.push(decode_session(def, field_bytes));
                    }
                    profile::MESG_TIME_IN_ZONE => {
                        zones.extend(decode_time_in_zone(def, field_bytes, section_base));
                    }
                    profile::MESG_ACTIVITY => {
                        if let Some(offset) = decode_activity_offset(def, field_bytes) {
                            utc_offsets.push(offset);
                        }
                    }
                    profile::MESG_DEVICE_INFO => {
                        if let Some(mut d) = decode_device(def, field_bytes) {
                            d.last_seen = result.devices.len();
                            result.devices.push(d);
                        }
                    }
                    profile::MESG_DEVELOPER_DATA_ID => {
                        if let Some((idx, uuid)) = decode_developer_data_id(def, field_bytes) {
                            current_app_for_idx.insert(idx, uuid);
                        }
                    }
                    profile::MESG_FIELD_DESCRIPTION => {
                        if let Some(fd) = decode_field_description(def, field_bytes) {
                            note_developer_metrics(&fd.desc.name, &mut metric_set);
                            register_field_owner(&fd, &current_app_for_idx, &mut dev_field_owners);
                        }
                    }
                    _ => {}
                }
            }

            FitEvent::FileHeader(_) => {
                // Sessions and devices accumulate across chained sections;
                // `time_in_zone` indices count from each section's first session.
                section_base = result.sessions.len();
            }
            FitEvent::Crc { .. } => {}
        }
    }

    result.record_metrics = metric_set.into_iter().collect();

    let empty = BTreeSet::new();
    result.developer_sensors = classify_developer_sensors(
        &dev_field_owners,
        &empty,
        false,
    );

    resolve_local_start_times(&mut result.sessions, &utc_offsets);
    resolve_athlete(&mut result.sessions, &zones);

    Ok(result)
}

// ---------------------------------------------------------------------------
// Per-message decoders
// ---------------------------------------------------------------------------

fn detect_metrics_from_def(def: &MessageDef, metrics: &mut HashSet<String>) {
    let mut has_lat = false;
    let mut has_long = false;

    for field in &def.fields {
        match field.number {
            3 => { metrics.insert("heart_rate".into()); }
            7 => { metrics.insert("power".into()); }
            6 | 73 => { metrics.insert("speed".into()); }
            4 => { metrics.insert("cadence".into()); }
            0 => has_lat = true,
            1 => has_long = true,
            2 | 78 => { metrics.insert("altitude".into()); }
            13 => { metrics.insert("temperature".into()); }
            5 => { metrics.insert("distance".into()); }
            _ => {}
        }
    }

    if has_lat && has_long {
        metrics.insert("gps".into());
    }
}

fn decode_session(def: &MessageDef, field_bytes: &[u8]) -> SessionMeta {
    let mut s = SessionMeta::default();
    let be = def.big_endian;

    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            5 => {
                // sport (enum, 1 byte)
                if let Some(v) = read_u8_valid(data) {
                    s.sport = Some(profile::sport_name(v).to_string());
                }
            }
            6 => {
                // sub_sport (enum, 1 byte)
                if let Some(v) = read_u8_valid(data) {
                    s.sub_sport = Some(profile::sub_sport_name(v).to_string());
                }
            }
            2 => {
                // start_time (uint32, date_time)
                if let Some(ts) = read_u32(data, be) {
                    let unix = ts as i64 + profile::FIT_EPOCH_OFFSET;
                    s.start_time = Some(unix as f64);
                    s.start_timestamp_us = Some(unix * 1_000_000);
                }
            }
            44 => {
                // pool_length (uint16, scale 100, metres) — pool swims only.
                if let Some(v) = read_u16(data, be) {
                    s.pool_length = Some(v as f64 / 100.0);
                }
            }
            _ => {}
        }
    }

    s
}

/// The Session fields the summary takes from the device or the athlete
/// (`SUMMARY_FIELDS` of kind `Device` and `Entered`), read by profile name.
/// Full parse only: on an opened file the summary loads with the records.
fn decode_session_summary(def: &MessageDef, field_bytes: &[u8]) -> Vec<(&'static str, Value)> {
    SUMMARY_FIELDS
        .iter()
        .filter(|f| f.kind != Kind::Computed)
        .filter_map(|f| {
            let v = read_field(def, field_bytes, profile::SESSION_FIELDS, f.source)?;
            Some((f.name, Value::typed(f.ty, v / f.divisor)))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Athlete — the heart rates the device applied to each session
// ---------------------------------------------------------------------------

/// A `time_in_zone` message (216) that describes a session.
struct SessionZones {
    /// Index of the session among all sessions of the (possibly chained) file.
    session: usize,
    athlete: AthleteMeta,
}

/// Decode a `time_in_zone` message whose `reference_mesg` is `session`;
/// entries for laps are ignored. `reference_index` counts the Session
/// messages of its own FIT file, so in a chained file it is offset by
/// `section_base`, the sessions of the sections before it.
fn decode_time_in_zone(def: &MessageDef, field_bytes: &[u8], section_base: usize) -> Option<SessionZones> {
    let read = |name| read_field(def, field_bytes, profile::TIME_IN_ZONE_FIELDS, name);
    if read("reference_mesg")? != f64::from(profile::MESG_SESSION) {
        return None;
    }
    let index = read("reference_index")? as usize;
    // 0 is "not set": Garmin writes `threshold_heart_rate = 0` for a sport
    // without one. A device sentinel, read as missing (docs/FIT-FORMAT.md §8).
    let heart_rate = |name| read(name).filter(|&v| v != 0.0).map(|v| v as i64);
    Some(SessionZones {
        session: section_base + index,
        athlete: AthleteMeta {
            max_heart_rate: heart_rate("max_heart_rate"),
            threshold_heart_rate: heart_rate("threshold_heart_rate"),
        },
    })
}

/// Give each session the athlete values of its `time_in_zone` entry. An index
/// beyond the file's sessions is ignored; a later entry for the same session
/// overwrites the values it carries.
fn resolve_athlete(sessions: &mut [SessionMeta], zones: &[SessionZones]) {
    for zone in zones {
        let Some(session) = sessions.get_mut(zone.session) else { continue };
        let athlete = &mut session.athlete;
        athlete.max_heart_rate = zone.athlete.max_heart_rate.or(athlete.max_heart_rate);
        athlete.threshold_heart_rate =
            zone.athlete.threshold_heart_rate.or(athlete.threshold_heart_rate);
    }
}

// ---------------------------------------------------------------------------
// Running time — timer events
// ---------------------------------------------------------------------------

/// An event of timer group 0. The FIT profile (Types → `event`) puts three
/// events there: `timer` ("Group 0. Start / stop_all"), `power_down` and
/// `power_up` (both "stop_all group 0").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimerEvent {
    Start,
    Stop,
    /// The device restarted after a crash or power loss.
    PowerUp,
}

fn decode_timer_event(def: &MessageDef, field_bytes: &[u8]) -> Option<TimerEvent> {
    let read = |name| read_field(def, field_bytes, profile::EVENT_FIELDS, name).map(|v| v as u8);
    match profile::event_name(read("event")?) {
        "timer" => match profile::event_type_name(read("event_type")?) {
            "start" => Some(TimerEvent::Start),
            "stop" | "stop_all" | "stop_disable" | "stop_disable_all" => Some(TimerEvent::Stop),
            _ => None,
        },
        "power_down" => Some(TimerEvent::Stop),
        "power_up" => Some(TimerEvent::PowerUp),
        _ => None,
    }
}

/// The file's running intervals from its timer events (µs, file order): a
/// `timer` start opens one and any group-0 stop closes it. A `power_up`
/// closes it at the last record before it — the device went down at a power
/// loss the file does not record — the one inference in the model. An
/// interval still open at the end runs on (`i64::MAX`): `summarize` cuts the
/// file's intervals to each session. `None` when the file has no `timer`
/// start: all time counts as running.
fn running_intervals(events: &[(i64, TimerEvent)], records: &[RecordRow]) -> Option<Vec<Interval>> {
    if !events.iter().any(|&(_, e)| e == TimerEvent::Start) {
        return None;
    }
    let mut intervals = Vec::new();
    let mut open: Option<i64> = None;
    for &(at, event) in events {
        match event {
            TimerEvent::Start => {
                open.get_or_insert(at);
            }
            TimerEvent::Stop => intervals.extend(open.take().map(|start| (start, at))),
            TimerEvent::PowerUp => {
                if let Some(start) = open.take() {
                    let last_record = records
                        .iter()
                        .filter_map(|r| r.timestamp)
                        .filter(|&t| t <= at)
                        .max()
                        .unwrap_or(start);
                    intervals.push((start, last_record.max(start)));
                }
            }
        }
    }
    intervals.extend(open.map(|start| (start, i64::MAX)));
    Some(intervals)
}

// ---------------------------------------------------------------------------
// Local time — the UTC offset carried by the Activity message
// ---------------------------------------------------------------------------

/// The UTC offset observed in an Activity message (mesg 34).
///
/// `local_timestamp` (field 5) is the local-time twin of that message's own
/// `timestamp` (field 253) — the *end* of the activity. Only their difference
/// is meaningful; it is applied to each session's `start_time`. Using
/// `local_timestamp` as a start time is wrong by the activity's duration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UtcOffset {
    /// Activity `timestamp`, Unix seconds.
    pub(crate) at: i64,
    /// `local_timestamp - timestamp`, seconds.
    pub(crate) seconds: i64,
}

fn decode_activity_offset(def: &MessageDef, field_bytes: &[u8]) -> Option<UtcOffset> {
    let be = def.big_endian;
    let mut timestamp = None;
    let mut local_timestamp = None;
    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            253 => timestamp = read_u32(data, be),
            5 => local_timestamp = read_u32(data, be),
            _ => {}
        }
    }
    let (ts, local) = (timestamp?, local_timestamp?);
    // Below `date_time.min` a value is relative (seconds since power-on), not
    // an epoch time — some Zwift files write `local_timestamp = 0` — so no
    // offset can be derived from it.
    if ts < profile::DATETIME_MIN || local < profile::DATETIME_MIN {
        return None;
    }
    Some(UtcOffset {
        at: ts as i64 + profile::FIT_EPOCH_OFFSET,
        seconds: local as i64 - ts as i64,
    })
}

/// Set `start_time_local` on every session from the Activity offsets.
///
/// A session takes the offset of the first Activity message (in file order)
/// written at or after its start — the one that summarizes it — and falls
/// back to the file's last Activity message. Message order does not matter,
/// so summary-first files work. Without any Activity message the offset is
/// unknowable and no local time is set. The offset is observed at the end of
/// the activity, so a DST transition mid-activity shifts the local start by
/// an hour; that is inherent to the format.
pub(crate) fn resolve_local_start_times(sessions: &mut [SessionMeta], offsets: &[UtcOffset]) {
    let Some(last) = offsets.last() else { return };
    for session in sessions.iter_mut() {
        let Some(start) = session.start_time else { continue };
        let offset = offsets.iter().find(|o| o.at as f64 >= start).unwrap_or(last);
        session.start_time_local = Some(start + offset.seconds as f64);
    }
}

/// Decode a DeviceInfo message (global 23). The metadata scan and the full
/// parse share this decoder, so both report the same devices. Values are kept
/// as the file writes them; naming happens after re-emissions are merged (see
/// `DeviceMeta::product_key`).
fn decode_device(def: &MessageDef, field_bytes: &[u8]) -> Option<DeviceMeta> {
    let be = def.big_endian;
    let mut d = DeviceMeta::default();
    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            0 => d.device_index = read_u8_valid(data),
            1 => d.device_type = read_u8_valid(data),
            2 => d.manufacturer = read_u16(data, be),
            // serial_number is uint32z: zero is the invalid value.
            3 => d.serial_number = read_u32z(data, be).map(|v| v.to_string()),
            4 => d.product = read_u16(data, be),
            25 => d.source_type = read_u8_valid(data),
            27 => d.product_name = read_string(data),
            _ => {}
        }
    }
    (d.manufacturer.is_some() || d.product.is_some() || d.product_name.is_some()).then_some(d)
}

fn decode_developer_data_id(def: &MessageDef, field_bytes: &[u8]) -> Option<(u8, String)> {
    let mut dev_idx: Option<u8> = None;
    let mut app_id: Option<String> = None;

    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            1 if data.len() >= 16 => {
                // application_id (16 bytes → UUID)
                app_id = bytes_to_uuid(data);
            }
            3 => {
                // developer_data_index (uint8)
                dev_idx = data.first().copied();
            }
            _ => {}
        }
    }

    dev_idx.zip(app_id)
}

// ---------------------------------------------------------------------------
// Developer fields — registration (FieldDescription, mesg 206) and decoding
// ---------------------------------------------------------------------------

/// How to decode one developer field, as declared by its FieldDescription.
///
/// Per the FIT SDK the bytes are interpreted with the description's
/// `fit_base_type_id` — never inferred from the field's name — and the
/// description's own `scale`/`offset` apply as `raw / scale - offset`. The
/// profile's native scale/offset never apply to developer fields.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DevFieldDesc {
    pub(crate) name: String,
    /// `fit_base_type_id` (field 2), a raw base-type byte.
    pub(crate) base_type: u8,
    /// `scale` (field 6); 1 when absent or zero.
    pub(crate) scale: f64,
    /// `offset` (field 7); 0 when absent.
    pub(crate) offset: f64,
    /// Slot of the owning CIQ app in `ParseResult::apps` (see `app_slot`),
    /// once its DeveloperDataId has been seen.
    pub(crate) app: Option<u8>,
}

/// A decoded FieldDescription message.
struct FieldDescription {
    /// `developer_data_index` (field 0), linking to a DeveloperDataId.
    dev_data_index: u8,
    /// `field_definition_number` (field 1), as used in Record definitions.
    /// Absent in malformed files: such a field never matches data but still
    /// identifies its app.
    field_number: Option<u8>,
    desc: DevFieldDesc,
}

/// Decode a FieldDescription. `None` without a `developer_data_index` and a
/// `field_name`, the minimum needed to tie a field to an app.
fn decode_field_description(def: &MessageDef, field_bytes: &[u8]) -> Option<FieldDescription> {
    let mut dev_data_index = None;
    let mut field_number = None;
    let mut name = None;
    let mut base_type = 0x88; // float32 when the description omits it
    let mut scale = 1.0;
    let mut offset = 0.0;

    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            0 => dev_data_index = read_u8_valid(data),
            1 => field_number = read_u8_valid(data),
            2 => base_type = read_u8_valid(data).unwrap_or(base_type),
            3 => name = read_string(data),
            6 => scale = read_u8_valid(data).filter(|&v| v != 0).map_or(scale, f64::from),
            7 => offset = read_i8_valid(data).map_or(offset, f64::from),
            _ => {}
        }
    }

    Some(FieldDescription {
        dev_data_index: dev_data_index?,
        field_number,
        desc: DevFieldDesc { name: name?, base_type, scale, offset, app: None },
    })
}

/// Slot of a CIQ app UUID in `apps`, appending it on first sight. Slots are
/// stable within a file because both parse passes meet DeveloperDataIds in
/// the same order.
fn app_slot(apps: &mut Vec<String>, uuid: &str) -> Option<u8> {
    let i = match apps.iter().position(|u| u == uuid) {
        Some(i) => i,
        None => {
            apps.push(uuid.to_string());
            apps.len() - 1
        }
    };
    u8::try_from(i).ok()
}

/// Attach the owning app's slot to a field description.
fn attach_app(
    fd: &mut FieldDescription,
    current_app_for_idx: &BTreeMap<u8, String>,
    apps: &mut Vec<String>,
) {
    fd.desc.app = current_app_for_idx
        .get(&fd.dev_data_index)
        .and_then(|uuid| app_slot(apps, uuid));
}

/// Record which CIQ apps register a developer field name, for sensor
/// classification. Several apps may register one name (Stryd and the
/// Concept2 data field both write `Power`); per-session data decides which
/// is credited — see `dev_column_winners` in `lib.rs`.
fn register_field_owner(
    fd: &FieldDescription,
    current_app_for_idx: &BTreeMap<u8, String>,
    dev_field_owners: &mut BTreeMap<String, BTreeSet<String>>,
) {
    if let Some(uuid) = current_app_for_idx.get(&fd.dev_data_index) {
        dev_field_owners
            .entry(fd.desc.name.clone())
            .or_default()
            .insert(uuid.clone());
    }
}

/// SmO2 (muscle-oxygen saturation, %) as a reading, or `None` for "no
/// reading". Muscle-oxygen sensors write 0 when they have no signal: the ANT+
/// muscle-oxygen profile has its own "no reading" codes, and devices and
/// Connect IQ apps map them to 0 on the way into the file rather than to the
/// FIT invalid value. A saturation of 0 % is physiologically impossible, so 0
/// is a sentinel, not a measurement. Applies to the native field and to
/// developer fields alike.
fn smo2_reading(percent: f32) -> Option<f32> {
    (percent != 0.0).then_some(percent)
}

/// Store a developer-sourced value, preferring a reading over a zero
/// placeholder when several apps write the same field in one record (the app
/// without its sensor writes 0). Remembers which app supplied the value kept.
fn take_reading<T: Copy + PartialEq + Default>(
    slot: &mut Option<T>,
    app: &mut Option<u8>,
    value: T,
    from: Option<u8>,
) {
    if slot.is_none() || value != T::default() {
        *slot = Some(value);
        *app = from;
    }
}

/// Add the metrics a developer field contributes (metadata scan only, where
/// no Record data is read to confirm them).
fn note_developer_metrics(name: &str, metrics: &mut HashSet<String>) {
    if let Some(metric) = classify_developer_field(name) {
        metrics.insert(metric.to_string());
    }
    let normalized = normalize_field_name(name);
    if !is_canonical_column(&normalized) {
        metrics.insert(normalized);
    }
}

/// Decode a developer field value per its description: the bytes are read
/// with the declared base type and the definition's byte order, then the
/// description's scale and offset are applied. `None` for the base type's
/// invalid sentinel, a non-finite float, or a size mismatch.
fn read_dev_value(data: &[u8], desc: &DevFieldDesc, big_endian: bool) -> Option<f64> {
    let raw = read_raw_f64(data, BaseType::from_byte(desc.base_type), big_endian)?;
    Some(raw / desc.scale - desc.offset)
}

/// Walk a data message's developer field slots, yielding each registered
/// field's description and bytes. Unregistered slots are skipped.
fn dev_field_values<'a>(
    def: &'a MessageDef,
    dev_field_bytes: &'a [u8],
    descs: &'a HashMap<(u8, u8), DevFieldDesc>,
) -> impl Iterator<Item = (&'a DevFieldDesc, &'a [u8])> + 'a {
    let mut offset = 0usize;
    def.dev_fields.iter().filter_map(move |field| {
        let start = offset;
        offset += field.size as usize;
        let data = dev_field_bytes.get(start..offset)?;
        let desc = descs.get(&(field.dev_data_index, field.number))?;
        Some((desc, data))
    })
}

// ---------------------------------------------------------------------------
// Parse configuration
// ---------------------------------------------------------------------------

/// Controls which Record fields are decoded during a full parse.
///
/// When `columns` is `None`, all fields are decoded (the default).
/// When set, only the listed columns are decoded — unwanted standard fields
/// are skipped, and extra columns are only discovered/decoded if requested.
pub struct ParseConfig {
    /// Column names to decode. `None` = all columns.
    pub columns: Option<Vec<String>>,
}

impl ParseConfig {
    /// Build a field-number mask from column names using the profile.
    /// Returns (standard field mask, decode extras flag).
    fn build_field_mask(&self) -> ([bool; 256], bool) {
        let columns = match &self.columns {
            None => return ([true; 256], true), // decode everything
            Some(c) if c.is_empty() => return ([true; 256], true),
            Some(c) => c,
        };

        let mut mask = [false; 256];
        let mut decode_extras = false;

        // Always decode timestamp — needed for session splitting and laps.
        mask[253] = true;

        for col in columns {
            match col.as_str() {
                "timestamp" => { mask[253] = true; }
                "heart_rate" => { mask[3] = true; }
                "power" => { mask[7] = true; }
                "cadence" => { mask[4] = true; }
                "speed" => { mask[6] = true; mask[73] = true; }
                "latitude" => { mask[0] = true; }
                "longitude" => { mask[1] = true; }
                "altitude" => { mask[2] = true; mask[78] = true; }
                "temperature" => { mask[13] = true; }
                "distance" => { mask[5] = true; }
                "smo2" => { mask[57] = true; }
                "core_temperature" => { mask[139] = true; }
                "respiration_rate" => { mask[99] = true; mask[108] = true; }
                // "lap" and "lap_trigger" are synthesized from Lap messages,
                // not from Record fields — always available.
                "lap" | "lap_trigger" => {}
                // Anything else is an extra column or developer field.
                _ => { decode_extras = true; }
            }
        }

        (mask, decode_extras)
    }
}

impl Default for ParseConfig {
    fn default() -> Self {
        Self { columns: None }
    }
}

// ---------------------------------------------------------------------------
// Full parse (records + metadata + laps + extras)
// ---------------------------------------------------------------------------

/// Fully parse a FIT file with optional column selection.
///
/// This is a two-pass design:
/// - Pass 1: scan definitions to discover extra columns + collect metadata
/// - Pass 2: decode Record fields into RecordRow + fill extra columns
///
/// When `config.columns` is set, only the requested fields are decoded.
pub fn full_parse(data: &[u8], config: &ParseConfig) -> Result<ParseResult, String> {
    let (field_mask, decode_extras) = config.build_field_mask();
    // ── Pass 1: metadata + extra column discovery ────────────────────────
    let mut reader = FitReader::new(data).map_err(|e| e.to_string())?;

    let mut file_type: Option<String> = None;
    let mut sessions = Vec::new();
    let mut devices = Vec::new();
    let mut laps = Vec::new();
    let mut lengths: Vec<LengthInterval> = Vec::new();
    let mut current_app_for_idx: BTreeMap<u8, String> = BTreeMap::new();
    let mut dev_field_owners: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut utc_offsets: Vec<UtcOffset> = Vec::new();
    let mut zones: Vec<SessionZones> = Vec::new();
    let mut section_base = 0usize;
    // CIQ app UUIDs by slot; `DevFieldDesc::app` and the `RecordRow::*_app`
    // fields index into this.
    let mut apps: Vec<String> = Vec::new();

    let mut n_rows = 0usize;
    let mut extra_types: BTreeMap<String, DataType> = BTreeMap::new();

    // Track which field numbers map to which normalized extra column names.
    // Key: field_number, Value: normalized column name (or None if handled).
    let mut field_to_extra: HashMap<u8, Option<String>> = HashMap::new();

    // Developer field registrations, for extra column discovery and decoding.
    // Key: (developer_data_index, field_number) as used in Record definitions.
    let mut dev_field_descs: HashMap<(u8, u8), DevFieldDesc> = HashMap::new();

    while let Some(event) = reader.next().map_err(|e| e.to_string())? {
        match event {
            FitEvent::Definition { local, global_message_number } => {
                if global_message_number == profile::MESG_RECORD {
                    if let Some(def) = reader.def(local) {
                        if decode_extras {
                        // Discover extra columns from field definitions.
                        for field in &def.fields {
                            if field_to_extra.contains_key(&field.number) {
                                continue;
                            }
                            // Look up in profile to get the field name.
                            if let Some(pf) = profile::FieldDef::lookup(profile::RECORD_FIELDS, field.number) {
                                if is_handled_field(pf.name) {
                                    field_to_extra.insert(field.number, None);
                                } else {
                                    let normalized = normalize_field_name(pf.name);
                                    if is_canonical_column(&normalized) {
                                        field_to_extra.insert(field.number, None);
                                    } else {
                                        // Arrow type from the base type, widened
                                        // to float when the profile scales it.
                                        if let Some(dtype) = field_arrow_type(field.base_type, pf.scale, pf.offset) {
                                            match extra_types.get_mut(&normalized) {
                                                Some(existing) => *existing = promote_type(existing, &dtype),
                                                None => { extra_types.insert(normalized.clone(), dtype); }
                                            }
                                            field_to_extra.insert(field.number, Some(normalized));
                                        } else {
                                            field_to_extra.insert(field.number, None);
                                        }
                                    }
                                }
                            } else {
                                // Unknown field — not in profile. Treat as extra.
                                let name = format!("unknown_field_{}", field.number);
                                if let Some(dtype) = base_type_to_arrow(field.base_type) {
                                    match extra_types.get_mut(&name) {
                                        Some(existing) => *existing = promote_type(existing, &dtype),
                                        None => { extra_types.insert(name.clone(), dtype); }
                                    }
                                    field_to_extra.insert(field.number, Some(name));
                                } else {
                                    field_to_extra.insert(field.number, None);
                                }
                            }
                        }

                        // Developer fields in Record definitions → extra columns.
                        for dev_field in &def.dev_fields {
                            let key = (dev_field.dev_data_index, dev_field.number);
                            if let Some(desc) = dev_field_descs.get(&key) {
                                if !is_handled_field(&desc.name) {
                                    if let Some(col) = column_for_developer_field(&desc.name) {
                                        let dtype = DataType::Float64;
                                        match extra_types.get_mut(&col) {
                                            Some(existing) => *existing = promote_type(existing, &dtype),
                                            None => { extra_types.insert(col, dtype); }
                                        }
                                    }
                                }
                            }
                        }
                        } // if decode_extras
                    }
                }
            }

            FitEvent::Data { local, field_bytes, .. }
            | FitEvent::CompressedData { local, field_bytes, .. } => {
                let def = reader.def(local)
                    .ok_or("data message without preceding definition")?;
                let global = def.global_message_number;

                match global {
                    profile::MESG_FILE_ID => {
                        if file_type.is_none() {
                            file_type = decode_file_type(def, field_bytes);
                        }
                    }
                    profile::MESG_RECORD => { n_rows += 1; }
                    profile::MESG_SESSION => {
                        let mut session = decode_session(def, field_bytes);
                        session.summary = decode_session_summary(def, field_bytes);
                        sessions.push(session);
                    }
                    profile::MESG_TIME_IN_ZONE => {
                        zones.extend(decode_time_in_zone(def, field_bytes, section_base));
                    }
                    profile::MESG_ACTIVITY => {
                        if let Some(offset) = decode_activity_offset(def, field_bytes) {
                            utc_offsets.push(offset);
                        }
                    }
                    profile::MESG_DEVICE_INFO => {
                        if let Some(mut d) = decode_device(def, field_bytes) {
                            d.last_seen = devices.len();
                            devices.push(d);
                        }
                    }
                    profile::MESG_LAP => {
                        if let Some(l) = decode_lap(def, field_bytes) {
                            laps.push(l);
                        }
                    }
                    profile::MESG_LENGTH => {
                        if let Some(l) = decode_length(def, field_bytes) {
                            lengths.push(l);
                        }
                    }
                    profile::MESG_DEVELOPER_DATA_ID => {
                        if let Some((idx, uuid)) = decode_developer_data_id(def, field_bytes) {
                            current_app_for_idx.insert(idx, uuid);
                        }
                    }
                    profile::MESG_FIELD_DESCRIPTION => {
                        if let Some(mut fd) = decode_field_description(def, field_bytes) {
                            attach_app(&mut fd, &current_app_for_idx, &mut apps);
                            register_field_owner(&fd, &current_app_for_idx, &mut dev_field_owners);
                            if let Some(num) = fd.field_number {
                                dev_field_descs.insert((fd.dev_data_index, num), fd.desc);
                            }
                        }
                    }
                    _ => {}
                }
            }

            // `time_in_zone` indices count from each chained section's first session.
            FitEvent::FileHeader(_) => section_base = sessions.len(),
            FitEvent::Crc { .. } => {}
        }
    }

    // Sessions may precede the Activity message that carries the UTC offset
    // (summary-first files), so local start times resolve after the pass.
    resolve_local_start_times(&mut sessions, &utc_offsets);
    resolve_athlete(&mut sessions, &zones);

    // Build extra column info and lookup.
    let extra_col_info: Vec<(String, DataType)> = extra_types.into_iter().collect();
    let norm_to_col: HashMap<&str, usize> = extra_col_info.iter()
        .enumerate()
        .map(|(i, (name, _))| (name.as_str(), i))
        .collect();
    // Map field_number → extra column index for fast lookup during pass 2.
    let field_to_col: HashMap<u8, usize> = field_to_extra.iter()
        .filter_map(|(&num, opt_name)| {
            let name = opt_name.as_ref()?;
            let &idx = norm_to_col.get(name.as_str())?;
            Some((num, idx))
        })
        .collect();
    let mut extra_data: Vec<TypedColumn> = extra_col_info.iter()
        .map(|(_, dtype)| TypedColumn::new(dtype, n_rows))
        .collect();

    // Developer sensor classification.
    let present_extra_columns: BTreeSet<String> =
        extra_col_info.iter().map(|(name, _)| name.clone()).collect();
    let developer_sensors = classify_developer_sensors(
        &dev_field_owners,
        &present_extra_columns,
        true,
    );

    // ── Pass 2: decode Record fields ─────────────────────────────────────
    let mut reader = FitReader::new(data).map_err(|e| e.to_string())?;
    let mut records = Vec::with_capacity(n_rows);
    let mut row_idx = 0usize;
    let mut base_timestamp: Option<u32> = None;
    let mut timer_events: Vec<(i64, TimerEvent)> = Vec::new();

    // Rebuild developer field registrations during pass 2 so lookups follow
    // file order (a developer_data_index may be reassigned mid-file).
    dev_field_descs.clear();
    current_app_for_idx.clear();

    while let Some(event) = reader.next().map_err(|e| e.to_string())? {
        // Extract compressed timestamp offset.
        let time_offset = match &event {
            FitEvent::CompressedData { time_offset, .. } => Some(*time_offset),
            _ => None,
        };

        let (local, field_bytes, dev_field_bytes) = match &event {
            FitEvent::Data { local, field_bytes, dev_field_bytes, .. } => (*local, *field_bytes, *dev_field_bytes),
            FitEvent::CompressedData { local, field_bytes, dev_field_bytes, .. } => (*local, *field_bytes, *dev_field_bytes),
            _ => continue,
        };

        let def = match reader.def(local) {
            Some(d) => d,
            None => continue,
        };

        if def.global_message_number != profile::MESG_RECORD {
            // Track timestamps from non-Record messages.
            let mut own_timestamp = None;
            for (num, fdata) in FieldIter::new(def, field_bytes) {
                if num == 253 {
                    if let Some(ts) = read_u32(fdata, def.big_endian) {
                        base_timestamp = Some(ts);
                        own_timestamp = Some(ts);
                    }
                }
            }
            match def.global_message_number {
                profile::MESG_EVENT => {
                    // A compressed header carries the event's time as an offset.
                    let at = match time_offset {
                        Some(offset) => resolve_compressed_timestamp(&mut base_timestamp, offset),
                        None => own_timestamp,
                    };
                    if let (Some(at), Some(event)) = (at, decode_timer_event(def, field_bytes)) {
                        timer_events.push(((at as i64 + profile::FIT_EPOCH_OFFSET) * 1_000_000, event));
                    }
                }
                profile::MESG_DEVELOPER_DATA_ID => {
                    if let Some((idx, uuid)) = decode_developer_data_id(def, field_bytes) {
                        current_app_for_idx.insert(idx, uuid);
                    }
                }
                profile::MESG_FIELD_DESCRIPTION => {
                    if let Some(mut fd) = decode_field_description(def, field_bytes) {
                        attach_app(&mut fd, &current_app_for_idx, &mut apps);
                        if let Some(num) = fd.field_number {
                            dev_field_descs.insert((fd.dev_data_index, num), fd.desc);
                        }
                    }
                }
                _ => {}
            }
            continue;
        }

        // Resolve timestamp.
        let timestamp = if let Some(offset) = time_offset {
            resolve_compressed_timestamp(&mut base_timestamp, offset)
        } else {
            // Look for field 253 (timestamp) in the record.
            let mut ts = None;
            for (num, data) in FieldIter::new(def, field_bytes) {
                if num == 253 {
                    ts = read_u32(data, def.big_endian);
                    break;
                }
            }
            if let Some(t) = ts {
                base_timestamp = Some(t);
            }
            ts
        };

        let mut row = RecordRow::default();
        if let Some(ts) = timestamp {
            row.timestamp = Some((ts as i64 + profile::FIT_EPOCH_OFFSET) * 1_000_000);
        }

        let be = def.big_endian;

        // Decode record fields, skipping unwanted ones based on field_mask.
        for (num, data) in FieldIter::new(def, field_bytes) {
            if !field_mask[num as usize] && !decode_extras {
                continue; // skip both standard and extra — nothing to do
            }
            match num {
                0 if field_mask[0] => {
                    if let Some(v) = read_i32(data, be) {
                        row.latitude = Some(v as f64 * SEMICIRCLE_TO_DEGREES);
                    }
                }
                1 if field_mask[1] => {
                    if let Some(v) = read_i32(data, be) {
                        row.longitude = Some(v as f64 * SEMICIRCLE_TO_DEGREES);
                    }
                }
                2 if field_mask[2] => {
                    if let Some(v) = read_u16(data, be) {
                        row.altitude = Some(v as f32 / 5.0 - 500.0);
                    }
                }
                3 if field_mask[3] => {
                    if let Some(v) = read_u8_valid(data) {
                        row.heart_rate = Some(v as i16);
                    }
                }
                4 if field_mask[4] => {
                    if let Some(v) = read_u8_valid(data) {
                        row.cadence = Some(v as i16);
                    }
                }
                5 if field_mask[5] => {
                    if let Some(v) = read_u32(data, be) {
                        row.distance = Some(v as f64 / 100.0);
                    }
                }
                6 if field_mask[6] => {
                    if row.speed.is_none() {
                        if let Some(v) = read_u16(data, be) {
                            row.speed = Some(v as f32 / 1000.0);
                        }
                    }
                }
                7 if field_mask[7] => {
                    if let Some(v) = read_u16(data, be) {
                        row.power = Some(v as i16);
                    }
                }
                13 if field_mask[13] => {
                    if !data.is_empty() {
                        let v = data[0] as i8;
                        if v != 0x7F { row.temperature = Some(v); }
                    }
                }
                73 if field_mask[73] => {
                    if let Some(v) = read_u32(data, be) {
                        row.speed = Some(v as f32 / 1000.0);
                    }
                }
                78 if field_mask[78] => {
                    if let Some(v) = read_u32(data, be) {
                        row.altitude = Some(v as f32 / 5.0 - 500.0);
                    }
                }
                57 if field_mask[57] => {
                    row.smo2 = read_u16(data, be).and_then(|v| smo2_reading(v as f32 / 10.0));
                }
                139 if field_mask[139] => {
                    if let Some(v) = read_u16(data, be) {
                        row.core_temperature = Some(v as f32 / 100.0);
                    }
                }
                99 if field_mask[99] => {
                    // respiration_rate (uint8): whole breaths/min — its profile
                    // component expands it into enhanced_respiration_rate at
                    // scale 1. Fallback only; field 108 is preferred.
                    if row.respiration_rate.is_none() {
                        if let Some(v) = read_u8_valid(data) {
                            row.respiration_rate = Some(v as f32);
                        }
                    }
                }
                108 if field_mask[108] => {
                    // enhanced_respiration_rate (uint16, scale 100, breaths/min)
                    if let Some(v) = read_u16(data, be) {
                        row.respiration_rate = Some(v as f32 / 100.0);
                    }
                }
                _ if decode_extras => {
                    // Extra columns — only when requested.
                    if let Some(&col_idx) = field_to_col.get(&num) {
                        let raw_bt = def.fields.iter()
                            .find(|f| f.number == num)
                            .map(|f| f.base_type)
                            .unwrap_or(0x02);
                        let (scale, offset) = profile::FieldDef::lookup(profile::RECORD_FIELDS, num)
                            .map(|pf| (pf.scale, pf.offset))
                            .unwrap_or((1.0, 0.0));
                        extra_data[col_idx].set_from_bytes(
                            row_idx, data, raw_bt, be, scale, offset,
                        );
                    }
                }
                _ => {}
            }
        }

        // Developer fields — special-cased ones (Power, Cadence, core_temp,
        // smo2) always decoded into RecordRow for merge logic. Extras only
        // when requested.
        decode_record_dev_fields(def, dev_field_bytes, &dev_field_descs, &mut row);
        if decode_extras {
            decode_record_dev_extras(
                def, dev_field_bytes, &dev_field_descs,
                &norm_to_col, &mut extra_data, row_idx,
            );
        }

        records.push(row);
        row_idx += 1;
    }

    laps.sort_by_key(|l| l.start_time_us);
    lengths.sort_by_key(|l| l.start_time_us);
    let running = running_intervals(&timer_events, &records);

    Ok(ParseResult {
        file_type,
        records,
        extra_col_info,
        extra_data,
        sessions,
        devices,
        developer_sensors,
        laps,
        lengths,
        apps,
        running,
    })
}

// ---------------------------------------------------------------------------
// Course parser
// ---------------------------------------------------------------------------

/// Parse a course FIT file, extracting the GPS trace and course point annotations.
///
/// Single-pass: decodes Record messages (lat/lon/altitude/distance), CoursePoint
/// messages, Course metadata, and Lap totals.
pub fn parse_course(data: &[u8]) -> Result<CourseResult, String> {
    let mut reader = FitReader::new(data).map_err(|e| e.to_string())?;

    let mut file_type: Option<String> = None;
    let mut records = Vec::new();
    let mut course_points = Vec::new();
    let mut meta = CourseMeta {
        name: None,
        total_distance: None,
        total_ascent: None,
        total_descent: None,
    };

    let mut base_timestamp: Option<u32> = None;

    while let Some(event) = reader.next().map_err(|e| e.to_string())? {
        let time_offset = match &event {
            FitEvent::CompressedData { time_offset, .. } => Some(*time_offset),
            _ => None,
        };

        let (local, field_bytes) = match &event {
            FitEvent::Data { local, field_bytes, .. } => (*local, *field_bytes),
            FitEvent::CompressedData { local, field_bytes, .. } => (*local, *field_bytes),
            _ => continue,
        };

        let def = match reader.def(local) {
            Some(d) => d,
            None => continue,
        };

        match def.global_message_number {
            profile::MESG_FILE_ID => {
                if file_type.is_none() {
                    file_type = decode_file_type(def, field_bytes);
                }
            }

            profile::MESG_RECORD => {
                let be = def.big_endian;

                // Resolve timestamp (needed to advance compressed timestamp state).
                let timestamp = if let Some(offset) = time_offset {
                    resolve_compressed_timestamp(&mut base_timestamp, offset)
                } else {
                    let mut ts = None;
                    for (num, fdata) in FieldIter::new(def, field_bytes) {
                        if num == 253 {
                            ts = read_u32(fdata, be);
                            break;
                        }
                    }
                    if let Some(t) = ts { base_timestamp = Some(t); }
                    ts
                };
                let _ = timestamp; // not stored — course timestamps are synthetic

                let mut row = RecordRow::default();
                for (num, fdata) in FieldIter::new(def, field_bytes) {
                    match num {
                        0 => {
                            if let Some(v) = read_i32(fdata, be) {
                                row.latitude = Some(v as f64 * SEMICIRCLE_TO_DEGREES);
                            }
                        }
                        1 => {
                            if let Some(v) = read_i32(fdata, be) {
                                row.longitude = Some(v as f64 * SEMICIRCLE_TO_DEGREES);
                            }
                        }
                        2 => {
                            if let Some(v) = read_u16(fdata, be) {
                                row.altitude = Some(v as f32 / 5.0 - 500.0);
                            }
                        }
                        5 => {
                            if let Some(v) = read_u32(fdata, be) {
                                row.distance = Some(v as f64 / 100.0);
                            }
                        }
                        78 => {
                            // enhanced_altitude (overrides field 2)
                            if let Some(v) = read_u32(fdata, be) {
                                row.altitude = Some(v as f32 / 5.0 - 500.0);
                            }
                        }
                        _ => {}
                    }
                }
                records.push(row);
            }

            profile::MESG_COURSE => {
                let be = def.big_endian;
                for (num, fdata) in FieldIter::new(def, field_bytes) {
                    if num == 5 {
                        // name (string)
                        meta.name = read_string(fdata);
                    }
                    // field 4 = sport, but courses don't always have it
                    let _ = (num, be);
                }
            }

            profile::MESG_COURSE_POINT => {
                let be = def.big_endian;
                let mut pt = CoursePoint {
                    latitude: None,
                    longitude: None,
                    distance: None,
                    name: None,
                    point_type: None,
                };
                for (num, fdata) in FieldIter::new(def, field_bytes) {
                    match num {
                        2 => {
                            // position_lat (sint32, semicircles)
                            if let Some(v) = read_i32(fdata, be) {
                                pt.latitude = Some(v as f64 * SEMICIRCLE_TO_DEGREES);
                            }
                        }
                        3 => {
                            // position_long (sint32, semicircles)
                            if let Some(v) = read_i32(fdata, be) {
                                pt.longitude = Some(v as f64 * SEMICIRCLE_TO_DEGREES);
                            }
                        }
                        4 => {
                            // distance (uint32, scale 100)
                            if let Some(v) = read_u32(fdata, be) {
                                pt.distance = Some(v as f64 / 100.0);
                            }
                        }
                        5 => {
                            // type (enum)
                            if let Some(v) = read_u8_valid(fdata) {
                                pt.point_type = Some(
                                    profile::course_point_name(v).to_string()
                                );
                            }
                        }
                        6 => {
                            // name (string)
                            pt.name = read_string(fdata);
                        }
                        _ => {}
                    }
                }
                course_points.push(pt);
            }

            profile::MESG_LAP => {
                // Extract totals from the (typically single) lap message.
                let be = def.big_endian;
                for (num, fdata) in FieldIter::new(def, field_bytes) {
                    match num {
                        9 => {
                            // total_distance (uint32, scale 100)
                            if let Some(v) = read_u32(fdata, be) {
                                meta.total_distance = Some(v as f64 / 100.0);
                            }
                        }
                        21 => {
                            // total_ascent (uint16)
                            if let Some(v) = read_u16(fdata, be) {
                                meta.total_ascent = Some(v);
                            }
                        }
                        22 => {
                            // total_descent (uint16)
                            if let Some(v) = read_u16(fdata, be) {
                                meta.total_descent = Some(v);
                            }
                        }
                        _ => {}
                    }
                }
            }

            _ => {
                // Track timestamps from other messages for compressed timestamp.
                for (num, fdata) in FieldIter::new(def, field_bytes) {
                    if num == 253 {
                        if let Some(ts) = read_u32(fdata, def.big_endian) {
                            base_timestamp = Some(ts);
                        }
                    }
                }
            }
        }
    }

    // Validate file type.
    match file_type.as_deref() {
        Some("course") => {}
        Some(other) => {
            let article = if other.starts_with(|c: char| "aeiou".contains(c)) { "an" } else { "a" };
            return Err(format!(
                "Expected a course file, got {} {} file. \
                 Use Activity.load_fit() or Session.load_fit() instead.",
                article, other,
            ));
        }
        None => {
            return Err("FIT file has no file_id type field".into());
        }
    }

    Ok(CourseResult { records, course_points, meta })
}

// ---------------------------------------------------------------------------
// Full-parse helpers
// ---------------------------------------------------------------------------

/// Resolve a 5-bit compressed timestamp offset against the base timestamp.
fn resolve_compressed_timestamp(base: &mut Option<u32>, time_offset: u8) -> Option<u32> {
    let base_ts = (*base)?;
    let offset = time_offset as u32;
    let mask: u32 = 0x1F;
    let mut ts = (base_ts & !mask) + offset;
    if offset < (base_ts & mask) {
        ts += 32;
    }
    *base = Some(ts);
    Some(ts)
}

/// Read an i32 from field bytes. Returns None if invalid (0x7FFFFFFF).
#[inline]
fn read_i32(data: &[u8], big_endian: bool) -> Option<i32> {
    if data.len() < 4 { return None; }
    let v = if big_endian {
        i32::from_be_bytes([data[0], data[1], data[2], data[3]])
    } else {
        i32::from_le_bytes([data[0], data[1], data[2], data[3]])
    };
    if v == 0x7FFFFFFF { None } else { Some(v) }
}

/// Decode a Lap message into a LapBoundary.
fn decode_lap(def: &MessageDef, field_bytes: &[u8]) -> Option<LapBoundary> {
    let be = def.big_endian;
    let mut start_time_us: Option<i64> = None;
    let mut end_time_us: Option<i64> = None;
    let mut trigger: Option<String> = None;

    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            2 => {
                // start_time
                if let Some(ts) = read_u32(data, be) {
                    start_time_us = Some((ts as i64 + profile::FIT_EPOCH_OFFSET) * 1_000_000);
                }
            }
            253 => {
                // timestamp (lap end time)
                if let Some(ts) = read_u32(data, be) {
                    end_time_us = Some((ts as i64 + profile::FIT_EPOCH_OFFSET) * 1_000_000);
                }
            }
            24 => {
                // lap_trigger (enum)
                if let Some(v) = read_u8_valid(data) {
                    trigger = Some(profile::lap_trigger_name(v).to_string());
                }
            }
            _ => {}
        }
    }

    // Require only `start_time` (field 2). The lap `timestamp`/end field (253) is
    // device-unreliable (some devices omit or pin it) and `end_time_us` is not used
    // for record→lap assignment (that keys off `start_time`), so default it to the
    // start rather than dropping the whole lap. Mirrors `decode_length` below.
    start_time_us.map(|start| LapBoundary {
        start_time_us: start,
        end_time_us: end_time_us.unwrap_or(start),
        trigger,
    })
}

/// Decode a Length message (mesg 101) into a [`LengthInterval`].
///
/// Returns `None` when the message has no `start_time` (field 2) — without it a
/// length cannot be placed on the timeline. A length with an unreadable
/// `length_type` defaults to idle (contributes no reconstructed distance), which
/// is the conservative choice: never fabricate distance from an ambiguous length.
fn decode_length(def: &MessageDef, field_bytes: &[u8]) -> Option<LengthInterval> {
    let be = def.big_endian;
    let mut start_time_us: Option<i64> = None;
    let mut duration_s = 0.0;
    let mut active = false;
    let mut cadence: Option<i16> = None;
    let mut swim_stroke: Option<String> = None;

    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            2 => {
                // start_time
                if let Some(ts) = read_u32(data, be) {
                    start_time_us = Some((ts as i64 + profile::FIT_EPOCH_OFFSET) * 1_000_000);
                }
            }
            3 => {
                // total_elapsed_time (uint32, scale 1000, seconds)
                if let Some(v) = read_u32(data, be) {
                    duration_s = v as f64 / 1000.0;
                }
            }
            7 => {
                // swim_stroke (enum)
                if let Some(v) = read_u8_valid(data) {
                    swim_stroke = Some(profile::swim_stroke_name(v).to_string());
                }
            }
            9 => {
                // avg_swimming_cadence (uint8, strokes/min)
                if let Some(v) = read_u8_valid(data) {
                    cadence = Some(v as i16);
                }
            }
            12 => {
                // length_type (enum): 1 = active (with strokes), 0 = idle (rest)
                if let Some(v) = read_u8_valid(data) {
                    active = profile::length_type_name(v) == "active";
                }
            }
            _ => {}
        }
    }

    start_time_us.map(|start_time_us| LengthInterval {
        start_time_us,
        duration_s,
        active,
        cadence,
        swim_stroke,
    })
}

/// Decode the developer fields that fold into canonical `RecordRow` columns
/// (Stryd Power/Cadence, CORE temperature, muscle-oxygen SmO2). The name set
/// mirrors `is_handled_field`. Values are read per their FieldDescription, so
/// integer and float encodings both work.
fn decode_record_dev_fields(
    def: &MessageDef,
    dev_field_bytes: &[u8],
    dev_field_descs: &HashMap<(u8, u8), DevFieldDesc>,
    row: &mut RecordRow,
) {
    for (desc, data) in dev_field_values(def, dev_field_bytes, dev_field_descs) {
        let Some(value) = read_dev_value(data, desc, def.big_endian) else { continue };
        let from = desc.app;
        match desc.name.as_str() {
            "Power" => {
                take_reading(&mut row.dev_power, &mut row.dev_power_app, value.round() as i16, from);
            }
            "Cadence" => {
                take_reading(&mut row.dev_cadence, &mut row.dev_cadence_app, value.round() as i16, from);
            }
            "Core Body Temperature" | "core_temperature" => {
                take_reading(&mut row.core_temperature, &mut row.core_temperature_app, value as f32, from);
            }
            "Current Saturated Hemoglobin Percent" | "SmO2" | "smo2"
            | "saturated_hemoglobin_percent" => {
                if let Some(v) = smo2_reading(value as f32) {
                    take_reading(&mut row.smo2, &mut row.smo2_app, v, from);
                }
            }
            _ => {}
        }
    }
}

/// Decode the remaining developer fields into their extra columns.
fn decode_record_dev_extras(
    def: &MessageDef,
    dev_field_bytes: &[u8],
    dev_field_descs: &HashMap<(u8, u8), DevFieldDesc>,
    norm_to_col: &HashMap<&str, usize>,
    extra_data: &mut [TypedColumn],
    row_idx: usize,
) {
    for (desc, data) in dev_field_values(def, dev_field_bytes, dev_field_descs) {
        if is_handled_field(&desc.name) {
            continue;
        }
        let Some(col_name) = column_for_developer_field(&desc.name) else { continue };
        if let Some(&col_idx) = norm_to_col.get(col_name.as_str()) {
            extra_data[col_idx].set_from_bytes(
                row_idx, data, desc.base_type, def.big_endian, desc.scale, desc.offset,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_summary_fit_name_resolves() {
        for f in SUMMARY_FIELDS.iter().filter(|f| f.kind != Kind::Computed) {
            assert!(
                profile::SESSION_FIELDS.iter().any(|pf| pf.name == f.source),
                "{} reads Session field {:?}, which the profile does not define",
                f.name, f.source,
            );
        }
        for name in ["reference_mesg", "reference_index", "max_heart_rate", "threshold_heart_rate"] {
            assert!(profile::TIME_IN_ZONE_FIELDS.iter().any(|pf| pf.name == name), "{name}");
        }
        for name in ["event", "event_type"] {
            assert!(profile::EVENT_FIELDS.iter().any(|pf| pf.name == name), "{name}");
        }
    }

    mod running_time {
        use super::*;
        use TimerEvent::{PowerUp, Start, Stop};

        fn records(ts: &[i64]) -> Vec<RecordRow> {
            ts.iter().map(|&t| RecordRow { timestamp: Some(t), ..Default::default() }).collect()
        }

        #[test]
        fn starts_and_stops() {
            let events = [(0, Start), (10, Stop), (20, Start), (30, Stop)];
            assert_eq!(running_intervals(&events, &[]), Some(vec![(0, 10), (20, 30)]));
        }

        #[test]
        fn a_repeated_start_or_stop_changes_nothing() {
            let events = [(0, Start), (5, Start), (10, Stop), (12, Stop)];
            assert_eq!(running_intervals(&events, &[]), Some(vec![(0, 10)]));
        }

        #[test]
        fn an_open_interval_runs_on() {
            assert_eq!(running_intervals(&[(7, Start)], &[]), Some(vec![(7, i64::MAX)]));
        }

        #[test]
        fn power_up_ends_at_the_last_record_before_it() {
            let events = [(0, Start), (45, PowerUp), (50, Start), (100, Stop)];
            let recs = records(&[0, 10, 30, 50, 100]);
            assert_eq!(running_intervals(&events, &recs), Some(vec![(0, 30), (50, 100)]));
        }

        #[test]
        fn no_timer_start_means_no_timer() {
            assert_eq!(running_intervals(&[], &[]), None);
            assert_eq!(running_intervals(&[(3, Stop), (5, PowerUp)], &[]), None);
        }
    }

    #[test]
    fn test_scan_basic_fit_file() {
        let path = std::path::Path::new("tests/fixtures/test.fit");
        if !path.exists() { return; }
        let data = std::fs::read(path).unwrap();

        let result = scan_metadata(&data).unwrap();

        assert!(!result.sessions.is_empty(), "expected at least one session");
        let session = &result.sessions[0];
        assert!(session.sport.is_some(), "expected sport");
        assert!(session.start_time.is_some(), "expected start_time");
        assert!(!result.record_metrics.is_empty(), "expected metrics");
    }

    #[test]
    fn test_scan_developer_fields_file() {
        let path = std::path::Path::new("tests/fixtures/with-developer-fields.fit");
        if !path.exists() { return; }
        let data = std::fs::read(path).unwrap();

        let result = scan_metadata(&data).unwrap();

        assert!(!result.sessions.is_empty());
        assert!(!result.developer_sensors.is_empty(), "expected developer sensors");
    }

    #[test]
    fn test_scan_multi_session_file() {
        let path = std::path::Path::new("tests/fixtures/cycling-rowing-cycling-rowing.fit");
        if !path.exists() { return; }
        let data = std::fs::read(path).unwrap();

        let result = scan_metadata(&data).unwrap();

        assert!(result.sessions.len() > 1, "expected multiple sessions, got {}", result.sessions.len());
    }


    // -- Full parse tests --

    #[test]
    fn test_full_parse_basic() {
        let path = std::path::Path::new("tests/fixtures/test.fit");
        if !path.exists() { return; }
        let data = std::fs::read(path).unwrap();

        let result = full_parse(&data, &ParseConfig::default()).unwrap();

        assert!(!result.records.is_empty(), "expected records");
        assert!(!result.sessions.is_empty(), "expected sessions");
        assert!(result.records[0].timestamp.is_some(), "first record should have timestamp");
    }

    #[test]
    fn test_full_parse_developer_fields() {
        let path = std::path::Path::new("tests/fixtures/with-developer-fields.fit");
        if !path.exists() { return; }
        let data = std::fs::read(path).unwrap();

        let result = full_parse(&data, &ParseConfig::default()).unwrap();

        assert!(!result.records.is_empty());
        assert!(!result.developer_sensors.is_empty());

        // Should have core_temperature from developer fields.
        let has_core_temp = result.records.iter().any(|r| r.core_temperature.is_some());
        assert!(has_core_temp, "expected core_temperature from developer fields");
    }

    #[test]
    fn test_full_parse_multi_session() {
        let path = std::path::Path::new("tests/fixtures/cycling-rowing-cycling-rowing.fit");
        if !path.exists() { return; }
        let data = std::fs::read(path).unwrap();

        let result = full_parse(&data, &ParseConfig::default()).unwrap();

        assert!(result.sessions.len() > 1, "expected multiple sessions");
        assert!(!result.records.is_empty());
        assert!(!result.laps.is_empty(), "expected laps");
    }

    // -- Developer field decoding --

    fn desc(base_type: u8, scale: f64, offset: f64) -> DevFieldDesc {
        DevFieldDesc { name: "x".into(), base_type, scale, offset, app: None }
    }

    #[test]
    fn reading_beats_zero_placeholder_regardless_of_order() {
        let (mut value, mut app) = (None, None);
        take_reading(&mut value, &mut app, 0i16, Some(1));
        take_reading(&mut value, &mut app, 250, Some(0));
        assert_eq!((value, app), (Some(250), Some(0)));
        take_reading(&mut value, &mut app, 0, Some(1));
        assert_eq!((value, app), (Some(250), Some(0)));
    }

    #[test]
    fn app_slots_are_stable_per_uuid() {
        let mut apps = Vec::new();
        assert_eq!(app_slot(&mut apps, "a"), Some(0));
        assert_eq!(app_slot(&mut apps, "b"), Some(1));
        assert_eq!(app_slot(&mut apps, "a"), Some(0));
        assert_eq!(apps, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn dev_value_uses_declared_base_type() {
        assert_eq!(read_dev_value(&[65], &desc(0x02, 1.0, 0.0), false), Some(65.0));
        assert_eq!(read_dev_value(&[0x8D, 0x02], &desc(0x84, 10.0, 0.0), false), Some(65.3));
        assert_eq!(read_dev_value(&[0xF4, 0xFF], &desc(0x83, 1.0, 0.0), false), Some(-12.0));
        let float = 65.3f32.to_le_bytes();
        assert_eq!(read_dev_value(&float, &desc(0x88, 1.0, 0.0), false), Some(65.3f32 as f64));
    }

    #[test]
    fn dev_value_applies_scale_then_offset() {
        // raw / scale - offset: 81 / 2 - 10 = 30.5
        assert_eq!(read_dev_value(&[81], &desc(0x02, 2.0, 10.0), false), Some(30.5));
    }

    #[test]
    fn dev_value_honours_byte_order() {
        assert_eq!(read_dev_value(&[0x02, 0x8D], &desc(0x84, 1.0, 0.0), true), Some(653.0));
    }

    #[test]
    fn dev_value_invalid_or_short_is_none() {
        assert_eq!(read_dev_value(&[0xFF], &desc(0x02, 1.0, 0.0), false), None);
        assert_eq!(read_dev_value(&[0xFF, 0xFF], &desc(0x84, 10.0, 0.0), false), None);
        assert_eq!(read_dev_value(&[0x8D], &desc(0x84, 1.0, 0.0), false), None);
        assert_eq!(read_dev_value(&[0xFF; 4], &desc(0x88, 1.0, 0.0), false), None);
    }

    // -- Local start time --

    fn activity_def() -> MessageDef {
        use crate::fit::binary::FieldLayout;
        MessageDef {
            global_message_number: profile::MESG_ACTIVITY,
            big_endian: false,
            fields: vec![
                FieldLayout { number: 253, size: 4, base_type: 0x86 },
                FieldLayout { number: 5, size: 4, base_type: 0x86 },
            ],
            dev_fields: vec![],
            data_size: 8,
            dev_data_size: 0,
        }
    }

    #[test]
    fn activity_offset_is_local_minus_utc() {
        let ts: u32 = 1_000_000_000;
        let mut bytes = ts.to_le_bytes().to_vec();
        bytes.extend((ts + 7_200).to_le_bytes());
        let offset = decode_activity_offset(&activity_def(), &bytes).unwrap();
        assert_eq!(offset.seconds, 7_200);
        assert_eq!(offset.at, ts as i64 + profile::FIT_EPOCH_OFFSET);
    }

    #[test]
    fn relative_activity_timestamps_give_no_offset() {
        // Zwift writes local_timestamp = 0, which is below date_time.min.
        let mut bytes = 1_000_000_000u32.to_le_bytes().to_vec();
        bytes.extend(0u32.to_le_bytes());
        assert_eq!(decode_activity_offset(&activity_def(), &bytes), None);
    }

    fn session_starting_at(start: i64) -> SessionMeta {
        SessionMeta { start_time: Some(start as f64), ..Default::default() }
    }

    #[test]
    fn local_start_applies_offset_of_covering_activity() {
        let mut sessions = vec![session_starting_at(1_000), session_starting_at(5_000)];
        let offsets = [UtcOffset { at: 9_000, seconds: 7_200 }];
        resolve_local_start_times(&mut sessions, &offsets);
        assert_eq!(sessions[0].start_time_local, Some(8_200.0));
        assert_eq!(sessions[1].start_time_local, Some(12_200.0));
    }

    #[test]
    fn local_start_picks_first_activity_at_or_after_session_start() {
        // Chained multisport: one Activity per leg, legs in different zones.
        let mut sessions = vec![session_starting_at(1_000), session_starting_at(5_000)];
        let offsets = [
            UtcOffset { at: 4_000, seconds: 3_600 },
            UtcOffset { at: 9_000, seconds: 7_200 },
        ];
        resolve_local_start_times(&mut sessions, &offsets);
        assert_eq!(sessions[0].start_time_local, Some(4_600.0));
        assert_eq!(sessions[1].start_time_local, Some(12_200.0));
    }

    #[test]
    fn local_start_falls_back_to_last_activity() {
        // Activity timestamp before the session start (pinned or odd clock).
        let mut sessions = vec![session_starting_at(5_000)];
        let offsets = [UtcOffset { at: 4_000, seconds: -18_000 }];
        resolve_local_start_times(&mut sessions, &offsets);
        assert_eq!(sessions[0].start_time_local, Some(-13_000.0));
    }

    #[test]
    fn local_start_unknown_without_activity_message() {
        let mut sessions = vec![session_starting_at(1_000)];
        resolve_local_start_times(&mut sessions, &[]);
        assert_eq!(sessions[0].start_time_local, None);
    }
}
