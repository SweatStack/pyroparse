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
use crate::fit::profile;
use crate::reference::{classify_developer_field, format_product_name};
use crate::fields::{normalize_field_name, is_canonical_column, is_handled_field};
use crate::types::{TypedColumn, promote_type, base_type_to_arrow};
use crate::{
    SessionMeta, DeviceMeta, ScanResult, ParseResult,
    RecordRow, LapBoundary, SEMICIRCLE_TO_DEGREES,
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
    let mut dev_field_owners: BTreeMap<String, String> = BTreeMap::new();

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
                    profile::MESG_ACTIVITY => {
                        result.local_timestamp = decode_activity_local_ts(def, field_bytes);
                    }
                    profile::MESG_DEVICE_INFO => {
                        if let Some(d) = decode_device(def, field_bytes) {
                            result.devices.push(d);
                        }
                    }
                    profile::MESG_DEVELOPER_DATA_ID => {
                        if let Some((idx, uuid)) = decode_developer_data_id(def, field_bytes) {
                            current_app_for_idx.insert(idx, uuid);
                        }
                    }
                    profile::MESG_FIELD_DESCRIPTION => {
                        decode_field_description(
                            def, field_bytes,
                            &current_app_for_idx,
                            &mut metric_set,
                            &mut dev_field_owners,
                        );
                    }
                    _ => {}
                }
            }

            FitEvent::FileHeader(_) | FitEvent::Crc { .. } => {
                // No state reset needed for metadata scan — sessions and
                // devices accumulate across chained sections.
            }
        }
    }

    result.record_metrics = metric_set.into_iter().collect();

    let empty = BTreeSet::new();
    result.developer_sensors = classify_developer_sensors(
        &dev_field_owners,
        &empty,
        false,
    );

    // Backfill local_timestamp from Activity message.
    if let Some(lt) = result.local_timestamp {
        for s in &mut result.sessions {
            if s.start_time_local.is_none() {
                s.start_time_local = Some(lt);
            }
        }
    }

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
            253 => {
                // timestamp (uint32, date_time) — session end time
                if let Some(ts) = read_u32(data, be) {
                    s.end_timestamp_us = Some((ts as i64 + profile::FIT_EPOCH_OFFSET) * 1_000_000);
                }
            }
            7 if s.duration.is_none() => {
                // total_timer_time — fallback if total_elapsed_time not present
                if let Some(v) = read_u32(data, be) {
                    s.duration = Some(v as f64 / 1000.0);
                }
            }
            8 => {
                // total_elapsed_time — preferred (overrides total_timer_time)
                if let Some(v) = read_u32(data, be) {
                    s.duration = Some(v as f64 / 1000.0);
                }
            }
            9 => {
                // total_distance
                if let Some(v) = read_u32(data, be) {
                    s.distance = Some(v as f64 / 100.0);
                }
            }
            _ => {}
        }
    }

    s
}

fn decode_activity_local_ts(def: &MessageDef, field_bytes: &[u8]) -> Option<f64> {
    let be = def.big_endian;
    for (num, data) in FieldIter::new(def, field_bytes) {
        if num == 5 {
            // local_timestamp (field 5 in activity message)
            if let Some(ts) = read_u32(data, be) {
                return Some((ts as i64 + profile::FIT_EPOCH_OFFSET) as f64);
            }
        }
    }
    None
}

fn decode_device(def: &MessageDef, field_bytes: &[u8]) -> Option<DeviceMeta> {
    let mut d = DeviceMeta::default();
    let be = def.big_endian;

    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            0 => {
                // device_index (uint8)
                if let Some(v) = read_u8_valid(data) {
                    d.device_index = Some(v);
                }
            }
            1 => {
                // device_type / ant_device_type (uint8)
                if let Some(v) = read_u8_valid(data) {
                    d.ant_device_type = Some(v);
                }
            }
            2 => {
                // manufacturer (uint16)
                if let Some(v) = read_u16(data, be) {
                    d.manufacturer = Some(profile::manufacturer_name(v).to_string());
                }
            }
            3 => {
                // serial_number (uint32z — invalid is 0, not 0xFFFFFFFF)
                if let Some(v) = read_u32z(data, be) {
                    d.serial_number = Some(format!("{v}"));
                }
            }
            27 => {
                // product_name (string)
                if let Some(s) = read_string(data) {
                    d.product = Some(s);
                }
            }
            _ => {}
        }
    }

    if d.manufacturer.is_some() || d.product.is_some() {
        Some(d)
    } else {
        None
    }
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

fn decode_field_description(
    def: &MessageDef,
    field_bytes: &[u8],
    current_app_for_idx: &BTreeMap<u8, String>,
    metrics: &mut HashSet<String>,
    dev_field_owners: &mut BTreeMap<String, String>,
) {
    let mut dev_idx: Option<u8> = None;
    let mut field_name: Option<String> = None;

    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            0 => dev_idx = data.first().copied(),
            3 => field_name = read_string(data),
            _ => {}
        }
    }

    if let Some(name) = &field_name {
        // Check if this developer field maps to a known metric.
        if let Some(metric) = classify_developer_field(name) {
            metrics.insert(metric.to_string());
        }

        // Normalize and add to metrics (for extra column detection).
        let normalized = normalize_field_name(name);
        if !is_canonical_column(&normalized) {
            metrics.insert(normalized);
        }

        // Track field → app UUID ownership.
        if let Some(idx) = dev_idx {
            if !dev_field_owners.contains_key(name) {
                if let Some(uuid) = current_app_for_idx.get(&idx) {
                    dev_field_owners.insert(name.clone(), uuid.clone());
                }
            }
        }
    }
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
    let mut current_app_for_idx: BTreeMap<u8, String> = BTreeMap::new();
    let mut dev_field_owners: BTreeMap<String, String> = BTreeMap::new();

    let mut n_rows = 0usize;
    let mut extra_types: BTreeMap<String, DataType> = BTreeMap::new();

    // Track which field numbers map to which normalized extra column names.
    // Key: field_number, Value: normalized column name (or None if handled).
    let mut field_to_extra: HashMap<u8, Option<String>> = HashMap::new();

    // Also track developer field names for extra column discovery.
    // Key: (dev_data_index, field_number) from definition, Value: field name from FieldDescription.
    let mut dev_field_names: HashMap<(u8, u8), (String, u8)> = HashMap::new();

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
                                        // Determine Arrow type from base type.
                                        if let Some(dtype) = base_type_to_arrow(field.base_type) {
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
                            if let Some((name, _bt)) = dev_field_names.get(&key) {
                                if !is_handled_field(name) {
                                    if let Some(col) = column_for_developer_field(name) {
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
                        sessions.push(decode_session(def, field_bytes));
                    }
                    profile::MESG_ACTIVITY => {
                        // Extract local_timestamp — backfill into sessions later.
                        if let Some(lt) = decode_activity_local_ts(def, field_bytes) {
                            for s in &mut sessions {
                                if s.start_time_local.is_none() {
                                    s.start_time_local = Some(lt);
                                }
                            }
                        }
                    }
                    profile::MESG_DEVICE_INFO => {
                        if let Some(d) = decode_device_full(def, field_bytes) {
                            devices.push(d);
                        }
                    }
                    profile::MESG_LAP => {
                        if let Some(l) = decode_lap(def, field_bytes) {
                            laps.push(l);
                        }
                    }
                    profile::MESG_DEVELOPER_DATA_ID => {
                        if let Some((idx, uuid)) = decode_developer_data_id(def, field_bytes) {
                            current_app_for_idx.insert(idx, uuid);
                        }
                    }
                    profile::MESG_FIELD_DESCRIPTION => {
                        decode_field_description_full(
                            def, field_bytes,
                            &current_app_for_idx,
                            &mut dev_field_owners,
                            &mut dev_field_names,
                        );
                    }
                    _ => {}
                }
            }

            FitEvent::FileHeader(_) | FitEvent::Crc { .. } => {}
        }
    }

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

    // Reset dev_field_names — rebuild during pass 2 to stay in sync with
    // session-local developer data index assignments.
    dev_field_names.clear();

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
            for (num, fdata) in FieldIter::new(def, field_bytes) {
                if num == 253 {
                    if let Some(ts) = read_u32(fdata, def.big_endian) {
                        base_timestamp = Some(ts);
                    }
                }
            }
            // Re-process FieldDescription to keep dev_field_names in sync.
            if def.global_message_number == profile::MESG_FIELD_DESCRIPTION {
                let mut dev_idx: Option<u8> = None;
                let mut field_def_num: Option<u8> = None;
                let mut field_name: Option<String> = None;
                let mut base_type_id: u8 = 0x88; // default float32
                for (num, fdata) in FieldIter::new(def, field_bytes) {
                    match num {
                        0 => dev_idx = fdata.first().copied(),
                        1 => field_def_num = fdata.first().copied(),
                        2 => base_type_id = fdata.first().copied().unwrap_or(0x88),
                        3 => field_name = read_string(fdata),
                        _ => {}
                    }
                }
                if let (Some(idx), Some(fdn), Some(name)) = (dev_idx, field_def_num, field_name) {
                    dev_field_names.insert((idx, fdn), (name, base_type_id));
                }
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
                    if let Some(v) = read_u16(data, be) {
                        row.smo2 = Some(v as f32 / 10.0);
                    }
                }
                139 if field_mask[139] => {
                    if let Some(v) = read_u16(data, be) {
                        row.core_temperature = Some(v as f32 / 100.0);
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
        decode_record_dev_fields(def, dev_field_bytes, &dev_field_names, &mut row);
        if decode_extras {
            decode_record_dev_extras(
                def, dev_field_bytes, &dev_field_names,
                &norm_to_col, &mut extra_data, row_idx,
            );
        }

        records.push(row);
        row_idx += 1;
    }

    laps.sort_by_key(|l| l.start_time_us);

    Ok(ParseResult {
        file_type,
        records,
        extra_col_info,
        extra_data,
        sessions,
        devices,
        developer_sensors,
        laps,
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
                                    profile::course_point_type_name(v).to_string()
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

/// Decode DeviceInfo with full field set (including garmin_product fallback).
fn decode_device_full(def: &MessageDef, field_bytes: &[u8]) -> Option<DeviceMeta> {
    let mut d = DeviceMeta::default();
    let be = def.big_endian;
    let mut garmin_product: Option<String> = None;

    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            0 => {
                if let Some(v) = read_u8_valid(data) {
                    d.device_index = Some(v);
                }
            }
            1 => {
                // device_type / ant_device_type
                if let Some(v) = read_u8_valid(data) {
                    d.ant_device_type = Some(v);
                }
            }
            2 => {
                // manufacturer
                if let Some(v) = read_u16(data, be) {
                    d.manufacturer = Some(profile::manufacturer_name(v).to_string());
                }
            }
            3 => {
                // serial_number (uint32z)
                if let Some(v) = read_u32z(data, be) {
                    d.serial_number = Some(format!("{v}"));
                }
            }
            4 => {
                // product (uint16) — resolve as garmin_product only when
                // manufacturer is garmin (1 or 2). This matches fitparser's
                // subfield resolution.
                if d.product.is_none() {
                    if let Some(v) = read_u16(data, be) {
                        let name = profile::garmin_product_name(v);
                        if name != "unknown" && !name.chars().all(|c| c.is_ascii_digit()) {
                            garmin_product = Some(format_product_name(name));
                        }
                    }
                }
            }
            27 => {
                // product_name (string)
                if let Some(s) = read_string(data) {
                    d.product = Some(s);
                }
            }
            _ => {}
        }
    }

    // Fallback: use garmin_product if no product_name and manufacturer is garmin.
    // Field 4 (product) is a generic uint16; it only resolves to a meaningful
    // garmin_product name when the manufacturer is actually garmin.
    if d.product.is_none() && garmin_product.is_some() {
        let is_garmin = d.manufacturer.as_deref()
            .is_some_and(|m| m == "garmin");
        if is_garmin {
            d.product = garmin_product;
        }
    }

    if d.manufacturer.is_some() || d.product.is_some() {
        Some(d)
    } else {
        None
    }
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

    match (start_time_us, end_time_us) {
        (Some(start), Some(end)) => Some(LapBoundary {
            start_time_us: start,
            end_time_us: end,
            trigger,
        }),
        _ => None,
    }
}

/// Extended FieldDescription decoder for full parse — also tracks
/// (dev_data_index, field_number) → field_name for developer field lookup.
fn decode_field_description_full(
    def: &MessageDef,
    field_bytes: &[u8],
    current_app_for_idx: &BTreeMap<u8, String>,
    dev_field_owners: &mut BTreeMap<String, String>,
    dev_field_names: &mut HashMap<(u8, u8), (String, u8)>,
) {
    let mut dev_idx: Option<u8> = None;
    let mut field_def_num: Option<u8> = None;
    let mut field_name: Option<String> = None;
    let mut base_type_id: u8 = 0x88; // default float32

    for (num, data) in FieldIter::new(def, field_bytes) {
        match num {
            0 => dev_idx = data.first().copied(),
            1 => field_def_num = data.first().copied(),
            2 => base_type_id = data.first().copied().unwrap_or(0x88),
            3 => field_name = read_string(data),
            _ => {}
        }
    }

    if let (Some(idx), Some(fdn)) = (dev_idx, field_def_num) {
        if let Some(name) = &field_name {
            dev_field_names.insert((idx, fdn), (name.clone(), base_type_id));
        }
    }

    if let (Some(idx), Some(name)) = (dev_idx, &field_name) {
        if !dev_field_owners.contains_key(name) {
            if let Some(uuid) = current_app_for_idx.get(&idx) {
                dev_field_owners.insert(name.clone(), uuid.clone());
            }
        }
    }
}

/// Decode developer fields in a Record message.
fn decode_record_dev_fields(
    def: &MessageDef,
    dev_field_bytes: &[u8],
    dev_field_names: &HashMap<(u8, u8), (String, u8)>,
    row: &mut RecordRow,
) {
    let mut offset = 0;
    for dev_field in &def.dev_fields {
        let size = dev_field.size as usize;
        if offset + size > dev_field_bytes.len() { break; }
        let data = &dev_field_bytes[offset..offset + size];
        offset += size;

        let key = (dev_field.dev_data_index, dev_field.number);
        let (name, _bt) = match dev_field_names.get(&key) {
            Some(v) => (v.0.as_str(), v.1),
            None => continue,
        };

        match name {
            "Power" => {
                if let Some(v) = read_dev_u16(data) {
                    row.dev_power = Some(v as i16);
                }
            }
            "Cadence" => {
                if let Some(v) = read_dev_u8(data) {
                    row.dev_cadence = Some(v as i16);
                }
            }
            "Core Body Temperature" | "core_temperature" => {
                row.core_temperature = read_dev_f32(data);
            }
            "Current Saturated Hemoglobin Percent" | "SmO2" | "smo2"
            | "saturated_hemoglobin_percent" => {
                row.smo2 = read_dev_f32(data);
            }
            _ => {}
        }
    }
}

/// Decode non-special developer fields into extra columns.
fn decode_record_dev_extras(
    def: &MessageDef,
    dev_field_bytes: &[u8],
    dev_field_names: &HashMap<(u8, u8), (String, u8)>,
    norm_to_col: &HashMap<&str, usize>,
    extra_data: &mut [TypedColumn],
    row_idx: usize,
) {
    let mut offset = 0;
    for dev_field in &def.dev_fields {
        let size = dev_field.size as usize;
        if offset + size > dev_field_bytes.len() { break; }
        let data = &dev_field_bytes[offset..offset + size];
        offset += size;

        let key = (dev_field.dev_data_index, dev_field.number);
        let (name, base_type_id) = match dev_field_names.get(&key) {
            Some(v) => (v.0.as_str(), v.1),
            None => continue,
        };

        // Skip fields already handled by decode_record_dev_fields.
        if is_handled_field(name) { continue; }

        // Resolve to an extra column name.
        if let Some(col_name) = column_for_developer_field(name) {
            if let Some(&col_idx) = norm_to_col.get(col_name.as_str()) {
                // Use the base type from the FieldDescription, no scale/offset
                // for developer fields.
                extra_data[col_idx].set_from_bytes(
                    row_idx, data, base_type_id, false, 1.0, 0.0,
                );
            }
        }
    }
}

/// Read a developer field as u16 (assumes LE, 2 bytes).
fn read_dev_u16(data: &[u8]) -> Option<u16> {
    if data.len() < 2 { return None; }
    let v = u16::from_le_bytes([data[0], data[1]]);
    if v == 0xFFFF { None } else { Some(v) }
}

/// Read a developer field as u8.
fn read_dev_u8(data: &[u8]) -> Option<u8> {
    let v = *data.first()?;
    if v == 0xFF { None } else { Some(v) }
}

/// Read a developer field as f32 (assumes LE, 4 bytes, IEEE 754).
fn read_dev_f32(data: &[u8]) -> Option<f32> {
    if data.len() < 4 { return None; }
    let v = f32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if v.is_finite() { Some(v) } else { None }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(session.duration.is_some(), "expected duration");
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

}
