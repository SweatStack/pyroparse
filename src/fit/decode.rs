//! FIT message decoder.
//!
//! Decodes raw binary events from [`FitReader`] into typed metadata
//! structures using the generated profile definitions. Supports a
//! metadata-only scan mode that skips Record messages entirely.
//!
//! This module replaces the hand-written `FitScanner` with a decoder
//! built on the binary reader and generated profile.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::fit::binary::{FitEvent, FitReader, MessageDef};
use crate::fit::profile;
use crate::reference::classify_developer_field;
use crate::fields::{normalize_field_name, is_canonical_column};
use crate::{
    SessionMeta, DeviceMeta, DeveloperSensor, ScanResult,
    classify_developer_sensors, bytes_to_uuid, name_for_uuid,
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

    #[test]
    fn test_scan_parity_with_fitscanner() {
        // Compare new scanner against the old FitScanner for all fixtures.
        use crate::FitScanner;

        for fixture in &["test.fit", "with-developer-fields.fit", "cycling-rowing-cycling-rowing.fit"] {
            let path = std::path::Path::new("tests/fixtures").join(fixture);
            if !path.exists() { continue; }
            let data = std::fs::read(&path).unwrap();

            let mut old_scanner = FitScanner::new(&data).unwrap();
            let old_result = old_scanner.scan().unwrap();
            let new_result = scan_metadata(&data).unwrap();

            // Session count.
            assert_eq!(
                old_result.sessions.len(), new_result.sessions.len(),
                "{fixture}: session count mismatch"
            );

            // Per-session fields.
            for (i, (old, new)) in old_result.sessions.iter().zip(&new_result.sessions).enumerate() {
                assert_eq!(old.sport, new.sport, "{fixture} session {i}: sport mismatch");
                assert_eq!(old.sub_sport, new.sub_sport, "{fixture} session {i}: sub_sport mismatch");
                assert_eq!(old.start_time, new.start_time, "{fixture} session {i}: start_time mismatch");
                assert_eq!(old.start_timestamp_us, new.start_timestamp_us, "{fixture} session {i}: start_timestamp_us mismatch");
                assert_eq!(old.end_timestamp_us, new.end_timestamp_us, "{fixture} session {i}: end_timestamp_us mismatch");
                assert_eq!(old.duration, new.duration, "{fixture} session {i}: duration mismatch");
                assert_eq!(old.distance, new.distance, "{fixture} session {i}: distance mismatch");
            }

            // Device count.
            assert_eq!(
                old_result.devices.len(), new_result.devices.len(),
                "{fixture}: device count mismatch"
            );

            // Metrics (as sorted sets for order-independent comparison).
            let mut old_metrics: Vec<_> = old_result.record_metrics.clone();
            let mut new_metrics: Vec<_> = new_result.record_metrics.clone();
            old_metrics.sort();
            new_metrics.sort();
            assert_eq!(old_metrics, new_metrics, "{fixture}: metrics mismatch");

            // Developer sensor count.
            assert_eq!(
                old_result.developer_sensors.len(), new_result.developer_sensors.len(),
                "{fixture}: developer sensor count mismatch"
            );
        }
    }
}
