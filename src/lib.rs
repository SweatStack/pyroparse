use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Read;
use std::sync::Arc;

use arrow::array::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use fitparser::profile::MesgNum;
use fitparser::Value;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Semicircles → degrees: 180 / 2^31.
const SEMICIRCLE_TO_DEGREES: f64 = 180.0 / 2_147_483_648.0;

// ═══════════════════════════════════════════════════════════════════════════
// Full parser — decodes every message via the fitparser crate
// ═══════════════════════════════════════════════════════════════════════════

// ---------------------------------------------------------------------------
// Value extraction helpers
// ---------------------------------------------------------------------------

fn value_to_i16(val: &Value) -> Option<i16> {
    match val {
        Value::UInt8(v) => Some(*v as i16),
        Value::UInt16(v) => i16::try_from(*v).ok(),
        Value::SInt8(v) => Some(*v as i16),
        Value::SInt16(v) => Some(*v),
        Value::Float32(v) => Some(*v as i16),
        Value::Float64(v) => Some(*v as i16),
        _ => None,
    }
}

fn value_to_i8(val: &Value) -> Option<i8> {
    match val {
        Value::SInt8(v) => Some(*v),
        Value::UInt8(v) => i8::try_from(*v).ok(),
        Value::SInt16(v) => i8::try_from(*v).ok(),
        Value::Float32(v) => Some(*v as i8),
        Value::Float64(v) => Some(*v as i8),
        _ => None,
    }
}

fn value_to_i32(val: &Value) -> Option<i32> {
    match val {
        Value::SInt8(v) => Some(*v as i32),
        Value::UInt8(v) => Some(*v as i32),
        Value::SInt16(v) => Some(*v as i32),
        Value::UInt16(v) => Some(*v as i32),
        Value::SInt32(v) => Some(*v),
        Value::Float32(v) => Some(*v as i32),
        Value::Float64(v) => Some(*v as i32),
        _ => None,
    }
}

fn value_to_i64(val: &Value) -> Option<i64> {
    match val {
        Value::SInt8(v) => Some(*v as i64),
        Value::UInt8(v) => Some(*v as i64),
        Value::SInt16(v) => Some(*v as i64),
        Value::UInt16(v) => Some(*v as i64),
        Value::SInt32(v) => Some(*v as i64),
        Value::UInt32(v) => Some(*v as i64),
        Value::SInt64(v) => Some(*v),
        Value::UInt64(v) => Some(*v as i64),
        Value::Float32(v) => Some(*v as i64),
        Value::Float64(v) => Some(*v as i64),
        _ => None,
    }
}

fn value_to_f32(val: &Value) -> Option<f32> {
    match val {
        Value::Float32(v) => Some(*v),
        Value::Float64(v) => Some(*v as f32),
        Value::UInt8(v) => Some(*v as f32),
        Value::UInt16(v) => Some(*v as f32),
        Value::SInt8(v) => Some(*v as f32),
        Value::SInt16(v) => Some(*v as f32),
        _ => None,
    }
}

fn value_to_f64(val: &Value) -> Option<f64> {
    match val {
        Value::Float32(v) => Some(*v as f64),
        Value::Float64(v) => Some(*v),
        Value::UInt8(v) => Some(*v as f64),
        Value::UInt16(v) => Some(*v as f64),
        Value::UInt32(v) => Some(*v as f64),
        Value::UInt64(v) => Some(*v as f64),
        Value::SInt8(v) => Some(*v as f64),
        Value::SInt16(v) => Some(*v as f64),
        Value::SInt32(v) => Some(*v as f64),
        Value::SInt64(v) => Some(*v as f64),
        _ => None,
    }
}

fn value_to_timestamp_us(val: &Value) -> Option<i64> {
    match val {
        Value::Timestamp(dt) => {
            Some(dt.timestamp() * 1_000_000 + dt.timestamp_subsec_micros() as i64)
        }
        _ => None,
    }
}

fn value_to_timestamp_secs(val: &Value) -> Option<f64> {
    match val {
        Value::Timestamp(dt) => {
            Some(dt.timestamp() as f64 + dt.timestamp_subsec_nanos() as f64 / 1e9)
        }
        _ => None,
    }
}

fn value_to_string(val: &Value) -> Option<String> {
    match val {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn value_to_u8(val: &Value) -> Option<u8> {
    match val {
        Value::UInt8(v) => Some(*v),
        Value::UInt16(v) => u8::try_from(*v).ok(),
        Value::SInt16(v) => u8::try_from(*v).ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Canonical merge helpers — developer field values enrich standard fields
// ---------------------------------------------------------------------------

/// Merge a developer i16 value into a canonical column.
///
/// Developer value wins when the canonical value is absent or zero.
/// This handles multi-sport files where a standard field carries a
/// placeholder 0 for sports that don't use that sensor.
fn merge_i16(canonical: &mut Option<i16>, val: &Value) {
    if canonical.unwrap_or(0) == 0 {
        if let Some(v) = value_to_i16(val) {
            *canonical = Some(v);
        }
    }
}

/// Merge a developer f32 value into a canonical column.
fn merge_f32(canonical: &mut Option<f32>, val: &Value) {
    if canonical.unwrap_or(0.0) == 0.0 {
        if let Some(v) = value_to_f32(val) {
            *canonical = Some(v);
        }
    }
}

// ---------------------------------------------------------------------------
// Column-oriented storage for extra (non-fixed) FIT fields
// ---------------------------------------------------------------------------

/// A single extra column stored as a typed vector, one entry per row.
/// Pre-allocated to n_rows with None, then filled during the second pass.
enum TypedColumn {
    I8(Vec<Option<i8>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
}

impl TypedColumn {
    fn new(dtype: &DataType, n_rows: usize) -> Self {
        match dtype {
            DataType::Int8 => Self::I8(vec![None; n_rows]),
            DataType::Int16 => Self::I16(vec![None; n_rows]),
            DataType::Int32 => Self::I32(vec![None; n_rows]),
            DataType::Int64 => Self::I64(vec![None; n_rows]),
            DataType::Float32 => Self::F32(vec![None; n_rows]),
            DataType::Float64 => Self::F64(vec![None; n_rows]),
            DataType::Utf8 => Self::Str(vec![None; n_rows]),
            _ => unreachable!("unsupported extra column type: {dtype:?}"),
        }
    }

    fn set(&mut self, idx: usize, val: &Value) {
        match self {
            Self::I8(v) => v[idx] = value_to_i8(val),
            Self::I16(v) => v[idx] = value_to_i16(val),
            Self::I32(v) => v[idx] = value_to_i32(val),
            Self::I64(v) => v[idx] = value_to_i64(val),
            Self::F32(v) => v[idx] = value_to_f32(val),
            Self::F64(v) => v[idx] = value_to_f64(val),
            Self::Str(v) => v[idx] = value_to_string(val),
        }
    }

    fn to_arrow_array(&self) -> Arc<dyn arrow::array::Array> {
        match self {
            Self::I8(v) => Arc::new(Int8Array::from_iter(v.iter().copied())),
            Self::I16(v) => Arc::new(Int16Array::from_iter(v.iter().copied())),
            Self::I32(v) => Arc::new(Int32Array::from_iter(v.iter().copied())),
            Self::I64(v) => Arc::new(Int64Array::from_iter(v.iter().copied())),
            Self::F32(v) => Arc::new(Float32Array::from_iter(v.iter().copied())),
            Self::F64(v) => Arc::new(Float64Array::from_iter(v.iter().copied())),
            Self::Str(v) => {
                let arr: StringArray = v.iter().map(|s| s.as_deref()).collect();
                Arc::new(arr)
            }
        }
    }
}

/// Map a FIT Value to its Arrow DataType without allocating.
fn fit_value_to_arrow_type(val: &Value) -> Option<DataType> {
    match val {
        Value::SInt8(_) => Some(DataType::Int8),
        Value::UInt8(_) | Value::SInt16(_) => Some(DataType::Int16),
        Value::UInt16(_) | Value::SInt32(_) => Some(DataType::Int32),
        Value::UInt32(_) | Value::SInt64(_) | Value::UInt64(_) => Some(DataType::Int64),
        Value::Float32(_) => Some(DataType::Float32),
        Value::Float64(_) => Some(DataType::Float64),
        Value::String(_) => Some(DataType::Utf8),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Field name normalization
// ---------------------------------------------------------------------------

/// Normalize a FIT field name to a clean column name.
///
/// "Form Power" → "form_power", "DragFactor" → "drag_factor",
/// "heart_rate" → "heart_rate" (unchanged).
fn normalize_field_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 4);
    let chars: Vec<char> = name.chars().collect();
    for (i, &ch) in chars.iter().enumerate() {
        if ch == ' ' {
            result.push('_');
        } else if ch.is_ascii_uppercase() && i > 0 && chars[i - 1].is_ascii_lowercase() {
            result.push('_');
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch.to_ascii_lowercase());
        }
    }
    result
}

/// The 10 standard columns + 2 canonical extras — none of these are added to
/// the dynamic extras discovered from the file.
fn is_canonical_column(name: &str) -> bool {
    matches!(
        name,
        "timestamp"
            | "heart_rate"
            | "power"
            | "cadence"
            | "speed"
            | "latitude"
            | "longitude"
            | "altitude"
            | "temperature"
            | "distance"
            | "core_temperature"
            | "smo2"
    )
}

/// Fast check for raw FIT field names handled by dedicated match arms.
/// Must stay in sync with the Record match arms in `process_messages`.
fn is_handled_field(name: &str) -> bool {
    matches!(
        name,
        // Standard fields
        "timestamp"
            | "heart_rate"
            | "power"
            | "cadence"
            | "speed"
            | "enhanced_speed"
            | "position_lat"
            | "position_long"
            | "altitude"
            | "enhanced_altitude"
            | "temperature"
            | "distance"
            // Developer fields merged into canonical columns
            | "Power"
            | "Cadence"
            | "Core Body Temperature"
            | "core_temperature"
            | "Current Saturated Hemoglobin Percent"
            | "SmO2"
            | "smo2"
            | "saturated_hemoglobin_percent"
    )
}

// ---------------------------------------------------------------------------
// Sensor classification — identify sensors from developer field names
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct DeveloperSensor {
    manufacturer: String,
    product: String,
    sensor_type: String,
    columns: Vec<String>,
}

/// Map a developer field name to a fixed column, if it contributes to one.
fn developer_field_to_fixed(name: &str) -> Option<&'static str> {
    match name {
        "Power" => Some("power"),
        "Core Body Temperature" | "core_temperature" => Some("core_temperature"),
        "Current Saturated Hemoglobin Percent" | "SmO2" | "smo2"
        | "saturated_hemoglobin_percent" => Some("smo2"),
        _ => None,
    }
}

/// Classify a single developer field name to its originating sensor.
/// Returns (manufacturer, product, sensor_type).
fn sensor_for_field(name: &str) -> Option<(&'static str, &'static str, &'static str)> {
    let lower = name.to_lowercase();

    // CORE body temperature sensor (check before Stryd — more specific patterns)
    if (lower.contains("core") && lower.contains("temp"))
        || (lower.contains("skin") && lower.contains("temp"))
        || lower.starts_with("ciq_core")
        || lower.starts_with("ciq_skin")
        || lower == "core_data_quality"
        || lower == "core_reserved"
    {
        return Some(("core", "CORE", "core_temp"));
    }

    // Stryd foot pod
    if lower == "power"
        || lower == "form power"
        || lower == "air power"
        || lower == "ground time"
        || lower == "leg spring stiffness"
        || lower == "vertical oscillation"
        || lower.contains("stryd")
        || lower == "dragfactor"
        || lower == "drag factor"
        || lower == "strokelength"
        || lower == "stroke length"
    {
        return Some(("stryd", "Stryd", "foot_pod"));
    }

    // Muscle oxygen (Moxy, Humon, Train.Red)
    if lower.contains("muscle oxygen")
        || lower == "smo2"
        || lower.contains("hemoglobin")
    {
        return Some(("unknown", "Muscle Oxygen Sensor", "muscle_oxygen"));
    }

    None
}

/// Determine the output column name for a developer field.
/// Returns None if the field would collide with a standard column and gets skipped.
fn column_for_developer_field(name: &str) -> Option<String> {
    if let Some(norm) = developer_field_to_fixed(name) {
        return Some(norm.to_string());
    }
    let normalized = normalize_field_name(name);
    if is_canonical_column(&normalized) {
        return None; // Redundant with standard field
    }
    Some(normalized)
}

/// Classify developer fields into sensors based on individual field names.
///
/// Works across developer_data_index boundaries — a single CIQ app can
/// report data from multiple physical sensors (e.g. Stryd relaying CORE data).
fn classify_developer_sensors(
    dev_field_groups: &BTreeMap<u8, Vec<String>>,
    present_extra_columns: &BTreeSet<String>,
) -> Vec<DeveloperSensor> {
    // Classify each field individually, group by sensor.
    let mut sensor_columns: BTreeMap<
        (&str, &str, &str), // (manufacturer, product, sensor_type)
        BTreeSet<String>,
    > = BTreeMap::new();

    for name in dev_field_groups.values().flatten() {
        if let Some(sensor) = sensor_for_field(name) {
            if let Some(col) = column_for_developer_field(name) {
                // Only include columns that actually appear in the output.
                let exists = present_extra_columns.contains(&col) || is_canonical_column(&col);
                if exists {
                    sensor_columns.entry(sensor).or_default().insert(col);
                }
            }
        }
    }

    sensor_columns
        .into_iter()
        .map(|((manufacturer, product, sensor_type), cols)| DeveloperSensor {
            manufacturer: manufacturer.into(),
            product: product.into(),
            sensor_type: sensor_type.into(),
            columns: cols.into_iter().collect(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct RecordRow {
    timestamp: Option<i64>,
    heart_rate: Option<i16>,
    power: Option<i16>,
    cadence: Option<i16>,
    speed: Option<f32>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    altitude: Option<f32>,
    temperature: Option<i8>,
    distance: Option<f64>,
    // Canonical columns kept in struct for developer field merge,
    // but emitted as extras (not part of the 10 standard columns).
    core_temperature: Option<f32>,
    smo2: Option<f32>,
}

#[derive(Default)]
struct SessionMeta {
    sport: Option<String>,
    sub_sport: Option<String>,
    name: Option<String>,
    start_time: Option<f64>,
    start_time_local: Option<f64>,
    duration: Option<f64>,
    distance: Option<f64>,
    start_timestamp_us: Option<i64>,
    end_timestamp_us: Option<i64>,
}

#[derive(Default)]
struct DeviceMeta {
    manufacturer: Option<String>,
    product: Option<String>,
    serial_number: Option<String>,
    device_index: Option<u8>,
}

struct ParseResult {
    records: Vec<RecordRow>,
    extra_col_info: Vec<(String, DataType)>,
    extra_data: Vec<TypedColumn>,
    sessions: Vec<SessionMeta>,
    devices: Vec<DeviceMeta>,
    developer_sensors: Vec<DeveloperSensor>,
}

// ---------------------------------------------------------------------------
// Message processing (shared by path and bytes entry points)
// ---------------------------------------------------------------------------

fn process_messages(messages: &[fitparser::FitDataRecord]) -> ParseResult {
    let mut sessions = Vec::new();
    let mut devices = Vec::new();
    let mut dev_field_groups: BTreeMap<u8, Vec<String>> = BTreeMap::new();

    // ── Pass 1: process metadata messages + discover extra columns ────────
    let mut n_rows = 0usize;
    let mut extra_types: BTreeMap<String, DataType> = BTreeMap::new();
    // Cache: raw FIT field name → Some(normalized) or None if it maps to a
    // fixed column. Populated once per unique name, avoids repeat allocations.
    let mut raw_to_normalized: HashMap<String, Option<String>> = HashMap::new();

    for msg in messages {
        match msg.kind() {
            MesgNum::Record => {
                n_rows += 1;
                for field in msg.fields() {
                    let raw = field.name();
                    if is_handled_field(raw) {
                        continue;
                    }
                    // Normalize once per unique raw name.
                    if !raw_to_normalized.contains_key(raw) {
                        let n = normalize_field_name(raw);
                        let val = if is_canonical_column(&n) { None } else { Some(n) };
                        raw_to_normalized.insert(raw.to_string(), val);
                    }
                    let Some(normalized) = raw_to_normalized.get(raw).unwrap().as_ref() else {
                        continue;
                    };
                    if let Some(dtype) = fit_value_to_arrow_type(field.value()) {
                        match extra_types.get_mut(normalized.as_str()) {
                            Some(existing) => *existing = promote_type(existing, &dtype),
                            None => {
                                extra_types.insert(normalized.clone(), dtype);
                            }
                        }
                    }
                }
            }
            MesgNum::Session => {
                let mut session = SessionMeta::default();
                for field in msg.fields() {
                    match field.name() {
                        "sport" => session.sport = value_to_string(field.value()),
                        "sub_sport" => session.sub_sport = value_to_string(field.value()),
                        "timestamp" => {
                            session.end_timestamp_us = value_to_timestamp_us(field.value());
                        }
                        "start_time" => {
                            session.start_time = value_to_timestamp_secs(field.value());
                            session.start_timestamp_us = value_to_timestamp_us(field.value());
                        }
                        "local_timestamp" => {
                            session.start_time_local = value_to_timestamp_secs(field.value());
                        }
                        "total_timer_time" => session.duration = value_to_f64(field.value()),
                        "total_distance" => session.distance = value_to_f64(field.value()),
                        _ => {}
                    }
                }
                sessions.push(session);
            }
            MesgNum::DeviceInfo => {
                let mut device = DeviceMeta::default();
                for field in msg.fields() {
                    match field.name() {
                        "manufacturer" => device.manufacturer = value_to_string(field.value()),
                        "product_name" => device.product = value_to_string(field.value()),
                        "serial_number" => {
                            device.serial_number =
                                value_to_f64(field.value()).map(|v| format!("{v:.0}"))
                        }
                        "device_index" => {
                            device.device_index = match field.value() {
                                Value::UInt8(v) => Some(*v),
                                Value::String(s) if s == "creator" => Some(0),
                                Value::String(s) => s.parse().ok(),
                                _ => value_to_f64(field.value()).map(|v| v as u8),
                            }
                        }
                        _ => {}
                    }
                }
                if device.manufacturer.is_some() || device.product.is_some() {
                    devices.push(device);
                }
            }
            MesgNum::FieldDescription => {
                let mut dev_idx: Option<u8> = None;
                let mut field_name: Option<String> = None;
                for field in msg.fields() {
                    match field.name() {
                        "developer_data_index" => dev_idx = value_to_u8(field.value()),
                        "field_name" => field_name = value_to_string(field.value()),
                        _ => {}
                    }
                }
                if let (Some(idx), Some(name)) = (dev_idx, field_name) {
                    if !name.is_empty() {
                        dev_field_groups.entry(idx).or_default().push(name);
                    }
                }
            }
            _ => {}
        }
    }

    // Build extra column info and raw-name → column-index lookup for pass 2.
    let extra_col_info: Vec<(String, DataType)> = extra_types.into_iter().collect();
    let norm_to_col: HashMap<&str, usize> = extra_col_info
        .iter()
        .enumerate()
        .map(|(i, (name, _))| (name.as_str(), i))
        .collect();
    let raw_to_col: HashMap<String, usize> = raw_to_normalized
        .into_iter()
        .filter_map(|(raw, opt_norm)| {
            let norm = opt_norm?;
            let &idx = norm_to_col.get(norm.as_str())?;
            Some((raw, idx))
        })
        .collect();
    let mut extra_data: Vec<TypedColumn> = extra_col_info
        .iter()
        .map(|(_, dtype)| TypedColumn::new(dtype, n_rows))
        .collect();

    // Classify developer sensors.
    let present_extra_columns: BTreeSet<String> =
        extra_col_info.iter().map(|(name, _)| name.clone()).collect();
    let developer_sensors = classify_developer_sensors(&dev_field_groups, &present_extra_columns);

    // ── Pass 2: fill record data ──────────────────────────────────────────
    let mut records = Vec::with_capacity(n_rows);
    let mut row_idx = 0usize;

    for msg in messages {
        if msg.kind() != MesgNum::Record {
            continue;
        }
        let mut row = RecordRow::default();
        for field in msg.fields() {
            match field.name() {
                // Standard fields — set directly
                "timestamp" => row.timestamp = value_to_timestamp_us(field.value()),
                "heart_rate" => row.heart_rate = value_to_i16(field.value()),
                "power" => row.power = value_to_i16(field.value()),
                "speed" | "enhanced_speed" => row.speed = value_to_f32(field.value()),
                "cadence" => row.cadence = value_to_i16(field.value()),
                "position_lat" => {
                    row.latitude =
                        value_to_f64(field.value()).map(|v| v * SEMICIRCLE_TO_DEGREES)
                }
                "position_long" => {
                    row.longitude =
                        value_to_f64(field.value()).map(|v| v * SEMICIRCLE_TO_DEGREES)
                }
                "altitude" | "enhanced_altitude" => {
                    row.altitude = value_to_f32(field.value())
                }
                "temperature" => row.temperature = value_to_i8(field.value()),
                "distance" => row.distance = value_to_f64(field.value()),
                // Developer fields — merge into canonical columns
                "Power" => merge_i16(&mut row.power, field.value()),
                "Cadence" => merge_i16(&mut row.cadence, field.value()),
                "Core Body Temperature" | "core_temperature" => {
                    merge_f32(&mut row.core_temperature, field.value())
                }
                "Current Saturated Hemoglobin Percent" | "SmO2" | "smo2"
                | "saturated_hemoglobin_percent" => {
                    merge_f32(&mut row.smo2, field.value())
                }
                other => {
                    if let Some(&ci) = raw_to_col.get(other) {
                        extra_data[ci].set(row_idx, field.value());
                    }
                }
            }
        }
        records.push(row);
        row_idx += 1;
    }

    ParseResult {
        records,
        extra_col_info,
        extra_data,
        sessions,
        devices,
        developer_sensors,
    }
}

fn read_fit_messages(reader: &mut impl Read) -> PyResult<Vec<fitparser::FitDataRecord>> {
    fitparser::from_reader(reader)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn parse_all_from_bytes(data: &[u8]) -> PyResult<ParseResult> {
    let messages = read_fit_messages(&mut std::io::Cursor::new(data))?;
    Ok(process_messages(&messages))
}

// ---------------------------------------------------------------------------
// Arrow schema & batch construction
// ---------------------------------------------------------------------------

/// Promote two Arrow types to the wider compatible type.
fn promote_type(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        return a.clone();
    }
    match (a, b) {
        (DataType::Int8, DataType::Int16) | (DataType::Int16, DataType::Int8) => DataType::Int16,
        (DataType::Int8 | DataType::Int16, DataType::Int32)
        | (DataType::Int32, DataType::Int8 | DataType::Int16) => DataType::Int32,
        (DataType::Int8 | DataType::Int16 | DataType::Int32, DataType::Int64)
        | (DataType::Int64, DataType::Int8 | DataType::Int16 | DataType::Int32) => DataType::Int64,
        (DataType::Float32, DataType::Float64) | (DataType::Float64, DataType::Float32) => {
            DataType::Float64
        }
        (DataType::Utf8, _) | (_, DataType::Utf8) => DataType::Utf8,
        _ => DataType::Float64,
    }
}

fn build_batch(
    records: &[RecordRow],
    extra_col_info: &[(String, DataType)],
    extra_data: &[TypedColumn],
) -> PyResult<RecordBatch> {
    // Schema: 12 fixed columns, then extras alphabetically.
    // Canonical extras from RecordRow (core_temperature, smo2) — only include
    // if they have at least one non-null value, sorted into the extras.
    let mut canonical_extras: Vec<(String, Arc<dyn arrow::array::Array>)> = Vec::new();
    if records.iter().any(|r| r.core_temperature.is_some()) {
        canonical_extras.push((
            "core_temperature".into(),
            Arc::new(Float32Array::from_iter(
                records.iter().map(|r| r.core_temperature),
            )),
        ));
    }
    if records.iter().any(|r| r.smo2.is_some()) {
        canonical_extras.push((
            "smo2".into(),
            Arc::new(Float32Array::from_iter(records.iter().map(|r| r.smo2))),
        ));
    }

    // Schema: 10 standard columns, then all extras sorted alphabetically.
    let mut fields = vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("heart_rate", DataType::Int16, true),
        Field::new("power", DataType::Int16, true),
        Field::new("cadence", DataType::Int16, true),
        Field::new("speed", DataType::Float32, true),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("altitude", DataType::Float32, true),
        Field::new("temperature", DataType::Int8, true),
        Field::new("distance", DataType::Float64, true),
    ];

    // Merge canonical extras and dynamic extras into one sorted list.
    let mut all_extras: Vec<(&str, &DataType, Option<&Arc<dyn arrow::array::Array>>)> = Vec::new();
    for (name, arr) in &canonical_extras {
        let dt = arr.data_type();
        all_extras.push((name.as_str(), dt, Some(arr)));
    }
    for (name, dtype) in extra_col_info {
        all_extras.push((name.as_str(), dtype, None));
    }
    all_extras.sort_by_key(|(name, _, _)| *name);

    for &(name, dtype, _) in &all_extras {
        fields.push(Field::new(name, dtype.clone(), true));
    }
    let schema = Schema::new(fields);

    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(
            TimestampMicrosecondArray::from(
                records.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
        Arc::new(Int16Array::from_iter(records.iter().map(|r| r.heart_rate))),
        Arc::new(Int16Array::from_iter(records.iter().map(|r| r.power))),
        Arc::new(Int16Array::from_iter(records.iter().map(|r| r.cadence))),
        Arc::new(Float32Array::from_iter(records.iter().map(|r| r.speed))),
        Arc::new(Float64Array::from_iter(records.iter().map(|r| r.latitude))),
        Arc::new(Float64Array::from_iter(
            records.iter().map(|r| r.longitude),
        )),
        Arc::new(Float32Array::from_iter(records.iter().map(|r| r.altitude))),
        Arc::new(Int8Array::from_iter(
            records.iter().map(|r| r.temperature),
        )),
        Arc::new(Float64Array::from_iter(records.iter().map(|r| r.distance))),
    ];
    let mut extra_data_idx = 0;
    for &(_, _, canonical_arr) in &all_extras {
        if let Some(arr) = canonical_arr {
            arrays.push(arr.clone());
        } else {
            arrays.push(extra_data[extra_data_idx].to_arrow_array());
            extra_data_idx += 1;
        }
    }

    RecordBatch::try_new(Arc::new(schema), arrays)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// Metrics detection — which data columns have non-null values
// ---------------------------------------------------------------------------

fn detect_metrics(batch: &RecordBatch) -> Vec<String> {
    let n = batch.num_rows();
    if n == 0 {
        return Vec::new();
    }
    let mut metrics = Vec::new();
    let has_data = |i: usize| batch.column(i).null_count() < n;

    // Standard columns: 0=timestamp 1=hr 2=power 3=cadence 4=speed
    //   5=latitude 6=longitude 7=altitude 8=temperature 9=distance
    if has_data(1) { metrics.push("heart_rate".into()); }
    if has_data(2) { metrics.push("power".into()); }
    if has_data(4) { metrics.push("speed".into()); }
    if has_data(3) { metrics.push("cadence".into()); }
    if has_data(5) && has_data(6) { metrics.push("gps".into()); }
    if has_data(7) { metrics.push("altitude".into()); }
    if has_data(8) { metrics.push("temperature".into()); }
    if has_data(9) { metrics.push("distance".into()); }

    // Extra columns (indices 10+), includes canonical extras like
    // core_temperature and smo2 alongside dynamic extras.
    let schema = batch.schema();
    for i in 10..batch.num_columns() {
        if has_data(i) {
            metrics.push(schema.field(i).name().clone());
        }
    }

    metrics
}

// ---------------------------------------------------------------------------
// Python dict construction helpers
// ---------------------------------------------------------------------------

fn session_to_dict<'py>(
    py: Python<'py>,
    session: &SessionMeta,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("sport", session.sport.as_deref())?;
    dict.set_item("sub_sport", session.sub_sport.as_deref())?;
    dict.set_item("name", session.name.as_deref())?;
    dict.set_item("start_time", session.start_time)?;
    dict.set_item("start_time_local", session.start_time_local)?;
    dict.set_item("duration", session.duration)?;
    dict.set_item("distance", session.distance)?;
    Ok(dict)
}

fn device_to_dict<'py>(
    py: Python<'py>,
    device: &DeviceMeta,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("manufacturer", device.manufacturer.as_deref())?;
    dict.set_item("product", device.product.as_deref())?;
    dict.set_item("serial_number", device.serial_number.as_deref())?;
    dict.set_item("device_index", device.device_index)?;
    Ok(dict)
}

fn sensor_to_dict<'py>(
    py: Python<'py>,
    sensor: &DeveloperSensor,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("manufacturer", &sensor.manufacturer)?;
    dict.set_item("product", &sensor.product)?;
    dict.set_item("sensor_type", &sensor.sensor_type)?;
    let cols = PyList::empty_bound(py);
    for c in &sensor.columns {
        cols.append(c)?;
    }
    dict.set_item("columns", cols)?;
    Ok(dict)
}

fn build_activity_dict<'py>(
    py: Python<'py>,
    batch: &RecordBatch,
    session: Option<&SessionMeta>,
    devices: &[DeviceMeta],
    developer_sensors: &[DeveloperSensor],
) -> PyResult<Bound<'py, PyDict>> {
    let metrics = detect_metrics(batch);

    let activity = PyDict::new_bound(py);
    activity.set_item(
        "records",
        pyo3_arrow::PyRecordBatch::new(batch.clone()).to_pyarrow(py)?,
    )?;

    let meta = match session {
        Some(s) => session_to_dict(py, s)?,
        None => {
            let d = PyDict::new_bound(py);
            for key in [
                "sport", "sub_sport", "name", "start_time", "start_time_local",
                "duration", "distance",
            ] {
                d.set_item(key, py.None())?;
            }
            d
        }
    };

    let metrics_list = PyList::empty_bound(py);
    for m in &metrics {
        metrics_list.append(m)?;
    }
    meta.set_item("metrics", metrics_list)?;

    let devices_list = PyList::empty_bound(py);
    for d in devices {
        devices_list.append(device_to_dict(py, d)?)?;
    }
    meta.set_item("devices", devices_list)?;

    let sensors_list = PyList::empty_bound(py);
    for s in developer_sensors {
        sensors_list.append(sensor_to_dict(py, s)?)?;
    }
    meta.set_item("developer_sensors", sensors_list)?;

    activity.set_item("metadata", meta)?;
    Ok(activity)
}

fn build_parse_result_dict(py: Python<'_>, parsed: ParseResult) -> PyResult<PyObject> {
    let result = PyDict::new_bound(py);
    let activities = PyList::empty_bound(py);

    let batch = build_batch(&parsed.records, &parsed.extra_col_info, &parsed.extra_data)?;

    if parsed.sessions.len() <= 1 {
        let session = parsed.sessions.first();
        activities.append(build_activity_dict(
            py,
            &batch,
            session,
            &parsed.devices,
            &parsed.developer_sensors,
        )?)?;
    } else {
        for session in &parsed.sessions {
            let start = session.start_timestamp_us.unwrap_or(i64::MIN);
            let end = session.end_timestamp_us.unwrap_or(i64::MAX);
            let first = parsed
                .records
                .iter()
                .position(|r| r.timestamp.unwrap_or(0) >= start)
                .unwrap_or(parsed.records.len());
            let last = parsed
                .records
                .iter()
                .rposition(|r| r.timestamp.unwrap_or(0) <= end)
                .map(|i| i + 1)
                .unwrap_or(first);
            let sliced = batch.slice(first, last.saturating_sub(first));
            activities.append(build_activity_dict(
                py,
                &sliced,
                Some(session),
                &parsed.devices,
                &parsed.developer_sensors,
            )?)?;
        }
    }

    result.set_item("activities", activities)?;
    Ok(result.into_any().unbind())
}


// ═══════════════════════════════════════════════════════════════════════════
// Metadata-only scanner — custom binary FIT reader, skips Record data
// ═══════════════════════════════════════════════════════════════════════════

/// Seconds between Unix epoch (1970-01-01) and FIT epoch (1989-12-31).
const FIT_EPOCH_OFFSET: i64 = 631_065_600;

const MESG_SESSION: u16 = 18;
const MESG_RECORD: u16 = 20;
const MESG_DEVICE_INFO: u16 = 23;
const MESG_ACTIVITY: u16 = 34;
const MESG_FIELD_DESCRIPTION: u16 = 206;

const SESSION_START_TIME: u8 = 2;
const SESSION_SPORT: u8 = 5;
const SESSION_SUB_SPORT: u8 = 6;
const SESSION_TOTAL_TIMER_TIME: u8 = 7;
const SESSION_TOTAL_DISTANCE: u8 = 9;
const SESSION_TIMESTAMP: u8 = 253;

const ACTIVITY_LOCAL_TIMESTAMP: u8 = 5;

const DEVICE_INDEX: u8 = 0;
const DEVICE_MANUFACTURER: u8 = 2;
const DEVICE_SERIAL_NUMBER: u8 = 3;
const DEVICE_PRODUCT_NAME: u8 = 27;

const RECORD_POSITION_LAT: u8 = 0;
const RECORD_POSITION_LONG: u8 = 1;
const RECORD_ALTITUDE: u8 = 2;
const RECORD_HEART_RATE: u8 = 3;
const RECORD_CADENCE: u8 = 4;
const RECORD_DISTANCE: u8 = 5;
const RECORD_SPEED: u8 = 6;
const RECORD_POWER: u8 = 7;
const RECORD_TEMPERATURE: u8 = 13;
const RECORD_ENHANCED_SPEED: u8 = 73;
const RECORD_ENHANCED_ALTITUDE: u8 = 78;

/// Map a developer field name to a known metric name, if recognized.
fn classify_developer_field(name: &str) -> Option<&'static str> {
    let lower = name.to_lowercase();
    if lower.contains("core") && lower.contains("temp") { return Some("core_temperature"); }
    if lower.contains("hemoglobin") || lower.contains("smo2") || lower.contains("muscle oxygen") {
        return Some("smo2");
    }
    if lower == "power" { return Some("power"); }
    None
}

fn sport_name(v: u8) -> &'static str {
    match v {
        0 => "generic", 1 => "running", 2 => "cycling", 3 => "transition",
        4 => "fitness_equipment", 5 => "swimming", 6 => "basketball",
        7 => "soccer", 8 => "tennis", 10 => "training", 11 => "walking",
        12 => "cross_country_skiing", 13 => "alpine_skiing",
        14 => "snowboarding", 15 => "rowing", 16 => "mountaineering",
        17 => "hiking", 18 => "multisport", 19 => "paddling",
        21 => "e_biking", 23 => "boating", 25 => "golf",
        37 => "stand_up_paddleboarding", 38 => "surfing", 53 => "diving",
        _ => "unknown",
    }
}

fn manufacturer_name(v: u16) -> &'static str {
    match v {
        1 | 15 | 44 => "garmin", 32 => "wahoo_fitness", 38 => "favero",
        69 => "stages_cycling", 76 => "mio", 86 => "shimano",
        89 => "concept2", 260 => "zwift", 263 => "hammerhead",
        _ => "unknown",
    }
}

// ---------------------------------------------------------------------------
// Binary scanner data structures
// ---------------------------------------------------------------------------

struct FieldDef {
    num: u8,
    size: u8,
}

struct MesgDef {
    global_num: u16,
    big_endian: bool,
    fields: Vec<FieldDef>,
    total_size: usize,
}

#[derive(Default)]
struct ScanResult {
    sessions: Vec<SessionMeta>,
    devices: Vec<DeviceMeta>,
    local_timestamp: Option<f64>,
    record_metrics: Vec<String>,
    developer_sensors: Vec<DeveloperSensor>,
}

// ---------------------------------------------------------------------------
// Binary scanner implementation
// ---------------------------------------------------------------------------

struct FitScanner<'a> {
    buf: &'a [u8],
    pos: usize,
    end: usize,
    defs: [Option<MesgDef>; 16],
}

impl<'a> FitScanner<'a> {
    fn new(buf: &'a [u8]) -> Result<Self, String> {
        if buf.len() < 12 { return Err("File too short for FIT header".into()); }
        if &buf[8..12] != b".FIT" { return Err("Missing .FIT signature".into()); }
        let header_size = buf[0] as usize;
        let data_size = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        let end = header_size + data_size;
        if buf.len() < end { return Err("File truncated".into()); }
        Ok(Self { buf, pos: header_size, end, defs: Default::default() })
    }

    fn scan(&mut self) -> Result<ScanResult, String> {
        let mut result = ScanResult::default();
        let mut metric_set = std::collections::HashSet::new();
        let mut dev_field_groups: BTreeMap<u8, Vec<String>> = BTreeMap::new();

        while self.pos < self.end {
            let header = self.read_byte()?;

            if header & 0x80 != 0 {
                let local_type = ((header >> 5) & 0x03) as usize;
                self.skip_data(local_type)?;
            } else if header & 0x40 != 0 {
                let has_dev = header & 0x20 != 0;
                let local_type = (header & 0x0F) as usize;
                self.read_definition(local_type, has_dev)?;

                if let Some(def) = &self.defs[local_type] {
                    if def.global_num == MESG_RECORD {
                        for m in Self::detect_metrics_from_def(def) {
                            metric_set.insert(m);
                        }
                    }
                }
            } else {
                let local_type = (header & 0x0F) as usize;
                let def = self.defs[local_type]
                    .as_ref()
                    .ok_or("Data message without preceding definition")?;

                if def.global_num == MESG_RECORD {
                    self.advance(def.total_size)?;
                } else {
                    let global_num = def.global_num;
                    let big_endian = def.big_endian;
                    let fields = self.read_fields(local_type)?;

                    match global_num {
                        MESG_SESSION => {
                            result.sessions.push(Self::decode_session(&fields, big_endian));
                        }
                        MESG_ACTIVITY => {
                            result.local_timestamp =
                                Self::decode_activity_local_ts(&fields, big_endian);
                        }
                        MESG_DEVICE_INFO => {
                            if let Some(d) = Self::decode_device(&fields, big_endian) {
                                result.devices.push(d);
                            }
                        }
                        MESG_FIELD_DESCRIPTION => {
                            if let Some(metric) = Self::decode_developer_metric(&fields) {
                                metric_set.insert(metric);
                            }
                            if let Some((idx, name)) =
                                Self::decode_developer_field_info(&fields)
                            {
                                let normalized = normalize_field_name(&name);
                                if !is_canonical_column(&normalized) {
                                    metric_set.insert(normalized);
                                }
                                dev_field_groups.entry(idx).or_default().push(name);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        result.record_metrics = metric_set.into_iter().collect();
        // Scanner doesn't parse records, so we use the developer field names
        // themselves as "present columns" for sensor classification.
        let dev_columns: BTreeSet<String> = dev_field_groups
            .values()
            .flatten()
            .filter_map(|n| column_for_developer_field(n))
            .collect();
        result.developer_sensors = classify_developer_sensors(&dev_field_groups, &dev_columns);

        if let Some(lt) = result.local_timestamp {
            for s in &mut result.sessions {
                if s.start_time_local.is_none() {
                    s.start_time_local = Some(lt);
                }
            }
        }

        Ok(result)
    }

    fn read_byte(&mut self) -> Result<u8, String> {
        if self.pos >= self.buf.len() { return Err("Unexpected end of FIT data".into()); }
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.pos + n > self.buf.len() { return Err("Unexpected end of FIT data".into()); }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn advance(&mut self, n: usize) -> Result<(), String> {
        if self.pos + n > self.buf.len() { return Err("Unexpected end of FIT data".into()); }
        self.pos += n;
        Ok(())
    }

    fn read_definition(&mut self, local_type: usize, has_dev: bool) -> Result<(), String> {
        let _reserved = self.read_byte()?;
        let arch = self.read_byte()?;
        let big_endian = arch == 1;
        let gm = self.read_bytes(2)?;
        let global_num = if big_endian { u16::from_be_bytes([gm[0], gm[1]]) }
                         else { u16::from_le_bytes([gm[0], gm[1]]) };
        let num_fields = self.read_byte()? as usize;
        let mut fields = Vec::with_capacity(num_fields);
        let mut total_size = 0;
        for _ in 0..num_fields {
            let num = self.read_byte()?;
            let size = self.read_byte()?;
            let _base_type = self.read_byte()?;
            total_size += size as usize;
            fields.push(FieldDef { num, size });
        }
        if has_dev {
            let num_dev = self.read_byte()? as usize;
            for _ in 0..num_dev {
                let _num = self.read_byte()?;
                let size = self.read_byte()?;
                let _idx = self.read_byte()?;
                total_size += size as usize;
            }
        }
        self.defs[local_type] = Some(MesgDef { global_num, big_endian, fields, total_size });
        Ok(())
    }

    fn skip_data(&mut self, local_type: usize) -> Result<(), String> {
        let size = self.defs[local_type].as_ref()
            .ok_or("Data message without preceding definition")?.total_size;
        self.advance(size)
    }

    fn read_fields(&mut self, local_type: usize) -> Result<Vec<(u8, Vec<u8>)>, String> {
        let def = self.defs[local_type].as_ref()
            .ok_or("Data message without preceding definition")?;
        let field_layout: Vec<(u8, u8)> = def.fields.iter().map(|f| (f.num, f.size)).collect();
        let total_size = def.total_size;
        let mut out = Vec::with_capacity(field_layout.len());
        let mut regular = 0usize;
        for (num, size) in &field_layout {
            let data = self.read_bytes(*size as usize)?.to_vec();
            regular += *size as usize;
            out.push((*num, data));
        }
        let remaining = total_size.saturating_sub(regular);
        if remaining > 0 { self.advance(remaining)?; }
        Ok(out)
    }

    fn detect_metrics_from_def(def: &MesgDef) -> Vec<String> {
        let mut metrics = Vec::new();
        let mut has_lat = false;
        let mut has_long = false;
        for f in &def.fields {
            match f.num {
                RECORD_HEART_RATE => metrics.push("heart_rate".into()),
                RECORD_POWER => metrics.push("power".into()),
                RECORD_SPEED | RECORD_ENHANCED_SPEED => {
                    if !metrics.contains(&"speed".to_string()) { metrics.push("speed".into()); }
                }
                RECORD_CADENCE => metrics.push("cadence".into()),
                RECORD_POSITION_LAT => has_lat = true,
                RECORD_POSITION_LONG => has_long = true,
                RECORD_ALTITUDE | RECORD_ENHANCED_ALTITUDE => {
                    if !metrics.contains(&"altitude".to_string()) { metrics.push("altitude".into()); }
                }
                RECORD_TEMPERATURE => metrics.push("temperature".into()),
                RECORD_DISTANCE => metrics.push("distance".into()),
                _ => {}
            }
        }
        if has_lat && has_long { metrics.push("gps".into()); }
        metrics
    }

    fn decode_session(fields: &[(u8, Vec<u8>)], big_endian: bool) -> SessionMeta {
        let mut s = SessionMeta::default();
        for (num, data) in fields {
            match *num {
                SESSION_SPORT if !data.is_empty() && data[0] != 0xFF => {
                    s.sport = Some(sport_name(data[0]).to_string());
                }
                SESSION_SUB_SPORT if !data.is_empty() && data[0] != 0xFF => {
                    s.sub_sport = Some(format!("{}", data[0]));
                }
                SESSION_START_TIME if data.len() >= 4 => {
                    if let Some(ts) = valid_u32(data, big_endian) {
                        s.start_time = Some((ts as i64 + FIT_EPOCH_OFFSET) as f64);
                        s.start_timestamp_us = Some((ts as i64 + FIT_EPOCH_OFFSET) * 1_000_000);
                    }
                }
                SESSION_TIMESTAMP if data.len() >= 4 => {
                    if let Some(ts) = valid_u32(data, big_endian) {
                        s.end_timestamp_us = Some((ts as i64 + FIT_EPOCH_OFFSET) * 1_000_000);
                    }
                }
                SESSION_TOTAL_TIMER_TIME if data.len() >= 4 => {
                    if let Some(v) = valid_u32(data, big_endian) {
                        s.duration = Some(v as f64 / 1000.0);
                    }
                }
                SESSION_TOTAL_DISTANCE if data.len() >= 4 => {
                    if let Some(v) = valid_u32(data, big_endian) {
                        s.distance = Some(v as f64 / 100.0);
                    }
                }
                _ => {}
            }
        }
        s
    }

    fn decode_activity_local_ts(fields: &[(u8, Vec<u8>)], big_endian: bool) -> Option<f64> {
        for (num, data) in fields {
            if *num == ACTIVITY_LOCAL_TIMESTAMP && data.len() >= 4 {
                if let Some(ts) = valid_u32(data, big_endian) {
                    return Some((ts as i64 + FIT_EPOCH_OFFSET) as f64);
                }
            }
        }
        None
    }

    fn decode_developer_metric(fields: &[(u8, Vec<u8>)]) -> Option<String> {
        // FieldDescription field 3 is the developer field name (string).
        for (num, data) in fields {
            if *num == 3 {
                let name = String::from_utf8_lossy(data)
                    .trim_end_matches('\0')
                    .to_string();
                if let Some(metric) = classify_developer_field(&name) {
                    return Some(metric.to_string());
                }
            }
        }
        None
    }

    /// Extract (developer_data_index, field_name) from a FieldDescription message.
    fn decode_developer_field_info(fields: &[(u8, Vec<u8>)]) -> Option<(u8, String)> {
        let mut dev_idx: Option<u8> = None;
        let mut name: Option<String> = None;
        for (num, data) in fields {
            match *num {
                0 => dev_idx = data.first().copied(), // developer_data_index
                3 => {
                    // field_name
                    let s = String::from_utf8_lossy(data)
                        .trim_end_matches('\0')
                        .to_string();
                    if !s.is_empty() {
                        name = Some(s);
                    }
                }
                _ => {}
            }
        }
        dev_idx.zip(name)
    }

    fn decode_device(fields: &[(u8, Vec<u8>)], big_endian: bool) -> Option<DeviceMeta> {
        let mut d = DeviceMeta::default();
        for (num, data) in fields {
            match *num {
                DEVICE_INDEX if !data.is_empty() && data[0] != 0xFF => {
                    d.device_index = Some(data[0]);
                }
                DEVICE_MANUFACTURER if data.len() >= 2 => {
                    let v = read_u16(data, big_endian);
                    if v != 0xFFFF { d.manufacturer = Some(manufacturer_name(v).to_string()); }
                }
                DEVICE_SERIAL_NUMBER if data.len() >= 4 => {
                    let v = read_u32(data, big_endian);
                    if v != 0 && v != 0xFFFFFFFF { d.serial_number = Some(format!("{v}")); }
                }
                DEVICE_PRODUCT_NAME => {
                    let s = String::from_utf8_lossy(data).trim_end_matches('\0').to_string();
                    if !s.is_empty() { d.product = Some(s); }
                }
                _ => {}
            }
        }
        if d.manufacturer.is_some() || d.product.is_some() { Some(d) } else { None }
    }
}

fn valid_u32(data: &[u8], big_endian: bool) -> Option<u32> {
    let v = read_u32(data, big_endian);
    if v == 0xFFFFFFFF { None } else { Some(v) }
}

fn read_u16(data: &[u8], big_endian: bool) -> u16 {
    if big_endian { u16::from_be_bytes([data[0], data[1]]) }
    else { u16::from_le_bytes([data[0], data[1]]) }
}

fn read_u32(data: &[u8], big_endian: bool) -> u32 {
    if big_endian { u32::from_be_bytes([data[0], data[1], data[2], data[3]]) }
    else { u32::from_le_bytes([data[0], data[1], data[2], data[3]]) }
}

fn build_scan_result_dict(py: Python<'_>, scan: &ScanResult) -> PyResult<PyObject> {
    let result = PyDict::new_bound(py);
    let activities = PyList::empty_bound(py);

    let build_one = |session: &SessionMeta| -> PyResult<Bound<'_, PyDict>> {
        let activity = PyDict::new_bound(py);
        let meta = session_to_dict(py, session)?;
        let metrics_list = PyList::empty_bound(py);
        for m in &scan.record_metrics { metrics_list.append(m)?; }
        meta.set_item("metrics", metrics_list)?;
        let devices_list = PyList::empty_bound(py);
        for d in &scan.devices { devices_list.append(device_to_dict(py, d)?)?; }
        meta.set_item("devices", devices_list)?;
        let sensors_list = PyList::empty_bound(py);
        for s in &scan.developer_sensors { sensors_list.append(sensor_to_dict(py, s)?)?; }
        meta.set_item("developer_sensors", sensors_list)?;
        activity.set_item("metadata", meta)?;
        Ok(activity)
    };

    if scan.sessions.is_empty() {
        let activity = PyDict::new_bound(py);
        let meta = PyDict::new_bound(py);
        for key in ["sport", "sub_sport", "name", "start_time", "start_time_local",
                     "duration", "distance"] {
            meta.set_item(key, py.None())?;
        }
        let metrics_list = PyList::empty_bound(py);
        for m in &scan.record_metrics { metrics_list.append(m)?; }
        meta.set_item("metrics", metrics_list)?;
        meta.set_item("devices", PyList::empty_bound(py))?;
        let sensors_list = PyList::empty_bound(py);
        for s in &scan.developer_sensors { sensors_list.append(sensor_to_dict(py, s)?)?; }
        meta.set_item("developer_sensors", sensors_list)?;
        activity.set_item("metadata", meta)?;
        activities.append(activity)?;
    } else {
        for session in &scan.sessions { activities.append(build_one(session)?)?; }
    }

    result.set_item("activities", activities)?;
    Ok(result.into_any().unbind())
}

// ═══════════════════════════════════════════════════════════════════════════
// Python-exposed functions
// ═══════════════════════════════════════════════════════════════════════════

fn do_parse(py: Python<'_>, data: &[u8]) -> PyResult<PyObject> {
    let parsed = parse_all_from_bytes(data)?;
    build_parse_result_dict(py, parsed)
}

#[pyfunction]
fn parse_fit(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let data = std::fs::read(path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    do_parse(py, &data)
}

#[pyfunction]
fn parse_fit_bytes(py: Python<'_>, data: &[u8]) -> PyResult<PyObject> {
    do_parse(py, data)
}

/// Metadata-only scan from file path — skips Record data.
#[pyfunction]
fn parse_fit_metadata(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let data = std::fs::read(path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let mut scanner =
        FitScanner::new(&data).map_err(pyo3::exceptions::PyValueError::new_err)?;
    let result = scanner.scan().map_err(pyo3::exceptions::PyValueError::new_err)?;
    build_scan_result_dict(py, &result)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_fit, m)?)?;
    m.add_function(wrap_pyfunction!(parse_fit_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(parse_fit_metadata, m)?)?;
    Ok(())
}
