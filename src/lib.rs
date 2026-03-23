use std::collections::{BTreeMap, BTreeSet};
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
// Dynamic value type — for FIT fields outside the fixed schema
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum DynValue {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(String),
}

impl DynValue {
    fn arrow_type(&self) -> DataType {
        match self {
            Self::I8(_) => DataType::Int8,
            Self::I16(_) => DataType::Int16,
            Self::I32(_) => DataType::Int32,
            Self::I64(_) => DataType::Int64,
            Self::F32(_) => DataType::Float32,
            Self::F64(_) => DataType::Float64,
            Self::Str(_) => DataType::Utf8,
        }
    }

    fn as_i8(&self) -> Option<i8> {
        match self {
            Self::I8(v) => Some(*v),
            _ => None,
        }
    }

    fn as_i16(&self) -> Option<i16> {
        match self {
            Self::I8(v) => Some(*v as i16),
            Self::I16(v) => Some(*v),
            _ => None,
        }
    }

    fn as_i32(&self) -> Option<i32> {
        match self {
            Self::I8(v) => Some(*v as i32),
            Self::I16(v) => Some(*v as i32),
            Self::I32(v) => Some(*v),
            _ => None,
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            Self::I8(v) => Some(*v as i64),
            Self::I16(v) => Some(*v as i64),
            Self::I32(v) => Some(*v as i64),
            Self::I64(v) => Some(*v),
            _ => None,
        }
    }

    fn as_f32(&self) -> Option<f32> {
        match self {
            Self::I8(v) => Some(*v as f32),
            Self::I16(v) => Some(*v as f32),
            Self::F32(v) => Some(*v),
            _ => None,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::I8(v) => Some(*v as f64),
            Self::I16(v) => Some(*v as f64),
            Self::I32(v) => Some(*v as f64),
            Self::I64(v) => Some(*v as f64),
            Self::F32(v) => Some(*v as f64),
            Self::F64(v) => Some(*v),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s),
            _ => None,
        }
    }
}

fn to_dyn_value(val: &Value) -> Option<DynValue> {
    match val {
        Value::SInt8(v) => Some(DynValue::I8(*v)),
        Value::UInt8(v) => Some(DynValue::I16(*v as i16)),
        Value::SInt16(v) => Some(DynValue::I16(*v)),
        Value::UInt16(v) => Some(DynValue::I32(*v as i32)),
        Value::SInt32(v) => Some(DynValue::I32(*v)),
        Value::UInt32(v) => Some(DynValue::I64(*v as i64)),
        Value::SInt64(v) => Some(DynValue::I64(*v)),
        Value::UInt64(v) => Some(DynValue::I64(*v as i64)),
        Value::Float32(v) => Some(DynValue::F32(*v)),
        Value::Float64(v) => Some(DynValue::F64(*v)),
        Value::String(s) => Some(DynValue::Str(s.clone())),
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

/// The 12 fixed columns — these are never added to `extra`.
fn is_fixed_column(name: &str) -> bool {
    matches!(
        name,
        "timestamp"
            | "heart_rate"
            | "power"
            | "cadence"
            | "speed"
            | "position_lat"
            | "position_long"
            | "altitude"
            | "temperature"
            | "distance"
            | "core_temperature"
            | "smo2"
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
    if is_fixed_column(&normalized) {
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
                let exists = present_extra_columns.contains(&col) || is_fixed_column(&col);
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
    position_lat: Option<f64>,
    position_long: Option<f64>,
    altitude: Option<f32>,
    temperature: Option<i8>,
    distance: Option<f64>,
    core_temperature: Option<f32>,
    smo2: Option<f32>,
    /// All other fields not in the fixed schema.
    extra: BTreeMap<String, DynValue>,
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
    sessions: Vec<SessionMeta>,
    devices: Vec<DeviceMeta>,
    developer_sensors: Vec<DeveloperSensor>,
}

// ---------------------------------------------------------------------------
// Message processing (shared by path and bytes entry points)
// ---------------------------------------------------------------------------

fn process_messages(messages: &[fitparser::FitDataRecord]) -> ParseResult {
    let mut records = Vec::new();
    let mut sessions = Vec::new();
    let mut devices = Vec::new();
    let mut dev_field_groups: BTreeMap<u8, Vec<String>> = BTreeMap::new();

    for msg in messages {
        match msg.kind() {
            MesgNum::Record => {
                let mut row = RecordRow::default();
                for field in msg.fields() {
                    match field.name() {
                        // === Normalized columns (stable schema) ===
                        "timestamp" => row.timestamp = value_to_timestamp_us(field.value()),
                        "heart_rate" => row.heart_rate = value_to_i16(field.value()),
                        "power" => row.power = value_to_i16(field.value()),
                        "speed" | "enhanced_speed" => row.speed = value_to_f32(field.value()),
                        "cadence" => row.cadence = value_to_i16(field.value()),
                        "position_lat" => {
                            row.position_lat =
                                value_to_f64(field.value()).map(|v| v * SEMICIRCLE_TO_DEGREES)
                        }
                        "position_long" => {
                            row.position_long =
                                value_to_f64(field.value()).map(|v| v * SEMICIRCLE_TO_DEGREES)
                        }
                        "altitude" | "enhanced_altitude" => {
                            row.altitude = value_to_f32(field.value())
                        }
                        "temperature" => row.temperature = value_to_i8(field.value()),
                        "distance" => row.distance = value_to_f64(field.value()),
                        // === Developer fields → fixed columns ===
                        "Power" if row.power.is_none() => {
                            row.power = value_to_i16(field.value())
                        }
                        "Core Body Temperature" | "core_temperature" => {
                            row.core_temperature = value_to_f32(field.value())
                        }
                        "Current Saturated Hemoglobin Percent" | "SmO2" | "smo2"
                        | "saturated_hemoglobin_percent" => {
                            row.smo2 = value_to_f32(field.value())
                        }
                        // === Everything else → extra columns ===
                        other => {
                            let name = normalize_field_name(other);
                            if !is_fixed_column(&name) {
                                if let Some(val) = to_dyn_value(field.value()) {
                                    row.extra.entry(name).or_insert(val);
                                }
                            }
                        }
                    }
                }
                records.push(row);
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

    let present_extra_columns: BTreeSet<String> = records
        .iter()
        .flat_map(|r| r.extra.keys().cloned())
        .collect();
    let developer_sensors = classify_developer_sensors(&dev_field_groups, &present_extra_columns);

    ParseResult {
        records,
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
        // Integer widening
        (DataType::Int8, DataType::Int16) | (DataType::Int16, DataType::Int8) => DataType::Int16,
        (DataType::Int8 | DataType::Int16, DataType::Int32)
        | (DataType::Int32, DataType::Int8 | DataType::Int16) => DataType::Int32,
        (DataType::Int8 | DataType::Int16 | DataType::Int32, DataType::Int64)
        | (DataType::Int64, DataType::Int8 | DataType::Int16 | DataType::Int32) => DataType::Int64,
        // Float widening
        (DataType::Float32, DataType::Float64) | (DataType::Float64, DataType::Float32) => {
            DataType::Float64
        }
        // Integer + Float → Float64
        (DataType::Utf8, _) | (_, DataType::Utf8) => DataType::Utf8,
        _ => DataType::Float64,
    }
}

/// Discover the extra columns present across all rows, with their Arrow types.
/// Returns (name, type) pairs sorted alphabetically by name.
fn discover_extra_columns(rows: &[RecordRow]) -> Vec<(String, DataType)> {
    let mut columns: BTreeMap<String, DataType> = BTreeMap::new();
    for row in rows {
        for (name, val) in &row.extra {
            let new_type = val.arrow_type();
            columns
                .entry(name.clone())
                .and_modify(|existing| *existing = promote_type(existing, &new_type))
                .or_insert(new_type);
        }
    }
    columns.into_iter().collect()
}

/// Build an Arrow array for a single extra column across all rows.
fn build_extra_array(
    rows: &[RecordRow],
    name: &str,
    dtype: &DataType,
) -> Arc<dyn arrow::array::Array> {
    match dtype {
        DataType::Int8 => Arc::new(Int8Array::from_iter(
            rows.iter().map(|r| r.extra.get(name).and_then(|v| v.as_i8())),
        )),
        DataType::Int16 => Arc::new(Int16Array::from_iter(
            rows.iter()
                .map(|r| r.extra.get(name).and_then(|v| v.as_i16())),
        )),
        DataType::Int32 => Arc::new(Int32Array::from_iter(
            rows.iter()
                .map(|r| r.extra.get(name).and_then(|v| v.as_i32())),
        )),
        DataType::Int64 => Arc::new(Int64Array::from_iter(
            rows.iter()
                .map(|r| r.extra.get(name).and_then(|v| v.as_i64())),
        )),
        DataType::Float32 => Arc::new(Float32Array::from_iter(
            rows.iter()
                .map(|r| r.extra.get(name).and_then(|v| v.as_f32())),
        )),
        DataType::Float64 => Arc::new(Float64Array::from_iter(
            rows.iter()
                .map(|r| r.extra.get(name).and_then(|v| v.as_f64())),
        )),
        DataType::Utf8 => {
            let arr: StringArray = rows
                .iter()
                .map(|r| r.extra.get(name).and_then(|v| v.as_str()))
                .collect();
            Arc::new(arr)
        }
        _ => unreachable!("unexpected extra column type"),
    }
}

fn rows_to_batch(rows: &[RecordRow]) -> PyResult<RecordBatch> {
    let extra_cols = discover_extra_columns(rows);

    // Build schema: fixed columns first, then extras alphabetically.
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
        Field::new("position_lat", DataType::Float64, true),
        Field::new("position_long", DataType::Float64, true),
        Field::new("altitude", DataType::Float32, true),
        Field::new("temperature", DataType::Int8, true),
        Field::new("distance", DataType::Float64, true),
        Field::new("core_temperature", DataType::Float32, true),
        Field::new("smo2", DataType::Float32, true),
    ];
    for (name, dtype) in &extra_cols {
        fields.push(Field::new(name, dtype.clone(), true));
    }
    let schema = Schema::new(fields);

    // Build arrays: fixed columns first.
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(
            TimestampMicrosecondArray::from(
                rows.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
        Arc::new(Int16Array::from_iter(rows.iter().map(|r| r.heart_rate))),
        Arc::new(Int16Array::from_iter(rows.iter().map(|r| r.power))),
        Arc::new(Int16Array::from_iter(rows.iter().map(|r| r.cadence))),
        Arc::new(Float32Array::from_iter(rows.iter().map(|r| r.speed))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.position_lat))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.position_long))),
        Arc::new(Float32Array::from_iter(rows.iter().map(|r| r.altitude))),
        Arc::new(Int8Array::from_iter(rows.iter().map(|r| r.temperature))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.distance))),
        Arc::new(Float32Array::from_iter(rows.iter().map(|r| r.core_temperature))),
        Arc::new(Float32Array::from_iter(rows.iter().map(|r| r.smo2))),
    ];
    for (name, dtype) in &extra_cols {
        arrays.push(build_extra_array(rows, name, dtype));
    }

    RecordBatch::try_new(Arc::new(schema), arrays)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// Metrics detection — which data columns have non-null values
// ---------------------------------------------------------------------------

fn detect_metrics(rows: &[RecordRow]) -> Vec<String> {
    let mut metrics = Vec::new();
    if rows.iter().any(|r| r.heart_rate.is_some()) { metrics.push("heart_rate".into()); }
    if rows.iter().any(|r| r.power.is_some()) { metrics.push("power".into()); }
    if rows.iter().any(|r| r.speed.is_some()) { metrics.push("speed".into()); }
    if rows.iter().any(|r| r.cadence.is_some()) { metrics.push("cadence".into()); }
    if rows.iter().any(|r| r.position_lat.is_some()) { metrics.push("gps".into()); }
    if rows.iter().any(|r| r.altitude.is_some()) { metrics.push("altitude".into()); }
    if rows.iter().any(|r| r.temperature.is_some()) { metrics.push("temperature".into()); }
    if rows.iter().any(|r| r.distance.is_some()) { metrics.push("distance".into()); }
    if rows.iter().any(|r| r.core_temperature.is_some()) { metrics.push("core_temperature".into()); }
    if rows.iter().any(|r| r.smo2.is_some()) { metrics.push("smo2".into()); }

    // Extra columns — any that appear have at least one non-null value.
    let mut extra_names: BTreeSet<String> = BTreeSet::new();
    for row in rows {
        extra_names.extend(row.extra.keys().cloned());
    }
    metrics.extend(extra_names);

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
    rows: &[RecordRow],
    session: Option<&SessionMeta>,
    devices: &[DeviceMeta],
    developer_sensors: &[DeveloperSensor],
) -> PyResult<Bound<'py, PyDict>> {
    let batch = rows_to_batch(rows)?;
    let metrics = detect_metrics(rows);

    let activity = PyDict::new_bound(py);
    activity.set_item("records", pyo3_arrow::PyRecordBatch::new(batch).to_pyarrow(py)?)?;

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

    if parsed.sessions.len() <= 1 {
        let session = parsed.sessions.first();
        activities.append(build_activity_dict(
            py,
            &parsed.records,
            session,
            &parsed.devices,
            &parsed.developer_sensors,
        )?)?;
    } else {
        for session in &parsed.sessions {
            let start = session.start_timestamp_us.unwrap_or(i64::MIN);
            let end = session.end_timestamp_us.unwrap_or(i64::MAX);
            let rows: Vec<RecordRow> = parsed
                .records
                .iter()
                .filter(|r| {
                    let ts = r.timestamp.unwrap_or(0);
                    ts >= start && ts <= end
                })
                .cloned()
                .collect();
            activities.append(build_activity_dict(
                py,
                &rows,
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
                                if !is_fixed_column(&normalized) {
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
