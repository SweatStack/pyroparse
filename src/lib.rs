use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow::array::{Float32Array, Float64Array, Int16Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use fitparser::profile::MesgNum;
use fitparser::Value;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Semicircles → degrees: 180 / 2^31.
const SEMICIRCLE_TO_DEGREES: f64 = 180.0 / 2_147_483_648.0;

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

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

struct RecordRow {
    timestamp: Option<i64>,
    heart_rate: Option<i16>,
    power: Option<i16>,
    cadence: Option<i16>,
    speed: Option<f32>,
    position_lat: Option<f64>,
    position_long: Option<f64>,
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
    device_type: Option<String>,
}

struct ParseResult {
    records: Vec<RecordRow>,
    sessions: Vec<SessionMeta>,
    devices: Vec<DeviceMeta>,
}

// ---------------------------------------------------------------------------
// FIT message parsing
// ---------------------------------------------------------------------------

fn parse_all(path: &str) -> PyResult<ParseResult> {
    let file =
        File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let messages = fitparser::from_reader(&mut BufReader::new(file))
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let mut records = Vec::new();
    let mut sessions = Vec::new();
    let mut devices = Vec::new();

    for msg in &messages {
        match msg.kind() {
            MesgNum::Record => {
                let mut row = RecordRow {
                    timestamp: None,
                    heart_rate: None,
                    power: None,
                    cadence: None,
                    speed: None,
                    position_lat: None,
                    position_long: None,
                };
                for field in msg.fields() {
                    match field.name() {
                        "timestamp" => row.timestamp = value_to_timestamp_us(field.value()),
                        "heart_rate" => row.heart_rate = value_to_i16(field.value()),
                        "power" => row.power = value_to_i16(field.value()),
                        "speed" | "enhanced_speed" => {
                            row.speed = value_to_f32(field.value())
                        }
                        "cadence" => row.cadence = value_to_i16(field.value()),
                        "position_lat" => {
                            row.position_lat =
                                value_to_f64(field.value()).map(|v| v * SEMICIRCLE_TO_DEGREES)
                        }
                        "position_long" => {
                            row.position_long =
                                value_to_f64(field.value()).map(|v| v * SEMICIRCLE_TO_DEGREES)
                        }
                        _ => {}
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
                            session.end_timestamp_us =
                                value_to_timestamp_us(field.value());
                        }
                        "start_time" => {
                            session.start_time =
                                value_to_timestamp_secs(field.value());
                            session.start_timestamp_us =
                                value_to_timestamp_us(field.value());
                        }
                        "local_timestamp" => {
                            session.start_time_local =
                                value_to_timestamp_secs(field.value());
                        }
                        "total_timer_time" => {
                            session.duration = value_to_f64(field.value());
                        }
                        "total_distance" => {
                            session.distance = value_to_f64(field.value());
                        }
                        _ => {}
                    }
                }
                sessions.push(session);
            }
            MesgNum::DeviceInfo => {
                let mut device = DeviceMeta::default();
                for field in msg.fields() {
                    match field.name() {
                        "manufacturer" => {
                            device.manufacturer = value_to_string(field.value())
                        }
                        "product_name" => {
                            device.product = value_to_string(field.value())
                        }
                        "serial_number" => {
                            device.serial_number =
                                value_to_f64(field.value()).map(|v| format!("{v:.0}"))
                        }
                        "device_type" | "source_type" => {
                            device.device_type = value_to_string(field.value())
                        }
                        _ => {}
                    }
                }
                if device.manufacturer.is_some() || device.product.is_some() {
                    devices.push(device);
                }
            }
            _ => {}
        }
    }

    Ok(ParseResult {
        records,
        sessions,
        devices,
    })
}

// ---------------------------------------------------------------------------
// Arrow schema & batch construction
// ---------------------------------------------------------------------------

fn build_schema() -> Schema {
    Schema::new(vec![
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
    ])
}

fn rows_to_batch(rows: &[RecordRow]) -> PyResult<RecordBatch> {
    let schema = build_schema();
    let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
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
        Arc::new(Float64Array::from_iter(
            rows.iter().map(|r| r.position_long),
        )),
    ];

    RecordBatch::try_new(Arc::new(schema), arrays)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// Metrics detection — which data columns have non-null values
// ---------------------------------------------------------------------------

fn detect_metrics(rows: &[RecordRow]) -> Vec<String> {
    let mut metrics = Vec::new();
    if rows.iter().any(|r| r.heart_rate.is_some()) {
        metrics.push("heart_rate".into());
    }
    if rows.iter().any(|r| r.power.is_some()) {
        metrics.push("power".into());
    }
    if rows.iter().any(|r| r.speed.is_some()) {
        metrics.push("speed".into());
    }
    if rows.iter().any(|r| r.cadence.is_some()) {
        metrics.push("cadence".into());
    }
    if rows.iter().any(|r| r.position_lat.is_some()) {
        metrics.push("gps".into());
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
    dict.set_item("device_type", device.device_type.as_deref())?;
    Ok(dict)
}

fn build_activity_dict<'py>(
    py: Python<'py>,
    rows: &[RecordRow],
    session: Option<&SessionMeta>,
    devices: &[DeviceMeta],
) -> PyResult<Bound<'py, PyDict>> {
    let batch = rows_to_batch(rows)?;
    let metrics = detect_metrics(rows);

    let activity = PyDict::new_bound(py);
    activity.set_item("records", pyo3_arrow::PyRecordBatch::new(batch).to_pyarrow(py)?)?;

    let meta = match session {
        Some(s) => session_to_dict(py, s)?,
        None => {
            let d = PyDict::new_bound(py);
            for key in ["sport", "sub_sport", "name", "start_time", "start_time_local",
                        "duration", "distance"] {
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

    activity.set_item("metadata", meta)?;
    Ok(activity)
}

// ---------------------------------------------------------------------------
// Python-exposed function
// ---------------------------------------------------------------------------

/// Parse a FIT file into a dict of activities, each with records + metadata.
#[pyfunction]
fn parse_fit(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let parsed = parse_all(path)?;

    let result = PyDict::new_bound(py);
    let activities = PyList::empty_bound(py);

    if parsed.sessions.len() <= 1 {
        // Single activity (or no session messages at all).
        let session = parsed.sessions.first();
        let dict =
            build_activity_dict(py, &parsed.records, session, &parsed.devices)?;
        activities.append(dict)?;
    } else {
        // Multi-activity: split records by session time boundaries.
        for session in &parsed.sessions {
            let start = session.start_timestamp_us.unwrap_or(i64::MIN);
            let end = session.end_timestamp_us.unwrap_or(i64::MAX);
            let session_rows: Vec<RecordRow> = parsed
                .records
                .iter()
                .filter(|r| {
                    let ts = r.timestamp.unwrap_or(0);
                    ts >= start && ts <= end
                })
                .map(|r| RecordRow {
                    timestamp: r.timestamp,
                    heart_rate: r.heart_rate,
                    power: r.power,
                    cadence: r.cadence,
                    speed: r.speed,
                    position_lat: r.position_lat,
                    position_long: r.position_long,
                })
                .collect();
            let dict =
                build_activity_dict(py, &session_rows, Some(session), &parsed.devices)?;
            activities.append(dict)?;
        }
    }

    result.set_item("activities", activities)?;
    Ok(result.into_any().unbind())
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_fit, m)?)?;
    Ok(())
}
