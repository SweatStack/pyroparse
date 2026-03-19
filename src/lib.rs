use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fitparser::profile::MesgNum;
use fitparser::Value;
use pyo3::prelude::*;

/// Semicircles to degrees: 180 / 2^31.
const SEMICIRCLE_TO_DEGREES: f64 = 180.0 / 2_147_483_648.0;

/// The columns extracted from FIT record messages.
const COLUMNS: &[&str] = &[
    "timestamp",
    "heart_rate",
    "power",
    "speed",
    "cadence",
    "position_lat",
    "position_long",
];

// ---------------------------------------------------------------------------
// Value extraction
// ---------------------------------------------------------------------------

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

fn value_to_timestamp(val: &Value) -> Option<f64> {
    match val {
        Value::Timestamp(dt) => {
            Some(dt.timestamp() as f64 + dt.timestamp_subsec_nanos() as f64 / 1e9)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Row-level parsing
// ---------------------------------------------------------------------------

struct RecordRow {
    timestamp: Option<f64>,
    heart_rate: Option<f64>,
    power: Option<f64>,
    speed: Option<f64>,
    cadence: Option<f64>,
    position_lat: Option<f64>,
    position_long: Option<f64>,
}

fn parse_records(path: &str) -> PyResult<Vec<RecordRow>> {
    let file =
        File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

    let messages = fitparser::from_reader(&mut BufReader::new(file))
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let rows = messages
        .iter()
        .filter(|msg| msg.kind() == MesgNum::Record)
        .map(|msg| {
            let mut row = RecordRow {
                timestamp: None,
                heart_rate: None,
                power: None,
                speed: None,
                cadence: None,
                position_lat: None,
                position_long: None,
            };
            for field in msg.fields() {
                match field.name() {
                    "timestamp" => row.timestamp = value_to_timestamp(field.value()),
                    "heart_rate" => row.heart_rate = value_to_f64(field.value()),
                    "power" => row.power = value_to_f64(field.value()),
                    "speed" | "enhanced_speed" => row.speed = value_to_f64(field.value()),
                    "cadence" => row.cadence = value_to_f64(field.value()),
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
            row
        })
        .collect();

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Row → columnar Arrow conversion
// ---------------------------------------------------------------------------

fn rows_to_record_batch(rows: Vec<RecordRow>) -> PyResult<RecordBatch> {
    let schema = Schema::new(
        COLUMNS
            .iter()
            .map(|name| Field::new(*name, DataType::Float64, true))
            .collect::<Vec<_>>(),
    );

    let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.timestamp))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.heart_rate))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.power))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.speed))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.cadence))),
        Arc::new(Float64Array::from_iter(
            rows.iter().map(|r| r.position_lat),
        )),
        Arc::new(Float64Array::from_iter(
            rows.iter().map(|r| r.position_long),
        )),
    ];

    RecordBatch::try_new(Arc::new(schema), arrays)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// Python-exposed function
// ---------------------------------------------------------------------------

/// Parse a FIT file and return a PyArrow RecordBatch.
#[pyfunction]
fn parse_fit(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let rows = parse_records(path)?;
    let batch = rows_to_record_batch(rows)?;
    pyo3_arrow::PyRecordBatch::new(batch).to_pyarrow(py)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_fit, m)?)?;
    Ok(())
}
