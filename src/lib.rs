mod fields;
mod fit;
pub(crate) mod reference;
mod types;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::Arc;

use fields::{is_canonical_column, normalize_field_name};
use types::TypedColumn;

use arrow::array::{
    Float32Array, Float64Array, Int16Array, Int8Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Semicircles → degrees: 180 / 2^31.
pub(crate) const SEMICIRCLE_TO_DEGREES: f64 = 180.0 / 2_147_483_648.0;

// ═══════════════════════════════════════════════════════════════════════════
// Full parser — decodes every message via the fitparser crate
// ═══════════════════════════════════════════════════════════════════════════


// ---------------------------------------------------------------------------
// Per-session merge — count non-null non-zero values to pick the winner
// ---------------------------------------------------------------------------

/// Count non-null non-zero values in a column across a record slice.
fn count_nonzero_i16(records: &[RecordRow], accessor: impl Fn(&RecordRow) -> Option<i16>) -> u32 {
    records
        .iter()
        .filter(|r| accessor(r).unwrap_or(0) != 0)
        .count() as u32
}

/// Canonical columns that can come from either a standard FIT field or a
/// developer field.  Each entry describes one such pair.
struct ColumnAttribution {
    /// Column name in the output schema (e.g. "power").
    name: &'static str,
    /// ANT+ device_type values that identify the standard-field source device.
    ant_device_types: &'static [u8],
    /// Manufacturers known to produce this metric (fallback when ANT+ device
    /// type is absent — common for Bluetooth/ANT-FS fitness equipment).
    known_manufacturers: &'static [&'static str],
    /// Whether this column can conflict with a developer field and needs the
    /// majority-wins merge logic.
    mergeable: bool,
}

const COLUMN_ATTRIBUTIONS: &[ColumnAttribution] = &[
    ColumnAttribution {
        name: "power",
        ant_device_types: &[11], // bike_power
        known_manufacturers: &[
            "concept2",
            "elite",
            "favero",
            "quarq",
            "rotor",
            "shimano",
            "saris",
            "srm",
            "stages_cycling",
            "tacx",
            "wahoo_fitness",
            "wattbike",
        ],
        mergeable: true,
    },
    ColumnAttribution {
        name: "cadence",
        ant_device_types: &[121, 122], // bike_speed_cadence, stride_speed_distance
        known_manufacturers: &[],
        mergeable: true,
    },
    ColumnAttribution {
        name: "heart_rate",
        ant_device_types: &[120], // heart_rate monitor
        known_manufacturers: &[],
        mergeable: false,
    },
    ColumnAttribution {
        name: "speed",
        ant_device_types: &[123, 121], // bike_speed, bike_speed_cadence
        known_manufacturers: &[],
        mergeable: false,
    },
    ColumnAttribution {
        name: "temperature",
        ant_device_types: &[25], // environment_sensor_legacy
        known_manufacturers: &[],
        mergeable: false,
    },
    ColumnAttribution {
        name: "distance",
        ant_device_types: &[],
        known_manufacturers: &[],
        mergeable: false,
    },
    ColumnAttribution {
        name: "altitude",
        ant_device_types: &[],
        known_manufacturers: &[],
        mergeable: false,
    },
];

// ---------------------------------------------------------------------------
// Sensor classification — identify CIQ apps from DeveloperDataId UUIDs
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct DeveloperSensor {
    pub(crate) manufacturer: String,
    pub(crate) product: String,
    pub(crate) columns: Vec<String>,
}

/// Known CIQ app UUIDs → (manufacturer, product).
///
/// This is purely cosmetic — it gives human-readable names to developer
/// sensors.  Unknown UUIDs still work; they just show the UUID as the name.
/// To add a new sensor, add a row.
const KNOWN_CIQ_APPS: &[(&str, &str, &str)] = &[
    // (uuid, manufacturer, product)
    ("6957fe68-83fe-4ed6-8613-413f70624bb5", "core", "CORE"),
    ("9a0508b9-0256-4639-88b3-a2690a14ddf9", "concept2", "Concept2"),
    ("18fb2cf0-1a4b-430d-ad66-988c847421f4", "stryd", "Stryd"),
];

/// Format 16 raw bytes as a lowercase UUID string (RFC 4122).
pub(crate) fn bytes_to_uuid(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 16 {
        return None;
    }
    Some(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-\
         {:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15],
    ))
}

/// Look up a CIQ app UUID → (manufacturer, product).  Returns the UUID
/// itself as both manufacturer and product for unknown apps.
pub(crate) fn name_for_uuid(uuid: &str) -> (&str, &str) {
    for &(u, mfr, product) in KNOWN_CIQ_APPS {
        if u.eq_ignore_ascii_case(uuid) {
            return (mfr, product);
        }
    }
    // Leak the UUID string so we can return &str with 'static-like lifetime.
    // This is fine — there are at most a handful of unknown UUIDs per file.
    let leaked: &str = Box::leak(uuid.to_string().into_boxed_str());
    (leaked, leaked)
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

/// Determine the output column name for a developer field.
/// Returns None if the field would collide with a standard column and gets skipped.
pub(crate) fn column_for_developer_field(name: &str) -> Option<String> {
    if let Some(norm) = developer_field_to_fixed(name) {
        return Some(norm.to_string());
    }
    let normalized = normalize_field_name(name);
    if is_canonical_column(&normalized) {
        return None;
    }
    Some(normalized)
}

/// Classify developer fields into sensors.
///
/// Uses the temporal relationship between DeveloperDataId and FieldDescription
/// messages: each FieldDescription belongs to the most recent DeveloperDataId
/// that registered its `developer_data_index`.  This is tracked during parsing
/// in `dev_field_owners` (field_name → app UUID).
///
/// When `include_columns` is true, each sensor's `columns` list is populated
/// with the output column names that appear in the data.  When false (metadata-
/// only scan), sensors are classified but columns are left empty.
pub(crate) fn classify_developer_sensors(
    dev_field_owners: &BTreeMap<String, String>,
    present_extra_columns: &BTreeSet<String>,
    include_columns: bool,
) -> Vec<DeveloperSensor> {
    // Group fields by their owning app UUID.
    let mut app_columns: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();

    for (field_name, uuid) in dev_field_owners {
        let (_, _) = name_for_uuid(uuid); // ensure entry exists
        let entry = app_columns.entry(uuid.as_str()).or_default();
        if include_columns {
            if let Some(col) = column_for_developer_field(field_name) {
                let exists =
                    present_extra_columns.contains(&col) || is_canonical_column(&col);
                if exists {
                    entry.insert(col);
                }
            }
        }
    }

    app_columns
        .into_iter()
        .map(|(uuid, cols)| {
            let (manufacturer, product) = name_for_uuid(uuid);
            DeveloperSensor {
                manufacturer: manufacturer.into(),
                product: product.into(),
                columns: cols.into_iter().collect(),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub(crate) struct RecordRow {
    pub(crate) timestamp: Option<i64>,
    pub(crate) heart_rate: Option<i16>,
    pub(crate) power: Option<i16>,
    pub(crate) cadence: Option<i16>,
    pub(crate) speed: Option<f32>,
    pub(crate) latitude: Option<f64>,
    pub(crate) longitude: Option<f64>,
    pub(crate) altitude: Option<f32>,
    pub(crate) temperature: Option<i8>,
    pub(crate) distance: Option<f64>,
    // Canonical columns kept in struct for developer field merge,
    // but emitted as extras (not part of the 10 standard columns).
    pub(crate) core_temperature: Option<f32>,
    pub(crate) smo2: Option<f32>,
    // Developer shadow fields — kept separate from standard fields so
    // we can pick the winner per-session after slicing.
    pub(crate) dev_power: Option<i16>,
    pub(crate) dev_cadence: Option<i16>,
}

#[derive(Default)]
pub(crate) struct SessionMeta {
    pub(crate) sport: Option<String>,
    pub(crate) sub_sport: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) start_time: Option<f64>,
    pub(crate) start_time_local: Option<f64>,
    pub(crate) duration: Option<f64>,
    pub(crate) distance: Option<f64>,
    pub(crate) start_timestamp_us: Option<i64>,
    pub(crate) end_timestamp_us: Option<i64>,
}

#[derive(Clone, Default)]
pub(crate) struct DeviceMeta {
    pub(crate) manufacturer: Option<String>,
    pub(crate) product: Option<String>,
    pub(crate) serial_number: Option<String>,
    pub(crate) device_index: Option<u8>,
    /// ANT+ device type from DeviceInfo (e.g. 11 = bike_power, 120 = heart_rate).
    pub(crate) ant_device_type: Option<u8>,
    /// Columns attributed to this device (populated per-session during merge).
    pub(crate) columns: Vec<String>,
}

/// A lap boundary extracted from a FIT Lap message.
pub(crate) struct LapBoundary {
    pub(crate) start_time_us: i64,
    pub(crate) end_time_us: i64,
    pub(crate) trigger: Option<String>,
}

pub(crate) struct ParseResult {
    pub(crate) file_type: Option<String>,
    pub(crate) records: Vec<RecordRow>,
    pub(crate) extra_col_info: Vec<(String, DataType)>,
    pub(crate) extra_data: Vec<TypedColumn>,
    pub(crate) sessions: Vec<SessionMeta>,
    /// All DeviceInfo entries, in order of appearance.  Split into per-session
    /// groups using `split_devices_per_session` before building output.
    pub(crate) devices: Vec<DeviceMeta>,
    pub(crate) developer_sensors: Vec<DeveloperSensor>,
    pub(crate) laps: Vec<LapBoundary>,
}

// ---------------------------------------------------------------------------
// Course data structures
// ---------------------------------------------------------------------------

pub(crate) struct CoursePoint {
    pub(crate) latitude: Option<f64>,
    pub(crate) longitude: Option<f64>,
    pub(crate) distance: Option<f64>,
    pub(crate) name: Option<String>,
    pub(crate) point_type: Option<String>,
}

pub(crate) struct CourseMeta {
    pub(crate) name: Option<String>,
    pub(crate) total_distance: Option<f64>,
    pub(crate) total_ascent: Option<u16>,
    pub(crate) total_descent: Option<u16>,
}

pub(crate) struct CourseResult {
    pub(crate) records: Vec<RecordRow>,
    pub(crate) course_points: Vec<CoursePoint>,
    pub(crate) meta: CourseMeta,
}

fn read_fit_messages(reader: &mut impl Read) -> PyResult<Vec<fitparser::FitDataRecord>> {
    fitparser::from_reader(reader)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// Metadata scan result (used by build_scan_result_dict)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct ScanResult {
    pub(crate) file_type: Option<String>,
    pub(crate) sessions: Vec<SessionMeta>,
    pub(crate) devices: Vec<DeviceMeta>,
    pub(crate) local_timestamp: Option<f64>,
    pub(crate) record_metrics: Vec<String>,
    pub(crate) developer_sensors: Vec<DeveloperSensor>,
}

// ---------------------------------------------------------------------------
// Arrow schema & batch construction
// ---------------------------------------------------------------------------

/// Assign lap index and trigger to each record based on lap boundaries.
///
/// Returns (lap_indices, lap_triggers) — one entry per record row.
/// If `laps` is empty, all records get lap=0 and trigger=None.
fn assign_laps(
    records: &[RecordRow],
    laps: &[LapBoundary],
) -> (Vec<i16>, Vec<Option<String>>) {
    let n = records.len();
    if laps.is_empty() {
        return (vec![0i16; n], vec![None; n]);
    }

    let mut lap_indices = Vec::with_capacity(n);
    let mut lap_triggers: Vec<Option<String>> = Vec::with_capacity(n);

    // Check if any records fall before the first lap boundary.
    let first_lap_start = laps[0].start_time_us;
    let has_pre_lap_records = records.iter().any(|r| {
        r.timestamp.is_some_and(|ts| ts < first_lap_start)
    });
    // When records exist before the first lap, they get a synthetic lap 0
    // and all real laps shift up by 1.
    let offset: i16 = if has_pre_lap_records { 1 } else { 0 };

    for record in records {
        let ts = record.timestamp.unwrap_or(0);

        if ts < first_lap_start {
            // Record before any lap boundary — synthetic lap 0.
            lap_indices.push(0);
            lap_triggers.push(None);
            continue;
        }

        // Binary search: find the last lap whose start_time <= record timestamp.
        let lap_pos = laps.partition_point(|l| l.start_time_us <= ts);
        // partition_point returns the first index where start_time > ts,
        // so the containing lap is at lap_pos - 1.
        let lap_idx = if lap_pos > 0 { lap_pos - 1 } else { 0 };
        lap_indices.push(lap_idx as i16 + offset);
        lap_triggers.push(laps[lap_idx].trigger.clone());
    }

    (lap_indices, lap_triggers)
}

fn build_batch(
    records: &[RecordRow],
    extra_col_info: &[(String, DataType)],
    extra_data: &[TypedColumn],
    laps: &[LapBoundary],
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

    // Compute lap assignment.
    let (lap_indices, lap_triggers) = assign_laps(records, laps);
    let has_lap_triggers = lap_triggers.iter().any(|t| t.is_some());

    // Schema: 11 standard columns, then all extras sorted alphabetically.
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
        Field::new("lap", DataType::Int16, true),
    ];

    // Include lap_trigger as a canonical extra if the file has lap data.
    if has_lap_triggers {
        let trigger_arr: StringArray = lap_triggers.iter().map(|t| t.as_deref()).collect();
        canonical_extras.push((
            "lap_trigger".into(),
            Arc::new(trigger_arr),
        ));
    }

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
        Arc::new(Int16Array::from(lap_indices)),
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
    //   5=latitude 6=longitude 7=altitude 8=temperature 9=distance 10=lap
    if has_data(1) { metrics.push("heart_rate".into()); }
    if has_data(2) { metrics.push("power".into()); }
    if has_data(4) { metrics.push("speed".into()); }
    if has_data(3) { metrics.push("cadence".into()); }
    if has_data(5) && has_data(6) { metrics.push("gps".into()); }
    if has_data(7) { metrics.push("altitude".into()); }
    if has_data(8) { metrics.push("temperature".into()); }
    if has_data(9) { metrics.push("distance".into()); }
    // Skip index 10 (lap) — it's structural, not a metric.

    // Extra columns (indices 11+), includes canonical extras like
    // core_temperature, smo2, and lap_trigger alongside dynamic extras.
    let schema = batch.schema();
    for i in 11..batch.num_columns() {
        if has_data(i) {
            let name = schema.field(i).name();
            // lap_trigger is structural, not a metric.
            if name != "lap_trigger" {
                metrics.push(name.clone());
            }
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
    let cols = PyList::empty_bound(py);
    for c in &device.columns {
        cols.append(c)?;
    }
    dict.set_item("columns", cols)?;
    Ok(dict)
}

fn sensor_to_dict<'py>(
    py: Python<'py>,
    sensor: &DeveloperSensor,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("manufacturer", &sensor.manufacturer)?;
    dict.set_item("product", &sensor.product)?;
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
    for m in &metrics { metrics_list.append(m)?; }
    meta.set_item("metrics", metrics_list)?;

    let devices_list = PyList::empty_bound(py);
    for d in devices { devices_list.append(device_to_dict(py, d)?)?; }
    meta.set_item("devices", devices_list)?;

    let sensors_list = PyList::empty_bound(py);
    for s in developer_sensors { sensors_list.append(sensor_to_dict(py, s)?)?; }
    meta.set_item("developer_sensors", sensors_list)?;

    activity.set_item("metadata", meta)?;
    Ok(activity)
}

// ---------------------------------------------------------------------------
// Per-session merge: resolve standard vs developer for each canonical column
// ---------------------------------------------------------------------------

/// For a slice of records, determine whether the developer field should replace
/// the standard field.  Returns true when the developer field has strictly more
/// non-null non-zero values (majority wins; ties go to the standard field).
fn developer_wins_i16(
    records: &[RecordRow],
    standard: impl Fn(&RecordRow) -> Option<i16>,
    developer: impl Fn(&RecordRow) -> Option<i16>,
) -> bool {
    let std_count = count_nonzero_i16(records, &standard);
    let dev_count = count_nonzero_i16(records, developer);
    dev_count > std_count
}

/// Resolve per-session merges and copy winners into canonical RecordRow fields.
/// Returns a bitmask: bit 0 = developer won power, bit 1 = developer won cadence.
fn resolve_merge(records: &mut [RecordRow]) -> u8 {
    let mut dev_won: u8 = 0;

    // Power: standard power vs developer Power (Stryd)
    let has_dev_power = records.iter().any(|r| r.dev_power.is_some());
    if has_dev_power {
        if developer_wins_i16(records, |r| r.power, |r| r.dev_power) {
            dev_won |= 1;
            for row in records.iter_mut() {
                row.power = row.dev_power;
            }
        }
    }

    // Cadence: standard cadence vs developer Cadence (Stryd)
    let has_dev_cadence = records.iter().any(|r| r.dev_cadence.is_some());
    if has_dev_cadence {
        if developer_wins_i16(records, |r| r.cadence, |r| r.dev_cadence) {
            dev_won |= 2;
            for row in records.iter_mut() {
                row.cadence = row.dev_cadence;
            }
        }
    }

    dev_won
}

/// Find the hardware device that matches an ANT+ device type.
/// Skips the creator device (device_index == 0).
/// When multiple devices match, prefer the one with the highest device_index.
fn find_device_by_ant_type(devices: &[DeviceMeta], ant_types: &[u8]) -> Option<usize> {
    devices
        .iter()
        .enumerate()
        .filter(|(_, d)| {
            d.device_index != Some(0)
                && d.ant_device_type.is_some_and(|t| ant_types.contains(&t))
        })
        .max_by_key(|(_, d)| d.device_index.unwrap_or(0))
        .map(|(i, _)| i)
}

/// Find a hardware device whose manufacturer is known to produce this metric.
/// Skips the creator device (device_index == 0) since that's the watch itself.
/// When multiple devices match, prefer the one with the highest device_index —
/// in multi-session files the watch re-emits all known devices, and the one
/// added most recently (highest index) is most likely the active source.
fn find_device_by_manufacturer(devices: &[DeviceMeta], manufacturers: &[&str]) -> Option<usize> {
    devices
        .iter()
        .enumerate()
        .filter(|(_, d)| {
            d.device_index != Some(0)
                && d.manufacturer
                    .as_deref()
                    .is_some_and(|m| manufacturers.contains(&m))
        })
        .max_by_key(|(_, d)| d.device_index.unwrap_or(0))
        .map(|(i, _)| i)
}

/// Find the creator device (device_index == 0).
fn find_creator_device(devices: &[DeviceMeta]) -> Option<usize> {
    devices.iter().position(|d| d.device_index == Some(0))
}

/// Build per-session device list with column attribution based on merge outcome.
///
/// `metrics` lists standard columns that actually contain data in this session,
/// so we only attribute columns the session really has.
fn attribute_devices(
    base_devices: &[DeviceMeta],
    developer_sensors: &[DeveloperSensor],
    dev_won: u8,
    metrics: &[String],
) -> (Vec<DeviceMeta>, Vec<DeveloperSensor>) {
    let mut devices: Vec<DeviceMeta> = base_devices.to_vec();
    let mut sensors: Vec<DeveloperSensor> = developer_sensors.to_vec();

    // Track which mergeable columns we've seen (for bitmask indexing).
    let mut merge_bit: usize = 0;
    for col in COLUMN_ATTRIBUTIONS {
        // Only attribute columns that actually have data in this session.
        if !metrics.contains(&col.name.to_string()) {
            if col.mergeable {
                merge_bit += 1;
            }
            continue;
        }

        if col.mergeable {
            let dev_won_this = dev_won & (1 << merge_bit) != 0;
            merge_bit += 1;
            if dev_won_this {
                // Developer sensor won — it already has this column from
                // classify_developer_sensors, nothing to do.
                continue;
            }
        }

        // Standard field won (or non-mergeable) — remove column from
        // developer sensors and attribute to hardware device.
        for sensor in &mut sensors {
            sensor.columns.retain(|c| c != col.name);
        }
        // Find the best device: consider both ANT+ type and known manufacturer,
        // preferring the one with the highest device_index (most recently
        // registered, most likely the active source in multi-sport sessions).
        let ant_match = find_device_by_ant_type(&devices, col.ant_device_types);
        let mfr_match = find_device_by_manufacturer(&devices, col.known_manufacturers);
        let device_idx = match (ant_match, mfr_match) {
            (Some(a), Some(m)) if a != m => {
                let a_idx = devices[a].device_index.unwrap_or(0);
                let m_idx = devices[m].device_index.unwrap_or(0);
                Some(if m_idx > a_idx { m } else { a })
            }
            (a, m) => a.or(m),
        }
        .or_else(|| find_creator_device(&devices));
        if let Some(idx) = device_idx {
            if !devices[idx].columns.contains(&col.name.to_string()) {
                devices[idx].columns.push(col.name.to_string());
            }
        }
    }

    // Remove sensors with no remaining columns.
    sensors.retain(|s| !s.columns.is_empty());

    (devices, sensors)
}

/// Deduplicate devices by device_index, merging information from re-emitted
/// DeviceInfo messages.  FIT files often emit DeviceInfo multiple times for the
/// same device (e.g. at start and end of activity).  Later emissions may carry
/// fields (like ANT+ device type) that earlier ones lack.
fn dedup_devices(devices: &[DeviceMeta]) -> Vec<DeviceMeta> {
    let mut by_index: Vec<(u8, DeviceMeta)> = Vec::new();
    for d in devices {
        let idx = match d.device_index {
            Some(i) => i,
            None => {
                by_index.push((u8::MAX, d.clone()));
                continue;
            }
        };
        if let Some(existing) = by_index.iter_mut().find(|(i, _)| *i == idx) {
            // Merge: prefer non-None values from later emission.
            if existing.1.manufacturer.is_none() {
                existing.1.manufacturer = d.manufacturer.clone();
            }
            if existing.1.product.is_none() {
                existing.1.product = d.product.clone();
            }
            if existing.1.serial_number.is_none() {
                existing.1.serial_number = d.serial_number.clone();
            }
            if existing.1.ant_device_type.is_none() {
                existing.1.ant_device_type = d.ant_device_type;
            }
        } else {
            by_index.push((idx, d.clone()));
        }
    }
    by_index.into_iter().map(|(_, d)| d).collect()
}

/// Split a flat list of DeviceInfo entries into per-session groups.
///
/// FIT files re-emit DeviceInfo messages for each session.  Each batch starts
/// with `device_index == 0` (the creator).  We split on those boundaries and
/// deduplicate within each group.  If there are more groups than sessions, we
/// merge the extras into the last group (Garmin sometimes emits a cleanup batch
/// at the end).  If there are fewer groups, we reuse the single group for all.
fn split_devices_per_session(
    all_devices: &[DeviceMeta],
    n_sessions: usize,
) -> Vec<Vec<DeviceMeta>> {
    if n_sessions == 0 {
        return vec![all_devices.to_vec()];
    }

    // Split into groups at each device_index==0 boundary.
    let mut groups: Vec<Vec<DeviceMeta>> = Vec::new();
    for device in all_devices {
        if device.device_index == Some(0) && !groups.is_empty() {
            groups.push(Vec::new());
        }
        if groups.is_empty() {
            groups.push(Vec::new());
        }
        groups.last_mut().unwrap().push(device.clone());
    }

    if groups.len() <= n_sessions {
        // Fewer groups than sessions (or equal) — use what we have, padding
        // with the full list for any unmatched sessions.
        let mut result = groups;
        while result.len() < n_sessions {
            result.push(all_devices.to_vec());
        }
        result
    } else {
        // More groups than sessions — take the first n_sessions groups and
        // merge any remaining devices into the last group.
        let mut result: Vec<Vec<DeviceMeta>> = groups[..n_sessions].to_vec();
        for extra_group in &groups[n_sessions..] {
            result.last_mut().unwrap().extend(extra_group.iter().cloned());
        }
        result
    }
}

// ---------------------------------------------------------------------------

fn build_parse_result_dict(py: Python<'_>, mut parsed: ParseResult) -> PyResult<PyObject> {
    let result = PyDict::new_bound(py);
    result.set_item("file_type", parsed.file_type.as_deref())?;
    let activities = PyList::empty_bound(py);

    if parsed.sessions.len() <= 1 {
        // Single session (or no sessions): merge across all records.
        let dev_won = resolve_merge(&mut parsed.records);
        let batch = build_batch(
            &parsed.records, &parsed.extra_col_info, &parsed.extra_data, &parsed.laps,
        )?;
        let metrics = detect_metrics(&batch);
        let deduped = dedup_devices(&parsed.devices);
        let (devices, sensors) =
            attribute_devices(&deduped, &parsed.developer_sensors, dev_won, &metrics);

        activities.append(build_activity_dict(
            py,
            &batch,
            parsed.sessions.first(),
            &devices,
            &sensors,
        )?)?;
    } else {
        // Multi-session: slice records per session, merge each independently.
        // Build full batch with all laps for the extras columns.
        let batch = build_batch(
            &parsed.records, &parsed.extra_col_info, &parsed.extra_data, &parsed.laps,
        )?;
        let device_groups =
            split_devices_per_session(&parsed.devices, parsed.sessions.len());

        for (si, session) in parsed.sessions.iter().enumerate() {
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
            let len = last.saturating_sub(first);

            // Filter laps to this session's time range and re-index from 0.
            let session_laps: Vec<LapBoundary> = parsed.laps
                .iter()
                .filter(|l| l.start_time_us >= start && l.start_time_us <= end)
                .map(|l| LapBoundary {
                    start_time_us: l.start_time_us,
                    end_time_us: l.end_time_us,
                    trigger: l.trigger.clone(),
                })
                .collect();

            // Resolve merge on a mutable slice of this session's records.
            let session_records = &mut parsed.records[first..first + len];
            let dev_won = resolve_merge(session_records);

            // Rebuild fixed columns (+ canonical extras) from the now-resolved
            // records.  Dynamic extras come from the pre-built batch via slice.
            let session_batch = build_batch(session_records, &[], &[], &session_laps)?;
            let full_batch = merge_fixed_with_extras(&session_batch, &batch.slice(first, len))?;

            let metrics = detect_metrics(&full_batch);
            let deduped = dedup_devices(&device_groups[si]);
            let (devices, sensors) =
                attribute_devices(&deduped, &parsed.developer_sensors, dev_won, &metrics);

            activities.append(build_activity_dict(
                py,
                &full_batch,
                Some(session),
                &devices,
                &sensors,
            )?)?;
        }
    }

    result.set_item("activities", activities)?;
    Ok(result.into_any().unbind())
}

/// Combine fixed columns (0..11) from `fixed_source` with extra columns (11+)
/// from `extras_source` into a single RecordBatch.
fn merge_fixed_with_extras(
    fixed_source: &RecordBatch,
    extras_source: &RecordBatch,
) -> PyResult<RecordBatch> {
    let n_fixed = 11;
    let mut fields: Vec<Field> = Vec::new();
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

    // Fixed columns from the resolved session batch.
    for i in 0..n_fixed.min(fixed_source.num_columns()) {
        fields.push(fixed_source.schema().field(i).clone());
        arrays.push(fixed_source.column(i).clone());
    }
    // Extra columns from the original sliced batch.
    for i in n_fixed..extras_source.num_columns() {
        fields.push(extras_source.schema().field(i).clone());
        arrays.push(extras_source.column(i).clone());
    }

    // Also include canonical extras (core_temperature, smo2) from fixed_source
    // if they exist there (indices 10+).
    let fixed_schema = fixed_source.schema();
    for i in n_fixed..fixed_source.num_columns() {
        let name = fixed_schema.field(i).name();
        // Only add if not already present from extras_source.
        if !fields.iter().any(|f| f.name() == name) {
            fields.push(fixed_schema.field(i).clone());
            arrays.push(fixed_source.column(i).clone());
        }
    }

    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), arrays)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}




fn build_scan_result_dict(py: Python<'_>, scan: &ScanResult) -> PyResult<PyObject> {
    let result = PyDict::new_bound(py);
    result.set_item("file_type", scan.file_type.as_deref())?;
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

fn do_parse(py: Python<'_>, data: &[u8], columns: Option<Vec<String>>) -> PyResult<PyObject> {
    let config = fit::decode::ParseConfig { columns };
    let parsed = fit::decode::full_parse(data, &config)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    build_parse_result_dict(py, parsed)
}

#[pyfunction]
#[pyo3(signature = (path, columns=None))]
fn parse_fit(py: Python<'_>, path: &str, columns: Option<Vec<String>>) -> PyResult<PyObject> {
    let data = std::fs::read(path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    do_parse(py, &data, columns)
}

#[pyfunction]
#[pyo3(signature = (data, columns=None))]
fn parse_fit_bytes(py: Python<'_>, data: &[u8], columns: Option<Vec<String>>) -> PyResult<PyObject> {
    do_parse(py, data, columns)
}

/// Metadata-only scan from file path — skips Record data.
#[pyfunction]
fn parse_fit_metadata(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let data = std::fs::read(path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let result = fit::decode::scan_metadata(&data)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    build_scan_result_dict(py, &result)
}

/// Dump all FIT messages as Python dicts — faithfully mirrors fitparser's
/// FitDataRecord / FitDataField structures with no pyroparse opinions applied.
#[pyfunction]
fn dump_fit_messages(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let data = std::fs::read(path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let messages = read_fit_messages(&mut std::io::Cursor::new(data))?;
    pythonize::pythonize(py, &messages)
        .map(|bound| bound.unbind())
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn dump_fit_messages_bytes(py: Python<'_>, data: &[u8]) -> PyResult<PyObject> {
    let messages = read_fit_messages(&mut std::io::Cursor::new(data))?;
    pythonize::pythonize(py, &messages)
        .map(|bound| bound.unbind())
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// Course: Arrow batch construction + Python dict
// ---------------------------------------------------------------------------

fn build_course_track_batch(records: &[RecordRow]) -> PyResult<RecordBatch> {
    let fields = vec![
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("altitude", DataType::Float32, true),
        Field::new("distance", DataType::Float64, true),
    ];
    let schema = Schema::new(fields);
    let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Float64Array::from_iter(records.iter().map(|r| r.latitude))),
        Arc::new(Float64Array::from_iter(records.iter().map(|r| r.longitude))),
        Arc::new(Float32Array::from_iter(records.iter().map(|r| r.altitude))),
        Arc::new(Float64Array::from_iter(records.iter().map(|r| r.distance))),
    ];
    RecordBatch::try_new(Arc::new(schema), arrays)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn build_course_result_dict(py: Python<'_>, parsed: CourseResult) -> PyResult<PyObject> {
    let result = PyDict::new_bound(py);

    let track = build_course_track_batch(&parsed.records)?;
    result.set_item(
        "track",
        pyo3_arrow::PyRecordBatch::new(track).to_pyarrow(py)?,
    )?;

    // Waypoints as a list of dicts (small data — not worth an Arrow batch).
    let waypoints = PyList::empty_bound(py);
    for pt in &parsed.course_points {
        let d = PyDict::new_bound(py);
        d.set_item("name", pt.name.as_deref())?;
        d.set_item("type", pt.point_type.as_deref())?;
        d.set_item("latitude", pt.latitude)?;
        d.set_item("longitude", pt.longitude)?;
        d.set_item("distance", pt.distance)?;
        waypoints.append(d)?;
    }

    let meta = PyDict::new_bound(py);
    meta.set_item("name", parsed.meta.name.as_deref())?;
    meta.set_item("total_distance", parsed.meta.total_distance)?;
    meta.set_item("total_ascent", parsed.meta.total_ascent)?;
    meta.set_item("total_descent", parsed.meta.total_descent)?;
    meta.set_item("waypoints", waypoints)?;
    result.set_item("metadata", meta)?;

    Ok(result.into_any().unbind())
}

fn do_parse_course(py: Python<'_>, data: &[u8]) -> PyResult<PyObject> {
    let parsed = fit::decode::parse_course(data)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    build_course_result_dict(py, parsed)
}

#[pyfunction]
fn parse_course(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let data = std::fs::read(path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    do_parse_course(py, &data)
}

#[pyfunction]
fn parse_course_bytes(py: Python<'_>, data: &[u8]) -> PyResult<PyObject> {
    do_parse_course(py, data)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_fit, m)?)?;
    m.add_function(wrap_pyfunction!(parse_fit_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(parse_fit_metadata, m)?)?;
    m.add_function(wrap_pyfunction!(parse_course, m)?)?;
    m.add_function(wrap_pyfunction!(parse_course_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(dump_fit_messages, m)?)?;
    m.add_function(wrap_pyfunction!(dump_fit_messages_bytes, m)?)?;
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── Lap assignment ───────────────────────────────────────────────────

    mod lap_assignment {
        use super::*;

        fn make_record(ts: i64) -> RecordRow {
            RecordRow {
                timestamp: Some(ts),
                ..Default::default()
            }
        }

        fn make_lap(start: i64, end: i64, trigger: Option<&str>) -> LapBoundary {
            LapBoundary {
                start_time_us: start,
                end_time_us: end,
                trigger: trigger.map(String::from),
            }
        }

        #[test]
        fn no_laps_all_records_get_lap_zero() {
            let records = vec![make_record(100), make_record(200), make_record(300)];
            let (indices, triggers) = assign_laps(&records, &[]);
            assert_eq!(indices, vec![0, 0, 0]);
            assert!(triggers.iter().all(|t| t.is_none()));
        }

        #[test]
        fn single_lap_all_records_inside() {
            let records = vec![make_record(100), make_record(200)];
            let laps = vec![make_lap(100, 300, Some("manual"))];
            let (indices, triggers) = assign_laps(&records, &laps);
            assert_eq!(indices, vec![0, 0]);
            assert_eq!(triggers, vec![Some("manual".into()), Some("manual".into())]);
        }

        #[test]
        fn two_laps_records_split() {
            let records = vec![
                make_record(100),
                make_record(200),
                make_record(300),
                make_record(400),
            ];
            let laps = vec![
                make_lap(100, 250, Some("manual")),
                make_lap(250, 500, Some("session_end")),
            ];
            let (indices, triggers) = assign_laps(&records, &laps);
            assert_eq!(indices, vec![0, 0, 1, 1]);
            assert_eq!(
                triggers,
                vec![
                    Some("manual".into()),
                    Some("manual".into()),
                    Some("session_end".into()),
                    Some("session_end".into()),
                ]
            );
        }

        #[test]
        fn pre_lap_records_get_synthetic_lap_zero() {
            let records = vec![
                make_record(50),  // before first lap
                make_record(100),
                make_record(200),
            ];
            let laps = vec![make_lap(100, 300, Some("manual"))];
            let (indices, triggers) = assign_laps(&records, &laps);
            // Pre-lap record → synthetic lap 0, real lap shifts to 1
            assert_eq!(indices, vec![0, 1, 1]);
            assert_eq!(triggers[0], None); // synthetic lap has no trigger
            assert_eq!(triggers[1], Some("manual".into()));
        }

        #[test]
        fn empty_records() {
            let (indices, triggers) = assign_laps(&[], &[]);
            assert!(indices.is_empty());
            assert!(triggers.is_empty());
        }
    }

    // ── Device deduplication ─────────────────────────────────────────────

    mod device_dedup {
        use super::*;

        fn make_device(idx: Option<u8>, mfr: Option<&str>, product: Option<&str>) -> DeviceMeta {
            DeviceMeta {
                device_index: idx,
                manufacturer: mfr.map(String::from),
                product: product.map(String::from),
                ..Default::default()
            }
        }

        #[test]
        fn no_duplicates_unchanged() {
            let devices = vec![
                make_device(Some(0), Some("garmin"), Some("fenix6")),
                make_device(Some(1), Some("wahoo"), Some("kickr")),
            ];
            let result = dedup_devices(&devices);
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].manufacturer.as_deref(), Some("garmin"));
            assert_eq!(result[1].manufacturer.as_deref(), Some("wahoo"));
        }

        #[test]
        fn duplicate_index_merges_fills_none() {
            let devices = vec![
                make_device(Some(1), Some("wahoo"), None), // first emission: no product
                make_device(Some(1), None, Some("kickr")), // second emission: has product
            ];
            let result = dedup_devices(&devices);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].manufacturer.as_deref(), Some("wahoo"));
            assert_eq!(result[0].product.as_deref(), Some("kickr"));
        }

        #[test]
        fn first_value_preserved_on_conflict() {
            let devices = vec![
                make_device(Some(0), Some("garmin"), Some("fenix6")),
                make_device(Some(0), Some("different"), Some("other")),
            ];
            let result = dedup_devices(&devices);
            assert_eq!(result.len(), 1);
            // First non-None value wins
            assert_eq!(result[0].manufacturer.as_deref(), Some("garmin"));
            assert_eq!(result[0].product.as_deref(), Some("fenix6"));
        }

        #[test]
        fn none_index_devices_not_merged() {
            let devices = vec![
                make_device(None, Some("unknown1"), None),
                make_device(None, Some("unknown2"), None),
            ];
            let result = dedup_devices(&devices);
            assert_eq!(result.len(), 2);
        }

        #[test]
        fn ant_device_type_merged() {
            let mut d1 = make_device(Some(1), Some("wahoo"), None);
            d1.ant_device_type = None;
            let mut d2 = make_device(Some(1), None, None);
            d2.ant_device_type = Some(11); // bike_power
            let result = dedup_devices(&[d1, d2]);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].ant_device_type, Some(11));
        }
    }


}
