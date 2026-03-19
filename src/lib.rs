use std::fs::File;
use std::io::{BufReader, Read};
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
// Message processing (shared by path and bytes entry points)
// ---------------------------------------------------------------------------

fn process_messages(messages: &[fitparser::FitDataRecord]) -> ParseResult {
    let mut records = Vec::new();
    let mut sessions = Vec::new();
    let mut devices = Vec::new();

    for msg in messages {
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

    ParseResult { records, sessions, devices }
}

fn read_fit_messages(reader: &mut impl Read) -> PyResult<Vec<fitparser::FitDataRecord>> {
    fitparser::from_reader(reader)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn parse_all(path: &str) -> PyResult<ParseResult> {
    let file =
        File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let messages = read_fit_messages(&mut BufReader::new(file))?;
    Ok(process_messages(&messages))
}

fn parse_all_bytes(data: &[u8]) -> PyResult<ParseResult> {
    let messages = read_fit_messages(&mut std::io::Cursor::new(data))?;
    Ok(process_messages(&messages))
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
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.position_long))),
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
            for key in [
                "sport",
                "sub_sport",
                "name",
                "start_time",
                "start_time_local",
                "duration",
                "distance",
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
            activities.append(build_activity_dict(
                py,
                &rows,
                Some(session),
                &parsed.devices,
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

// FIT global message numbers
const MESG_SESSION: u16 = 18;
const MESG_RECORD: u16 = 20;
const MESG_DEVICE_INFO: u16 = 23;
const MESG_ACTIVITY: u16 = 34;

// Session field definition numbers (from FIT SDK profile)
const SESSION_START_TIME: u8 = 2;
const SESSION_SPORT: u8 = 5;
const SESSION_SUB_SPORT: u8 = 6;
const SESSION_TOTAL_TIMER_TIME: u8 = 7;
const SESSION_TOTAL_DISTANCE: u8 = 9;
const SESSION_TIMESTAMP: u8 = 253;

// Activity field definition numbers
const ACTIVITY_LOCAL_TIMESTAMP: u8 = 5;

// DeviceInfo field definition numbers
const DEVICE_MANUFACTURER: u8 = 2;
const DEVICE_SERIAL_NUMBER: u8 = 3;
const DEVICE_PRODUCT_NAME: u8 = 27;

// Record field definition numbers (for metrics detection from definition only)
const RECORD_POSITION_LAT: u8 = 0;
const RECORD_POSITION_LONG: u8 = 1;
const RECORD_HEART_RATE: u8 = 3;
const RECORD_CADENCE: u8 = 4;
const RECORD_SPEED: u8 = 6;
const RECORD_POWER: u8 = 7;
const RECORD_ENHANCED_SPEED: u8 = 73;

fn sport_name(v: u8) -> &'static str {
    match v {
        0 => "generic",
        1 => "running",
        2 => "cycling",
        3 => "transition",
        4 => "fitness_equipment",
        5 => "swimming",
        6 => "basketball",
        7 => "soccer",
        8 => "tennis",
        10 => "training",
        11 => "walking",
        12 => "cross_country_skiing",
        13 => "alpine_skiing",
        14 => "snowboarding",
        15 => "rowing",
        16 => "mountaineering",
        17 => "hiking",
        18 => "multisport",
        19 => "paddling",
        21 => "e_biking",
        23 => "boating",
        25 => "golf",
        37 => "stand_up_paddleboarding",
        38 => "surfing",
        53 => "diving",
        _ => "unknown",
    }
}

fn manufacturer_name(v: u16) -> &'static str {
    match v {
        1 | 15 | 44 => "garmin",
        32 => "wahoo_fitness",
        38 => "favero",
        69 => "stages_cycling",
        76 => "mio",
        86 => "shimano",
        89 => "concept2",
        260 => "zwift",
        263 => "hammerhead",
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
        if buf.len() < 12 {
            return Err("File too short for FIT header".into());
        }
        if &buf[8..12] != b".FIT" {
            return Err("Missing .FIT signature".into());
        }
        let header_size = buf[0] as usize;
        let data_size = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        let end = header_size + data_size;
        if buf.len() < end {
            return Err("File truncated".into());
        }
        Ok(Self {
            buf,
            pos: header_size,
            end,
            defs: Default::default(),
        })
    }

    fn scan(&mut self) -> Result<ScanResult, String> {
        let mut result = ScanResult::default();
        let mut metric_set = std::collections::HashSet::new();

        while self.pos < self.end {
            let header = self.read_byte()?;

            if header & 0x80 != 0 {
                // Compressed timestamp data message.
                let local_type = ((header >> 5) & 0x03) as usize;
                self.skip_data(local_type)?;
            } else if header & 0x40 != 0 {
                // Definition message.
                let has_dev = header & 0x20 != 0;
                let local_type = (header & 0x0F) as usize;
                self.read_definition(local_type, has_dev)?;

                // Accumulate available metrics across all Record definitions.
                if let Some(def) = &self.defs[local_type] {
                    if def.global_num == MESG_RECORD {
                        for m in Self::detect_metrics_from_def(def) {
                            metric_set.insert(m);
                        }
                    }
                }
            } else {
                // Normal data message.
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
                        _ => {}
                    }
                }
            }
        }

        result.record_metrics = metric_set.into_iter().collect();

        // Propagate local_timestamp from Activity message to each session.
        if let Some(lt) = result.local_timestamp {
            for s in &mut result.sessions {
                if s.start_time_local.is_none() {
                    s.start_time_local = Some(lt);
                }
            }
        }

        Ok(result)
    }

    // -- Low-level readers ----------------------------------------------------

    fn read_byte(&mut self) -> Result<u8, String> {
        if self.pos >= self.buf.len() {
            return Err("Unexpected end of FIT data".into());
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.pos + n > self.buf.len() {
            return Err("Unexpected end of FIT data".into());
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn advance(&mut self, n: usize) -> Result<(), String> {
        if self.pos + n > self.buf.len() {
            return Err("Unexpected end of FIT data".into());
        }
        self.pos += n;
        Ok(())
    }

    // -- Definition parsing ---------------------------------------------------

    fn read_definition(&mut self, local_type: usize, has_dev: bool) -> Result<(), String> {
        let _reserved = self.read_byte()?;
        let arch = self.read_byte()?;
        let big_endian = arch == 1;

        let gm = self.read_bytes(2)?;
        let global_num = if big_endian {
            u16::from_be_bytes([gm[0], gm[1]])
        } else {
            u16::from_le_bytes([gm[0], gm[1]])
        };

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

        self.defs[local_type] = Some(MesgDef {
            global_num,
            big_endian,
            fields,
            total_size,
        });
        Ok(())
    }

    // -- Data message reading -------------------------------------------------

    fn skip_data(&mut self, local_type: usize) -> Result<(), String> {
        let size = self.defs[local_type]
            .as_ref()
            .ok_or("Data message without preceding definition")?
            .total_size;
        self.advance(size)
    }

    fn read_fields(&mut self, local_type: usize) -> Result<Vec<(u8, Vec<u8>)>, String> {
        let def = self.defs[local_type]
            .as_ref()
            .ok_or("Data message without preceding definition")?;
        // Copy field layout so we can release the borrow on self.
        let field_layout: Vec<(u8, u8)> = def.fields.iter().map(|f| (f.num, f.size)).collect();
        let total_size = def.total_size;

        let mut out = Vec::with_capacity(field_layout.len());
        let mut regular = 0usize;
        for (num, size) in &field_layout {
            let data = self.read_bytes(*size as usize)?.to_vec();
            regular += *size as usize;
            out.push((*num, data));
        }
        // Skip developer field bytes.
        let remaining = total_size.saturating_sub(regular);
        if remaining > 0 {
            self.advance(remaining)?;
        }
        Ok(out)
    }

    // -- Metrics detection from Record definition (no data needed) ------------

    fn detect_metrics_from_def(def: &MesgDef) -> Vec<String> {
        let mut metrics = Vec::new();
        let mut has_lat = false;
        let mut has_long = false;

        for f in &def.fields {
            match f.num {
                RECORD_HEART_RATE => metrics.push("heart_rate".into()),
                RECORD_POWER => metrics.push("power".into()),
                RECORD_SPEED | RECORD_ENHANCED_SPEED => {
                    if !metrics.contains(&"speed".to_string()) {
                        metrics.push("speed".into());
                    }
                }
                RECORD_CADENCE => metrics.push("cadence".into()),
                RECORD_POSITION_LAT => has_lat = true,
                RECORD_POSITION_LONG => has_long = true,
                _ => {}
            }
        }
        if has_lat && has_long {
            metrics.push("gps".into());
        }
        metrics
    }

    // -- Message decoders -----------------------------------------------------

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
                        s.start_timestamp_us =
                            Some((ts as i64 + FIT_EPOCH_OFFSET) * 1_000_000);
                    }
                }
                SESSION_TIMESTAMP if data.len() >= 4 => {
                    if let Some(ts) = valid_u32(data, big_endian) {
                        s.end_timestamp_us =
                            Some((ts as i64 + FIT_EPOCH_OFFSET) * 1_000_000);
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

    fn decode_device(fields: &[(u8, Vec<u8>)], big_endian: bool) -> Option<DeviceMeta> {
        let mut d = DeviceMeta::default();
        for (num, data) in fields {
            match *num {
                DEVICE_MANUFACTURER if data.len() >= 2 => {
                    let v = read_u16(data, big_endian);
                    if v != 0xFFFF {
                        d.manufacturer = Some(manufacturer_name(v).to_string());
                    }
                }
                DEVICE_SERIAL_NUMBER if data.len() >= 4 => {
                    let v = read_u32(data, big_endian);
                    if v != 0 && v != 0xFFFFFFFF {
                        d.serial_number = Some(format!("{v}"));
                    }
                }
                DEVICE_PRODUCT_NAME => {
                    let s = String::from_utf8_lossy(data)
                        .trim_end_matches('\0')
                        .to_string();
                    if !s.is_empty() {
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
}

// Helpers for reading integers, returning None for FIT "invalid" sentinel values.

fn valid_u32(data: &[u8], big_endian: bool) -> Option<u32> {
    let v = read_u32(data, big_endian);
    if v == 0xFFFFFFFF { None } else { Some(v) }
}

fn read_u16(data: &[u8], big_endian: bool) -> u16 {
    if big_endian {
        u16::from_be_bytes([data[0], data[1]])
    } else {
        u16::from_le_bytes([data[0], data[1]])
    }
}

fn read_u32(data: &[u8], big_endian: bool) -> u32 {
    if big_endian {
        u32::from_be_bytes([data[0], data[1], data[2], data[3]])
    } else {
        u32::from_le_bytes([data[0], data[1], data[2], data[3]])
    }
}

// ---------------------------------------------------------------------------
// Scanner result → Python dict (same shape as full parser, minus "records")
// ---------------------------------------------------------------------------

fn build_scan_result_dict(py: Python<'_>, scan: &ScanResult) -> PyResult<PyObject> {
    let result = PyDict::new_bound(py);
    let activities = PyList::empty_bound(py);

    let build_one = |session: &SessionMeta| -> PyResult<Bound<'_, PyDict>> {
        let activity = PyDict::new_bound(py);
        let meta = session_to_dict(py, session)?;

        let metrics_list = PyList::empty_bound(py);
        for m in &scan.record_metrics {
            metrics_list.append(m)?;
        }
        meta.set_item("metrics", metrics_list)?;

        let devices_list = PyList::empty_bound(py);
        for d in &scan.devices {
            devices_list.append(device_to_dict(py, d)?)?;
        }
        meta.set_item("devices", devices_list)?;

        activity.set_item("metadata", meta)?;
        Ok(activity)
    };

    if scan.sessions.is_empty() {
        // No sessions found — return one activity with empty metadata.
        let activity = PyDict::new_bound(py);
        let meta = PyDict::new_bound(py);
        for key in [
            "sport",
            "sub_sport",
            "name",
            "start_time",
            "start_time_local",
            "duration",
            "distance",
        ] {
            meta.set_item(key, py.None())?;
        }
        let metrics_list = PyList::empty_bound(py);
        for m in &scan.record_metrics {
            metrics_list.append(m)?;
        }
        meta.set_item("metrics", metrics_list)?;
        meta.set_item("devices", PyList::empty_bound(py))?;
        activity.set_item("metadata", meta)?;
        activities.append(activity)?;
    } else {
        for session in &scan.sessions {
            activities.append(build_one(session)?)?;
        }
    }

    result.set_item("activities", activities)?;
    Ok(result.into_any().unbind())
}

// ═══════════════════════════════════════════════════════════════════════════
// Python-exposed functions
// ═══════════════════════════════════════════════════════════════════════════

/// Full parse from file path — returns records + metadata.
#[pyfunction]
fn parse_fit(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    build_parse_result_dict(py, parse_all(path)?)
}

/// Full parse from bytes — returns records + metadata.
#[pyfunction]
fn parse_fit_bytes(py: Python<'_>, data: &[u8]) -> PyResult<PyObject> {
    build_parse_result_dict(py, parse_all_bytes(data)?)
}

/// Metadata-only scan from file path — skips Record data.
#[pyfunction]
fn parse_fit_metadata(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let data = std::fs::read(path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let mut scanner =
        FitScanner::new(&data).map_err(pyo3::exceptions::PyValueError::new_err)?;
    let result = scanner
        .scan()
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    build_scan_result_dict(py, &result)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_fit, m)?)?;
    m.add_function(wrap_pyfunction!(parse_fit_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(parse_fit_metadata, m)?)?;
    Ok(())
}
