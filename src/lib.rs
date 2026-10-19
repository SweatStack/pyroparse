mod fields;
mod fit;
pub(crate) mod reference;
mod summary;
mod types;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::Arc;

use fields::{is_canonical_column, normalize_field_name};
use fit::profile;
use types::TypedColumn;

use arrow::array::{
    ArrowPrimitiveType, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, PrimitiveArray, StringArray, TimestampMicrosecondArray,
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
    /// Sensor types that produce this column: `device_info.device_type` names
    /// (see `DeviceMeta::sensor_type`). ANT+ and BLE name the same sensors alike.
    sensor_types: &'static [&'static str],
    /// Manufacturers known to produce this metric: the fallback for channels
    /// whose type does not say what they measure (fitness equipment, rowers).
    /// FIT profile manufacturer names.
    known_manufacturers: &'static [&'static str],
    /// When a paired foot pod produced this column (see `FootPod`).
    foot_pod: FootPod,
    /// Whether this column can conflict with a developer field and needs the
    /// majority-wins merge logic.
    mergeable: bool,
}

/// Sensor types of a foot pod: ANT+ `stride_speed_distance`, BLE `footpod`.
/// An HRM-Pro's running channel is one too, and it is registered in every
/// sport, so a foot pod is a source only in on-foot sessions (`is_on_foot`).
const FOOT_POD_TYPES: &[&str] = &["stride_speed_distance", "footpod"];

/// When a paired foot pod produced a column, in an on-foot session.
#[derive(Clone, Copy)]
enum FootPod {
    Never,
    /// The watch prefers a foot pod over its wrist sensor (running cadence).
    Always,
    /// A foot pod is the source only without GPS (a treadmill, or GPS off).
    /// With GPS the watch uses GPS unless the runner chose the foot pod as
    /// speed source, a setting the file does not record: we then credit GPS.
    WithoutGps,
}

/// FIT sports in which a foot pod measures.
fn is_on_foot(session: Option<&SessionMeta>) -> bool {
    matches!(
        session.and_then(|s| s.sport.as_deref()),
        Some("running" | "walking" | "hiking")
    )
}

const COLUMN_ATTRIBUTIONS: &[ColumnAttribution] = &[
    ColumnAttribution {
        name: "power",
        sensor_types: &["bike_power"],
        foot_pod: FootPod::Never,
        known_manufacturers: &[
            "concept2",
            "elite",
            "favero_electronics",
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
        sensor_types: &["bike_speed_cadence", "bike_cadence"],
        foot_pod: FootPod::Always,
        known_manufacturers: &[],
        mergeable: true,
    },
    ColumnAttribution {
        name: "heart_rate",
        sensor_types: &["heart_rate"],
        foot_pod: FootPod::Never,
        known_manufacturers: &[],
        mergeable: false,
    },
    ColumnAttribution {
        name: "speed",
        sensor_types: &["bike_speed", "bike_speed_cadence"],
        foot_pod: FootPod::WithoutGps,
        known_manufacturers: &[],
        mergeable: false,
    },
    ColumnAttribution {
        name: "temperature",
        sensor_types: &["env_sensor"],
        foot_pod: FootPod::Never,
        known_manufacturers: &[],
        mergeable: false,
    },
    ColumnAttribution {
        name: "distance",
        sensor_types: &[],
        foot_pod: FootPod::WithoutGps,
        known_manufacturers: &[],
        mergeable: false,
    },
    ColumnAttribution {
        name: "altitude",
        sensor_types: &[],
        foot_pod: FootPod::Never,
        known_manufacturers: &[],
        mergeable: false,
    },
];

// ---------------------------------------------------------------------------
// Sensor classification — identify CIQ apps from DeveloperDataId UUIDs
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct DeveloperSensor {
    /// The app's maker when it is a known app, else `None`.
    pub(crate) manufacturer: Option<String>,
    /// The app's label (`CORE`, `Concept2`), or its UUID for an unknown app.
    pub(crate) product_name: String,
    /// CIQ application UUID; matches `ParseResult::apps` entries.
    pub(crate) uuid: String,
    pub(crate) columns: Vec<String>,
}

/// Known CIQ app UUIDs → (manufacturer, label).
///
/// This is purely cosmetic — it gives human-readable names to developer
/// sensors.  Unknown UUIDs still work; they just show the UUID as the name.
/// To add a new sensor, add a row.
const KNOWN_CIQ_APPS: &[(&str, &str, &str)] = &[
    // (uuid, manufacturer, label)
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

/// A CIQ app's identity: `(manufacturer, label)` for a known app, else no
/// manufacturer and the UUID as the label.
pub(crate) fn ciq_app_identity(uuid: &str) -> (Option<&'static str>, String) {
    KNOWN_CIQ_APPS
        .iter()
        .find(|(u, _, _)| u.eq_ignore_ascii_case(uuid))
        .map_or((None, uuid.to_string()), |&(_, mfr, label)| (Some(mfr), label.to_string()))
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
/// in `dev_field_owners` (field_name → app UUIDs). Several apps may register
/// the same name; every registrant starts out credited with the column, and
/// `attribute_devices` narrows that to the app that supplied the readings.
///
/// When `include_columns` is true, each sensor's `columns` list is populated
/// with the output column names that appear in the data.  When false (metadata-
/// only scan), sensors are classified but columns are left empty.
pub(crate) fn classify_developer_sensors(
    dev_field_owners: &BTreeMap<String, BTreeSet<String>>,
    present_extra_columns: &BTreeSet<String>,
    include_columns: bool,
) -> Vec<DeveloperSensor> {
    // Group fields by their owning app UUID.
    let mut app_columns: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();

    for (field_name, uuids) in dev_field_owners {
        for uuid in uuids {
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
    }

    app_columns
        .into_iter()
        .map(|(uuid, cols)| {
            let (manufacturer, product_name) = ciq_app_identity(uuid);
            DeveloperSensor {
                manufacturer: manufacturer.map(String::from),
                product_name,
                uuid: uuid.into(),
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
    /// Breaths/min, from `enhanced_respiration_rate` (field 108) or the
    /// legacy whole-number `respiration_rate` (field 99).
    pub(crate) respiration_rate: Option<f32>,
    // Developer shadow fields — kept separate from standard fields so
    // we can pick the winner per-session after slicing.
    pub(crate) dev_power: Option<i16>,
    pub(crate) dev_cadence: Option<i16>,
    // App slot (into `ParseResult::apps`) that supplied each developer-sourced
    // value, so a column can be credited per session when several CIQ apps
    // register the same field name.
    pub(crate) dev_power_app: Option<u8>,
    pub(crate) dev_cadence_app: Option<u8>,
    pub(crate) smo2_app: Option<u8>,
    pub(crate) core_temperature_app: Option<u8>,
}

#[derive(Default)]
pub(crate) struct SessionMeta {
    pub(crate) sport: Option<String>,
    pub(crate) sub_sport: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) start_time: Option<f64>,
    pub(crate) start_time_local: Option<f64>,
    /// Pool length in metres (Session field 44), present only for pool swims.
    /// Anchors per-record distance reconstruction — see [`assign_lengths`].
    pub(crate) pool_length: Option<f64>,
    pub(crate) start_timestamp_us: Option<i64>,
    /// The device and athlete-entered summary fields (see `summary.rs`), by
    /// attribute name. Full parse only.
    pub(crate) summary: Vec<(&'static str, summary::Value)>,
    pub(crate) athlete: AthleteMeta,
}

/// What the recording device assumed about the athlete for one session, from
/// its `time_in_zone` message (see `resolve_athlete`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct AthleteMeta {
    pub(crate) max_heart_rate: Option<i64>,
    pub(crate) threshold_heart_rate: Option<i64>,
}

#[derive(Clone, Default)]
pub(crate) struct DeviceMeta {
    /// `manufacturer` (field 2), raw. See `manufacturer_name`.
    pub(crate) manufacturer: Option<u16>,
    /// `product` (field 4), raw. See `product_key`.
    pub(crate) product: Option<u16>,
    /// The file's free-text `product_name` (field 27), verbatim. Often the
    /// head unit's own label for the sensor, not the product's name.
    pub(crate) product_name: Option<String>,
    pub(crate) serial_number: Option<String>,
    pub(crate) device_index: Option<u8>,
    /// Position of this entry's latest DeviceInfo emission among all
    /// DeviceInfo messages in the file. Attribution prefers the entry seen last.
    pub(crate) last_seen: usize,
    /// `device_type` (field 1), raw. Its meaning depends on `source_type`;
    /// see `sensor_type`.
    pub(crate) device_type: Option<u8>,
    /// `source_type` (field 25), raw: ANT+, BLE, local, ….
    pub(crate) source_type: Option<u8>,
    /// Columns attributed to this device (populated per-session during merge).
    pub(crate) columns: Vec<String>,
}

/// A lap boundary extracted from a FIT Lap message.
pub(crate) struct LapBoundary {
    pub(crate) start_time_us: i64,
    pub(crate) end_time_us: i64,
    pub(crate) trigger: Option<String>,
}

/// A pool "length" interval extracted from a FIT Length message (mesg 101).
///
/// In lap (pool) swimming the Record stream carries no distance/speed/cadence —
/// those live per pool length here. [`assign_lengths`] reconstructs the missing
/// Record columns from these intervals. See `plans/029-POOL-SWIM-DISTANCE.md`
/// and `docs/FIT-FORMAT.md` for the model and its rationale.
#[derive(Clone)]
pub(crate) struct LengthInterval {
    /// Length start (Length field 2), microseconds since Unix epoch. This is the
    /// **sole** basis for record→length assignment (carry-forward). The per-length
    /// `timestamp` (field 253) is unreliable on some devices and is never used.
    pub(crate) start_time_us: i64,
    /// total_elapsed_time (field 3), seconds. Used **only** for the intra-length
    /// ramp of *active* lengths; never for assignment (idle `total_elapsed_time`
    /// can be absurd — e.g. a paused rest encoded as ~131 071 s).
    pub(crate) duration_s: f64,
    /// `length_type == active` (field 12). Active lengths contribute `pool_length`
    /// metres; idle lengths (rest) contribute zero.
    pub(crate) active: bool,
    /// avg_swimming_cadence (field 9), strokes/min.
    pub(crate) cadence: Option<i16>,
    /// swim_stroke (field 7), decoded enum name.
    pub(crate) swim_stroke: Option<String>,
}

pub(crate) struct ParseResult {
    pub(crate) file_type: Option<String>,
    pub(crate) records: Vec<RecordRow>,
    pub(crate) extra_col_info: Vec<(String, DataType)>,
    pub(crate) extra_data: Vec<TypedColumn>,
    pub(crate) sessions: Vec<SessionMeta>,
    /// All DeviceInfo entries, in order of appearance.  Split into per-session
    /// groups using `devices_per_session` before building output.
    pub(crate) devices: Vec<DeviceMeta>,
    pub(crate) developer_sensors: Vec<DeveloperSensor>,
    pub(crate) laps: Vec<LapBoundary>,
    /// Pool-swim Length intervals, sorted by `start_time_us`. Empty for every
    /// non-pool-swim file.
    pub(crate) lengths: Vec<LengthInterval>,
    /// CIQ app UUIDs by slot, as referenced by `RecordRow::*_app`.
    pub(crate) apps: Vec<String>,
    /// The file's running intervals (see `running_intervals`); `None` when it
    /// has no timer events.
    pub(crate) running: Option<Vec<summary::Interval>>,
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

/// Reconstructed per-record columns for a pool swim, one entry per record.
/// `distance`/`speed` are `None` throughout when no `pool_length` anchor is
/// available — we never fabricate distance without one.
struct LengthColumns {
    distance: Vec<Option<f64>>,
    speed: Vec<Option<f32>>,
    cadence: Vec<Option<i16>>,
    stroke: Vec<Option<String>>,
    index: Vec<i16>,
    /// Active lengths × `pool_length`: the swim's distance. The `distance`
    /// column interpolates within each length and the last record falls
    /// before the last length ends, so its maximum can stop short of this.
    total_distance: Option<f64>,
}

/// Reconstruct `distance`/`speed`/`cadence`/`swim_stroke`/`length` for each
/// record from pool-swim [`LengthInterval`]s. `lengths` must be sorted by
/// `start_time_us` and non-empty.
///
/// Each record is assigned to the length with the greatest `start_time_us <=`
/// its timestamp (carry-forward) — so rest gaps between lengths, and the
/// unreliable per-length `timestamp` field, never affect assignment. Within an
/// *active* length distance ramps linearly by elapsed-time fraction: the unique
/// representation consistent with a piecewise-constant `speed` (`distance′ =
/// speed`). Distance is anchored on the running active-length count times
/// `pool_length`, so it is monotone non-decreasing and lands on the running
/// total at each wall; the last record can fall before the last length ends,
/// so the swim's total is `LengthColumns::total_distance`. See
/// `plans/029-POOL-SWIM-DISTANCE.md`.
fn assign_lengths(
    records: &[RecordRow],
    lengths: &[LengthInterval],
    pool_length: Option<f64>,
) -> LengthColumns {
    let n = records.len();

    // cum_start[i] = reconstructed distance (m) at the *start* of length i,
    // i.e. the sum of pool_length over all preceding active lengths. Only
    // meaningful when we have a pool_length anchor.
    let cum_start: Vec<f64> = match pool_length {
        Some(pool) => {
            let mut acc = 0.0;
            let mut v = Vec::with_capacity(lengths.len());
            for l in lengths {
                v.push(acc);
                if l.active {
                    acc += pool;
                }
            }
            v
        }
        None => Vec::new(),
    };

    let mut distance = Vec::with_capacity(n);
    let mut speed = Vec::with_capacity(n);
    let mut cadence = Vec::with_capacity(n);
    let mut stroke: Vec<Option<String>> = Vec::with_capacity(n);
    let mut index = Vec::with_capacity(n);

    let first_start = lengths.first().map(|l| l.start_time_us);

    // An idle length is a rest at the wall, a stop: 0 in each column this swim
    // records, null in a column it does not (AGENTS.md, "A stop is 0, missing
    // is null").
    let records_speed = pool_length.is_some() && lengths.iter().any(|l| l.active && l.duration_s > 0.0);
    let records_cadence = lengths.iter().any(|l| l.active && l.cadence.is_some_and(|c| c != 0));
    let stopped_speed = records_speed.then_some(0.0);
    let stopped_cadence = records_cadence.then_some(0);

    for record in records {
        let ts = record.timestamp.unwrap_or(0);

        // Records before the first length (warmup at the wall) belong to no
        // length: index 0, distance 0, no pace/stroke.
        let before_first = match first_start {
            Some(start) => ts < start,
            None => true,
        };
        if before_first {
            index.push(0);
            distance.push(pool_length.map(|_| 0.0));
            speed.push(None);
            cadence.push(None);
            stroke.push(None);
            continue;
        }

        // Carry-forward: greatest start_time_us <= ts. `pos >= 1` here because
        // ts >= first_start, so `li` is in bounds.
        let pos = lengths.partition_point(|l| l.start_time_us <= ts);
        let li = pos - 1;
        let length = &lengths[li];

        index.push(li as i16);
        cadence.push(if length.active { length.cadence } else { stopped_cadence });
        stroke.push(length.swim_stroke.clone());

        let Some(pool) = pool_length else {
            distance.push(None);
            speed.push(None);
            continue;
        };

        if length.active && length.duration_s > 0.0 {
            let elapsed_us = length.duration_s * 1_000_000.0;
            let frac = (((ts - length.start_time_us) as f64) / elapsed_us).clamp(0.0, 1.0);
            distance.push(Some(cum_start[li] + pool * frac));
            speed.push(Some((pool / length.duration_s) as f32));
        } else if length.active {
            // Ramp guard: active length with non-positive `total_elapsed_time`
            // (a corrupt duration). Attribute the whole pool length at once
            // rather than divide by zero; distance stays monotone, pace unknown.
            distance.push(Some(cum_start[li] + pool));
            speed.push(None);
        } else {
            // Idle length (rest): distance holds flat; a stop.
            distance.push(Some(cum_start[li]));
            speed.push(stopped_speed);
        }
    }

    let total_distance =
        pool_length.map(|pool| lengths.iter().filter(|l| l.active).count() as f64 * pool);
    LengthColumns { distance, speed, cadence, stroke, index, total_distance }
}

/// Re-anchor a later session's cumulative `distance` at zero.
///
/// Record `distance` is cumulative. In a multi-session file some devices keep
/// one running total across sessions while others restart it per session, so a
/// later session may begin at the previous one's end. `carried` is the last
/// distance recorded before this session started. When the session's first
/// distance continues from it (is at least `carried`), the device carried the
/// total over and `carried` is subtracted; when it is lower, the device reset
/// and nothing changes. No threshold is involved, and a first sample of a few
/// metres is kept as the movement it is. Results are rounded to the field's
/// centimetre resolution, so a rebased value is bit-identical to what a
/// resetting device would have recorded.
fn rebase_session_distance(records: &mut [RecordRow], carried: Option<f64>) {
    let Some(carried) = carried.filter(|&c| c > 0.0) else { return };
    let Some(first) = records.iter().find_map(|r| r.distance) else { return };
    if first < carried {
        return;
    }
    for d in records.iter_mut().filter_map(|r| r.distance.as_mut()) {
        *d = ((*d - carried) * 100.0).round() / 100.0;
    }
}

/// Indoor-rowing rests: a run of more than this many consecutive records in
/// which `distance` does not advance.
const ROWING_REST_MIN_RECORDS: usize = 4;
/// "Does not advance": by at most one FIT distance unit (1 cm), which also
/// absorbs the flywheel's coast-down after the last stroke.
const ROWING_REST_MAX_ADVANCE_CM: f64 = 1.0;

/// Zero `power`, `cadence` and `speed` through indoor-rowing rests, returning
/// the columns in which a value changed (in schema order).
///
/// A rowing monitor reports per-stroke values and holds each until the next
/// stroke. When the rower stops there is no next stroke, so the last stroke's
/// power, rate and pace keep arriving and the watch keeps writing them; only
/// distance stops. A rest is a stop, and a stop is 0 (AGENTS.md, "A stop is 0,
/// missing is null"): no work, no strokes, no pace, as a bike computer writes
/// when the rider stops. Rest records get 0 whatever the file held there, held
/// value or null, in each of these columns the session recorded; a column it
/// never recorded stays null. Distance, heart rate and every other column are
/// left as written.
///
/// Applies only to FIT `sub_sport = indoor_rowing` sessions whose `distance`
/// is a reading (non-null, non-zero somewhere): without a distance signal a
/// rest cannot be told from rowing. Runs are found in timestamp order; a record
/// without distance belongs to no run and ends one.
fn clean_rowing_rests(records: &mut [RecordRow], session: Option<&SessionMeta>) -> Vec<String> {
    let indoor_rowing = session.is_some_and(|s| s.sub_sport.as_deref() == Some("indoor_rowing"));
    if !indoor_rowing || !records.iter().any(|r| is_reading(r.distance)) {
        return Vec::new();
    }

    let mut order: Vec<usize> = (0..records.len()).collect();
    order.sort_by_key(|&i| records[i].timestamp); // stable: ties keep file order
    // still[k]: the k-th record in time did not advance distance over the one before.
    let still: Vec<bool> = (0..order.len())
        .map(|k| {
            let current = records[order[k]].distance;
            let previous = k.checked_sub(1).and_then(|p| records[order[p]].distance);
            matches!((previous, current), (Some(a), Some(b))
                if ((b - a) * 100.0).round() <= ROWING_REST_MAX_ADVANCE_CM)
        })
        .collect();

    // Only the columns this session recorded are zeroed.
    let recorded = (
        records.iter().any(|r| is_reading(r.power)),
        records.iter().any(|r| is_reading(r.cadence)),
        records.iter().any(|r| is_reading(r.speed)),
    );
    let (mut power, mut cadence, mut speed) = (false, false, false);
    let mut k = 0;
    while k < still.len() {
        let run = still[k..].iter().take_while(|&&s| s).count();
        if run >= ROWING_REST_MIN_RECORDS {
            for &i in &order[k..k + run] {
                let row = &mut records[i];
                power |= recorded.0 && set_zero(&mut row.power);
                cadence |= recorded.1 && set_zero(&mut row.cadence);
                speed |= recorded.2 && set_zero(&mut row.speed);
            }
        }
        k += run.max(1);
    }

    [("power", power), ("cadence", cadence), ("speed", speed)]
        .into_iter()
        .filter(|&(_, changed)| changed)
        .map(|(name, _)| name.to_string())
        .collect()
}

/// A reading: a present, non-zero value (the convention of `has_reading`).
fn is_reading<T: PartialEq + Default>(value: Option<T>) -> bool {
    value.is_some_and(|v| v != T::default())
}

/// Set a value to 0, returning whether that changed it.
fn set_zero<T: Copy + PartialEq + Default>(value: &mut Option<T>) -> bool {
    let changed = *value != Some(T::default());
    *value = Some(T::default());
    changed
}

/// Build the Arrow RecordBatch for a set of records.
///
/// Returns the batch and the list of standard columns whose values were
/// *reconstructed* from pool-swim Length messages (rather than measured) — empty
/// for every non-pool-swim file. See [`assign_lengths`].
fn build_batch(
    records: &[RecordRow],
    extra_col_info: &[(String, DataType)],
    extra_data: &[TypedColumn],
    laps: &[LapBoundary],
    lengths: &[LengthInterval],
    pool_length: Option<f64>,
) -> PyResult<(RecordBatch, Reconstruction)> {
    // Pool-swim reconstruction. Present only when the file has Length messages.
    let recon = if lengths.is_empty() {
        None
    } else {
        Some(assign_lengths(records, lengths, pool_length))
    };

    // Gate: reconstruct a standard column only when Length data exists *and* the
    // Record stream carries no measured value for it — all-or-nothing per column,
    // so a column is never a mix of measured and reconstructed values.
    let reconstruct_distance =
        recon.is_some() && records.iter().all(|r| r.distance.is_none());
    let reconstruct_speed =
        recon.is_some() && records.iter().all(|r| r.speed.is_none());
    let reconstruct_cadence =
        recon.is_some() && records.iter().all(|r| r.cadence.is_none());

    let mut reconstructed_columns = Vec::new();
    if reconstruct_distance { reconstructed_columns.push("distance".to_string()); }
    if reconstruct_speed { reconstructed_columns.push("speed".to_string()); }
    if reconstruct_cadence { reconstructed_columns.push("cadence".to_string()); }

    // Canonical extras from RecordRow — only included when they have at least
    // one non-null value, then sorted in among the dynamic extras.
    let mut canonical_extras: Vec<(String, Arc<dyn arrow::array::Array>)> = Vec::new();
    let float_extras: [(&str, fn(&RecordRow) -> Option<f32>); 3] = [
        ("core_temperature", |r| r.core_temperature),
        ("respiration_rate", |r| r.respiration_rate),
        ("smo2", |r| r.smo2),
    ];
    for (name, value) in float_extras {
        if records.iter().any(|r| value(r).is_some()) {
            canonical_extras.push((
                name.into(),
                Arc::new(Float32Array::from_iter(records.iter().map(value))),
            ));
        }
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

    // Pool-swim structural extras: `length` (0-based index) whenever Length
    // messages exist, and `swim_stroke` when any length carries a stroke.
    if let Some(recon) = &recon {
        canonical_extras.push((
            "length".into(),
            Arc::new(Int16Array::from(recon.index.clone())),
        ));
        if recon.stroke.iter().any(|s| s.is_some()) {
            let stroke_arr: StringArray = recon.stroke.iter().map(|s| s.as_deref()).collect();
            canonical_extras.push(("swim_stroke".into(), Arc::new(stroke_arr)));
        }
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

    // Fixed columns whose values may be reconstructed for pool swims. When the
    // gate is off, they are built straight from the Record stream as before.
    let cadence_arr: Arc<dyn arrow::array::Array> = if reconstruct_cadence {
        Arc::new(Int16Array::from(recon.as_ref().unwrap().cadence.clone()))
    } else {
        Arc::new(Int16Array::from_iter(records.iter().map(|r| r.cadence)))
    };
    let speed_arr: Arc<dyn arrow::array::Array> = if reconstruct_speed {
        Arc::new(Float32Array::from(recon.as_ref().unwrap().speed.clone()))
    } else {
        Arc::new(Float32Array::from_iter(records.iter().map(|r| r.speed)))
    };
    let distance_arr: Arc<dyn arrow::array::Array> = if reconstruct_distance {
        Arc::new(Float64Array::from(recon.as_ref().unwrap().distance.clone()))
    } else {
        Arc::new(Float64Array::from_iter(records.iter().map(|r| r.distance)))
    };

    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(
            TimestampMicrosecondArray::from(
                records.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
        Arc::new(Int16Array::from_iter(records.iter().map(|r| r.heart_rate))),
        Arc::new(Int16Array::from_iter(records.iter().map(|r| r.power))),
        cadence_arr,
        speed_arr,
        Arc::new(Float64Array::from_iter(records.iter().map(|r| r.latitude))),
        Arc::new(Float64Array::from_iter(
            records.iter().map(|r| r.longitude),
        )),
        Arc::new(Float32Array::from_iter(records.iter().map(|r| r.altitude))),
        Arc::new(Int8Array::from_iter(
            records.iter().map(|r| r.temperature),
        )),
        distance_arr,
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

    let batch = RecordBatch::try_new(Arc::new(schema), arrays)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let distance = if reconstruct_distance { recon.and_then(|r| r.total_distance) } else { None };
    Ok((batch, Reconstruction { columns: reconstructed_columns, distance }))
}

/// What pool-swim reconstruction produced for one batch.
struct Reconstruction {
    /// Columns reconstructed from Length messages.
    columns: Vec<String>,
    /// The swim's distance when `distance` was reconstructed (see
    /// `LengthColumns::total_distance`).
    distance: Option<f64>,
}

// ---------------------------------------------------------------------------
// Metrics detection — which data columns have non-null values
// ---------------------------------------------------------------------------

/// The metrics a session recorded: its data columns holding at least one
/// reading (a non-null, non-zero value, see `has_reading`). An idle sensor
/// writing only zeros did not record its metric; the column itself is left
/// as written. Latitude and longitude are reported together as `gps`, and the
/// structural `lap` and `lap_trigger` columns are not metrics.
///
/// Developer sensors are credited only with these columns (see
/// `attribute_devices`), so one definition decides both.
fn detect_metrics(batch: &RecordBatch) -> Vec<String> {
    let schema = batch.schema();
    let has = |name: &str| {
        schema.index_of(name).is_ok_and(|i| has_reading(batch.column(i).as_ref()))
    };
    let mut metrics = Vec::new();
    for field in schema.fields() {
        match field.name().as_str() {
            "timestamp" | "lap" | "lap_trigger" | "longitude" => {}
            "latitude" => {
                if has("latitude") && has("longitude") {
                    metrics.push("gps".to_string());
                }
            }
            name => {
                if has(name) {
                    metrics.push(name.to_string());
                }
            }
        }
    }
    metrics
}

fn has_reading(array: &dyn arrow::array::Array) -> bool {
    fn any_nonzero<T: ArrowPrimitiveType>(a: &PrimitiveArray<T>) -> bool {
        a.iter().any(|v| v.is_some_and(|x| x != T::Native::default()))
    }
    let any = array.as_any();
    if let Some(a) = any.downcast_ref::<Int8Array>() { return any_nonzero(a); }
    if let Some(a) = any.downcast_ref::<Int16Array>() { return any_nonzero(a); }
    if let Some(a) = any.downcast_ref::<Int32Array>() { return any_nonzero(a); }
    if let Some(a) = any.downcast_ref::<Int64Array>() { return any_nonzero(a); }
    if let Some(a) = any.downcast_ref::<Float32Array>() { return any_nonzero(a); }
    if let Some(a) = any.downcast_ref::<Float64Array>() { return any_nonzero(a); }
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return a.iter().any(|v| v.is_some_and(|s| !s.is_empty()));
    }
    array.null_count() < array.len()
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
    dict.set_item("pool_length", session.pool_length)?;
    let summary = PyDict::new_bound(py);
    for &(name, value) in &session.summary {
        set_value(&summary, name, value)?;
    }
    dict.set_item("summary", summary)?;
    let athlete = PyDict::new_bound(py);
    athlete.set_item("max_heart_rate", session.athlete.max_heart_rate)?;
    athlete.set_item("threshold_heart_rate", session.athlete.threshold_heart_rate)?;
    dict.set_item("athlete", athlete)?;
    Ok(dict)
}

fn set_value(dict: &Bound<'_, PyDict>, name: &str, value: summary::Value) -> PyResult<()> {
    match value {
        summary::Value::Int(v) => dict.set_item(name, v),
        summary::Value::Float(v) => dict.set_item(name, v),
    }
}

fn device_to_dict<'py>(
    py: Python<'py>,
    device: &DeviceMeta,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("manufacturer", device.manufacturer_name())?;
    dict.set_item("product", device.product_key())?;
    dict.set_item("product_name", device.product_name.as_deref())?;
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
    dict.set_item("manufacturer", sensor.manufacturer.as_deref())?;
    dict.set_item("product_name", &sensor.product_name)?;
    let cols = PyList::empty_bound(py);
    for c in &sensor.columns {
        cols.append(c)?;
    }
    dict.set_item("columns", cols)?;
    Ok(dict)
}

/// What one activity's record columns hold, beyond their values.
struct ColumnNotes<'a> {
    /// Columns that recorded something (see `detect_metrics`).
    metrics: &'a [String],
    /// Columns reconstructed from pool-swim Length messages (see `assign_lengths`).
    reconstructed: &'a [String],
    /// Columns in which held values were nulled (see `clean_rowing_rests`).
    cleaned: &'a [String],
}


fn build_activity_dict<'py>(
    py: Python<'py>,
    batch: &RecordBatch,
    session: Option<&SessionMeta>,
    devices: &[DeviceMeta],
    developer_sensors: &[DeveloperSensor],
    columns: &ColumnNotes,
    inputs: &SummaryInputs,
) -> PyResult<Bound<'py, PyDict>> {

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
                "sport", "sub_sport", "name", "start_time", "start_time_local", "pool_length",
            ] {
                d.set_item(key, py.None())?;
            }
            d
        }
    };

    // Provenance lists are empty when nothing was reconstructed or cleaned.
    meta.set_item("reconstructed_columns", PyList::new_bound(py, columns.reconstructed))?;
    meta.set_item("cleaned_columns", PyList::new_bound(py, columns.cleaned))?;
    meta.set_item("metrics", PyList::new_bound(py, columns.metrics))?;

    let devices_list = PyList::empty_bound(py);
    for d in devices { devices_list.append(device_to_dict(py, d)?)?; }
    meta.set_item("devices", devices_list)?;

    let sensors_list = PyList::empty_bound(py);
    for s in developer_sensors { sensors_list.append(sensor_to_dict(py, s)?)?; }
    meta.set_item("developer_sensors", sensors_list)?;

    activity.set_item("metadata", meta)?;

    let summary_inputs = PyDict::new_bound(py);
    summary_inputs.set_item("start", session.and_then(|s| s.start_timestamp_us))?;
    summary_inputs.set_item("end", inputs.end)?;
    summary_inputs.set_item("running", inputs.running)?;
    summary_inputs.set_item("pool_distance", inputs.pool_distance)?;
    activity.set_item("summary_inputs", summary_inputs)?;
    Ok(activity)
}

/// What `summarize` needs about one session beyond its records.
struct SummaryInputs<'a> {
    /// The next session's start in a multi-session file: where this one ends.
    end: Option<i64>,
    running: Option<&'a [summary::Interval]>,
    pool_distance: Option<f64>,
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

/// The app credited with a developer-sourced column: the one that supplied
/// the most readings (non-null, non-zero values) in this record slice. Ties
/// go to the lowest slot, for determinism.
fn winning_app(
    records: &[RecordRow],
    has_reading: impl Fn(&RecordRow) -> bool,
    app: impl Fn(&RecordRow) -> Option<u8>,
) -> Option<u8> {
    let mut counts: BTreeMap<u8, u32> = BTreeMap::new();
    for row in records.iter().filter(|r| has_reading(r)) {
        if let Some(slot) = app(row) {
            *counts.entry(slot).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .max_by_key(|&(slot, n)| (n, std::cmp::Reverse(slot)))
        .map(|(slot, _)| slot)
}

/// Which app is credited with each developer-sourced canonical column in this
/// session. `dev_won` is `resolve_merge`'s bitmask: power and cadence are
/// developer-sourced only when the developer field won the merge.
fn dev_column_winners(records: &[RecordRow], dev_won: u8) -> Vec<(&'static str, u8)> {
    let mut winners = Vec::new();
    if dev_won & 1 != 0 {
        if let Some(a) = winning_app(records, |r| r.dev_power.unwrap_or(0) != 0, |r| r.dev_power_app) {
            winners.push(("power", a));
        }
    }
    if dev_won & 2 != 0 {
        if let Some(a) = winning_app(records, |r| r.dev_cadence.unwrap_or(0) != 0, |r| r.dev_cadence_app) {
            winners.push(("cadence", a));
        }
    }
    // SmO2 decodes zeros to null (see `smo2_reading`): every value is a reading.
    if let Some(a) = winning_app(records, |r| r.smo2.is_some(), |r| r.smo2_app) {
        winners.push(("smo2", a));
    }
    if let Some(a) = winning_app(
        records,
        |r| r.core_temperature.is_some_and(|v| v != 0.0),
        |r| r.core_temperature_app,
    ) {
        winners.push(("core_temperature", a));
    }
    winners
}

/// Find the hardware device whose sensor type produces a column.
/// Skips the creator device (device_index == 0).
/// When multiple devices match, prefer the one emitted last (see `last_seen`).
fn find_device_by_sensor_type(
    devices: &[DeviceMeta],
    sensor_types: &[&str],
    foot_pod: bool,
) -> Option<usize> {
    devices
        .iter()
        .enumerate()
        .filter(|(_, d)| {
            d.device_index != Some(0)
                && d.sensor_type().is_some_and(|t| {
                    sensor_types.contains(&t) || (foot_pod && FOOT_POD_TYPES.contains(&t))
                })
        })
        .max_by_key(|(_, d)| d.last_seen)
        .map(|(i, _)| i)
}

/// True when a sensor type is a dedicated sensor for some column (a heart-rate
/// strap, a power meter). Such a channel is matched by its type alone: its
/// manufacturer is no evidence that it produced another column (a Wahoo TICKR
/// is not a Wahoo KICKR).
fn is_dedicated_sensor_type(sensor_type: &str) -> bool {
    FOOT_POD_TYPES.contains(&sensor_type)
        || COLUMN_ATTRIBUTIONS.iter().any(|c| c.sensor_types.contains(&sensor_type))
}

/// Find a hardware device whose manufacturer is known to produce this metric.
/// Skips the creator device (device_index == 0) since that's the watch itself,
/// and channels that declare themselves dedicated sensors for something else.
/// When multiple devices match, prefer the one emitted last — in multi-session
/// files the watch re-emits all known devices, and a head unit that reuses an
/// index for another sensor emits the sensor it switched to after the old one.
fn find_device_by_manufacturer(devices: &[DeviceMeta], manufacturers: &[&str]) -> Option<usize> {
    devices
        .iter()
        .enumerate()
        .filter(|(_, d)| {
            d.device_index != Some(0)
                && !d.sensor_type().is_some_and(is_dedicated_sensor_type)
                && d.manufacturer_name().is_some_and(|m| manufacturers.contains(&m))
        })
        .max_by_key(|(_, d)| d.last_seen)
        .map(|(i, _)| i)
}

/// Find the creator device (device_index == 0).
fn find_creator_device(devices: &[DeviceMeta]) -> Option<usize> {
    devices.iter().position(|d| d.device_index == Some(0))
}

/// Build per-session device list with column attribution based on merge outcome.
///
/// `metrics` lists the columns holding at least one reading in this session
/// (see `detect_metrics`): only those are attributed, developer sensors (CIQ
/// apps) are credited only with those, and an app credited with none is
/// dropped. `dev_winners` (see `dev_column_winners`)
/// names, per developer-sourced canonical column, the app slot in `apps`
/// that supplied the readings; other apps registering the same field name
/// lose the column. `on_foot` (see `is_on_foot`) lets a foot pod be a source.
fn attribute_devices(
    base_devices: &[DeviceMeta],
    developer_sensors: &[DeveloperSensor],
    dev_won: u8,
    metrics: &[String],
    dev_winners: &[(&str, u8)],
    apps: &[String],
    on_foot: bool,
) -> (Vec<DeviceMeta>, Vec<DeveloperSensor>) {
    let has_gps = metrics.iter().any(|m| m == "gps");
    let mut devices: Vec<DeviceMeta> = base_devices.to_vec();
    let mut sensors: Vec<DeveloperSensor> = developer_sensors.to_vec();

    // A developer sensor's columns come from Record *definitions*, which
    // declare an app's fields whether or not the app ever wrote a reading (an
    // installed Concept2 data field on a run writes only sentinels or zeros).
    // Keep only columns with a reading in this session; a sensor left with
    // none is dropped below.
    for sensor in &mut sensors {
        sensor.columns.retain(|c| metrics.contains(c));
    }

    // When several apps register the same field name (Stryd and the Concept2
    // data field both write `Power`), the column goes to the app whose
    // readings were kept.
    for &(col, slot) in dev_winners {
        let winner = apps.get(slot as usize);
        for sensor in &mut sensors {
            if Some(&sensor.uuid) != winner {
                sensor.columns.retain(|c| c != col);
            }
        }
    }

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
        // Find the best device: consider both sensor type and known manufacturer,
        // preferring the one emitted last (the active source when a head unit
        // re-registers or swaps sensors).
        let foot_pod = on_foot
            && match col.foot_pod {
                FootPod::Never => false,
                FootPod::Always => true,
                FootPod::WithoutGps => !has_gps,
            };
        let type_match = find_device_by_sensor_type(&devices, col.sensor_types, foot_pod);
        let mfr_match = find_device_by_manufacturer(&devices, col.known_manufacturers);
        let device_idx = match (type_match, mfr_match) {
            (Some(t), Some(m)) if t != m => {
                Some(if devices[m].last_seen > devices[t].last_seen { m } else { t })
            }
            (t, m) => t.or(m),
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

/// `source_type` values in the FIT profile.
const SOURCE_TYPE_ANTPLUS: u8 = 1;
const SOURCE_TYPE_BLE: u8 = 3; // bluetooth_low_energy

impl DeviceMeta {
    /// The FIT profile name of the manufacturer (`garmin`, `wahoo_fitness`).
    pub(crate) fn manufacturer_name(&self) -> Option<&'static str> {
        self.manufacturer.map(profile::manufacturer_name)
    }

    /// The product as a key: the name the profile's `product` subfield gives
    /// it for this manufacturer (`hrm_pro`, `assioma_duo`), else the number
    /// itself, which identifies a product only together with the manufacturer.
    pub(crate) fn product_key(&self) -> Option<String> {
        let product = self.product?;
        Some(
            self.manufacturer
                .and_then(|m| profile::device_info_product_name(m, product))
                .map_or_else(|| product.to_string(), String::from),
        )
    }

    /// What the channel says it is (`heart_rate`, `bike_power`, `footpod`): the
    /// name `device_type`'s profile subfield gives it for this `source_type`.
    /// A file that predates `source_type` wrote ANT+ types, and so do Wahoo
    /// head units on Bluetooth channels (120 for a Bluetooth heart-rate strap):
    /// a Bluetooth value that is not a Bluetooth type is read as ANT+. The two
    /// numberings do not overlap, so this cannot misread a Bluetooth type.
    pub(crate) fn sensor_type(&self) -> Option<&'static str> {
        let device_type = self.device_type?;
        let read_as = |source| profile::device_info_device_type_name(source, device_type);
        match self.source_type {
            None => read_as(SOURCE_TYPE_ANTPLUS),
            Some(SOURCE_TYPE_BLE) => read_as(SOURCE_TYPE_BLE).or_else(|| read_as(SOURCE_TYPE_ANTPLUS)),
            Some(source) => read_as(source),
        }
    }

    /// True when no identity field that both entries carry disagrees.
    fn agrees_with(&self, other: &DeviceMeta) -> bool {
        fn agree<T: PartialEq>(a: &Option<T>, b: &Option<T>) -> bool {
            !matches!((a, b), (Some(x), Some(y)) if x != y)
        }
        agree(&self.manufacturer, &other.manufacturer)
            && agree(&self.product, &other.product)
            && agree(&self.product_name, &other.product_name)
            && agree(&self.serial_number, &other.serial_number)
            && agree(&self.device_type, &other.device_type)
            && agree(&self.source_type, &other.source_type)
    }

    /// Merge a later emission of the same entry: fill gaps, keep what is known.
    fn absorb(&mut self, later: &DeviceMeta) {
        fn fill<T: Clone>(slot: &mut Option<T>, value: &Option<T>) {
            if slot.is_none() {
                *slot = value.clone();
            }
        }
        fill(&mut self.manufacturer, &later.manufacturer);
        fill(&mut self.product, &later.product);
        fill(&mut self.product_name, &later.product_name);
        fill(&mut self.serial_number, &later.serial_number);
        fill(&mut self.device_type, &later.device_type);
        fill(&mut self.source_type, &later.source_type);
        self.last_seen = self.last_seen.max(later.last_seen);
    }
}

/// Collapse re-emitted DeviceInfo messages into one entry per channel.
///
/// Devices re-emit DeviceInfo (at the start and end of each session), and a
/// later emission may carry fields an earlier one lacked, such as the ANT+
/// type. A message is a re-emission of an existing entry when it has the same
/// `device_index` and no identity field both carry disagrees; it then fills
/// that entry's gaps. Anything else is a new entry, even under a reused index:
/// Wahoo head units hand an index to a different sensor mid-ride. Entries
/// without an index are never merged.
///
/// An entry is a channel, not a physical device: an HRM-Pro is several entries
/// (heart rate, running dynamics, …) sharing one serial. Attribution works per
/// channel; the Python layer folds channels that share a serial.
fn dedup_devices(devices: &[DeviceMeta]) -> Vec<DeviceMeta> {
    let mut entries: Vec<DeviceMeta> = Vec::new();
    for d in devices {
        let existing = d.device_index.and_then(|_| {
            entries
                .iter_mut()
                .rev()
                .find(|e| e.device_index == d.device_index && e.agrees_with(d))
        });
        match existing {
            Some(entry) => entry.absorb(d),
            None => entries.push(d.clone()),
        }
    }
    entries
}

/// Each session's device entries: the file's DeviceInfo messages split per
/// session, then deduplicated. The full parse and the metadata scan both use
/// this, so `load_fit` and `open_fit` list the same devices.
fn devices_per_session(all: &[DeviceMeta], n_sessions: usize) -> Vec<Vec<DeviceMeta>> {
    if n_sessions <= 1 {
        return vec![dedup_devices(all)];
    }
    split_devices_per_session(all, n_sessions).iter().map(|g| dedup_devices(g)).collect()
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
        let cleaned = clean_rowing_rests(&mut parsed.records, parsed.sessions.first());
        let pool_length = parsed.sessions.first().and_then(|s| s.pool_length);
        let (batch, reconstruction) = build_batch(
            &parsed.records, &parsed.extra_col_info, &parsed.extra_data, &parsed.laps,
            &parsed.lengths, pool_length,
        )?;
        let metrics = detect_metrics(&batch);
        let winners = dev_column_winners(&parsed.records, dev_won);
        let deduped = devices_per_session(&parsed.devices, 1).remove(0);
        let (devices, sensors) = attribute_devices(
            &deduped, &parsed.developer_sensors, dev_won, &metrics,
            &winners, &parsed.apps, is_on_foot(parsed.sessions.first()),
        );

        activities.append(build_activity_dict(
            py,
            &batch,
            parsed.sessions.first(),
            &devices,
            &sensors,
            &ColumnNotes { metrics: &metrics, reconstructed: &reconstruction.columns, cleaned: &cleaned },
            &SummaryInputs { end: None, running: parsed.running.as_deref(), pool_distance: reconstruction.distance },
        )?)?;
    } else {
        // Multi-session: slice records per session, merge each independently.
        // The full batch supplies the *dynamic extra* columns only; it is built
        // without Length data (pool_length is per-session, and length/swim_stroke
        // come from each session_batch so their indices reset per session).
        let (batch, _) = build_batch(
            &parsed.records, &parsed.extra_col_info, &parsed.extra_data, &parsed.laps,
            &[], None,
        )?;
        let device_groups = devices_per_session(&parsed.devices, parsed.sessions.len());

        // Assign records to sessions by carry-forward on `start_time` (field 2),
        // NOT the session `timestamp`/end field (253): some devices pin field 253
        // to a constant, which collapses the old time-window slicing (see plan 031
        // and docs/FIT-FORMAT.md). FIT sessions are sequential and non-overlapping,
        // and records are ascending by timestamp, so each session owns the
        // contiguous range [start_i, start_{i+1}); session 0 also captures any
        // pre-roll. A missing start_time maps to i64::MAX so that session stays
        // empty rather than greedily capturing earlier records.
        let n_sessions = parsed.sessions.len();
        let starts: Vec<i64> = parsed
            .sessions
            .iter()
            .map(|s| s.start_timestamp_us.unwrap_or(i64::MAX))
            .collect();

        // The cumulative distance each session inherits: the last distance
        // recorded before it starts. Read up front, from unmodified records —
        // rebasing session i would otherwise change what session i+1 sees.
        let carried: Vec<Option<f64>> = starts
            .iter()
            .enumerate()
            .map(|(si, &start)| {
                if si == 0 {
                    return None;
                }
                let first = parsed.records.partition_point(|r| r.timestamp.unwrap_or(0) < start);
                parsed.records[..first].iter().rev().find_map(|r| r.distance)
            })
            .collect();

        for (si, session) in parsed.sessions.iter().enumerate() {
            let window_start = if si == 0 { i64::MIN } else { starts[si] };
            let window_end = if si + 1 == n_sessions { i64::MAX } else { starts[si + 1] };
            let first = parsed
                .records
                .partition_point(|r| r.timestamp.unwrap_or(0) < window_start);
            let last = parsed
                .records
                .partition_point(|r| r.timestamp.unwrap_or(0) < window_end);
            let len = last.saturating_sub(first);

            // Filter laps to this session's window and re-index from 0.
            let session_laps: Vec<LapBoundary> = parsed.laps
                .iter()
                .filter(|l| l.start_time_us >= window_start && l.start_time_us < window_end)
                .map(|l| LapBoundary {
                    start_time_us: l.start_time_us,
                    end_time_us: l.end_time_us,
                    trigger: l.trigger.clone(),
                })
                .collect();

            // Filter Length intervals to this session's window and re-index from 0.
            let session_lengths: Vec<LengthInterval> = parsed.lengths
                .iter()
                .filter(|l| l.start_time_us >= window_start && l.start_time_us < window_end)
                .cloned()
                .collect();

            // Resolve merge on a mutable slice of this session's records.
            let session_records = &mut parsed.records[first..first + len];
            let dev_won = resolve_merge(session_records);
            rebase_session_distance(session_records, carried[si]);
            let cleaned = clean_rowing_rests(session_records, Some(session));

            // Rebuild fixed columns (+ canonical extras, incl. pool-swim
            // reconstruction) from the now-resolved records.  Dynamic extras come
            // from the pre-built batch via slice.
            let (session_batch, reconstruction) = build_batch(
                session_records, &[], &[], &session_laps,
                &session_lengths, session.pool_length,
            )?;
            let full_batch = merge_fixed_with_extras(&session_batch, &batch.slice(first, len))?;

            let metrics = detect_metrics(&full_batch);
            let winners = dev_column_winners(session_records, dev_won);
            let deduped = &device_groups[si];
            let (devices, sensors) = attribute_devices(
                deduped, &parsed.developer_sensors, dev_won, &metrics,
                &winners, &parsed.apps, is_on_foot(Some(session)),
            );

            activities.append(build_activity_dict(
                py,
                &full_batch,
                Some(session),
                &devices,
                &sensors,
                &ColumnNotes { metrics: &metrics, reconstructed: &reconstruction.columns, cleaned: &cleaned },
                &SummaryInputs {
                    end: (si + 1 < n_sessions).then_some(window_end),
                    running: parsed.running.as_deref(),
                    pool_distance: reconstruction.distance,
                },
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
    let device_groups = devices_per_session(&scan.devices, scan.sessions.len());

    let build_one = |session: Option<&SessionMeta>, devices: &[DeviceMeta]| -> PyResult<()> {
        let meta = match session {
            Some(s) => session_to_dict(py, s)?,
            None => {
                let d = PyDict::new_bound(py);
                for key in ["sport", "sub_sport", "name", "start_time", "start_time_local"] {
                    d.set_item(key, py.None())?;
                }
                d
            }
        };
        let metrics_list = PyList::empty_bound(py);
        for m in &scan.record_metrics { metrics_list.append(m)?; }
        meta.set_item("metrics", metrics_list)?;
        let devices_list = PyList::empty_bound(py);
        for d in devices { devices_list.append(device_to_dict(py, d)?)?; }
        meta.set_item("devices", devices_list)?;
        let sensors_list = PyList::empty_bound(py);
        for s in &scan.developer_sensors { sensors_list.append(sensor_to_dict(py, s)?)?; }
        meta.set_item("developer_sensors", sensors_list)?;
        let activity = PyDict::new_bound(py);
        activity.set_item("metadata", meta)?;
        activities.append(activity)
    };

    if scan.sessions.is_empty() {
        build_one(None, &device_groups[0])?;
    } else {
        for (session, devices) in scan.sessions.iter().zip(&device_groups) {
            build_one(Some(session), devices)?;
        }
    }

    result.set_item("activities", activities)?;
    Ok(result.into_any().unbind())
}

// ═══════════════════════════════════════════════════════════════════════════
// Python-exposed functions
// ═══════════════════════════════════════════════════════════════════════════

fn do_parse(py: Python<'_>, data: &[u8], columns: Option<Vec<String>>) -> PyResult<PyObject> {
    let config = fit::decode::ParseConfig { columns };
    // Decoding touches no Python object: release the GIL so that threads
    // (`scan_fit`, `load_fit_batch`) parse files in parallel.
    let parsed = py
        .allow_threads(|| fit::decode::full_parse(data, &config))
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
    let result = py
        .allow_threads(|| fit::decode::scan_metadata(&data))
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

/// The computed summary fields of a shaped record table (see
/// `summary::summarize`), as a dict holding the fields that have a value.
#[pyfunction]
#[pyo3(signature = (table, start=None, end=None, running=None, pool_distance=None))]
fn summarize(
    py: Python<'_>,
    table: pyo3_arrow::PyTable,
    start: Option<i64>,
    end: Option<i64>,
    running: Option<Vec<summary::Interval>>,
    pool_distance: Option<f64>,
) -> PyResult<PyObject> {
    let (batches, schema) = table.into_inner();
    let batch = summary::single_batch(&schema, &batches)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    let values = summary::summarize(&batch, start, end, running.as_deref(), pool_distance)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    let dict = PyDict::new_bound(py);
    for (name, value) in values {
        set_value(&dict, name, value)?;
    }
    Ok(dict.into_any().unbind())
}

/// The summary fields as `(name, kind, source)`, in declaration order (see
/// `summary::SUMMARY_FIELDS`).
#[pyfunction]
fn summary_fields() -> Vec<(&'static str, &'static str, &'static str)> {
    summary::SUMMARY_FIELDS.iter().map(|f| (f.name, f.kind.as_str(), f.source)).collect()
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(summarize, m)?)?;
    m.add_function(wrap_pyfunction!(summary_fields, m)?)?;
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

    // ── Pool-swim length reconstruction ──────────────────────────────────

    mod length_reconstruction {
        use super::*;

        /// Record at `t` seconds (timestamps are stored in microseconds).
        fn rec(t: i64) -> RecordRow {
            RecordRow { timestamp: Some(t * 1_000_000), ..Default::default() }
        }

        fn active(start_s: i64, dur_s: f64) -> LengthInterval {
            LengthInterval {
                start_time_us: start_s * 1_000_000,
                duration_s: dur_s,
                active: true,
                cadence: Some(30),
                swim_stroke: Some("freestyle".into()),
            }
        }

        fn idle(start_s: i64, dur_s: f64) -> LengthInterval {
            LengthInterval {
                start_time_us: start_s * 1_000_000,
                duration_s: dur_s,
                active: false,
                cadence: None,
                swim_stroke: None,
            }
        }

        #[test]
        fn distance_ramps_and_reconciles_exactly() {
            // Two 25 m active lengths, 20 s each. Distance ramps 0→25→50 and its
            // maximum equals num_active × pool exactly.
            let lengths = vec![active(0, 20.0), active(20, 20.0)];
            let records = vec![rec(0), rec(10), rec(20), rec(30), rec(40)];
            let c = assign_lengths(&records, &lengths, Some(25.0));
            assert_eq!(
                c.distance,
                vec![Some(0.0), Some(12.5), Some(25.0), Some(37.5), Some(50.0)]
            );
            assert_eq!(c.index, vec![0, 0, 1, 1, 1]);
            // speed = pool / duration = 25 / 20 = 1.25 m/s on every active row.
            assert!(c.speed.iter().all(|s| *s == Some(1.25)));
            assert_eq!(c.cadence, vec![Some(30); 5]);
        }

        #[test]
        fn distance_is_monotone_non_decreasing() {
            let lengths = vec![active(0, 20.0), idle(20, 10.0), active(30, 20.0)];
            let records: Vec<_> = (0..=50).map(rec).collect();
            let c = assign_lengths(&records, &lengths, Some(25.0));
            let vals: Vec<f64> = c.distance.iter().map(|d| d.unwrap()).collect();
            assert!(vals.windows(2).all(|w| w[0] <= w[1]));
            assert!(vals.iter().all(|v| v.is_finite()));
        }

        #[test]
        fn non_25_pool_scales() {
            // Same shape, 50 m pool → distances double, total is 100 m.
            let lengths = vec![active(0, 20.0), active(20, 20.0)];
            let records = vec![rec(0), rec(10), rec(40)];
            let c = assign_lengths(&records, &lengths, Some(50.0));
            assert_eq!(c.distance, vec![Some(0.0), Some(25.0), Some(100.0)]);
        }

        #[test]
        fn idle_length_is_a_stop() {
            let lengths = vec![active(0, 20.0), idle(20, 10.0), active(30, 20.0)];
            let c = assign_lengths(&[rec(25)], &lengths, Some(25.0));
            // Mid-rest: distance sits at the 25 m already swum, speed and
            // cadence are 0, no stroke.
            assert_eq!(c.distance, vec![Some(25.0)]);
            assert_eq!(c.speed, vec![Some(0.0)]);
            assert_eq!(c.cadence, vec![Some(0)]);
            assert_eq!(c.stroke, vec![None]);
            assert_eq!(c.index, vec![1]);
        }

        #[test]
        fn zero_duration_active_length_ramp_guard() {
            // A corrupt zero-elapsed active length must not divide by zero; it
            // attributes the whole pool length at once, pace unknown, no NaN.
            let lengths = vec![active(0, 20.0), active(20, 0.0), active(21, 20.0)];
            let c = assign_lengths(&[rec(20), rec(21), rec(41)], &lengths, Some(25.0));
            assert_eq!(c.distance[0], Some(50.0)); // 25 (first) + 25 (guarded)
            assert_eq!(c.speed[0], None);
            assert!(c.distance.iter().all(|d| d.unwrap().is_finite()));
        }

        #[test]
        fn huge_idle_elapsed_does_not_affect_assignment() {
            // A paused rest encoded as an idle length with an absurd elapsed time
            // (~131 071 s seen in real files). Assignment is by start_time
            // carry-forward, so the bogus duration is irrelevant.
            let lengths = vec![active(0, 20.0), idle(20, 131_071.0), active(300, 20.0)];
            let c = assign_lengths(&[rec(20), rec(100), rec(300)], &lengths, Some(25.0));
            assert_eq!(c.index, vec![1, 1, 2]); // rec(100) carries forward into idle
            assert_eq!(c.distance, vec![Some(25.0), Some(25.0), Some(25.0)]);
            assert!(c.distance.iter().all(|d| d.unwrap().is_finite()));
        }

        #[test]
        fn all_idle_degenerate_swim_is_zero_distance() {
            // An aborted swim: only idle lengths, no distance, must not panic.
            let lengths = vec![idle(0, 10.0), idle(10, 10.0)];
            let c = assign_lengths(&[rec(0), rec(5), rec(15)], &lengths, Some(25.0));
            assert_eq!(c.distance, vec![Some(0.0), Some(0.0), Some(0.0)]);
            // No length was swum, so speed and cadence were never recorded:
            // no column of zeros.
            assert!(c.speed.iter().all(|s| s.is_none()));
            assert!(c.cadence.iter().all(|s| s.is_none()));
        }

        #[test]
        fn missing_pool_length_yields_no_distance_but_keeps_structure() {
            // Without an anchor we never fabricate distance or speed, not even
            // 0 at rest (speed is not recorded), but the length index, cadence
            // and stroke are still surfaced.
            let lengths = vec![active(0, 20.0), idle(20, 10.0)];
            let c = assign_lengths(&[rec(0), rec(10), rec(25)], &lengths, None);
            assert!(c.distance.iter().all(|d| d.is_none()));
            assert!(c.speed.iter().all(|s| s.is_none()));
            assert_eq!(c.index, vec![0, 0, 1]);
            assert_eq!(c.cadence, vec![Some(30), Some(30), Some(0)]);
            assert_eq!(c.stroke, vec![Some("freestyle".into()), Some("freestyle".into()), None]);
        }

        #[test]
        fn a_rest_is_not_zero_in_a_column_the_swim_does_not_record() {
            let mut swum = active(0, 20.0);
            swum.cadence = None; // no cadence on any length
            let c = assign_lengths(&[rec(25)], &[swum, idle(20, 10.0)], Some(25.0));
            assert_eq!(c.speed, vec![Some(0.0)]);
            assert_eq!(c.cadence, vec![None]);
        }

        #[test]
        fn missing_optional_fields_handled() {
            let mut length = active(0, 20.0);
            length.cadence = None;
            length.swim_stroke = None;
            let c = assign_lengths(&[rec(0)], &[length], Some(25.0));
            assert_eq!(c.cadence, vec![None]);
            assert_eq!(c.stroke, vec![None]);
            assert_eq!(c.distance, vec![Some(0.0)]); // distance still reconstructed
        }

        #[test]
        fn records_before_first_length_are_warmup() {
            // Standing at the wall before the first stroke: length 0, distance 0,
            // no pace or stroke.
            let lengths = vec![active(10, 20.0)];
            let c = assign_lengths(&[rec(0), rec(10)], &lengths, Some(25.0));
            assert_eq!(c.index, vec![0, 0]);
            assert_eq!(c.distance, vec![Some(0.0), Some(0.0)]);
            assert_eq!(c.speed, vec![None, Some(1.25)]);
            assert_eq!(c.stroke, vec![None, Some("freestyle".into())]);
        }
    }

    // ── Per-session distance rebasing ────────────────────────────────────

    mod distance_rebasing {
        use super::*;

        fn rows(distances: &[Option<f64>]) -> Vec<RecordRow> {
            distances
                .iter()
                .map(|&distance| RecordRow { distance, ..Default::default() })
                .collect()
        }

        fn distances(records: &[RecordRow]) -> Vec<Option<f64>> {
            records.iter().map(|r| r.distance).collect()
        }

        #[test]
        fn carried_over_total_is_rebased_to_zero() {
            let mut records = rows(&[None, Some(8455.5), Some(8456.73), Some(8839.8)]);
            rebase_session_distance(&mut records, Some(8455.5));
            assert_eq!(distances(&records), vec![None, Some(0.0), Some(1.23), Some(384.3)]);
        }

        #[test]
        fn rebased_value_matches_a_decoded_centimetre_value() {
            // 8456.73 - 8455.5 is 1.2300000000004729 in floating point; the
            // result must equal the decoder's own 123 / 100.
            let mut records = rows(&[Some(8456.73)]);
            rebase_session_distance(&mut records, Some(8455.5));
            assert_eq!(records[0].distance, Some(123.0 / 100.0));
        }

        #[test]
        fn device_reset_is_left_alone() {
            let mut records = rows(&[Some(2.35), Some(10.0)]);
            rebase_session_distance(&mut records, Some(5106.0));
            assert_eq!(distances(&records), vec![Some(2.35), Some(10.0)]);
        }

        #[test]
        fn nothing_carried_is_left_alone() {
            let mut records = rows(&[Some(2.35)]);
            rebase_session_distance(&mut records, None);
            rebase_session_distance(&mut records, Some(0.0));
            assert_eq!(distances(&records), vec![Some(2.35)]);
        }

        #[test]
        fn session_without_distance_is_left_alone() {
            let mut records = rows(&[None, None]);
            rebase_session_distance(&mut records, Some(100.0));
            assert_eq!(distances(&records), vec![None, None]);
        }
    }

    // ── Indoor-rowing rests ──────────────────────────────────────────────

    mod rowing_rests {
        use super::*;

        fn session(sub_sport: Option<&str>) -> SessionMeta {
            SessionMeta { sub_sport: sub_sport.map(String::from), ..Default::default() }
        }

        fn rowing() -> Option<SessionMeta> {
            Some(session(Some("indoor_rowing")))
        }

        /// One record per second: (distance, power); cadence 24, no speed.
        fn rows(samples: &[(Option<f64>, Option<i16>)]) -> Vec<RecordRow> {
            samples
                .iter()
                .enumerate()
                .map(|(i, &(distance, power))| RecordRow {
                    timestamp: Some(i as i64 * 1_000_000),
                    distance,
                    power,
                    cadence: Some(24),
                    ..Default::default()
                })
                .collect()
        }

        fn powers(records: &[RecordRow]) -> Vec<Option<i16>> {
            records.iter().map(|r| r.power).collect()
        }

        /// 0 m, 10 m, then 10 m (and 10.01 m, within 1 cm) for four records, then 20 m.
        fn with_rest(rest_power: [Option<i16>; 4]) -> Vec<RecordRow> {
            let [a, b, c, d] = rest_power;
            rows(&[
                (Some(0.0), Some(150)), (Some(10.0), Some(200)),
                (Some(10.0), a), (Some(10.01), b), (Some(10.01), c), (Some(10.01), d),
                (Some(20.0), Some(210)),
            ])
        }

        #[test]
        fn a_rest_is_a_stop() {
            let held = Some(200);
            let mut records = with_rest([held; 4]);
            let changed = clean_rowing_rests(&mut records, rowing().as_ref());
            let zero = Some(0);
            assert_eq!(powers(&records), vec![Some(150), held, zero, zero, zero, zero, Some(210)]);
            assert_eq!(records[2].cadence, Some(0));
            assert_eq!(records[1].cadence, Some(24));
            // No speed was recorded: it stays null, and is not listed.
            assert!(records.iter().all(|r| r.speed.is_none()));
            assert_eq!(changed, vec!["power", "cadence"]);
        }

        #[test]
        fn a_null_on_a_rest_is_zero_too() {
            let mut records = with_rest([Some(0), None, Some(0), Some(0)]);
            let changed = clean_rowing_rests(&mut records, rowing().as_ref());
            assert_eq!(records[3].power, Some(0));
            assert!(changed.contains(&"power".to_string()));
        }

        #[test]
        fn a_rest_already_at_zero_changes_nothing_in_that_column() {
            let mut records = with_rest([Some(0); 4]);
            for r in &mut records[2..6] {
                r.cadence = Some(0);
            }
            assert!(clean_rowing_rests(&mut records, rowing().as_ref()).is_empty());
        }

        #[test]
        fn three_still_records_are_not_a_rest() {
            let d = |m: f64| Some(m);
            let p = Some(200);
            let mut records = rows(&[(d(0.0), p), (d(10.0), p), (d(10.0), p), (d(10.0), p), (d(10.0), p), (d(20.0), p)]);
            assert!(clean_rowing_rests(&mut records, rowing().as_ref()).is_empty());
            assert!(records.iter().all(|r| r.power == p));
        }

        #[test]
        fn a_record_without_distance_ends_a_run() {
            let p = Some(1);
            let mut records = rows(&[
                (Some(5.0), p), (Some(5.0), p), (Some(5.0), p), (None, p),
                (Some(5.0), p), (Some(5.0), p), (Some(5.0), p),
            ]);
            assert!(clean_rowing_rests(&mut records, rowing().as_ref()).is_empty());
        }

        #[test]
        fn runs_follow_time_not_file_order() {
            let d = |m: f64| Some(m);
            let p = Some(2);
            let mut records = rows(&[(d(0.0), Some(1)), (d(10.0), p), (d(10.0), p), (d(10.0), p), (d(10.0), p), (d(10.0), p)]);
            // A late-written record from before the rest: distance 5 m at t = 0.5 s.
            records.push(RecordRow { timestamp: Some(500_000), distance: d(5.0), power: Some(9), ..Default::default() });
            clean_rowing_rests(&mut records, rowing().as_ref());
            let zero = Some(0);
            assert_eq!(powers(&records), vec![Some(1), p, zero, zero, zero, zero, Some(9)]);
        }

        #[test]
        fn only_indoor_rowing_with_a_distance_signal() {
            let still = [(Some(10.0), Some(200)); 6];
            for s in [None, Some(session(None)), Some(session(Some("indoor_cycling")))] {
                let mut records = rows(&still);
                assert!(clean_rowing_rests(&mut records, s.as_ref()).is_empty());
            }
            // The Concept2 data field on a watch whose native distance is 0.
            let mut records = rows(&[(Some(0.0), Some(200)); 6]);
            assert!(clean_rowing_rests(&mut records, rowing().as_ref()).is_empty());
        }
    }

    // ── Device deduplication ─────────────────────────────────────────────

    mod device_dedup {
        use super::*;

        // FIT profile values.
        const GARMIN: u16 = 1;
        const WAHOO: u16 = 32;
        const FAVERO: u16 = 263;
        const FENIX6: u16 = 3290;
        const ASSIOMA_DUO: u16 = 12;

        fn make_device(idx: Option<u8>, mfr: Option<u16>, product: Option<u16>) -> DeviceMeta {
            DeviceMeta { device_index: idx, manufacturer: mfr, product, ..Default::default() }
        }

        #[test]
        fn no_duplicates_unchanged() {
            let devices = vec![
                make_device(Some(0), Some(GARMIN), Some(FENIX6)),
                make_device(Some(1), Some(WAHOO), Some(7)),
            ];
            let result = dedup_devices(&devices);
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].manufacturer, Some(GARMIN));
            assert_eq!(result[1].manufacturer, Some(WAHOO));
        }

        #[test]
        fn duplicate_index_merges_fills_none() {
            let devices = vec![
                make_device(Some(1), Some(WAHOO), None), // first emission: no product
                make_device(Some(1), None, Some(7)), // second emission: has product
            ];
            let result = dedup_devices(&devices);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].manufacturer, Some(WAHOO));
            assert_eq!(result[0].product, Some(7));
        }

        fn with_serial(mut d: DeviceMeta, serial: &str, last_seen: usize) -> DeviceMeta {
            d.serial_number = Some(serial.into());
            d.last_seen = last_seen;
            d
        }

        #[test]
        fn reused_index_with_another_identity_is_a_new_entry() {
            // Wahoo: index 2 is an Assioma, then a TICKR.
            let devices = vec![
                make_device(Some(2), Some(FAVERO), Some(ASSIOMA_DUO)),
                make_device(Some(2), Some(WAHOO), None),
            ];
            let result = dedup_devices(&devices);
            assert_eq!(result.len(), 2);
            assert_eq!(result[1].manufacturer, Some(WAHOO));
        }

        #[test]
        fn returning_identity_merges_into_its_own_entry() {
            // Index 2: serial A, then serial B, then serial A again.
            let assioma = || make_device(Some(2), Some(FAVERO), None);
            let devices = vec![
                with_serial(assioma(), "1", 0),
                with_serial(assioma(), "2", 1),
                with_serial(assioma(), "1", 2),
            ];
            let result = dedup_devices(&devices);
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].serial_number.as_deref(), Some("1"));
            assert_eq!(result[0].last_seen, 2);
            assert_eq!(result[1].last_seen, 1);
        }

        #[test]
        fn product_key_resolves_after_a_sparse_re_emission() {
            // The product arrives in an emission without the manufacturer.
            let devices = vec![make_device(Some(2), Some(FAVERO), None), make_device(Some(2), None, Some(ASSIOMA_DUO))];
            let result = dedup_devices(&devices);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].product_key().as_deref(), Some("assioma_duo"));
            // No subfield applies without a manufacturer: the number is the key.
            assert_eq!(make_device(Some(2), None, Some(ASSIOMA_DUO)).product_key().as_deref(), Some("12"));
        }

        #[test]
        fn sensor_type_follows_source_type() {
            let channel = |source_type: Option<u8>, device_type: u8| DeviceMeta {
                source_type,
                device_type: Some(device_type),
                ..Default::default()
            };
            assert_eq!(profile::source_type_name(SOURCE_TYPE_ANTPLUS), "antplus");
            assert_eq!(profile::source_type_name(SOURCE_TYPE_BLE), "bluetooth_low_energy");
            assert_eq!(channel(Some(1), 120).sensor_type(), Some("heart_rate")); // ANT+
            assert_eq!(channel(Some(3), 1).sensor_type(), Some("heart_rate")); // BLE
            assert_eq!(channel(Some(3), 120).sensor_type(), Some("heart_rate")); // Wahoo: ANT+ type on BLE
            assert_eq!(channel(Some(5), 11).sensor_type(), None); // local: never read as ANT+
            assert_eq!(channel(Some(5), 1).sensor_type(), Some("glonass")); // local
            assert_eq!(channel(None, 120).sensor_type(), Some("heart_rate")); // pre-source_type
            assert_eq!(channel(Some(0), 120).sensor_type(), None); // plain ANT: a number
        }

        #[test]
        fn sensor_types_are_profile_names() {
            let names: BTreeSet<&str> = (0..=u8::MAX)
                .flat_map(|v| [profile::antplus_device_type_name(v), profile::ble_device_type_name(v)])
                .collect();
            for t in COLUMN_ATTRIBUTIONS.iter().flat_map(|c| c.sensor_types).chain(FOOT_POD_TYPES) {
                assert!(names.contains(t), "{t} is not a FIT device type");
            }
        }

        #[test]
        fn a_foot_pod_measures_only_on_foot() {
            let channel = |device_type: u8, last_seen: usize| DeviceMeta {
                device_index: Some(1),
                device_type: Some(device_type),
                last_seen,
                ..Default::default()
            };
            let creator = DeviceMeta { device_index: Some(0), ..Default::default() };
            let devices = [creator, channel(124, 1)]; // an HRM-Pro running channel
            let attributed = |metrics: &[&str], on_foot: bool| {
                let metrics: Vec<String> = metrics.iter().map(|m| m.to_string()).collect();
                let (devices, _) = attribute_devices(&devices, &[], 0, &metrics, &[], &[], on_foot);
                devices[1].columns.clone()
            };
            // A run: cadence always, speed and distance only without GPS.
            assert_eq!(attributed(&["cadence", "speed", "distance", "gps"], true), vec!["cadence"]);
            assert_eq!(attributed(&["cadence", "speed", "distance"], true), vec!["cadence", "speed", "distance"]);
            // A ride: the channel is registered, but nothing came from it.
            assert!(attributed(&["cadence", "speed", "distance"], false).is_empty());
        }

        #[test]
        fn known_manufacturers_are_profile_names() {
            let names: BTreeSet<&str> = (0..=u16::MAX).map(profile::manufacturer_name).collect();
            for col in COLUMN_ATTRIBUTIONS {
                for m in col.known_manufacturers {
                    assert!(names.contains(m), "{}: {m} is not a FIT manufacturer", col.name);
                }
            }
        }

        #[test]
        fn a_dedicated_sensor_is_not_matched_by_manufacturer() {
            // A Wahoo TICKR (heart rate) next to a Favero power meter.
            let mut tickr = make_device(Some(3), Some(WAHOO), None);
            tickr.device_type = Some(120);
            tickr.last_seen = 1;
            let mut assioma = make_device(Some(2), Some(FAVERO), Some(ASSIOMA_DUO));
            assioma.device_type = Some(11);
            let mut kickr = make_device(Some(4), Some(WAHOO), None);
            kickr.device_type = Some(17); // fitness_equipment
            let devices = vec![assioma.clone(), tickr.clone()];
            assert_eq!(find_device_by_manufacturer(&devices, &["wahoo_fitness"]), None);
            let devices = vec![assioma, tickr, kickr];
            assert_eq!(find_device_by_manufacturer(&devices, &["wahoo_fitness"]), Some(2));
        }

        #[test]
        fn attribution_prefers_the_device_emitted_last() {
            let power = |serial: &str, idx: u8, last_seen: usize| {
                let mut d = with_serial(make_device(Some(idx), Some(FAVERO), None), serial, last_seen);
                d.device_type = Some(11); // bike_power
                d
            };
            // The later emission sits under the lower index.
            let devices = vec![power("1", 3, 0), power("2", 2, 1)];
            assert_eq!(find_device_by_sensor_type(&devices, &["bike_power"], false), Some(1));
        }

        #[test]
        fn none_index_devices_not_merged() {
            let devices = vec![
                make_device(None, Some(1), None),
                make_device(None, Some(2), None),
            ];
            let result = dedup_devices(&devices);
            assert_eq!(result.len(), 2);
        }

        #[test]
        fn device_type_merged() {
            let d1 = make_device(Some(1), Some(WAHOO), None);
            let mut d2 = make_device(Some(1), None, None);
            d2.device_type = Some(11); // bike_power
            let result = dedup_devices(&[d1, d2]);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].sensor_type(), Some("bike_power"));
        }
    }

    // ── Developer sensor attribution ─────────────────────────────────────

    mod sensor_attribution {
        use super::*;

        fn sensor(uuid: &str, columns: &[&str]) -> DeveloperSensor {
            DeveloperSensor {
                manufacturer: None,
                product_name: uuid.into(),
                uuid: uuid.into(),
                columns: columns.iter().map(|c| c.to_string()).collect(),
            }
        }

        fn strings(items: &[&str]) -> Vec<String> {
            items.iter().map(|s| s.to_string()).collect()
        }

        #[test]
        fn sensor_without_readings_is_dropped() {
            // Fields declared in the Record definition but never written with
            // a reading: the column exists in the schema, not in `metrics`.
            let (_, sensors) = attribute_devices(
                &[], &[sensor("c2", &["stroke_rate"])], 0, &[], &[], &[], false,
            );
            assert!(sensors.is_empty());
        }

        #[test]
        fn sensor_is_credited_only_with_columns_it_read() {
            // drag_factor is present and non-null but all zeros: not a metric.
            let (_, sensors) = attribute_devices(
                &[],
                &[sensor("c2", &["stroke_rate", "drag_factor"])],
                0,
                &strings(&["stroke_rate"]),
                &[],
                &[],
                false,
            );
            assert_eq!(sensors.len(), 1);
            assert_eq!(sensors[0].columns, vec!["stroke_rate".to_string()]);
        }

        #[test]
        fn shared_field_name_goes_to_the_app_that_supplied_the_readings() {
            // Both apps registered `Power`; slot 0 (stryd) supplied the readings.
            let apps = strings(&["stryd", "c2"]);
            let (_, sensors) = attribute_devices(
                &[],
                &[sensor("stryd", &["power"]), sensor("c2", &["power"])],
                1, // developer power won the merge
                &strings(&["power"]),
                &[("power", 0)],
                &apps,
                false,
            );
            assert_eq!(sensors.len(), 1);
            assert_eq!(sensors[0].uuid, "stryd");
            assert_eq!(sensors[0].columns, vec!["power".to_string()]);
        }

        #[test]
        fn winner_is_the_app_with_most_readings() {
            let row = |power: i16, app: u8| RecordRow {
                dev_power: Some(power),
                dev_power_app: Some(app),
                ..Default::default()
            };
            let records = vec![row(0, 1), row(250, 0), row(251, 0), row(0, 1)];
            assert_eq!(dev_column_winners(&records, 1), vec![("power", 0)]);
            // Standard power won the merge: power is not developer-sourced.
            assert!(dev_column_winners(&records, 0).is_empty());
        }

        #[test]
        fn metrics_are_columns_with_a_reading() {
            let schema = Schema::new(vec![
                Field::new("timestamp", DataType::Int64, true),
                Field::new("heart_rate", DataType::Int16, true),
                Field::new("power", DataType::Int16, true),
                Field::new("latitude", DataType::Float64, true),
                Field::new("longitude", DataType::Float64, true),
                Field::new("lap", DataType::Int16, true),
                Field::new("smo2", DataType::Float32, true),
            ]);
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(Int16Array::from(vec![Some(120), None])),
                    Arc::new(Int16Array::from(vec![Some(0), Some(0)])), // idle sensor
                    Arc::new(Float64Array::from(vec![Some(52.1), Some(52.2)])),
                    Arc::new(Float64Array::from(vec![Some(4.3), Some(4.4)])),
                    Arc::new(Int16Array::from(vec![Some(0), Some(1)])),
                    Arc::new(Float32Array::from(vec![None::<f32>, None])),
                ],
            )
            .unwrap();
            assert_eq!(detect_metrics(&batch), vec!["heart_rate", "gps"]);
        }

        #[test]
        fn reading_means_non_null_and_non_zero() {
            assert!(!has_reading(&Int16Array::from(vec![None, Some(0), Some(0)])));
            assert!(has_reading(&Int16Array::from(vec![None, Some(0), Some(24)])));
            assert!(!has_reading(&Float64Array::from(vec![Some(0.0), None])));
            assert!(has_reading(&Float64Array::from(vec![Some(0.5)])));
            assert!(!has_reading(&StringArray::from(vec![None::<&str>, Some("")])));
            assert!(has_reading(&StringArray::from(vec![Some("freestyle")])));
        }
    }
}
