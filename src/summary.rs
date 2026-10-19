//! The session summary: its fields, and the kernel that computes the computed
//! ones from a record table. See `plans/037-SESSION-METADATA.md` and the
//! README's "Summary" section.
//!
//! [`SUMMARY_FIELDS`] is the single source of truth for the summary: the
//! kernel, the session decoder (`decode_session_summary`), Python's
//! `SessionSummary` and the Parquet `columns=` rule all derive from it.

use arrow::array::{Array, AsArray};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, Float64Type, Int64Type, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

/// Where a summary field's value comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// Computed by [`summarize`] from the record column named by `source`.
    Computed,
    /// The device's value: the Session field named by `source`.
    Device,
    /// Entered by the athlete on the device: the Session field named by `source`.
    Entered,
}

impl Kind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Kind::Computed => "computed",
            Kind::Device => "device",
            Kind::Entered => "entered",
        }
    }
}

/// A summary value's type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ty {
    Int,
    Float,
}

pub(crate) struct SummaryField {
    pub(crate) name: &'static str,
    pub(crate) kind: Kind,
    /// A record column for computed fields (`timestamp` for the durations);
    /// a Session field's profile name otherwise.
    pub(crate) source: &'static str,
    pub(crate) ty: Ty,
    /// Applied after the profile's own scale and offset. Only `rpe` uses it:
    /// the profile gives `workout_rpe` no scale but describes it as "Common
    /// Borg CR10 / 0-10 RPE scale, multiplied 10x".
    pub(crate) divisor: f64,
}

const fn field(name: &'static str, kind: Kind, source: &'static str, ty: Ty) -> SummaryField {
    SummaryField { name, kind, source, ty, divisor: 1.0 }
}

use Kind::{Computed, Device, Entered};
use Ty::{Float, Int};

/// Every summary field, in the order Python's `SessionSummary` declares them.
pub(crate) const SUMMARY_FIELDS: &[SummaryField] = &[
    field("duration", Computed, "timestamp", Float),
    field("timer_time", Computed, "timestamp", Float),
    field("distance", Computed, "distance", Float),
    field("avg_heart_rate", Computed, "heart_rate", Float),
    field("max_heart_rate", Computed, "heart_rate", Int),
    field("avg_power", Computed, "power", Float),
    field("max_power", Computed, "power", Int),
    field("avg_cadence", Computed, "cadence", Float),
    field("max_cadence", Computed, "cadence", Int),
    field("avg_speed", Computed, "speed", Float),
    field("max_speed", Computed, "speed", Float),
    field("avg_temperature", Computed, "temperature", Float),
    field("max_temperature", Computed, "temperature", Int),
    field("ascent", Device, "total_ascent", Int),
    field("descent", Device, "total_descent", Int),
    field("calories", Device, "total_calories", Int),
    field("training_effect", Device, "total_training_effect", Float),
    field("anaerobic_training_effect", Device, "total_anaerobic_training_effect", Float),
    field("training_load_peak", Device, "training_load_peak", Float),
    SummaryField { divisor: 10.0, ..field("rpe", Entered, "workout_rpe", Float) },
    field("feel", Entered, "workout_feel", Int),
];

/// A summary value, typed by its field's [`Ty`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Value {
    Int(i64),
    Float(f64),
}

impl Value {
    /// `v` as the field's type. Integer fields hold whole values by
    /// construction (profile scale 1, or a column maximum).
    pub(crate) fn typed(ty: Ty, v: f64) -> Value {
        match ty {
            Ty::Int => Value::Int(v.round() as i64),
            Ty::Float => Value::Float(v),
        }
    }
}

/// Running intervals, microseconds since the Unix epoch, `[start, end)`.
pub(crate) type Interval = (i64, i64);

/// Compute the computed summary fields of one session's record table.
///
/// `batch` is the table as returned to the caller before column selection:
/// sorted by `timestamp`, ties in file order (`finalize_rows`). The session
/// spans `[start, end]`: `start` is its `start_time`, `end` the next
/// session's `start_time` in a multi-session file; either defaults to the
/// first or last sample. A span known from the file gives `duration` and
/// `timer_time` without any sample (a session that is only a button press
/// between two legs of a multisport file). `running` is the file's timer intervals, `None`
/// when it has no timer events (all time counts as running);
/// `pool_distance` the active Lengths × pool length of a pool swim whose
/// distance was reconstructed.
///
/// - **Weights**: each sample counts for the running time between it and the
///   next sample, or the session's end.
/// - **Ties**: only the last row of a timestamp takes part (the row
///   `deduplicate=True` keeps), so the result does not depend on
///   `deduplicate`.
/// - **Averages** are time-weighted; zeros count, nulls are skipped.
/// - Without a usable `timestamp` column, the durations and averages are
///   `None`; maxima and `distance` are taken over all rows.
///
/// Returns `(field name, value)` for every computed field that has a value.
pub(crate) fn summarize(
    batch: &RecordBatch,
    start: Option<i64>,
    end: Option<i64>,
    running: Option<&[Interval]>,
    pool_distance: Option<f64>,
) -> Result<Vec<(&'static str, Value)>, String> {
    let timestamps = timestamp_values(batch)?;
    // Rows that take part: with time, the last row of each timestamp (null
    // timestamps cannot be placed); without, every row.
    let rows: Vec<usize> = match &timestamps {
        Some(ts) => {
            let valid: Vec<usize> = (0..ts.len()).filter(|&i| ts.is_valid(i)).collect();
            if valid.windows(2).any(|w| ts.value(w[0]) > ts.value(w[1])) {
                return Err("summarize: the table must be sorted by timestamp".into());
            }
            let mut rows: Vec<usize> = valid
                .windows(2)
                .filter(|w| ts.value(w[0]) != ts.value(w[1]))
                .map(|w| w[0])
                .collect();
            rows.extend(valid.last());
            rows
        }
        None => (0..batch.num_rows()).collect(),
    };

    let mut out = Vec::new();
    let mut put = |name: &'static str, v: Option<f64>| {
        if let Some(v) = v.filter(|v| v.is_finite()) {
            let f = SUMMARY_FIELDS.iter().find(|f| f.name == name).expect("summary field");
            out.push((name, Value::typed(f.ty, v)));
        }
    };

    // Seconds of running time each row counts for, when there is time.
    let weights: Option<Vec<f64>> = timestamps.as_ref().and_then(|ts| {
        let times: Vec<i64> = rows.iter().map(|&i| ts.value(i)).collect();
        // The session's span: the file's bounds where it has them, else the
        // samples'. A start after the last sample is not a start of these
        // samples.
        let last = match (end, times.last()) {
            (Some(e), Some(&l)) => e.max(l),
            (e, l) => e.or(l.copied())?,
        };
        let first = start.filter(|&s| s <= last).or(times.first().copied())?;
        let window = Window::new(first, last, running);
        put("duration", Some(seconds(last - first)));
        put("timer_time", running.map(|_| window.total()));
        let next = times.iter().skip(1).copied().chain(std::iter::once(last));
        Some(times.iter().zip(next).map(|(&t, n)| window.between(t, n)).collect())
    });

    for column in ["heart_rate", "power", "cadence", "speed", "temperature"] {
        let Some(values) = float_column(batch, column)? else { continue };
        let valid = || rows.iter().enumerate().filter(|(_, &i)| values.is_valid(i));
        let max = valid().map(|(_, &i)| values.value(i)).fold(None, |m: Option<f64>, v| {
            Some(m.map_or(v, |m| m.max(v)))
        });
        put(max_name(column), max);
        if let Some(weights) = &weights {
            let (sum, total) = valid().fold((0.0, 0.0), |(s, t), (j, &i)| {
                (s + values.value(i) * weights[j], t + weights[j])
            });
            put(avg_name(column), (total > 0.0).then(|| sum / total));
        }
    }

    if let Some(values) = float_column(batch, "distance")? {
        let max = rows
            .iter()
            .filter(|&&i| values.is_valid(i))
            .map(|&i| values.value(i))
            .fold(None, |m: Option<f64>, v| Some(m.map_or(v, |m| m.max(v))));
        put("distance", pool_distance.or(max));
    }

    Ok(out)
}

fn max_name(column: &str) -> &'static str {
    match column {
        "heart_rate" => "max_heart_rate",
        "power" => "max_power",
        "cadence" => "max_cadence",
        "speed" => "max_speed",
        _ => "max_temperature",
    }
}

fn avg_name(column: &str) -> &'static str {
    match column {
        "heart_rate" => "avg_heart_rate",
        "power" => "avg_power",
        "cadence" => "avg_cadence",
        "speed" => "avg_speed",
        _ => "avg_temperature",
    }
}

fn seconds(us: i64) -> f64 {
    us as f64 / 1_000_000.0
}

/// Running time inside `[first, last]`: the running intervals clipped to the
/// session, sorted and merged. `running = None` means all time runs.
struct Window {
    /// Disjoint, sorted intervals.
    intervals: Vec<Interval>,
    /// `cumulative[k]`: running time (µs) before `intervals[k]` starts.
    cumulative: Vec<i64>,
}

impl Window {
    fn new(first: i64, last: i64, running: Option<&[Interval]>) -> Window {
        let mut clipped: Vec<Interval> = match running {
            None => vec![(first, last)],
            Some(running) => running
                .iter()
                .map(|&(a, b)| (a.max(first), b.min(last)))
                .filter(|(a, b)| a < b)
                .collect(),
        };
        clipped.sort_unstable();
        let mut intervals: Vec<Interval> = Vec::with_capacity(clipped.len());
        for (a, b) in clipped {
            match intervals.last_mut() {
                Some(prev) if a <= prev.1 => prev.1 = prev.1.max(b),
                _ => intervals.push((a, b)),
            }
        }
        let mut cumulative = Vec::with_capacity(intervals.len());
        let mut acc = 0;
        for &(a, b) in &intervals {
            cumulative.push(acc);
            acc += b - a;
        }
        Window { intervals, cumulative }
    }

    /// Running time (µs) up to `t`.
    fn up_to(&self, t: i64) -> i64 {
        // Intervals starting at or before `t`; the last of them may contain it.
        let k = self.intervals.partition_point(|&(a, _)| a <= t);
        if k == 0 {
            return 0;
        }
        let (a, b) = self.intervals[k - 1];
        self.cumulative[k - 1] + (t.min(b) - a)
    }

    /// Running time in seconds between `t0` and `t1`.
    fn between(&self, t0: i64, t1: i64) -> f64 {
        seconds(self.up_to(t1) - self.up_to(t0))
    }

    /// All running time in the window, in seconds.
    fn total(&self) -> f64 {
        seconds(self.intervals.iter().map(|(a, b)| b - a).sum())
    }
}

/// The `timestamp` column as microseconds, or `None` when the table has no
/// timestamp column (a CSV without one) or one that is not a timestamp.
fn timestamp_values(batch: &RecordBatch) -> Result<Option<arrow::array::Int64Array>, String> {
    let Some(column) = batch.column_by_name("timestamp") else { return Ok(None) };
    if !matches!(column.data_type(), DataType::Timestamp(..)) {
        return Ok(None);
    }
    let micros = cast(column, &DataType::Timestamp(TimeUnit::Microsecond, None))
        .and_then(|a| cast(&a, &DataType::Int64))
        .map_err(|e| e.to_string())?;
    Ok(Some(micros.as_primitive::<Int64Type>().clone()))
}

/// A numeric column as `f64`, or `None` when absent or not numeric.
fn float_column(batch: &RecordBatch, name: &str) -> Result<Option<arrow::array::Float64Array>, String> {
    let Some(column) = batch.column_by_name(name) else { return Ok(None) };
    if !column.data_type().is_numeric() {
        return Ok(None);
    }
    let values = cast(column, &DataType::Float64).map_err(|e| e.to_string())?;
    Ok(Some(values.as_primitive::<Float64Type>().clone()))
}

/// One batch from a table's batches.
pub(crate) fn single_batch(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch, String> {
    concat_batches(schema, batches).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Int16Array, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    const S: i64 = 1_000_000;

    fn table(ts: &[i64], power: &[Option<i16>]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("power", DataType::Int16, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(TimestampMicrosecondArray::from(ts.iter().map(|t| t * S).collect::<Vec<_>>()).with_timezone("UTC")),
                Arc::new(Int16Array::from(power.to_vec())),
            ],
        )
        .unwrap()
    }

    fn get(out: &[(&str, Value)], name: &str) -> Option<Value> {
        out.iter().find(|(n, _)| *n == name).map(|(_, v)| *v)
    }

    fn float(out: &[(&str, Value)], name: &str) -> f64 {
        match get(out, name) {
            Some(Value::Float(v)) => v,
            other => panic!("{name}: {other:?}"),
        }
    }

    #[test]
    fn every_name_is_unique() {
        let mut names: Vec<_> = SUMMARY_FIELDS.iter().map(|f| f.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), SUMMARY_FIELDS.len());
    }

    #[test]
    fn no_timer_events_count_all_time() {
        let t = table(&[0, 10, 30], &[Some(100), Some(200), Some(0)]);
        let out = summarize(&t, None, None, None, None).unwrap();
        assert_eq!(float(&out, "duration"), 30.0);
        assert_eq!(get(&out, "timer_time"), None);
        // 100 W for 10 s, 200 W for 20 s; the last sample ends the session.
        assert!((float(&out, "avg_power") - 500.0 / 3.0).abs() < 1e-9);
        assert_eq!(get(&out, "max_power"), Some(Value::Int(200)));
    }

    #[test]
    fn a_pause_does_not_count() {
        let t = table(&[0, 10, 30], &[Some(100), Some(200), Some(0)]);
        // Running 0–10 and 20–30: the 200 W sample counts for 10 s, not 20.
        let out = summarize(&t, Some(0), None, Some(&[(0, 10 * S), (20 * S, 30 * S)]), None).unwrap();
        assert_eq!(float(&out, "timer_time"), 20.0);
        assert_eq!(float(&out, "duration"), 30.0);
        assert!((float(&out, "avg_power") - 150.0).abs() < 1e-9);
    }

    #[test]
    fn intervals_are_cut_to_the_session() {
        let t = table(&[100, 110, 120], &[Some(1), Some(1), Some(1)]);
        // One file-wide interval, running from before the session to after
        // its last record (an unclosed start, a stop written while saving).
        let out = summarize(&t, Some(100 * S), None, Some(&[(0, i64::MAX)]), None).unwrap();
        assert_eq!(float(&out, "timer_time"), 20.0);
        // An interval wholly outside the session adds nothing.
        let out = summarize(&t, Some(100 * S), None, Some(&[(0, 50 * S)]), None).unwrap();
        assert_eq!(float(&out, "timer_time"), 0.0);
        assert_eq!(get(&out, "avg_power"), None);
    }

    #[test]
    fn a_session_ends_where_the_next_one_starts() {
        // A multi-session file: the session runs to the next start (40 s),
        // and its last sample counts until then.
        let t = table(&[0, 10, 30], &[Some(100), Some(200), Some(300)]);
        let out = summarize(&t, Some(0), Some(40 * S), None, None).unwrap();
        assert_eq!(float(&out, "duration"), 40.0);
        assert!((float(&out, "avg_power") - (1000.0 + 4000.0 + 3000.0) / 40.0).abs() < 1e-9);
        let out = summarize(&t, Some(0), Some(40 * S), Some(&[(0, i64::MAX)]), None).unwrap();
        assert_eq!(float(&out, "timer_time"), 40.0);
    }

    #[test]
    fn overlapping_intervals_merge() {
        let t = table(&[0, 10], &[Some(1), Some(1)]);
        let out = summarize(&t, Some(0), None, Some(&[(0, 6 * S), (4 * S, 10 * S)]), None).unwrap();
        assert_eq!(float(&out, "timer_time"), 10.0);
    }

    #[test]
    fn duration_runs_from_the_session_start() {
        let t = table(&[5, 15], &[Some(1), Some(1)]);
        assert_eq!(float(&summarize(&t, Some(2 * S), None, None, None).unwrap(), "duration"), 13.0);
        // A start after the last record is not a start of these records.
        assert_eq!(float(&summarize(&t, Some(99 * S), None, None, None).unwrap(), "duration"), 10.0);
    }

    #[test]
    fn only_the_last_row_of_a_timestamp_counts() {
        // Duplicate timestamp 10: the 999 W row is not the one deduplicate keeps.
        let dup = table(&[0, 10, 10, 20], &[Some(100), Some(999), Some(200), Some(0)]);
        let dedup = table(&[0, 10, 20], &[Some(100), Some(200), Some(0)]);
        let a = summarize(&dup, None, None, None, None).unwrap();
        let b = summarize(&dedup, None, None, None, None).unwrap();
        assert_eq!(a, b);
        assert_eq!(get(&a, "max_power"), Some(Value::Int(200)));
    }

    #[test]
    fn nulls_are_skipped_zeros_count() {
        let t = table(&[0, 10, 20, 30], &[Some(100), None, Some(0), Some(5)]);
        let out = summarize(&t, None, None, None, None).unwrap();
        // 100 W × 10 s and 0 W × 10 s over 20 s; the null's 10 s is skipped.
        assert!((float(&out, "avg_power") - 50.0).abs() < 1e-9);
    }

    #[test]
    fn all_null_column_has_no_value() {
        let t = table(&[0, 10], &[None, None]);
        let out = summarize(&t, None, None, None, None).unwrap();
        assert_eq!(get(&out, "avg_power"), None);
        assert_eq!(get(&out, "max_power"), None);
    }

    #[test]
    fn a_session_without_samples_has_its_span() {
        // A sport button pressed between two legs: no records, but the file
        // says the session ran from 100 s to 104 s, with the timer running.
        let t = table(&[], &[]);
        let out = summarize(&t, Some(100 * S), Some(104 * S), Some(&[(0, i64::MAX)]), None).unwrap();
        assert_eq!(float(&out, "duration"), 4.0);
        assert_eq!(float(&out, "timer_time"), 4.0);
        assert_eq!(get(&out, "avg_power"), None);
        assert_eq!(get(&out, "max_power"), None);
        // The last session of a file has no end without samples.
        assert!(summarize(&t, Some(100 * S), None, None, None).unwrap().is_empty());
    }

    #[test]
    fn one_sample() {
        let t = table(&[3], &[Some(250)]);
        let out = summarize(&t, None, None, None, None).unwrap();
        assert_eq!(float(&out, "duration"), 0.0);
        assert_eq!(get(&out, "avg_power"), None); // it counts for no time
        assert_eq!(get(&out, "max_power"), Some(Value::Int(250)));
        // Within a known span it counts until the session's end.
        let out = summarize(&t, Some(0), Some(10 * S), None, None).unwrap();
        assert_eq!(float(&out, "duration"), 10.0);
        assert_eq!(float(&out, "avg_power"), 250.0);
        // Two rows under one timestamp are one sample.
        let out = summarize(&table(&[3, 3], &[Some(1), Some(2)]), None, None, None, None).unwrap();
        assert_eq!(get(&out, "max_power"), Some(Value::Int(2)));
    }

    #[test]
    fn unsorted_input_is_an_error() {
        assert!(summarize(&table(&[10, 0], &[Some(1), Some(1)]), None, None, None, None).is_err());
    }

    #[test]
    fn without_timestamps_only_maxima_and_distance() {
        let schema = Schema::new(vec![
            Field::new("speed", DataType::Float32, true),
            Field::new("distance", DataType::Float64, true),
        ]);
        let t = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Float32Array::from(vec![Some(2.0), Some(3.0)])),
                Arc::new(arrow::array::Float64Array::from(vec![Some(1.0), Some(9.0)])),
            ],
        )
        .unwrap();
        let out = summarize(&t, None, None, None, None).unwrap();
        assert_eq!(get(&out, "max_speed"), Some(Value::Float(3.0)));
        assert_eq!(get(&out, "distance"), Some(Value::Float(9.0)));
        assert_eq!(get(&out, "duration"), None);
        assert_eq!(get(&out, "avg_speed"), None);
    }

    #[test]
    fn pool_distance_replaces_the_column_maximum() {
        let schema = Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("distance", DataType::Float64, true),
        ]);
        let t = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![0, S])),
                Arc::new(arrow::array::Float64Array::from(vec![Some(0.0), Some(299.8)])),
            ],
        )
        .unwrap();
        let out = summarize(&t, None, None, None, Some(300.0)).unwrap();
        assert_eq!(get(&out, "distance"), Some(Value::Float(300.0)));
    }

    #[test]
    fn many_intervals() {
        let ts: Vec<i64> = (0..=1000).collect();
        let power: Vec<Option<i16>> = ts.iter().map(|_| Some(100)).collect();
        // Running for the first half of every 10 s.
        let running: Vec<Interval> = (0..100).map(|k| (k * 10 * S, (k * 10 + 5) * S)).collect();
        let out = summarize(&table(&ts, &power), Some(0), None, Some(&running), None).unwrap();
        assert_eq!(float(&out, "timer_time"), 500.0);
        assert!((float(&out, "avg_power") - 100.0).abs() < 1e-9);
    }
}
