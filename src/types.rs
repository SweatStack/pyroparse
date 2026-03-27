// ---------------------------------------------------------------------------
// Column type system — TypedColumn storage, FIT→Arrow type mapping, promotion
// ---------------------------------------------------------------------------

use std::sync::Arc;

use arrow::array::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
};
use arrow::datatypes::DataType;
use fitparser::Value;

use crate::values::{
    value_to_f32, value_to_f64, value_to_i16, value_to_i32, value_to_i64, value_to_i8,
    value_to_string,
};

/// A single extra column stored as a typed vector, one entry per row.
/// Pre-allocated to n_rows with None, then filled during the second pass.
pub enum TypedColumn {
    I8(Vec<Option<i8>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
}

impl TypedColumn {
    pub fn new(dtype: &DataType, n_rows: usize) -> Self {
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

    pub fn set(&mut self, idx: usize, val: &Value) {
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

    /// Set a value from raw FIT bytes with scale/offset applied.
    /// Used by the custom decoder to bypass fitparser's Value enum.
    pub fn set_from_bytes(&mut self, idx: usize, data: &[u8], base_type: u8, big_endian: bool, scale: f64, offset: f64) {
        use crate::fit::profile::BaseType;
        let bt = BaseType::from_byte(base_type);
        match self {
            Self::I8(v) => {
                if let Some(raw) = read_raw_i64(data, bt, big_endian) {
                    v[idx] = Some(apply_scale_i8(raw, scale, offset));
                }
            }
            Self::I16(v) => {
                if let Some(raw) = read_raw_i64(data, bt, big_endian) {
                    v[idx] = Some(apply_scale_i16(raw, scale, offset));
                }
            }
            Self::I32(v) => {
                if let Some(raw) = read_raw_i64(data, bt, big_endian) {
                    v[idx] = Some(apply_scale_i32(raw, scale, offset));
                }
            }
            Self::I64(v) => {
                if let Some(raw) = read_raw_i64(data, bt, big_endian) {
                    v[idx] = Some(apply_scale_i64(raw, scale, offset));
                }
            }
            Self::F32(v) => {
                if let Some(raw) = read_raw_f64(data, bt, big_endian) {
                    v[idx] = Some(apply_scale_f32(raw, scale, offset));
                }
            }
            Self::F64(v) => {
                if let Some(raw) = read_raw_f64(data, bt, big_endian) {
                    v[idx] = Some(apply_scale_f64(raw, scale, offset));
                }
            }
            Self::Str(v) => {
                // Strings: NUL-terminated, no scale/offset.
                let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
                if end > 0 {
                    if let Ok(s) = String::from_utf8(data[..end].to_vec()) {
                        v[idx] = Some(s);
                    }
                }
            }
        }
    }

    pub fn to_arrow_array(&self) -> Arc<dyn arrow::array::Array> {
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
pub fn fit_value_to_arrow_type(val: &Value) -> Option<DataType> {
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

/// Promote two Arrow types to the wider compatible type.
pub fn promote_type(a: &DataType, b: &DataType) -> DataType {
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

// ---------------------------------------------------------------------------
// Raw byte → typed value helpers (for custom decoder, no fitparser Value)
// ---------------------------------------------------------------------------

use crate::fit::profile::BaseType;

/// Read a raw integer value from FIT bytes, returning None for invalid values.
fn read_raw_i64(data: &[u8], bt: BaseType, big_endian: bool) -> Option<i64> {
    match bt {
        BaseType::Enum | BaseType::UInt8 => {
            let v = *data.first()?;
            if v == 0xFF { None } else { Some(v as i64) }
        }
        BaseType::SInt8 => {
            let v = *data.first()? as i8;
            if v == 0x7F { None } else { Some(v as i64) }
        }
        BaseType::UInt16 => {
            if data.len() < 2 { return None; }
            let v = if big_endian { u16::from_be_bytes([data[0], data[1]]) }
                    else { u16::from_le_bytes([data[0], data[1]]) };
            if v == 0xFFFF { None } else { Some(v as i64) }
        }
        BaseType::SInt16 => {
            if data.len() < 2 { return None; }
            let v = if big_endian { i16::from_be_bytes([data[0], data[1]]) }
                    else { i16::from_le_bytes([data[0], data[1]]) };
            if v == 0x7FFF { None } else { Some(v as i64) }
        }
        BaseType::UInt32 => {
            if data.len() < 4 { return None; }
            let v = if big_endian { u32::from_be_bytes([data[0], data[1], data[2], data[3]]) }
                    else { u32::from_le_bytes([data[0], data[1], data[2], data[3]]) };
            if v == 0xFFFFFFFF { None } else { Some(v as i64) }
        }
        BaseType::SInt32 => {
            if data.len() < 4 { return None; }
            let v = if big_endian { i32::from_be_bytes([data[0], data[1], data[2], data[3]]) }
                    else { i32::from_le_bytes([data[0], data[1], data[2], data[3]]) };
            if v == 0x7FFFFFFF { None } else { Some(v as i64) }
        }
        BaseType::UInt8z => {
            let v = *data.first()?;
            if v == 0 { None } else { Some(v as i64) }
        }
        BaseType::UInt16z => {
            if data.len() < 2 { return None; }
            let v = if big_endian { u16::from_be_bytes([data[0], data[1]]) }
                    else { u16::from_le_bytes([data[0], data[1]]) };
            if v == 0 { None } else { Some(v as i64) }
        }
        BaseType::UInt32z => {
            if data.len() < 4 { return None; }
            let v = if big_endian { u32::from_be_bytes([data[0], data[1], data[2], data[3]]) }
                    else { u32::from_le_bytes([data[0], data[1], data[2], data[3]]) };
            if v == 0 { None } else { Some(v as i64) }
        }
        BaseType::SInt64 => {
            if data.len() < 8 { return None; }
            let v = if big_endian { i64::from_be_bytes(data[..8].try_into().ok()?) }
                    else { i64::from_le_bytes(data[..8].try_into().ok()?) };
            if v == 0x7FFFFFFFFFFFFFFF { None } else { Some(v) }
        }
        BaseType::UInt64 => {
            if data.len() < 8 { return None; }
            let v = if big_endian { u64::from_be_bytes(data[..8].try_into().ok()?) }
                    else { u64::from_le_bytes(data[..8].try_into().ok()?) };
            if v == 0xFFFFFFFFFFFFFFFF { None } else { Some(v as i64) }
        }
        BaseType::UInt64z => {
            if data.len() < 8 { return None; }
            let v = if big_endian { u64::from_be_bytes(data[..8].try_into().ok()?) }
                    else { u64::from_le_bytes(data[..8].try_into().ok()?) };
            if v == 0 { None } else { Some(v as i64) }
        }
        _ => None,
    }
}

/// Read a raw float value from FIT bytes.
fn read_raw_f64(data: &[u8], bt: BaseType, big_endian: bool) -> Option<f64> {
    match bt {
        BaseType::Float32 => {
            if data.len() < 4 { return None; }
            let v = if big_endian { f32::from_be_bytes([data[0], data[1], data[2], data[3]]) }
                    else { f32::from_le_bytes([data[0], data[1], data[2], data[3]]) };
            if !v.is_finite() { None } else { Some(v as f64) }
        }
        BaseType::Float64 => {
            if data.len() < 8 { return None; }
            let v = if big_endian { f64::from_be_bytes(data[..8].try_into().ok()?) }
                    else { f64::from_le_bytes(data[..8].try_into().ok()?) };
            if !v.is_finite() { None } else { Some(v) }
        }
        // Fall back to integer reading for integer types with scale.
        _ => read_raw_i64(data, bt, big_endian).map(|v| v as f64),
    }
}

fn apply_scale_i8(raw: i64, scale: f64, offset: f64) -> i8 {
    if scale == 1.0 && offset == 0.0 { raw as i8 } else { (raw as f64 / scale - offset) as i8 }
}
fn apply_scale_i16(raw: i64, scale: f64, offset: f64) -> i16 {
    if scale == 1.0 && offset == 0.0 { raw as i16 } else { (raw as f64 / scale - offset) as i16 }
}
fn apply_scale_i32(raw: i64, scale: f64, offset: f64) -> i32 {
    if scale == 1.0 && offset == 0.0 { raw as i32 } else { (raw as f64 / scale - offset) as i32 }
}
fn apply_scale_i64(raw: i64, scale: f64, offset: f64) -> i64 {
    if scale == 1.0 && offset == 0.0 { raw } else { (raw as f64 / scale - offset) as i64 }
}
fn apply_scale_f32(raw: f64, scale: f64, offset: f64) -> f32 {
    if scale == 1.0 && offset == 0.0 { raw as f32 } else { (raw / scale - offset) as f32 }
}
fn apply_scale_f64(raw: f64, scale: f64, offset: f64) -> f64 {
    if scale == 1.0 && offset == 0.0 { raw } else { raw / scale - offset }
}

/// Map a FIT base type to an Arrow DataType for extra column discovery.
pub fn base_type_to_arrow(base_type: u8) -> Option<DataType> {
    match BaseType::from_byte(base_type) {
        BaseType::SInt8 => Some(DataType::Int8),
        BaseType::Enum | BaseType::UInt8 | BaseType::UInt8z | BaseType::SInt16 => Some(DataType::Int16),
        BaseType::UInt16 | BaseType::UInt16z | BaseType::SInt32 => Some(DataType::Int32),
        BaseType::UInt32 | BaseType::UInt32z | BaseType::SInt64 | BaseType::UInt64 | BaseType::UInt64z => Some(DataType::Int64),
        BaseType::Float32 => Some(DataType::Float32),
        BaseType::Float64 => Some(DataType::Float64),
        BaseType::String => Some(DataType::Utf8),
        BaseType::Byte => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── FIT value → Arrow type mapping ───────────────────────────────────

    #[test]
    fn sint8_maps_to_int8() {
        assert_eq!(fit_value_to_arrow_type(&Value::SInt8(0)), Some(DataType::Int8));
    }

    #[test]
    fn uint8_maps_to_int16() {
        assert_eq!(fit_value_to_arrow_type(&Value::UInt8(0)), Some(DataType::Int16));
    }

    #[test]
    fn sint16_maps_to_int16() {
        assert_eq!(fit_value_to_arrow_type(&Value::SInt16(0)), Some(DataType::Int16));
    }

    #[test]
    fn uint16_maps_to_int32() {
        assert_eq!(fit_value_to_arrow_type(&Value::UInt16(0)), Some(DataType::Int32));
    }

    #[test]
    fn sint32_maps_to_int32() {
        assert_eq!(fit_value_to_arrow_type(&Value::SInt32(0)), Some(DataType::Int32));
    }

    #[test]
    fn uint32_maps_to_int64() {
        assert_eq!(fit_value_to_arrow_type(&Value::UInt32(0)), Some(DataType::Int64));
    }

    #[test]
    fn float32_maps_to_float32() {
        assert_eq!(fit_value_to_arrow_type(&Value::Float32(0.0)), Some(DataType::Float32));
    }

    #[test]
    fn float64_maps_to_float64() {
        assert_eq!(fit_value_to_arrow_type(&Value::Float64(0.0)), Some(DataType::Float64));
    }

    #[test]
    fn string_maps_to_utf8() {
        assert_eq!(
            fit_value_to_arrow_type(&Value::String("x".into())),
            Some(DataType::Utf8)
        );
    }

    #[test]
    fn array_maps_to_none() {
        assert_eq!(fit_value_to_arrow_type(&Value::Array(vec![])), None);
    }

    // ── Type promotion ───────────────────────────────────────────────────

    #[test]
    fn same_type_returns_self() {
        assert_eq!(promote_type(&DataType::Int16, &DataType::Int16), DataType::Int16);
    }

    #[test]
    fn int8_int16_promotes_to_int16() {
        assert_eq!(promote_type(&DataType::Int8, &DataType::Int16), DataType::Int16);
        assert_eq!(promote_type(&DataType::Int16, &DataType::Int8), DataType::Int16);
    }

    #[test]
    fn int8_int32_promotes_to_int32() {
        assert_eq!(promote_type(&DataType::Int8, &DataType::Int32), DataType::Int32);
        assert_eq!(promote_type(&DataType::Int32, &DataType::Int8), DataType::Int32);
    }

    #[test]
    fn int16_int32_promotes_to_int32() {
        assert_eq!(promote_type(&DataType::Int16, &DataType::Int32), DataType::Int32);
    }

    #[test]
    fn any_int_with_int64_promotes_to_int64() {
        assert_eq!(promote_type(&DataType::Int8, &DataType::Int64), DataType::Int64);
        assert_eq!(promote_type(&DataType::Int16, &DataType::Int64), DataType::Int64);
        assert_eq!(promote_type(&DataType::Int32, &DataType::Int64), DataType::Int64);
        assert_eq!(promote_type(&DataType::Int64, &DataType::Int8), DataType::Int64);
    }

    #[test]
    fn float32_float64_promotes_to_float64() {
        assert_eq!(promote_type(&DataType::Float32, &DataType::Float64), DataType::Float64);
        assert_eq!(promote_type(&DataType::Float64, &DataType::Float32), DataType::Float64);
    }

    #[test]
    fn utf8_with_anything_is_utf8() {
        assert_eq!(promote_type(&DataType::Utf8, &DataType::Int16), DataType::Utf8);
        assert_eq!(promote_type(&DataType::Float32, &DataType::Utf8), DataType::Utf8);
    }

    #[test]
    fn unrelated_types_fallback_to_float64() {
        assert_eq!(promote_type(&DataType::Int8, &DataType::Float32), DataType::Float64);
    }
}
