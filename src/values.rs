// ---------------------------------------------------------------------------
// Value extraction helpers — convert FIT Value types to Rust primitives
// ---------------------------------------------------------------------------

use fitparser::Value;

pub fn value_to_i16(val: &Value) -> Option<i16> {
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

pub fn value_to_i8(val: &Value) -> Option<i8> {
    match val {
        Value::SInt8(v) => Some(*v),
        Value::UInt8(v) => i8::try_from(*v).ok(),
        Value::SInt16(v) => i8::try_from(*v).ok(),
        Value::Float32(v) => Some(*v as i8),
        Value::Float64(v) => Some(*v as i8),
        _ => None,
    }
}

pub fn value_to_i32(val: &Value) -> Option<i32> {
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

pub fn value_to_i64(val: &Value) -> Option<i64> {
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

pub fn value_to_f32(val: &Value) -> Option<f32> {
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

pub fn value_to_f64(val: &Value) -> Option<f64> {
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

pub fn value_to_timestamp_us(val: &Value) -> Option<i64> {
    match val {
        Value::Timestamp(dt) => {
            Some(dt.timestamp() * 1_000_000 + dt.timestamp_subsec_micros() as i64)
        }
        _ => None,
    }
}

pub fn value_to_timestamp_secs(val: &Value) -> Option<f64> {
    match val {
        Value::Timestamp(dt) => {
            Some(dt.timestamp() as f64 + dt.timestamp_subsec_nanos() as f64 / 1e9)
        }
        _ => None,
    }
}

pub fn value_to_string(val: &Value) -> Option<String> {
    match val {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

pub fn value_to_u8(val: &Value) -> Option<u8> {
    match val {
        Value::UInt8(v) => Some(*v),
        Value::UInt16(v) => u8::try_from(*v).ok(),
        Value::SInt16(v) => u8::try_from(*v).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i16_from_uint8() {
        assert_eq!(value_to_i16(&Value::UInt8(200)), Some(200));
    }

    #[test]
    fn i16_from_sint16() {
        assert_eq!(value_to_i16(&Value::SInt16(-100)), Some(-100));
    }

    #[test]
    fn i16_from_uint16_overflow_returns_none() {
        assert_eq!(value_to_i16(&Value::UInt16(40000)), None);
    }

    #[test]
    fn i16_from_uint16_in_range() {
        assert_eq!(value_to_i16(&Value::UInt16(1000)), Some(1000));
    }

    #[test]
    fn i16_from_float32_truncates() {
        assert_eq!(value_to_i16(&Value::Float32(150.7)), Some(150));
    }

    #[test]
    fn i16_from_string_returns_none() {
        assert_eq!(value_to_i16(&Value::String("hello".into())), None);
    }

    #[test]
    fn i8_from_sint8() {
        assert_eq!(value_to_i8(&Value::SInt8(-50)), Some(-50));
    }

    #[test]
    fn i8_from_uint8_overflow_returns_none() {
        assert_eq!(value_to_i8(&Value::UInt8(200)), None);
    }

    #[test]
    fn i8_from_uint8_in_range() {
        assert_eq!(value_to_i8(&Value::UInt8(25)), Some(25));
    }

    #[test]
    fn i32_from_sint32() {
        assert_eq!(value_to_i32(&Value::SInt32(-100_000)), Some(-100_000));
    }

    #[test]
    fn i64_from_uint64() {
        assert_eq!(value_to_i64(&Value::UInt64(12345678)), Some(12345678));
    }

    #[test]
    fn f32_from_float32() {
        assert_eq!(value_to_f32(&Value::Float32(3.14)), Some(3.14));
    }

    #[test]
    fn f32_from_uint16() {
        assert_eq!(value_to_f32(&Value::UInt16(1000)), Some(1000.0));
    }

    #[test]
    fn f64_from_float64() {
        assert_eq!(value_to_f64(&Value::Float64(3.14159)), Some(3.14159));
    }

    #[test]
    fn f64_from_sint32() {
        assert_eq!(value_to_f64(&Value::SInt32(-500)), Some(-500.0));
    }

    #[test]
    fn u8_from_uint8() {
        assert_eq!(value_to_u8(&Value::UInt8(42)), Some(42));
    }

    #[test]
    fn u8_from_uint16_overflow_returns_none() {
        assert_eq!(value_to_u8(&Value::UInt16(300)), None);
    }

    #[test]
    fn string_from_string() {
        assert_eq!(
            value_to_string(&Value::String("cycling".into())),
            Some("cycling".into())
        );
    }

    #[test]
    fn string_from_int_returns_none() {
        assert_eq!(value_to_string(&Value::UInt8(42)), None);
    }
}
