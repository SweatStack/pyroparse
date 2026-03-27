// ---------------------------------------------------------------------------
// Field name normalization — FIT field names → clean column names
// ---------------------------------------------------------------------------

/// Normalize a FIT field name to a clean column name.
///
/// "Form Power" → "form_power", "DragFactor" → "drag_factor",
/// "heart_rate" → "heart_rate" (unchanged).
pub fn normalize_field_name(name: &str) -> String {
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

/// The 11 standard columns + canonical extras — none of these are added to
/// the dynamic extras discovered from the file.
pub fn is_canonical_column(name: &str) -> bool {
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
            | "lap"
            | "lap_trigger"
            | "core_temperature"
            | "smo2"
    )
}

/// Fast check for raw FIT field names handled by dedicated match arms
/// in the Record decoder (`fit::decode`). Fields listed here are decoded
/// directly into `RecordRow` fields, not routed through the extras system.
pub fn is_handled_field(name: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spaces_to_underscores() {
        assert_eq!(normalize_field_name("Form Power"), "form_power");
    }

    #[test]
    fn camel_case_to_snake_case() {
        assert_eq!(normalize_field_name("DragFactor"), "drag_factor");
    }

    #[test]
    fn already_snake_case_unchanged() {
        assert_eq!(normalize_field_name("heart_rate"), "heart_rate");
    }

    #[test]
    fn single_word_lowered() {
        assert_eq!(normalize_field_name("Power"), "power");
    }

    #[test]
    fn mixed_case_with_spaces() {
        assert_eq!(
            normalize_field_name("Core Body Temperature"),
            "core_body_temperature"
        );
    }

    #[test]
    fn consecutive_uppercase_no_double_underscore() {
        assert_eq!(normalize_field_name("SMO2"), "smo2");
    }

    #[test]
    fn is_canonical_standard_columns() {
        for col in [
            "timestamp", "heart_rate", "power", "cadence", "speed",
            "latitude", "longitude", "altitude", "temperature", "distance",
            "lap", "lap_trigger", "core_temperature", "smo2",
        ] {
            assert!(is_canonical_column(col), "{col} should be canonical");
        }
    }

    #[test]
    fn is_canonical_rejects_extras() {
        for col in ["form_power", "drag_factor", "stance_time", "vertical_ratio"] {
            assert!(!is_canonical_column(col), "{col} should not be canonical");
        }
    }

    #[test]
    fn is_handled_standard_fields() {
        for name in [
            "timestamp", "heart_rate", "power", "cadence", "speed",
            "enhanced_speed", "position_lat", "position_long",
            "altitude", "enhanced_altitude", "temperature", "distance",
        ] {
            assert!(is_handled_field(name), "{name} should be handled");
        }
    }

    #[test]
    fn is_handled_developer_fields() {
        for name in [
            "Power", "Cadence", "Core Body Temperature",
            "core_temperature", "Current Saturated Hemoglobin Percent",
            "SmO2", "smo2", "saturated_hemoglobin_percent",
        ] {
            assert!(is_handled_field(name), "{name} should be handled");
        }
    }

    #[test]
    fn is_handled_rejects_extras() {
        assert!(!is_handled_field("Form Power"));
        assert!(!is_handled_field("drag_factor"));
    }
}
