// ---------------------------------------------------------------------------
// Reference lookups — developer field classification
//
// Sport, sub-sport, manufacturer, and ANT+ type mappings have moved to
// the generated fit::profile module.
// ---------------------------------------------------------------------------

/// Map a developer field name to a known metric name, if recognized.
pub fn classify_developer_field(name: &str) -> Option<&'static str> {
    let lower = name.to_lowercase();
    if lower.contains("core") && lower.contains("temp") { return Some("core_temperature"); }
    if lower.contains("hemoglobin") || lower.contains("smo2") || lower.contains("muscle oxygen") {
        return Some("smo2");
    }
    if lower == "power" { return Some("power"); }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_developer_field_core_temp() {
        assert_eq!(classify_developer_field("Core Body Temperature"), Some("core_temperature"));
    }

    #[test]
    fn classify_developer_field_smo2() {
        assert_eq!(classify_developer_field("Current Saturated Hemoglobin Percent"), Some("smo2"));
        assert_eq!(classify_developer_field("SmO2"), Some("smo2"));
        assert_eq!(classify_developer_field("Muscle Oxygen"), Some("smo2"));
    }

    #[test]
    fn classify_developer_field_power() {
        assert_eq!(classify_developer_field("Power"), Some("power"));
    }

    #[test]
    fn classify_developer_field_unknown() {
        assert_eq!(classify_developer_field("Form Power"), None);
        assert_eq!(classify_developer_field("Drag Factor"), None);
    }
}
