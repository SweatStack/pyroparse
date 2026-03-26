// ---------------------------------------------------------------------------
// Reference lookups — manufacturer IDs, sport IDs, ANT+ types, product names
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

pub fn sport_name(v: u8) -> &'static str {
    match v {
        0 => "generic", 1 => "running", 2 => "cycling", 3 => "transition",
        4 => "fitness_equipment", 5 => "swimming", 6 => "basketball",
        7 => "soccer", 8 => "tennis", 10 => "training", 11 => "walking",
        12 => "cross_country_skiing", 13 => "alpine_skiing",
        14 => "snowboarding", 15 => "rowing", 16 => "mountaineering",
        17 => "hiking", 18 => "multisport", 19 => "paddling",
        21 => "e_biking", 23 => "boating", 25 => "golf",
        37 => "stand_up_paddleboarding", 38 => "surfing", 53 => "diving",
        _ => "unknown",
    }
}

pub fn sub_sport_name(v: u8) -> &'static str {
    match v {
        0 => "generic",
        1 => "treadmill",
        2 => "street",
        3 => "trail",
        4 => "track",
        5 => "spin",
        6 => "indoor_cycling",
        7 => "road",
        8 => "mountain",
        9 => "downhill",
        10 => "recumbent",
        11 => "cyclocross",
        12 => "hand_cycling",
        13 => "track_cycling",
        14 => "indoor_rowing",
        15 => "elliptical",
        16 => "stair_climbing",
        17 => "lap_swimming",
        18 => "open_water",
        20 => "strength_training",
        21 => "warm_up",
        22 => "match",
        23 => "exercise",
        24 => "challenge",
        25 => "indoor_skiing",
        26 => "cardio_training",
        28 => "e_bike_fitness",
        37 => "e_bike_mountain",
        42 => "gravel_cycling",
        46 => "skate_skiing",
        254 => "all",
        _ => "unknown",
    }
}

/// Map ANT+ device type string names back to numeric codes.
/// The fitparser crate decodes these from the FIT profile.
pub fn antplus_type_from_name(name: &str) -> Option<u8> {
    match name {
        "heart_rate" => Some(120),
        "bike_speed_cadence" => Some(121),
        "bike_cadence" => Some(122),
        "bike_speed" => Some(123),
        "bike_power" => Some(11),
        "stride_speed_distance" => Some(124),
        _ => None,
    }
}

/// Clean a raw product name like "fenix6" or "hrm_pro" by replacing
/// underscores and hyphens with spaces.  Preserves original casing.
pub fn format_product_name(raw: &str) -> String {
    raw.chars()
        .map(|c| if c == '_' || c == '-' { ' ' } else { c })
        .collect()
}

/// Map FIT manufacturer IDs to names.  Derived from the FIT SDK profile.
/// Only includes manufacturers commonly seen in fitness device files.
pub fn manufacturer_name(v: u16) -> &'static str {
    match v {
        1 | 15 => "garmin",
        6 => "srm",
        7 => "quarq",
        9 => "saris",
        23 => "suunto",
        32 => "wahoo_fitness",
        38 => "osynce",
        40 => "concept2",
        41 => "shimano",
        44 => "brim_brothers",
        48 => "pioneer",
        60 => "rotor",
        63 => "specialized",
        69 => "stages_cycling",
        70 => "sigmasport",
        73 => "wattbike",
        76 => "moxy",
        81 => "bontrager",
        86 => "elite",
        89 => "tacx",
        95 => "stryd",
        260 => "zwift",
        263 => "favero",
        265 => "coros",
        289 => "hammerhead",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manufacturer_known_ids() {
        assert_eq!(manufacturer_name(1), "garmin");
        assert_eq!(manufacturer_name(15), "garmin");
        assert_eq!(manufacturer_name(32), "wahoo_fitness");
        assert_eq!(manufacturer_name(40), "concept2");
        assert_eq!(manufacturer_name(95), "stryd");
        assert_eq!(manufacturer_name(263), "favero");
        assert_eq!(manufacturer_name(265), "coros");
    }

    #[test]
    fn manufacturer_unknown_returns_unknown() {
        assert_eq!(manufacturer_name(9999), "unknown");
    }

    #[test]
    fn sport_known_ids() {
        assert_eq!(sport_name(0), "generic");
        assert_eq!(sport_name(1), "running");
        assert_eq!(sport_name(2), "cycling");
        assert_eq!(sport_name(5), "swimming");
        assert_eq!(sport_name(15), "rowing");
    }

    #[test]
    fn sport_unknown_returns_unknown() {
        assert_eq!(sport_name(255), "unknown");
    }

    #[test]
    fn sub_sport_known_ids() {
        assert_eq!(sub_sport_name(0), "generic");
        assert_eq!(sub_sport_name(6), "indoor_cycling");
        assert_eq!(sub_sport_name(7), "road");
        assert_eq!(sub_sport_name(14), "indoor_rowing");
        assert_eq!(sub_sport_name(17), "lap_swimming");
        assert_eq!(sub_sport_name(3), "trail");
    }

    #[test]
    fn sub_sport_unknown_returns_unknown() {
        assert_eq!(sub_sport_name(255), "unknown");
    }

    #[test]
    fn antplus_type_known_names() {
        assert_eq!(antplus_type_from_name("heart_rate"), Some(120));
        assert_eq!(antplus_type_from_name("bike_power"), Some(11));
        assert_eq!(antplus_type_from_name("bike_speed_cadence"), Some(121));
    }

    #[test]
    fn antplus_type_unknown_returns_none() {
        assert_eq!(antplus_type_from_name("unknown_sensor"), None);
    }

    #[test]
    fn format_product_name_replaces_separators() {
        assert_eq!(format_product_name("fenix6"), "fenix6");
        assert_eq!(format_product_name("hrm_pro"), "hrm pro");
        assert_eq!(format_product_name("hrm-pro-plus"), "hrm pro plus");
    }

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
