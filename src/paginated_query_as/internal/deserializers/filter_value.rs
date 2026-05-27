use crate::paginated_query_as::models::FilterValue;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};

/// Parses a string value into a FilterValue with automatic type inference.
/// Type inference order: Bool -> Uuid -> DateTime -> Date -> Time -> Int -> Float -> String
pub(crate) fn parse_filter_value(s: &str) -> FilterValue {
    if s.is_empty() {
        return FilterValue::Null;
    }

    match s.to_lowercase().as_str() {
        "true" => return FilterValue::Bool(true),
        "false" => return FilterValue::Bool(false),
        _ => {}
    }

    if let Ok(uuid) = uuid::Uuid::parse_str(s) {
        return FilterValue::Uuid(uuid);
    }

    if let Ok(dt) = DateTime::<FixedOffset>::parse_from_rfc3339(s) {
        return FilterValue::DateTime(dt.to_utc().to_string());
    }

    if NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").is_ok()
        || NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").is_ok()
        || NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
    {
        return FilterValue::DateTime(s.to_string());
    }

    if NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok() {
        return FilterValue::Date(s.to_string());
    }

    if NaiveTime::parse_from_str(s, "%H:%M:%S").is_ok() || NaiveTime::parse_from_str(s, "%H:%M").is_ok()
    {
        return FilterValue::Time(s.to_string());
    }

    if let Ok(i) = s.parse::<i64>() {
        return FilterValue::Int(i);
    }

    if let Ok(f) = s.parse::<f64>() {
        return FilterValue::Float(f);
    }

    FilterValue::String(s.to_string())
}

pub(crate) fn parse_array_values(s: &str) -> Vec<FilterValue> {
    s.split(',').map(|v| parse_filter_value(v.trim())).collect()
}
