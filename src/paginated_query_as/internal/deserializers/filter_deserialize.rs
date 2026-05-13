use super::FilterParseError;
use crate::paginated_query_as::models::{
    Filter, FilterExpression, FilterExpressionGroup, FilterOperator, FilterValue, LogicalOperator,
};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use serde::{de::Error, Deserialize, Deserializer};
use serde_json::{Map, Value};
use std::collections::HashMap;

const RESERVED_QUERY_PARAMS: &[&str] = &[
    "page",
    "page_size",
    "sort_column",
    "sort_direction",
    "search",
    "search_columns",
];

/// Parses a string value into a FilterValue with automatic type inference.
/// Type inference order: Bool -> Uuid -> DateTime -> Date -> Time -> Int -> Float -> String
fn parse_filter_value(s: &str) -> FilterValue {
    if s.is_empty() {
        return FilterValue::Null;
    }

    // Try boolean
    match s.to_lowercase().as_str() {
        "true" => return FilterValue::Bool(true),
        "false" => return FilterValue::Bool(false),
        _ => {}
    }

    // Try UUID
    if let Ok(uuid) = uuid::Uuid::parse_str(s) {
        return FilterValue::Uuid(uuid);
    }

    // Try DateTime (RFC 3339 / ISO 8601 with timezone)
    // Examples: 2025-12-02T10:30:00Z, 2025-12-02T10:30:00+00:00
    if let Ok(dt) = DateTime::<FixedOffset>::parse_from_rfc3339(s) {
        return FilterValue::DateTime(dt.to_utc().to_string());
    }

    // Try NaiveDateTime (ISO 8601 without timezone)
    // Examples: 2025-12-02T10:30:00, 2025-12-02 10:30:00
    if NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").is_ok()
        || NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").is_ok()
        || NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
    {
        return FilterValue::DateTime(s.to_string());
    }

    // Try Date (YYYY-MM-DD)
    if NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok() {
        return FilterValue::Date(s.to_string());
    }

    // Try Time (HH:MM:SS)
    if NaiveTime::parse_from_str(s, "%H:%M:%S").is_ok()
        || NaiveTime::parse_from_str(s, "%H:%M").is_ok()
    {
        return FilterValue::Time(s.to_string());
    }

    // Try integer
    if let Ok(i) = s.parse::<i64>() {
        return FilterValue::Int(i);
    }

    // Try float
    if let Ok(f) = s.parse::<f64>() {
        return FilterValue::Float(f);
    }

    // Fallback to string
    FilterValue::String(s.to_string())
}

/// Parses comma-separated values into a Vec<FilterValue>.
fn parse_array_values(s: &str) -> Vec<FilterValue> {
    s.split(',').map(|v| parse_filter_value(v.trim())).collect()
}

fn filter_value_from_json(value: &Value) -> FilterValue {
    match value {
        Value::Null => FilterValue::Null,
        Value::Bool(value) => FilterValue::Bool(*value),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                FilterValue::Int(value)
            } else if let Some(value) = value.as_f64() {
                FilterValue::Float(value)
            } else {
                FilterValue::String(value.to_string())
            }
        }
        Value::String(value) => parse_filter_value(value),
        Value::Array(values) => {
            FilterValue::Array(values.iter().map(filter_value_from_json).collect())
        }
        Value::Object(_) => FilterValue::String(value.to_string()),
    }
}

fn filter_value_for_operator(operator: FilterOperator, value: &Value) -> FilterValue {
    match operator {
        FilterOperator::IsNull | FilterOperator::IsNotNull => FilterValue::Null,
        FilterOperator::In | FilterOperator::NotIn | FilterOperator::Between => {
            match filter_value_from_json(value) {
                FilterValue::Array(values) => FilterValue::Array(values),
                value => FilterValue::Array(vec![value]),
            }
        }
        _ => filter_value_from_json(value),
    }
}

fn parse_filter_from_raw(field: &str, raw_value: &str) -> Result<Filter, FilterParseError> {
    let (operator_str, value_str) =
        raw_value
            .split_once(':')
            .ok_or_else(|| FilterParseError::InvalidFilterFormat {
                field: field.to_string(),
                raw_value: raw_value.to_string(),
            })?;

    let operator = operator_str
        .parse::<FilterOperator>()
        .map_err(|raw_operator| FilterParseError::InvalidOperator {
            field: field.to_string(),
            raw_operator,
        })?;

    let value = match operator {
        FilterOperator::IsNull | FilterOperator::IsNotNull => FilterValue::Null,
        FilterOperator::In | FilterOperator::NotIn | FilterOperator::Between => {
            FilterValue::Array(parse_array_values(value_str))
        }
        _ => parse_filter_value(value_str),
    };

    Ok(Filter {
        field: field.to_string(),
        operator,
        value,
    })
}

fn filter_expression(field: &str, operator: FilterOperator, value: &Value) -> FilterExpression {
    FilterExpression::Condition(Filter {
        field: field.to_string(),
        operator,
        value: filter_value_for_operator(operator, value),
    })
}

fn parse_field_expression(
    field: &str,
    value: &Value,
) -> Result<Vec<FilterExpression>, FilterParseError> {
    match value {
        Value::Object(operators) if !operators.is_empty() => operators
            .iter()
            .map(|(operator, value)| {
                operator
                    .parse::<FilterOperator>()
                    .map(|operator| filter_expression(field, operator, value))
                    .map_err(|raw_operator| FilterParseError::InvalidOperator {
                        field: field.to_string(),
                        raw_operator,
                    })
            })
            .collect(),
        _ => Ok(vec![filter_expression(field, FilterOperator::Eq, value)]),
    }
}

fn parse_logical_group(
    operator: LogicalOperator,
    value: &Value,
) -> Result<FilterExpression, FilterParseError> {
    let Value::Array(items) = value else {
        return Err(FilterParseError::InvalidLogicalFilter {
            path: operator.as_str().to_string(),
            reason: "expected an array".to_string(),
        });
    };

    let children = items
        .iter()
        .map(parse_expression_value)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(FilterExpression::Group(FilterExpressionGroup {
        operator,
        children,
    }))
}

fn parse_expression_object(map: &Map<String, Value>) -> Result<FilterExpression, FilterParseError> {
    let mut children = Vec::new();

    for (field, value) in map {
        if let Ok(operator) = field.parse::<LogicalOperator>() {
            children.push(parse_logical_group(operator, value)?);
        } else {
            children.extend(parse_field_expression(field, value)?);
        }
    }

    match children.len() {
        0 => Ok(FilterExpression::Group(FilterExpressionGroup::default())),
        1 => Ok(children.remove(0)),
        _ => Ok(FilterExpression::Group(FilterExpressionGroup::and(
            children,
        ))),
    }
}

fn parse_expression_value(value: &Value) -> Result<FilterExpression, FilterParseError> {
    let Value::Object(map) = value else {
        return Err(FilterParseError::InvalidLogicalFilter {
            path: value.to_string(),
            reason: "expected an object".to_string(),
        });
    };

    parse_expression_object(map)
}

fn parse_bracket_path(field: &str) -> Option<Vec<&str>> {
    let (root, _) = field.split_once('[')?;
    root.parse::<LogicalOperator>().ok()?;

    let mut tokens = vec![root];
    let mut rest = &field[root.len()..];

    while let Some(stripped) = rest.strip_prefix('[') {
        let (token, next) = stripped.split_once(']')?;
        if token.is_empty() {
            return None;
        }
        tokens.push(token);
        rest = next;
    }

    rest.is_empty().then_some(tokens)
}

fn insert_bracket_value(root: &mut Value, path: &[&str], value: Value) {
    let Some((head, tail)) = path.split_first() else {
        *root = value;
        return;
    };

    if let Ok(index) = head.parse::<usize>() {
        if !root.is_array() {
            *root = Value::Array(Vec::new());
        }
        let items = root.as_array_mut().expect("root was just made an array");
        items.resize(index + 1, Value::Null);
        insert_bracket_value(&mut items[index], tail, value);
    } else {
        if !root.is_object() {
            *root = Value::Object(Map::new());
        }
        let entry = root
            .as_object_mut()
            .expect("root was just made an object")
            .entry((*head).to_string())
            .or_insert(Value::Null);
        insert_bracket_value(entry, tail, value);
    }
}

fn parse_query_value(value: String) -> Value {
    serde_json::from_str(&value).unwrap_or(Value::String(value))
}

/// Deserializes query parameters into a root AND filter expression group.
///
/// Expected format: `field=Operator:value`
/// Examples:
/// - `?status=Eq:Pending` -> Filter { field: "status", operator: Eq, value: String("Pending") }
/// - `?age=Gt:18` -> Filter { field: "age", operator: Gt, value: Int(18) }
/// - `?status=In:Active,Pending` -> Filter { field: "status", operator: In, value: Array([...]) }
/// - `?deleted_at=IsNull:` -> Filter { field: "deleted_at", operator: IsNull, value: Null }
pub fn filters_deserialize<'de, D>(
    deserializer: D,
) -> Result<Option<FilterExpressionGroup>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<HashMap<String, String>>::deserialize(deserializer)?;

    let map = match value {
        None => return Ok(None),
        Some(m) if m.is_empty() => return Ok(None),
        Some(m) => m,
    };

    let mut children = Vec::new();
    let mut bracket_filters = Value::Object(Map::new());

    for (field, raw_value) in map {
        if RESERVED_QUERY_PARAMS.contains(&field.as_str()) {
            continue;
        }

        if let Some(path) = parse_bracket_path(&field) {
            insert_bracket_value(&mut bracket_filters, &path, parse_query_value(raw_value));
            continue;
        }

        let filter = parse_filter_from_raw(&field, &raw_value).map_err(D::Error::custom)?;
        children.push(FilterExpression::Condition(filter));
    }

    if bracket_filters
        .as_object()
        .is_some_and(|map| !map.is_empty())
    {
        children.push(parse_expression_value(&bracket_filters).map_err(D::Error::custom)?);
    }

    if children.is_empty() {
        Ok(None)
    } else {
        Ok(Some(FilterExpressionGroup::and(children)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct TestParams {
        #[serde(flatten, default, deserialize_with = "filters_deserialize")]
        filters: Option<FilterExpressionGroup>,
    }

    #[test]
    fn test_parse_operator() {
        assert_eq!("Eq".parse(), Ok(FilterOperator::Eq));
        assert_eq!("Ne".parse(), Ok(FilterOperator::Ne));
        assert_eq!("Gt".parse(), Ok(FilterOperator::Gt));
        assert_eq!("Lt".parse(), Ok(FilterOperator::Lt));
        assert_eq!("Gte".parse(), Ok(FilterOperator::Gte));
        assert_eq!("Lte".parse(), Ok(FilterOperator::Lte));
        assert_eq!("Like".parse(), Ok(FilterOperator::Like));
        assert_eq!("ILike".parse(), Ok(FilterOperator::ILike));
        assert_eq!("In".parse(), Ok(FilterOperator::In));
        assert_eq!("NotIn".parse(), Ok(FilterOperator::NotIn));
        assert_eq!("IsNull".parse(), Ok(FilterOperator::IsNull));
        assert_eq!("IsNotNull".parse(), Ok(FilterOperator::IsNotNull));
        assert_eq!("Between".parse(), Ok(FilterOperator::Between));
        assert_eq!("Contains".parse(), Ok(FilterOperator::Contains));
        assert_eq!(
            "Invalid".parse::<FilterOperator>(),
            Err("Invalid".to_string())
        );
    }

    #[test]
    fn test_flat_filters_deserialize_into_root_and_group() {
        let params: TestParams = serde_json::from_value(serde_json::json!({
            "status": "Eq:active",
            "age": "Gte:18"
        }))
        .unwrap();

        let group = params.filters.unwrap();
        assert_eq!(group.operator, LogicalOperator::And);
        assert_eq!(group.children.len(), 2);
    }

    #[test]
    fn test_bracket_logical_filters_deserialize() {
        let params: TestParams = serde_json::from_value(serde_json::json!({
            "$and[0][$or][0][username][Eq]": "phiberber",
            "$and[0][$or][1][age][Gte]": "18",
            "$and[1][organizationId]": "550e8400-e29b-41d4-a716-446655440000"
        }))
        .unwrap();

        let root = params.filters.unwrap();
        assert_eq!(root.operator, LogicalOperator::And);
        assert_eq!(root.children.len(), 1);

        let FilterExpression::Group(and_group) = &root.children[0] else {
            panic!("expected nested AND group");
        };
        assert_eq!(and_group.operator, LogicalOperator::And);
        assert_eq!(and_group.children.len(), 2);
    }

    #[test]
    fn test_parse_filter_value_bool() {
        assert_eq!(parse_filter_value("true"), FilterValue::Bool(true));
        assert_eq!(parse_filter_value("false"), FilterValue::Bool(false));
        assert_eq!(parse_filter_value("TRUE"), FilterValue::Bool(true));
        assert_eq!(parse_filter_value("False"), FilterValue::Bool(false));
    }

    #[test]
    fn test_parse_filter_value_int() {
        assert_eq!(parse_filter_value("123"), FilterValue::Int(123));
        assert_eq!(parse_filter_value("-456"), FilterValue::Int(-456));
        assert_eq!(parse_filter_value("0"), FilterValue::Int(0));
    }

    #[test]
    fn test_parse_filter_value_float() {
        let pi = std::f64::consts::PI;
        assert_eq!(parse_filter_value(&pi.to_string()), FilterValue::Float(pi));
        assert_eq!(parse_filter_value("-2.5"), FilterValue::Float(-2.5));
    }

    #[test]
    fn test_parse_filter_value_uuid() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let expected = uuid::Uuid::parse_str(uuid_str).unwrap();
        assert_eq!(parse_filter_value(uuid_str), FilterValue::Uuid(expected));
    }

    #[test]
    fn test_parse_filter_value_string() {
        assert_eq!(
            parse_filter_value("hello"),
            FilterValue::String("hello".to_string())
        );
        assert_eq!(
            parse_filter_value("Pending"),
            FilterValue::String("Pending".to_string())
        );
    }

    #[test]
    fn test_parse_filter_value_empty() {
        assert_eq!(parse_filter_value(""), FilterValue::Null);
    }

    #[test]
    fn test_parse_array_values() {
        let result = parse_array_values("Active,Pending,Done");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], FilterValue::String("Active".to_string()));
        assert_eq!(result[1], FilterValue::String("Pending".to_string()));
        assert_eq!(result[2], FilterValue::String("Done".to_string()));
    }

    #[test]
    fn test_parse_array_values_mixed_types() {
        let result = parse_array_values("18,65");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], FilterValue::Int(18));
        assert_eq!(result[1], FilterValue::Int(65));
    }

    #[test]
    fn test_parse_filter_value_date() {
        assert_eq!(
            parse_filter_value("2025-12-02"),
            FilterValue::Date("2025-12-02".to_string())
        );
        assert_eq!(
            parse_filter_value("2024-01-15"),
            FilterValue::Date("2024-01-15".to_string())
        );
    }

    #[test]
    fn test_parse_filter_value_datetime() {
        // RFC 3339 with timezone - converted to UTC string format
        assert_eq!(
            parse_filter_value("2025-12-02T10:30:00Z"),
            FilterValue::DateTime("2025-12-02 10:30:00 UTC".to_string())
        );
        assert_eq!(
            parse_filter_value("2025-12-02T10:30:00+00:00"),
            FilterValue::DateTime("2025-12-02 10:30:00 UTC".to_string())
        );
        // Naive datetime without timezone - preserved as-is
        assert_eq!(
            parse_filter_value("2025-12-02T10:30:00"),
            FilterValue::DateTime("2025-12-02T10:30:00".to_string())
        );
        assert_eq!(
            parse_filter_value("2025-12-02 10:30:00"),
            FilterValue::DateTime("2025-12-02 10:30:00".to_string())
        );
    }

    #[test]
    fn test_parse_filter_value_datetime_timezone_conversion() {
        // Positive offset: +05:30 means 5 hours 30 minutes ahead of UTC
        // 10:30:00+05:30 -> 05:00:00 UTC
        assert_eq!(
            parse_filter_value("2025-12-02T10:30:00+05:30"),
            FilterValue::DateTime("2025-12-02 05:00:00 UTC".to_string())
        );

        // Negative offset: -05:00 means 5 hours behind UTC
        // 10:30:00-05:00 -> 15:30:00 UTC
        assert_eq!(
            parse_filter_value("2025-12-02T10:30:00-05:00"),
            FilterValue::DateTime("2025-12-02 15:30:00 UTC".to_string())
        );

        // Edge case: crossing date boundary
        // 02:00:00+05:00 on Dec 2 -> 21:00:00 UTC on Dec 1
        assert_eq!(
            parse_filter_value("2025-12-02T02:00:00+05:00"),
            FilterValue::DateTime("2025-12-01 21:00:00 UTC".to_string())
        );

        // Edge case: crossing date boundary the other way
        // 23:00:00-05:00 on Dec 2 -> 04:00:00 UTC on Dec 3
        assert_eq!(
            parse_filter_value("2025-12-02T23:00:00-05:00"),
            FilterValue::DateTime("2025-12-03 04:00:00 UTC".to_string())
        );
    }

    #[test]
    fn test_parse_filter_value_time() {
        assert_eq!(
            parse_filter_value("10:30:00"),
            FilterValue::Time("10:30:00".to_string())
        );
        assert_eq!(
            parse_filter_value("14:45"),
            FilterValue::Time("14:45".to_string())
        );
    }
}
