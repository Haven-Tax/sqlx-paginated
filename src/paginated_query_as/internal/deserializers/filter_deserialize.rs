use super::filter_bracket::{
    bracket_insert, bracket_tree_is_empty, bracket_tree_to_expression, is_logical_bracket_field,
    new_bracket_tree, parse_bracket_path, parse_query_value,
};
use super::filter_condition::{parse_condition, ConditionSyntax};
use super::FilterParseError;
use crate::paginated_query_as::models::{FilterExpression, FilterExpressionGroup};
use serde::{de::Error, Deserialize, Deserializer};
use std::collections::HashMap;

const RESERVED_QUERY_PARAMS: &[&str] = &[
    "page",
    "page_size",
    "sort_column",
    "sort_direction",
    "search",
    "search_columns",
];

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
    let mut bracket_filters = new_bracket_tree();

    for (field, raw_value) in map {
        if RESERVED_QUERY_PARAMS.contains(&field.as_str()) {
            continue;
        }

        if let Some(path) = parse_bracket_path(&field) {
            bracket_insert(
                &mut bracket_filters,
                &path,
                parse_query_value(raw_value),
            )
            .map_err(D::Error::custom)?;
            continue;
        }

        if is_logical_bracket_field(&field)
            || (field.starts_with('[') && (field.contains("$and") || field.contains("$or")))
        {
            return Err(D::Error::custom(FilterParseError::InvalidLogicalFilter {
                path: field,
                reason: "malformed logical filter path".to_string(),
            }));
        }

        let filter =
            parse_condition(&field, &raw_value, ConditionSyntax::Flat).map_err(D::Error::custom)?;
        children.push(FilterExpression::Condition(filter));
    }

    if !bracket_tree_is_empty(&bracket_filters) {
        children.push(
            bracket_tree_to_expression(bracket_filters).map_err(D::Error::custom)?,
        );
    }

    if children.is_empty() {
        Ok(None)
    } else {
        Ok(Some(FilterExpressionGroup::and(children)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::filter_value::{parse_array_values, parse_filter_value};
    use super::*;
    use crate::paginated_query_as::models::{
        FilterExpression, FilterOperator, FilterValue, LogicalOperator,
    };
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
            "$and[0][$or][0][username]": "Eq:phiberber",
            "$and[0][$or][1][age]": "Gte:18",
            "$and[1][organizationId]": "Eq:550e8400-e29b-41d4-a716-446655440000"
        }))
        .unwrap();

        let root = params.filters.unwrap();
        assert_eq!(root.operator, LogicalOperator::And);
        let conditions = root.collect_conditions();
        assert_eq!(conditions.len(), 3);
        assert!(conditions.iter().any(|filter| {
            filter.field == "username"
                && filter.operator == FilterOperator::Eq
                && filter.value == FilterValue::String("phiberber".to_string())
        }));
        assert!(conditions.iter().any(|filter| {
            filter.field == "age"
                && filter.operator == FilterOperator::Gte
                && filter.value == FilterValue::Int(18)
        }));
        assert!(conditions.iter().any(|filter| {
            filter.field == "organizationId" && filter.operator == FilterOperator::Eq
        }));
    }

    #[test]
    fn test_bracket_is_not_null_filter_deserialize() {
        let params: TestParams = serde_json::from_value(serde_json::json!({
            "$and[0][deleted_at]": "IsNotNull"
        }))
        .unwrap();

        let root = params.filters.unwrap();
        assert_eq!(root.operator, LogicalOperator::And);
        assert_eq!(root.children.len(), 1);

        let FilterExpression::Group(and_group) = &root.children[0] else {
            panic!("expected nested AND group");
        };
        assert_eq!(and_group.children.len(), 1);

        let FilterExpression::Condition(filter) = &and_group.children[0] else {
            panic!("expected condition");
        };
        assert_eq!(filter.field, "deleted_at");
        assert_eq!(filter.operator, FilterOperator::IsNotNull);
        assert_eq!(filter.value, FilterValue::Null);
    }

    #[test]
    fn test_old_bracket_operator_key_is_rejected() {
        let params = serde_json::from_value::<TestParams>(serde_json::json!({
            "$and[0][username][Eq]": "phiberber"
        }));

        assert!(params.is_err());
    }

    #[test]
    fn test_malformed_logical_bracket_key_is_rejected() {
        let params = serde_json::from_value::<TestParams>(serde_json::json!({
            "$or[1][$or[0][status]]": "Eq:Scheduled"
        }));

        assert!(params.is_err());
    }

    #[test]
    fn test_logical_bracket_key_with_opening_bracket_parses() {
        let params: TestParams = serde_json::from_value(serde_json::json!({
            "[$or][0][status]": "Eq:Pending"
        }))
        .unwrap();

        let root = params.filters.unwrap();
        assert_eq!(root.operator, LogicalOperator::And);
        assert_eq!(root.children.len(), 1);
        assert!(matches!(
            &root.children[0],
            FilterExpression::Group(group) if group.operator == LogicalOperator::Or
        ));
        assert_eq!(root.collect_conditions().len(), 1);
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
