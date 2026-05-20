use super::filter_value::{parse_array_values, parse_filter_value};
use super::FilterParseError;
use crate::paginated_query_as::models::{Filter, FilterExpression, FilterExpressionGroup, FilterOperator, FilterValue};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConditionSyntax {
    /// `field=Operator:value` — the colon separator is required.
    Flat,
    /// Bracket fields: `Operator:value`, bare `IsNotNull`, optional value segments.
    BracketEncoded,
}

pub(crate) fn parse_condition(
    field: &str,
    raw_value: &str,
    syntax: ConditionSyntax,
) -> Result<Filter, FilterParseError> {
    let (operator_str, value_str) = match syntax {
        ConditionSyntax::Flat => {
            let (operator_str, value_str) =
                raw_value
                    .split_once(':')
                    .ok_or_else(|| FilterParseError::InvalidFilterFormat {
                        field: field.to_string(),
                        raw_value: raw_value.to_string(),
                    })?;
            (operator_str, Some(value_str))
        }
        ConditionSyntax::BracketEncoded => match raw_value.split_once(':') {
            Some((operator_str, value_str)) => (operator_str, Some(value_str)),
            None => (raw_value, None),
        },
    };

    let operator = operator_str
        .parse::<FilterOperator>()
        .map_err(|raw_operator| FilterParseError::InvalidOperator {
            field: field.to_string(),
            raw_operator,
        })?;

    let value = coerce_filter_value(operator, value_str, syntax, field, raw_value)?;

    Ok(Filter {
        field: field.to_string(),
        operator,
        value,
    })
}

fn coerce_filter_value(
    operator: FilterOperator,
    value_str: Option<&str>,
    syntax: ConditionSyntax,
    field: &str,
    raw_value: &str,
) -> Result<FilterValue, FilterParseError> {
    let invalid_format = || FilterParseError::InvalidFilterFormat {
        field: field.to_string(),
        raw_value: raw_value.to_string(),
    };

    match operator {
        FilterOperator::IsNull | FilterOperator::IsNotNull => Ok(FilterValue::Null),
        FilterOperator::In | FilterOperator::NotIn | FilterOperator::Between => {
            let value_str = value_str.ok_or_else(invalid_format)?;

            if value_str.is_empty() && syntax == ConditionSyntax::BracketEncoded {
                Ok(FilterValue::Array(Vec::new()))
            } else {
                Ok(FilterValue::Array(parse_array_values(value_str)))
            }
        }
        _ => {
            let value_str = value_str.ok_or_else(invalid_format)?;
            Ok(parse_filter_value(value_str))
        }
    }
}

pub(crate) fn combine_expression_children(mut children: Vec<FilterExpression>) -> FilterExpression {
    match children.len() {
        0 => FilterExpression::Group(FilterExpressionGroup::default()),
        1 => children.remove(0),
        _ => FilterExpression::Group(FilterExpressionGroup::and(children)),
    }
}
