use super::filter_condition::{combine_expression_children, parse_condition, ConditionSyntax};
use super::FilterParseError;
use crate::paginated_query_as::models::{
    FilterExpression, FilterExpressionGroup, LogicalOperator,
};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub(crate) enum BracketTree {
    #[default]
    Empty,
    Object(HashMap<String, BracketTree>),
    Array(Vec<BracketTree>),
    Leaf(Value),
}

pub(crate) fn parse_bracket_path(field: &str) -> Option<Vec<&str>> {
    let (root, mut rest) = if let Some(stripped) = field.strip_prefix('[') {
        let (root, next) = stripped.split_once(']')?;
        (root, next)
    } else {
        let (root, _) = field.split_once('[')?;
        (root, &field[root.len()..])
    };

    root.parse::<LogicalOperator>().ok()?;

    let mut tokens = vec![root];

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

pub(crate) fn is_logical_bracket_field(field: &str) -> bool {
    matches!(field, "$and" | "$or") || field.starts_with("$and[") || field.starts_with("$or[")
}

pub(crate) fn parse_query_value(value: String) -> Value {
    serde_json::from_str(&value).unwrap_or(Value::String(value))
}

pub(crate) fn bracket_insert(
    tree: &mut BracketTree,
    path: &[&str],
    value: Value,
) -> Result<(), FilterParseError> {
    let Some((head, tail)) = path.split_first() else {
        *tree = BracketTree::Leaf(value);
        return Ok(());
    };

    if let Ok(index) = head.parse::<usize>() {
        let BracketTree::Array(items) = tree else {
            *tree = BracketTree::Array(Vec::new());
            return bracket_insert(tree, path, value);
        };
        if items.len() <= index {
            items.resize(index + 1, BracketTree::Empty);
        }
        bracket_insert(&mut items[index], tail, value)
    } else {
        let BracketTree::Object(map) = tree else {
            *tree = BracketTree::Object(HashMap::new());
            return bracket_insert(tree, path, value);
        };
        let entry = map.entry((*head).to_string()).or_insert(BracketTree::Empty);
        bracket_insert(entry, tail, value)
    }
}

pub(crate) fn bracket_tree_to_expression(tree: BracketTree) -> Result<FilterExpression, FilterParseError> {
    match tree {
        BracketTree::Empty => Ok(FilterExpression::Group(FilterExpressionGroup::default())),
        BracketTree::Object(map) => object_to_expression(map),
        BracketTree::Array(_) => Err(FilterParseError::InvalidLogicalFilter {
            path: String::new(),
            reason: "expected an object".to_string(),
        }),
        BracketTree::Leaf(_) => Err(FilterParseError::InvalidLogicalFilter {
            path: String::new(),
            reason: "expected an object".to_string(),
        }),
    }
}

pub(crate) fn new_bracket_tree() -> BracketTree {
    BracketTree::Object(HashMap::new())
}

pub(crate) fn bracket_tree_is_empty(tree: &BracketTree) -> bool {
    match tree {
        BracketTree::Empty => true,
        BracketTree::Object(map) => map.is_empty(),
        _ => false,
    }
}

fn object_to_expression(map: HashMap<String, BracketTree>) -> Result<FilterExpression, FilterParseError> {
    let mut children = Vec::new();

    for (field, node) in map {
        if let Ok(operator) = field.parse::<LogicalOperator>() {
            children.push(logical_group_to_expression(operator, node)?);
        } else {
            children.extend(field_to_expressions(&field, node)?);
        }
    }

    Ok(combine_expression_children(children))
}

fn logical_group_to_expression(
    operator: LogicalOperator,
    node: BracketTree,
) -> Result<FilterExpression, FilterParseError> {
    let BracketTree::Array(items) = node else {
        return Err(FilterParseError::InvalidLogicalFilter {
            path: operator.as_str().to_string(),
            reason: "expected an array".to_string(),
        });
    };

    let children = items
        .into_iter()
        .map(bracket_tree_to_expression)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(FilterExpression::Group(FilterExpressionGroup {
        operator,
        children,
    }))
}

fn field_to_expressions(field: &str, node: BracketTree) -> Result<Vec<FilterExpression>, FilterParseError> {
    let BracketTree::Leaf(Value::String(raw_value)) = node else {
        return Err(FilterParseError::InvalidFilterFormat {
            field: field.to_string(),
            raw_value: match node {
                BracketTree::Leaf(value) => value.to_string(),
                _ => String::new(),
            },
        });
    };

    Ok(vec![FilterExpression::Condition(parse_condition(
        field,
        &raw_value,
        ConditionSyntax::BracketEncoded,
    )?)])
}
