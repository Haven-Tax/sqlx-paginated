use crate::paginated_query_as::internal::{
    filters_deserialize, page_deserialize, page_size_deserialize, quote_identifier,
    FilterParseError, QueryPaginationParams, QuerySearchParams, QuerySortParams,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

impl From<uuid::Uuid> for FilterValue {
    fn from(value: uuid::Uuid) -> Self {
        FilterValue::Uuid(value)
    }
}

impl<T: Clone + Into<FilterValue>> From<&[T]> for FilterValue {
    fn from(value: &[T]) -> Self {
        FilterValue::Array(value.iter().map(|v| v.clone().into()).collect())
    }
}

impl<T: Into<FilterValue>> From<Vec<T>> for FilterValue {
    fn from(value: Vec<T>) -> Self {
        FilterValue::Array(value.into_iter().map(|v| v.into()).collect())
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct PaginatedResponse<T> {
    pub records: Vec<T>,

    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<QueryPaginationParams>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_pages: Option<i64>,
}

/// Query parameters for paginated queries, deserialized from query strings.
///
/// Use `TryInto<QueryParams>` to convert this to `QueryParams` with validation.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct FlatQueryParams {
    #[serde(default, deserialize_with = "page_deserialize")]
    pub page: Option<i64>,
    #[serde(default, deserialize_with = "page_size_deserialize")]
    pub page_size: Option<i64>,
    #[serde(flatten)]
    pub sort: Option<QuerySortParams>,
    #[serde(flatten)]
    pub search: Option<QuerySearchParams>,
    #[serde(flatten, default, deserialize_with = "filters_deserialize")]
    pub filters: Option<FilterExpressionGroup>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum FilterOperator {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
    Like,
    ILike,
    In,
    NotIn,
    IsNull,
    IsNotNull,
    Between,
    Contains,
}

impl FilterOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "Eq",
            Self::Ne => "Ne",
            Self::Gt => "Gt",
            Self::Lt => "Lt",
            Self::Gte => "Gte",
            Self::Lte => "Lte",
            Self::Like => "Like",
            Self::ILike => "ILike",
            Self::In => "In",
            Self::NotIn => "NotIn",
            Self::IsNull => "IsNull",
            Self::IsNotNull => "IsNotNull",
            Self::Between => "Between",
            Self::Contains => "Contains",
        }
    }
}

impl FromStr for FilterOperator {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "Eq" => Ok(Self::Eq),
            "Ne" => Ok(Self::Ne),
            "Gt" => Ok(Self::Gt),
            "Lt" => Ok(Self::Lt),
            "Gte" => Ok(Self::Gte),
            "Lte" => Ok(Self::Lte),
            "Like" => Ok(Self::Like),
            "ILike" => Ok(Self::ILike),
            "In" => Ok(Self::In),
            "NotIn" => Ok(Self::NotIn),
            "IsNull" => Ok(Self::IsNull),
            "IsNotNull" => Ok(Self::IsNotNull),
            "Between" => Ok(Self::Between),
            "Contains" => Ok(Self::Contains),
            _ => Err(value.to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum FilterValue {
    String(String),
    Uuid(uuid::Uuid),
    Int(i64),
    Float(f64),
    Bool(bool),
    DateTime(String),
    Date(String),
    Time(String),
    Array(Vec<FilterValue>),
    Null,
}

impl FilterValue {
    pub fn to_bindable_string(&self) -> String {
        match self {
            FilterValue::String(s) => s.clone(),
            FilterValue::Int(i) => i.to_string(),
            FilterValue::Float(f) => f.to_string(),
            FilterValue::Bool(b) => b.to_string(),
            FilterValue::Uuid(uuid) => uuid.to_string(),
            FilterValue::DateTime(dt) => dt.clone(),
            FilterValue::Date(d) => d.clone(),
            FilterValue::Time(t) => t.clone(),
            FilterValue::Array(arr) => arr
                .first()
                .map(|v| v.to_bindable_string())
                .unwrap_or_default(),
            FilterValue::Null => String::new(),
        }
    }

    pub fn to_bindable_strings(&self) -> Vec<String> {
        match self {
            FilterValue::Array(arr) => arr.iter().map(|v| v.to_bindable_string()).collect(),
            _ => vec![self.to_bindable_string()],
        }
    }

    pub fn to_sql_string(&self) -> String {
        match self {
            FilterValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            FilterValue::Int(i) => i.to_string(),
            FilterValue::Float(f) => f.to_string(),
            FilterValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            FilterValue::DateTime(dt) => format!("'{}'", dt),
            FilterValue::Date(d) => format!("'{}'", d),
            FilterValue::Time(t) => format!("'{}'", t),
            FilterValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| v.to_sql_string()).collect();
                format!("({})", items.join(", "))
            }
            FilterValue::Null => "NULL".to_string(),
            FilterValue::Uuid(uuid) => format!("'{}'", uuid.to_string()),
        }
    }

    /// Converts the filter value to a corresponding FieldType for type casting.
    /// This is used as a fallback when the struct field type cannot be inferred
    /// (e.g., Option<T> fields that default to None).
    pub fn to_field_type(&self) -> crate::paginated_query_as::internal::FieldType {
        use crate::paginated_query_as::internal::FieldType;
        match self {
            FilterValue::String(_) => FieldType::String,
            FilterValue::Int(_) => FieldType::Int,
            FilterValue::Float(_) => FieldType::Float,
            FilterValue::Bool(_) => FieldType::Bool,
            FilterValue::Uuid(_) => FieldType::Uuid,
            FilterValue::DateTime(_) => FieldType::DateTime,
            FilterValue::Date(_) => FieldType::Date,
            FilterValue::Time(_) => FieldType::Time,
            FilterValue::Array(arr) => arr
                .first()
                .map(|v| v.to_field_type())
                .unwrap_or(FieldType::Unknown),
            FilterValue::Null => FieldType::Unknown,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Filter {
    pub field: String,
    pub operator: FilterOperator,
    pub value: FilterValue,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum LogicalOperator {
    And,
    Or,
}

impl LogicalOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::And => "$and",
            Self::Or => "$or",
        }
    }
}

impl FromStr for LogicalOperator {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "$and" => Ok(Self::And),
            "$or" => Ok(Self::Or),
            _ => Err(value.to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum FilterExpression {
    Condition(Filter),
    Group(FilterExpressionGroup),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FilterExpressionGroup {
    pub operator: LogicalOperator,
    pub children: Vec<FilterExpression>,
}

impl FilterExpressionGroup {
    pub fn and(children: Vec<FilterExpression>) -> Self {
        Self {
            operator: LogicalOperator::And,
            children,
        }
    }

    pub fn or(children: Vec<FilterExpression>) -> Self {
        Self {
            operator: LogicalOperator::Or,
            children,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    pub fn collect_conditions(&self) -> Vec<Filter> {
        let mut filters = Vec::new();
        self.collect_conditions_into(&mut filters);
        filters
    }

    fn collect_conditions_into(&self, filters: &mut Vec<Filter>) {
        for child in &self.children {
            match child {
                FilterExpression::Condition(filter) => filters.push(filter.clone()),
                FilterExpression::Group(group) => group.collect_conditions_into(filters),
            }
        }
    }

    pub fn partition_field(self, field: &str) -> (Vec<Filter>, Self) {
        let mut extracted = Vec::new();
        let children = partition_field_children(self.children, field, &mut extracted);
        (
            extracted,
            Self {
                operator: self.operator,
                children,
            },
        )
    }
}

fn partition_field_children(
    children: Vec<FilterExpression>,
    field: &str,
    extracted: &mut Vec<Filter>,
) -> Vec<FilterExpression> {
    let mut kept = Vec::new();
    for child in children {
        match child {
            FilterExpression::Condition(filter) if filter.field == field => extracted.push(filter),
            FilterExpression::Condition(filter) => {
                kept.push(FilterExpression::Condition(filter));
            }
            FilterExpression::Group(group) => {
                let nested = partition_field_children(group.children, field, extracted);
                if !nested.is_empty() {
                    kept.push(FilterExpression::Group(FilterExpressionGroup {
                        operator: group.operator,
                        children: nested,
                    }));
                }
            }
        }
    }
    kept
}

impl Default for FilterExpressionGroup {
    fn default() -> Self {
        Self::and(Vec::new())
    }
}

/// Validated query parameters for paginated queries.
///
/// Created from `FlatQueryParams` via `TryFrom`/`TryInto`.
#[derive(Clone, Debug)]
pub struct QueryParams<'q, T> {
    pub pagination: Option<QueryPaginationParams>,
    pub sort: Option<QuerySortParams>,
    pub search: QuerySearchParams,
    pub filters: FilterExpressionGroup,
    pub(crate) _phantom: PhantomData<&'q T>,
}

impl<'q, T> Default for QueryParams<'q, T> {
    fn default() -> Self {
        Self {
            pagination: None,
            sort: None,
            search: QuerySearchParams::default(),
            filters: FilterExpressionGroup::default(),
            _phantom: PhantomData,
        }
    }
}

impl<'q, T> TryFrom<FlatQueryParams> for QueryParams<'q, T> {
    type Error = FilterParseError;

    fn try_from(params: FlatQueryParams) -> Result<Self, Self::Error> {
        let pagination = match (params.page, params.page_size) {
            (None, None) => None,
            (Some(page), Some(page_size)) => Some(QueryPaginationParams { page, page_size }),
            (Some(_), None) => {
                return Err(FilterParseError::IncompletePagination {
                    provided: "page".to_string(),
                })
            }
            (None, Some(_)) => {
                return Err(FilterParseError::IncompletePagination {
                    provided: "page_size".to_string(),
                })
            }
        };

        Ok(QueryParams {
            pagination,
            sort: params.sort,
            search: params.search.unwrap_or_default(),
            filters: params.filters.unwrap_or_default(),
            _phantom: PhantomData,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum QuerySortDirection {
    Ascending,
    #[default]
    Descending,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortItem {
    Column(String),
    Expression(String),
}

impl SortItem {
    pub fn column(name: &str) -> Self {
        SortItem::Column(name.to_string())
    }

    pub fn expression(expr: &str) -> Self {
        SortItem::Expression(expr.to_string())
    }

    pub fn to_sql(&self, table_alias: &str) -> String {
        match self {
            SortItem::Column(col) => format!(
                "{}.{}",
                quote_identifier(table_alias),
                quote_identifier(col)
            ),
            SortItem::Expression(expr) => expr.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortEntry {
    pub item: SortItem,
    pub direction: QuerySortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryBuildError {
    pub message: String,
}

impl QueryBuildError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for QueryBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for QueryBuildError {}

#[derive(Debug)]
pub enum PaginatedQueryError {
    QueryBuild(QueryBuildError),
    Sqlx(sqlx::Error),
}

impl fmt::Display for PaginatedQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PaginatedQueryError::QueryBuild(e) => write!(f, "{e}"),
            PaginatedQueryError::Sqlx(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PaginatedQueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PaginatedQueryError::QueryBuild(e) => Some(e),
            PaginatedQueryError::Sqlx(e) => Some(e),
        }
    }
}

impl From<QueryBuildError> for PaginatedQueryError {
    fn from(value: QueryBuildError) -> Self {
        Self::QueryBuild(value)
    }
}

impl From<sqlx::Error> for PaginatedQueryError {
    fn from(value: sqlx::Error) -> Self {
        Self::Sqlx(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paginated_query_as::internal::FieldType;

    #[test]
    fn test_to_field_type_int() {
        assert_eq!(FilterValue::Int(42).to_field_type(), FieldType::Int);
    }

    #[test]
    fn test_to_field_type_float() {
        assert_eq!(
            FilterValue::Float(std::f64::consts::PI).to_field_type(),
            FieldType::Float
        );
    }

    #[test]
    fn test_to_field_type_bool() {
        assert_eq!(FilterValue::Bool(true).to_field_type(), FieldType::Bool);
    }

    #[test]
    fn test_to_field_type_string() {
        assert_eq!(
            FilterValue::String("test".to_string()).to_field_type(),
            FieldType::String
        );
    }

    #[test]
    fn test_to_field_type_uuid() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(FilterValue::Uuid(uuid).to_field_type(), FieldType::Uuid);
    }

    #[test]
    fn test_to_field_type_datetime() {
        assert_eq!(
            FilterValue::DateTime("2025-12-02T10:30:00Z".to_string()).to_field_type(),
            FieldType::DateTime
        );
    }

    #[test]
    fn test_to_field_type_date() {
        assert_eq!(
            FilterValue::Date("2025-12-02".to_string()).to_field_type(),
            FieldType::Date
        );
    }

    #[test]
    fn test_to_field_type_time() {
        assert_eq!(
            FilterValue::Time("10:30:00".to_string()).to_field_type(),
            FieldType::Time
        );
    }

    #[test]
    fn test_to_field_type_array_uses_first_element() {
        let arr = FilterValue::Array(vec![FilterValue::Int(1), FilterValue::Int(2)]);
        assert_eq!(arr.to_field_type(), FieldType::Int);
    }

    #[test]
    fn test_to_field_type_empty_array_returns_unknown() {
        let arr = FilterValue::Array(vec![]);
        assert_eq!(arr.to_field_type(), FieldType::Unknown);
    }

    #[test]
    fn test_to_field_type_null_returns_unknown() {
        assert_eq!(FilterValue::Null.to_field_type(), FieldType::Unknown);
    }

    #[test]
    fn test_sort_item_column_to_sql_quotes_identifiers() {
        assert_eq!(
            SortItem::column("display\"name").to_sql("user\"records"),
            "\"user\"\"records\".\"display\"\"name\""
        );
    }

    #[test]
    fn test_partition_field_extracts_and_prunes() {
        let group = FilterExpressionGroup::and(vec![
            FilterExpression::Condition(Filter {
                field: "status".to_string(),
                operator: FilterOperator::Eq,
                value: FilterValue::String("Pending".to_string()),
            }),
            FilterExpression::Condition(Filter {
                field: "reviewable_by".to_string(),
                operator: FilterOperator::Eq,
                value: FilterValue::Uuid(uuid::Uuid::nil()),
            }),
            FilterExpression::Group(FilterExpressionGroup::or(vec![
                FilterExpression::Condition(Filter {
                    field: "reviewable_by".to_string(),
                    operator: FilterOperator::Eq,
                    value: FilterValue::Uuid(uuid::Uuid::nil()),
                }),
                FilterExpression::Condition(Filter {
                    field: "created_by".to_string(),
                    operator: FilterOperator::Eq,
                    value: FilterValue::Uuid(uuid::Uuid::nil()),
                }),
            ])),
        ]);

        let (extracted, remaining) = group.partition_field("reviewable_by");

        assert_eq!(extracted.len(), 2);
        assert!(extracted.iter().all(|f| f.field == "reviewable_by"));
        assert_eq!(remaining.children.len(), 2);
        assert_eq!(
            remaining.children[0],
            FilterExpression::Condition(Filter {
                field: "status".to_string(),
                operator: FilterOperator::Eq,
                value: FilterValue::String("Pending".to_string()),
            })
        );
        match &remaining.children[1] {
            FilterExpression::Group(g) => {
                assert_eq!(g.operator, LogicalOperator::Or);
                assert_eq!(g.children.len(), 1);
            }
            _ => panic!("expected nested or group"),
        }
    }

    #[test]
    fn test_sort_item_expression_to_sql_preserves_expression() {
        assert_eq!(
            SortItem::expression("LOWER(name)").to_sql("ignored"),
            "LOWER(name)"
        );
    }

    #[test]
    fn test_query_build_error_converts_to_paginated_query_error() {
        let build_err = QueryBuildError::new("unknown or disallowed filter column: foo");
        let paginated_err: PaginatedQueryError = build_err.into();
        assert!(matches!(paginated_err, PaginatedQueryError::QueryBuild(e) if e.message.contains("foo")));
    }
}
