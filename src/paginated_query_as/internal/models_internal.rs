use crate::paginated_query_as::internal::{
    default_search_columns, default_sort_column, default_sort_direction,
    search_columns_deserialize, search_deserialize,
};

use crate::QuerySortDirection;
use serde::{Deserialize, Serialize};

/// Pagination parameters for queries.
///
/// This struct is only created when both `page` and `page_size` are provided.
/// If neither is provided, pagination is disabled (no LIMIT/OFFSET).
#[derive(Serialize, Clone, Debug)]
pub struct QueryPaginationParams {
    pub page: i64,
    pub page_size: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct QuerySortParams {
    #[serde(default = "default_sort_direction")]
    pub sort_direction: QuerySortDirection,
    #[serde(default = "default_sort_column")]
    pub sort_column: String,
}

impl Default for QuerySortParams {
    fn default() -> Self {
        Self {
            sort_direction: default_sort_direction(),
            sort_column: default_sort_column(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct QuerySearchParams {
    #[serde(deserialize_with = "search_deserialize")]
    pub search: Option<String>,
    #[serde(
        deserialize_with = "search_columns_deserialize",
        default = "default_search_columns"
    )]
    pub search_columns: Option<Vec<String>>,
}

impl Default for QuerySearchParams {
    fn default() -> Self {
        Self {
            search: None,
            search_columns: default_search_columns(),
        }
    }
}


use crate::paginated_query_as::internal::internal_utils::FieldType;

/// Builder for configuring a virtual column within the closure.
///
/// Used with `QueryBuilder::with_virtual_column()` to define virtual columns
/// that map to SQL expressions, optionally with required JOIN clauses.
#[derive(Debug, Clone)]
pub struct VirtualColumnBuilder {
    pub(crate) joins: Vec<String>,
    pub(crate) column_type: FieldType,
}

impl Default for VirtualColumnBuilder {
    fn default() -> Self {
        Self {
            joins: Vec::new(),
            column_type: FieldType::String,
        }
    }
}

impl VirtualColumnBuilder {
    /// Creates a new VirtualColumnBuilder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a JOIN clause required for this virtual column.
    ///
    /// Multiple joins can be added by calling this method multiple times.
    /// Joins are only included in the final query if the virtual column
    /// is actually used in search or filter operations.
    ///
    /// # Example
    /// ```rust,ignore
    /// // Used within QueryBuilder::with_virtual_column closure:
    /// // IMPORTANT: When using with PaginatedQueryBuilder, reference "base_query" not the original table
    /// builder.with_virtual_column("counterparty_name", |vc| {
    ///     vc.with_join("LEFT JOIN counterparty ON counterparty.id = base_query.counterparty_id");
    ///     "counterparty.legal_name"
    /// })
    /// ```
    pub fn with_join(&mut self, clause: impl Into<String>) -> &mut Self {
        self.joins.push(clause.into());
        self
    }

    /// Set the column type for proper type casting in filters.
    ///
    /// Defaults to `FieldType::String` if not specified.
    pub fn with_column_type(&mut self, column_type: FieldType) -> &mut Self {
        self.column_type = column_type;
        self
    }
}

/// Stored virtual column definition.
///
/// Represents a virtual column that maps to a SQL expression,
/// with optional JOIN clauses that are activated when the column is used.
#[derive(Debug, Clone)]
pub struct VirtualColumn {
    /// The SQL expression (e.g., "counterparty.name" or "(amount_micros / 1000000)::money")
    pub expression: String,
    /// JOIN clauses needed for this virtual column
    pub joins: Vec<String>,
    /// Column type for proper type casting in filters
    pub column_type: FieldType,
}
