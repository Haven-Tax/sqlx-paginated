use crate::paginated_query_as::internal::{
    ColumnProtection, FieldType, QueryDialect, VirtualColumn, VirtualColumnBuilder, get_struct_field_meta,
};
use crate::paginated_query_as::models::{FilterOperator, FilterValue, QueryParams, SortEntry, SortItem, QuerySortDirection};
use serde::Serialize;
use sqlx::{Arguments, Database, Encode, Type};
use std::collections::HashMap;
use std::marker::PhantomData;

/// Configuration for outer query wrapping.
///
/// When window functions need filtering, the query is wrapped in a subquery.
/// This struct configures the outer query's SELECT, WHERE, and GROUP BY clauses.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OuterQuery {
    pub conditions: Vec<String>,
    pub select_columns: Vec<String>,
    pub group_by_columns: Vec<String>,
}

impl OuterQuery {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if this outer query has any configuration.
    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty() && self.select_columns.is_empty() && self.group_by_columns.is_empty()
    }
}

/// Builder for configuring the outer query in a callback pattern.
///
/// Used with `QueryBuilder::with_outer()` to configure outer query conditions,
/// selects, and group by clauses.
#[derive(Debug, Default)]
pub struct OuterQueryBuilder {
    pub(crate) conditions: Vec<String>,
    pub(crate) select_columns: Vec<String>,
    pub(crate) group_by_columns: Vec<String>,
}

impl OuterQueryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a condition to the outer WHERE clause.
    ///
    /// Use this for filtering on window function results like RANK(), ROW_NUMBER(), etc.
    pub fn condition(&mut self, condition: impl Into<String>) -> &mut Self {
        self.conditions.push(condition.into());
        self
    }

    /// Adds a column to the outer SELECT clause.
    ///
    /// Use this for aggregations that should be applied after filtering.
    pub fn select(&mut self, column: impl Into<String>) -> &mut Self {
        self.select_columns.push(column.into());
        self
    }

    /// Adds a column to the outer GROUP BY clause.
    ///
    /// Use this with aggregation selects to group the results.
    pub fn group_by(&mut self, column: impl Into<String>) -> &mut Self {
        self.group_by_columns.push(column.into());
        self
    }

    /// Converts to OuterQuery
    pub(crate) fn build(self) -> OuterQuery {
        OuterQuery {
            conditions: self.conditions,
            select_columns: self.select_columns,
            group_by_columns: self.group_by_columns,
        }
    }
}

/// Result of building a query with conditions, arguments, and joins.
///
/// This struct is returned by `QueryBuilder::build()` and contains all the
/// information needed to construct the final SQL query.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryBuildResult<'q, DB: Database> {
    /// SQL conditions to be combined with AND in the WHERE clause
    pub conditions: Vec<String>,
    /// Database-specific arguments for parameter binding
    pub arguments: DB::Arguments<'q>,
    /// JOIN clauses that should be included in the query (in order)
    pub joins: Vec<String>,
    /// Table alias for column references in SELECT clause (e.g., "base_query")
    pub table_alias: String,
    /// Custom SELECT columns (overrides default "table_alias.*")
    pub select_columns: Option<Vec<String>>,
    /// GROUP BY columns
    pub group_by_columns: Option<Vec<String>>,
    /// DISTINCT ON columns (PostgreSQL-specific)
    pub distinct_on_columns: Option<Vec<String>>,
    /// Sort entries for ORDER BY clause
    pub sort_entries: Vec<SortEntry>,
    /// Outer query configuration (for window function filtering and aggregations)
    pub outer_query: Option<OuterQuery>,
}

pub struct QueryBuilder<'q, T, DB: Database> {
    pub conditions: Vec<String>,
    pub arguments: DB::Arguments<'q>,
    pub mappers: HashMap<String, Box<dyn Fn(&str, &str) -> (String, Option<String>)>>,
    pub(crate) valid_columns: Vec<String>,
    pub(crate) field_meta: HashMap<String, FieldType>,
    pub(crate) protection: Option<ColumnProtection>,
    pub(crate) protection_enabled: bool,
    pub(crate) column_validation_enabled: bool,
    pub(crate) dialect: Box<dyn QueryDialect>,
    pub(crate) _phantom: PhantomData<&'q T>,
    /// Virtual columns (e.g., from joins or computed expressions)
    pub(crate) virtual_columns: HashMap<String, VirtualColumn>,
    /// Active JOIN clauses (in order, no duplicates)
    pub(crate) active_joins: Vec<String>,
    /// Table alias for column references (e.g., "base_query" for CTE contexts)
    pub(crate) table_alias: String,
    /// Explicit column cast overrides (takes precedence over inferred types)
    pub(crate) column_cast_overrides: HashMap<String, FieldType>,
    /// Custom SELECT columns (overrides default "table_alias.*")
    pub(crate) select_columns: Option<Vec<String>>,
    /// GROUP BY columns
    pub(crate) group_by_columns: Option<Vec<String>>,
    /// DISTINCT ON columns (PostgreSQL-specific)
    pub(crate) distinct_on_columns: Option<Vec<String>>,
    /// Sort entries for ORDER BY clause
    pub(crate) sort_entries: Vec<SortEntry>,
    /// Outer query configuration (for window function filtering and aggregations)
    pub(crate) outer_query: Option<OuterQuery>,
}

impl<'q, T, DB> QueryBuilder<'q, T, DB>
where
    T: Default + Serialize,
    DB: Database,
    String: for<'a> Encode<'a, DB> + Type<DB>,
{
    /// Checks if a column exists in the list of valid columns for T struct
    /// or is a registered computed property.
    ///
    /// # Arguments
    ///
    /// * `column` - The name of the column to check
    ///
    /// # Returns
    ///
    /// Returns `true` if the column exists in the valid columns list or is a virtual column.
    pub(crate) fn has_column(&self, column: &str) -> bool {
        self.valid_columns.contains(&column.to_string())
            || self.virtual_columns.contains_key(column)
    }

    fn is_column_safe(&self, column: &str) -> bool {
        // Virtual columns bypass validation (developer-trusted)
        if self.virtual_columns.contains_key(column) {
            return true;
        }

        let column_exists = if self.column_validation_enabled { 
            self.valid_columns.contains(&column.to_string())
        } else { 
            true 
        };

        if !self.protection_enabled {
            return column_exists;
        }

        match &self.protection {
            Some(protection) => column_exists && protection.is_safe(column),
            None => column_exists,
        }
    }

    /// Activates joins for a virtual column (adds to active_joins if not already present).
    fn activate_joins(&mut self, virtual_col: &VirtualColumn) {
        for join in &virtual_col.joins {
            if !self.active_joins.contains(join) {
                self.active_joins.push(join.clone());
            }
        }
    }

    /// Returns the active JOIN clauses in the order they were added.
    pub fn get_active_joins(&self) -> Vec<String> {
        self.active_joins.clone()
    }

    /// Sets a table alias for column references.
    ///
    /// When using `QueryBuilder` with `PaginatedQueryBuilder`, the query is wrapped in a CTE
    /// named `base_query`. If you have JOINs that introduce columns with the same names as
    /// your main table, you need to use an alias to avoid ambiguity.
    ///
    /// By default, the alias is set to `"base_query"`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::Postgres;
    /// use serde::Serialize;
    /// use sqlx_paginated::QueryBuilder;
    ///
    /// #[derive(Serialize, Default)]
    /// struct Order {
    ///     id: i64,
    ///     organization_id: i64,
    /// }
    ///
    /// let result = QueryBuilder::<Order, Postgres>::new()
    ///     .with_table_alias("orders")  // Columns become "orders"."column_name"
    ///     .build();
    /// ```
    pub fn with_table_alias(mut self, alias: impl Into<String>) -> Self {
        self.table_alias = alias.into();
        self
    }

    /// Overrides the cast type for a specific column when filtering.
    ///
    /// This is useful when the column type cannot be inferred from the struct
    /// (e.g., `Option<T>` fields that serialize to null) or when the parsed filter
    /// value type doesn't match the actual database column type.
    ///
    /// This override takes precedence over types inferred from:
    /// - Struct field metadata
    /// - Virtual column types
    /// - Filter value type inference
    ///
    /// # Arguments
    ///
    /// * `column` - The column name to override the cast for
    /// * `field_type` - The `FieldType` to use for casting this column
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::Postgres;
    /// use serde::Serialize;
    /// use sqlx_paginated::{QueryBuilder, FieldType};
    ///
    /// #[derive(Serialize, Default)]
    /// struct Bill {
    ///     id: i64,
    ///     gl_code: Option<String>,  // Serializes to null, type is Unknown
    /// }
    ///
    /// // Without override: gl_code=Eq:123 would generate "gl_code" = $1::bigint
    /// // With override: gl_code=Eq:123 generates "gl_code" = $1 (no cast for String)
    /// let query_builder = QueryBuilder::<Bill, Postgres>::new()
    ///     .with_column_cast("gl_code", FieldType::String)
    ///     .build();
    /// ```
    pub fn with_column_cast(mut self, column: impl Into<String>, field_type: FieldType) -> Self {
        self.column_cast_overrides.insert(column.into(), field_type);
        self
    }

    /// Formats a column name with the table alias.
    /// Returns `"alias"."column"` format.
    fn format_column(&self, column: &str) -> String {
        format!("{}.{}", self.dialect.quote_identifier(&self.table_alias), self.dialect.quote_identifier(column))
    }

    /// Registers a virtual column that can be used in search and filter operations.
    ///
    /// Virtual columns allow you to search and filter by columns that don't exist directly
    /// in your struct, such as columns from joined tables or computed SQL expressions.
    ///
    /// The closure receives a `VirtualColumnBuilder` that can be used to configure joins,
    /// and returns the SQL expression to use for this column.
    ///
    /// **IMPORTANT**: When using with `PaginatedQueryBuilder`, JOINs must reference `base_query`
    /// (not the original table name) because the query is wrapped in a CTE:
    /// ```sql
    /// WITH base_query AS (SELECT * FROM your_table WHERE ...) SELECT * FROM base_query LEFT JOIN ...
    /// ```
    ///
    /// # Arguments
    ///
    /// * `name` - The virtual column name to use in search_columns and filters
    /// * `f` - Closure that configures the column and returns the SQL expression
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::Postgres;
    /// use serde::Serialize;
    /// use sqlx_paginated::QueryBuilder;
    ///
    /// #[derive(Serialize, Default)]
    /// struct Order {
    ///     id: i64,
    ///     counterparty_id: i64,
    /// }
    ///
    /// // When using with PaginatedQueryBuilder, reference base_query in JOINs:
    /// let result = QueryBuilder::<Order, Postgres>::new()
    ///     .with_virtual_column("counterparty_name", |vc| {
    ///         // Note: Use "base_query" not "orders" when used with PaginatedQueryBuilder
    ///         vc.with_join("LEFT JOIN counterparty ON counterparty.id = base_query.counterparty_id");
    ///         "counterparty.legal_name"
    ///     })
    ///     // For computed expressions without joins (no table reference needed)
    ///     .with_virtual_column("amount_money", |_vc| {
    ///         "(amount_micros / 1000000)::money"
    ///     })
    ///     .build();
    /// ```
    pub fn with_virtual_column<F>(mut self, name: impl Into<String>, f: F) -> Self
    where
        F: FnOnce(&mut VirtualColumnBuilder) -> &str,
    {
        let mut builder = VirtualColumnBuilder::new();
        let expression = f(&mut builder);

        self.virtual_columns.insert(
            name.into(),
            VirtualColumn {
                expression: expression.to_string(),
                joins: builder.joins,
                column_type: builder.column_type,
            },
        );
        self
    }

    /// Used to extend the field meta with additional columns.
    /// This is useful when you want to add columns that are not in the struct but are in the database.
    ///
    /// # Arguments
    ///
    /// * `columns` - A vector of column names to add to the field meta
    ///
    /// # Returns
    /// Returns self for method chaining
    pub fn with_fields_from<U: Default + Serialize>(mut self) -> Self {
        let additional_fields = get_struct_field_meta::<U>();
        for column in additional_fields.keys() {
            if !self.valid_columns.contains(column) {
                self.valid_columns.push(column.clone());
            }
        }
        self.field_meta.extend(additional_fields);
        self
    }

    pub fn map_column<F>(mut self, column: &str, mapper: F) -> Self 
    where
        F: Fn(&str, &str) -> (String, Option<String>) + 'static,
    {
        self.mappers.insert(column.to_string(), Box::new(mapper));
        self
    }

    /// Adds search functionality to the query by creating LIKE conditions for specified columns.
    ///
    /// # Arguments
    ///
    /// * `params` - Query parameters containing search text and columns to search in
    ///
    /// # Details
    ///
    /// - Only searches in columns that are both specified and considered safe
    /// - Creates case-insensitive LIKE conditions with wildcards
    /// - Multiple search columns are combined with OR operators
    /// - Empty search text or no valid columns results in no conditions being added
    ///
    /// # Returns
    ///
    /// Returns self for method chaining
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::Postgres;
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryBuilder, QueryParamsBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    ///
    /// let initial_params = QueryParamsBuilder::<UserExample>::new()
    ///         .with_search("john", vec!["name", "email"])
    ///         .build();
    /// let query_builder = QueryBuilder::<UserExample, Postgres>::new()
    ///     .with_search(&initial_params)
    ///     .build();
    /// ```
    pub fn with_search(mut self, params: &QueryParams<T>) -> Self {
        if let Some(search) = &params.search.search {
            if let Some(columns) = &params.search.search_columns {
                if !columns.is_empty() && !search.trim().is_empty() {
                    let pattern = format!("%{}%", search);
                    let next_argument = self.arguments.len() + 1;

                    let mut joins_to_activate: Vec<VirtualColumn> = Vec::new();

                    let search_conditions: Vec<String> = columns
                        .iter()
                        .filter_map(|column| {
                            if let Some(vc) = self.virtual_columns.get(column).cloned() {
                                joins_to_activate.push(vc.clone());
                                let placeholder = self.dialect.placeholder(next_argument);
                                return if vc.column_type == FieldType::String {
                                    Some(format!(
                                        "LOWER({}) LIKE LOWER({})",
                                        vc.expression, placeholder
                                    ))
                                } else {
                                    Some(format!(
                                        "({})::text LIKE {}",
                                        vc.expression, placeholder
                                    ))
                                };
                            }

                            let mapper = self.mappers.get(column);

                            // Mappers allow for custom column names and types added by the developer
                            // For these we're skipping column validation
                            if mapper.is_none() && !self.is_column_safe(column) {
                                return None;
                            }

                            let field_type = self.field_meta.get(column).cloned().unwrap_or(FieldType::Unknown);

                            let mapped_column = mapper.map(|mapper| mapper(column, search));

                            let table_column: String = mapped_column
                                .as_ref()
                                .map(|(tc, _)| tc.clone())
                                .unwrap_or_else(|| self.format_column(column));

                            let placeholder: String = mapped_column
                                .as_ref()
                                .and_then(|(_, p)| p.clone())
                                .unwrap_or_else(|| self.dialect.placeholder(next_argument));

                            if field_type == FieldType::String {
                                Some(format!("LOWER({}) LIKE LOWER({})", table_column, placeholder))
                            } else {
                                Some(format!("{}::text LIKE {}", table_column, placeholder))
                            }
                        })
                        .collect();

                    // Activate joins for used virtual columns
                    for vc in joins_to_activate {
                        self.activate_joins(&vc);
                    }

                    if !search_conditions.is_empty() {
                        self.conditions
                            .push(format!("({})", search_conditions.join(" OR ")));
                        self.arguments.add(pattern).unwrap_or_default();
                    }
                }
            }
        }
        self
    }

    /// Adds filters to the query based on provided Filter structs.
    ///
    /// # Arguments
    ///
    /// * `params` - Query parameters containing filters
    ///
    /// # Details
    ///
    /// - Supports multiple operators: Eq, Ne, Gt, Lt, Gte, Lte, Like, ILike, In, NotIn, IsNull, IsNotNull, Between, Contains
    /// - Only applies filters for columns that exist and are considered safe
    /// - Skips invalid columns with a warning when tracing is enabled
    ///
    /// # Returns
    ///
    /// Returns self for method chaining
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::Postgres;
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryBuilder, QueryParamsBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    ///
    /// let initial_params = QueryParamsBuilder::<UserExample>::new()
    ///         .with_search("john", vec!["name", "email"])
    ///         .build();
    ///
    /// let query_builder = QueryBuilder::<UserExample, Postgres>::new()
    ///     .with_filters(&initial_params)
    ///     .build();
    /// ```
    pub fn with_filters(mut self, params: &QueryParams<T>) -> Self {
        for filter in &params.filters {
            let field = &filter.field;

            // Check for virtual column first
            let (table_column, field_type) =
                if let Some(vc) = self.virtual_columns.get(field).cloned() {
                    self.activate_joins(&vc);
                    (vc.expression.clone(), vc.column_type.clone())
                } else {
                    if !self.is_column_safe(field) {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(column = %field, valid_columns = %self.valid_columns.join(", "), "Skipping invalid filter column");
                        continue;
                    }
                    (
                        self.format_column(field),
                        self.field_meta.get(field).cloned().unwrap_or(FieldType::Unknown),
                    )
                };


            // Type resolution order (highest priority first):
            // 1. Explicit column_cast_overrides (from with_column_cast)
            // 2. Virtual column or field_meta type (if not Unknown)
            // 3. Filter value type inference (fallback when type is Unknown)
            let filter_value_type = filter.value.to_field_type();

            let mut effective_field_type = if let Some(override_type) = self.column_cast_overrides.get(field) {
                override_type.clone()
            } else if field_type == FieldType::Unknown {
                filter_value_type.clone()
            } else {
                field_type.clone()
            };

            // Down casting DateTime to Date for proper comparison
            if effective_field_type == FieldType::DateTime && filter_value_type == FieldType::Date {
                effective_field_type = FieldType::Date;
            }

            let type_cast = self.dialect.type_cast(&effective_field_type);
            // If the filter value is a Date, cast the column to date for proper comparison
            // This ensures timestamp columns match all records on that calendar day
            let column_expr = if effective_field_type == FieldType::Date {
                format!("{}::date", table_column)
            } else {
                table_column.clone()
            };

            let condition = match filter.operator {
                FilterOperator::Eq => {
                    if let FilterValue::Bool(b) = filter.value {
                        let bool_literal = if b { "TRUE" } else { "FALSE" };
                        format!("{} IS {}", column_expr, bool_literal)
                    } else {
                        let value = filter.value.to_bindable_string();
                        let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                        self.arguments.add(value).unwrap_or_default();
                        format!("{} = {}{}", column_expr, placeholder, type_cast)
                    }
                }
                FilterOperator::Ne => {
                    if let FilterValue::Bool(b) = filter.value {
                        let bool_literal = if b { "TRUE" } else { "FALSE" };
                        format!("{} IS NOT {}", column_expr, bool_literal)
                    } else {
                        let value = filter.value.to_bindable_string();
                        let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                        self.arguments.add(value).unwrap_or_default();
                        format!("{} <> {}{}", column_expr, placeholder, type_cast)
                    }
                }
                FilterOperator::Gt => {
                    let value = filter.value.to_bindable_string();
                    let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                    self.arguments.add(value).unwrap_or_default();
                    format!("{} > {}{}", column_expr, placeholder, type_cast)
                }
                FilterOperator::Lt => {
                    let value = filter.value.to_bindable_string();
                    let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                    self.arguments.add(value).unwrap_or_default();
                    format!("{} < {}{}", column_expr, placeholder, type_cast)
                }
                FilterOperator::Gte => {
                    let value = filter.value.to_bindable_string();
                    let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                    self.arguments.add(value).unwrap_or_default();
                    format!("{} >= {}{}", column_expr, placeholder, type_cast)
                }
                FilterOperator::Lte => {
                    let value = filter.value.to_bindable_string();
                    let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                    self.arguments.add(value).unwrap_or_default();
                    format!("{} <= {}{}", column_expr, placeholder, type_cast)
                }
                FilterOperator::Like => {
                    let value = filter.value.to_bindable_string();
                    let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                    self.arguments.add(value).unwrap_or_default();
                    // Cast column to text for pattern matching on non-text types
                    if effective_field_type != FieldType::String && effective_field_type != FieldType::Unknown {
                        format!("{}::text LIKE {}", table_column, placeholder)
                    } else {
                        format!("{} LIKE {}", table_column, placeholder)
                    }
                }
                FilterOperator::ILike => {
                    let value = filter.value.to_bindable_string();
                    let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                    self.arguments.add(value).unwrap_or_default();
                    // Cast column to text for pattern matching on non-text types
                    if effective_field_type != FieldType::String && effective_field_type != FieldType::Unknown {
                        format!("{}::text ILIKE {}", table_column, placeholder)
                    } else {
                        format!("{} ILIKE {}", table_column, placeholder)
                    }
                }
                FilterOperator::In => {
                    let values = filter.value.to_bindable_strings();
                    let placeholders: Vec<String> = values
                        .iter()
                        .map(|v| {
                            let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                            self.arguments.add(v.clone()).unwrap_or_default();
                            format!("{}{}", placeholder, type_cast)
                        })
                        .collect();
                    format!("{} IN ({})", column_expr, placeholders.join(", "))
                }
                FilterOperator::NotIn => {
                    let values = filter.value.to_bindable_strings();
                    let placeholders: Vec<String> = values
                        .iter()
                        .map(|v| {
                            let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                            self.arguments.add(v.clone()).unwrap_or_default();
                            format!("{}{}", placeholder, type_cast)
                        })
                        .collect();
                    format!("{} NOT IN ({})", column_expr, placeholders.join(", "))
                }
                FilterOperator::IsNull => format!("{} IS NULL", table_column),
                FilterOperator::IsNotNull => format!("{} IS NOT NULL", table_column),
                FilterOperator::Between => {
                    let values = filter.value.to_bindable_strings();
                    if values.len() >= 2 {
                        let placeholder1 = self.dialect.placeholder(self.arguments.len() + 1);
                        self.arguments.add(values[0].clone()).unwrap_or_default();
                        let placeholder2 = self.dialect.placeholder(self.arguments.len() + 1);
                        self.arguments.add(values[1].clone()).unwrap_or_default();
                        format!("{} BETWEEN {}{} AND {}{}", column_expr, placeholder1, type_cast, placeholder2, type_cast)
                    } else {
                        continue;
                    }
                }
                FilterOperator::Contains => {
                    let value = filter.value.to_bindable_string();
                    let placeholder = self.dialect.placeholder(self.arguments.len() + 1);
                    self.arguments.add(value).unwrap_or_default();
                    format!("{} @> {}{}", table_column, placeholder, type_cast)
                }
            };

            self.conditions.push(condition);
        }
        self
    }

    /// Adds a custom condition for a specific column with a provided operator and value.
    ///
    /// # Arguments
    ///
    /// * `column` - The column name to apply the condition to
    /// * `condition` - The operator or condition to use (e.g., ">", "LIKE", etc.)
    /// * `value` - The value to compare against
    ///
    /// # Details
    ///
    /// - Only applies to columns that exist and are considered safe
    /// - Automatically handles parameter binding
    /// - Skips invalid columns with a warning when tracing is enabled
    ///
    /// # Returns
    ///
    /// Returns self for method chaining
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::Postgres;
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    ///
    /// let query_builder = QueryBuilder::<UserExample, Postgres>::new()
    ///     .with_condition("age", ">", "18".to_string())
    ///     .build();
    /// ```
    pub fn with_condition(
        mut self,
        column: &str,
        condition: impl Into<String>,
        value: String,
    ) -> Self {
        if self.is_column_safe(column) {
            let next_argument = self.arguments.len() + 1;
            self.conditions.push(format!(
                "{} {} {}",
                self.format_column(column),
                condition.into(),
                self.dialect.placeholder(next_argument)
            ));
            let _ = self.arguments.add(value);
        } else {
            #[cfg(feature = "tracing")]
            tracing::warn!(column = %column, "Skipping invalid condition column");
        }
        self
    }

    /// Adds a raw SQL condition to the query without any safety checks.
    ///
    /// # Arguments
    ///
    /// * `condition` - Raw SQL condition to add to the query
    ///
    /// # Safety
    ///
    /// This method bypasses column safety checks. Use with caution to prevent SQL injection.
    ///
    /// # Returns
    ///
    /// Returns self for method chaining
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::Postgres;
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    ///
    /// let query_builder = QueryBuilder::<UserExample, Postgres>::new()
    ///     .with_raw_condition("status != 'deleted'")
    ///     .build();
    /// ```
    pub fn with_raw_condition(mut self, condition: impl Into<String>) -> Self {
        self.conditions.push(condition.into());
        self
    }

    /// Allows adding multiple conditions using a closure.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that takes a mutable reference to the QueryBuilder
    ///
    /// # Details
    ///
    /// Useful for grouping multiple conditions that are logically related
    ///
    /// # Returns
    ///
    /// Returns self for method chaining
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::Postgres;
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    /// let query_builder = QueryBuilder::<UserExample, Postgres>::new()
    ///     .with_combined_conditions(|builder| {
    ///         builder.conditions.push("status = 'active'".to_string());
    ///         builder.conditions.push("age >= 18".to_string());
    ///     })
    ///     .build();
    /// ```
    pub fn with_combined_conditions<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut QueryBuilder<T, DB>),
    {
        f(&mut self);
        self
    }

    /// Disables column protection for this query builder instance.
    ///
    /// # Safety
    ///
    /// This removes all column safety checks. Use with caution as it may expose
    /// the application to SQL injection if used with untrusted input.
    ///
    /// # Returns
    ///
    /// Returns self for method chaining
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::Postgres;
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    ///
    /// let query_builder = QueryBuilder::<UserExample, Postgres>::new()
    ///     .disable_protection()
    ///     .with_raw_condition("custom_column = 'value'")
    ///     .build();
    /// ```
    pub fn disable_protection(mut self) -> Self {
        self.protection_enabled = false;
        self
    }

    pub fn enable_protection(mut self) -> Self {
        self.protection_enabled = true;
        self
    }

    pub fn disable_column_validation(mut self) -> Self {
        self.column_validation_enabled = false;
        self
    }

    pub fn enable_column_validation(mut self) -> Self {
        self.column_validation_enabled = true;
        self
    }

    /// Builds the final query conditions, arguments, and joins.
    ///
    /// # Returns
    ///
    /// Returns a `QueryBuildResult` containing:
    /// - `conditions`: List of SQL conditions for the WHERE clause
    /// - `arguments`: Database-specific arguments for parameter binding
    /// - `joins`: JOIN clauses to include (only those needed by used virtual columns)
    /// - `table_alias`: Table alias for SELECT clause (defaults to "base_query")
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::Postgres;
    /// use serde::Serialize;
    /// use sqlx_paginated::{QueryBuilder, QueryParamsBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    ///
    /// let initial_params = QueryParamsBuilder::<UserExample>::new()
    ///         .with_search("john", vec!["name", "email"])
    ///         .build();
    /// let result = QueryBuilder::<UserExample, Postgres>::new()
    ///     .with_search(&initial_params)
    ///     .build();
    /// // Use result.conditions, result.arguments, result.joins, result.table_alias
    /// ```
    /// Adds a column to the SELECT clause.
    ///
    /// When custom SELECT columns are specified, the query will use these columns
    /// instead of the default `table_alias.*`. This is useful for aggregate queries.
    ///
    /// # Arguments
    ///
    /// * `column` - A column expression to select (e.g., "id", "COUNT(*) as count")
    pub fn with_select(mut self, column: impl Into<String>) -> Self {
        let columns = self.select_columns.get_or_insert_with(Vec::new);
        columns.push(column.into());
        self
    }

    /// Adds a column to the GROUP BY clause.
    ///
    /// When GROUP BY columns are specified, the query will include a GROUP BY clause.
    /// This is typically used with aggregate functions in SELECT.
    ///
    /// # Arguments
    ///
    /// * `column` - A column name or expression to group by
    pub fn with_group_by(mut self, column: impl Into<String>) -> Self {
        let columns = self.group_by_columns.get_or_insert_with(Vec::new);
        columns.push(column.into());
        self
    }

    /// Adds columns for PostgreSQL DISTINCT ON clause.
    ///
    /// DISTINCT ON selects the first row for each unique combination of the specified columns,
    /// based on the ORDER BY clause. This is useful for deduplication without subqueries.
    ///
    /// Note: DISTINCT ON requires the DISTINCT columns to appear first in the ORDER BY clause.
    ///
    /// # Arguments
    ///
    /// * `columns` - Column names to use in DISTINCT ON
    pub fn with_distinct_on(mut self, columns: Vec<&str>) -> Self {
        self.distinct_on_columns = Some(columns.iter().map(|s| s.to_string()).collect());
        self
    }

    /// Adds a sort entry to the ORDER BY clause.
    ///
    /// # Arguments
    ///
    /// * `item` - A SortItem (Column or Expression)
    /// * `direction` - Sort direction (Ascending or Descending)
    pub fn with_sort(mut self, item: SortItem, direction: QuerySortDirection) -> Self {
        self.sort_entries.push(SortEntry { item, direction });
        self
    }

    /// Adds a column to the ORDER BY clause.
    ///
    /// # Arguments
    ///
    /// * `column` - Column name to sort by
    /// * `direction` - Sort direction (Ascending or Descending)
    pub fn with_sort_column(self, column: &str, direction: QuerySortDirection) -> Self {
        self.with_sort(SortItem::column(column), direction)
    }

    /// Adds a raw SQL expression to the ORDER BY clause.
    ///
    /// # Arguments
    ///
    /// * `expression` - Raw SQL expression to sort by (e.g., "CASE WHEN ... END")
    /// * `direction` - Sort direction (Ascending or Descending)
    pub fn with_sort_expression(self, expression: &str, direction: QuerySortDirection) -> Self {
        self.with_sort(SortItem::expression(expression), direction)
    }

    /// Configures the outer query using a callback pattern.
    ///
    /// Use this when you need to filter on window function results AND apply aggregations
    /// to the filtered results. The callback receives an `OuterQueryBuilder` to configure
    /// conditions, selects, and group by clauses.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// .with_outer(|outer| {
    ///     outer
    ///         .condition("transaction_rank = 1")
    ///         .select("DATE(created_at) as date")
    ///         .select("SUM(amount) as total")
    ///         .group_by("DATE(created_at)");
    /// })
    /// ```
    pub fn with_outer<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut OuterQueryBuilder),
    {
        let mut builder = self
            .outer_query
            .take()
            .map(|oq| OuterQueryBuilder {
                conditions: oq.conditions,
                select_columns: oq.select_columns,
                group_by_columns: oq.group_by_columns,
            })
            .unwrap_or_default();
        f(&mut builder);
        self.outer_query = Some(builder.build());
        self
    }

    pub fn build(self) -> QueryBuildResult<'q, DB> {
        QueryBuildResult {
            conditions: self.conditions,
            arguments: self.arguments,
            joins: self.active_joins,
            table_alias: self.table_alias,
            select_columns: self.select_columns,
            group_by_columns: self.group_by_columns,
            distinct_on_columns: self.distinct_on_columns,
            sort_entries: self.sort_entries,
            outer_query: self.outer_query,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paginated_query_as::models::{Filter, FilterOperator, FilterValue, QueryParams};
    use serde::Serialize;
    use sqlx::Postgres;

    #[derive(Default, Serialize)]
    struct TestModel {
        id: i64,
        name: String,
        amount: f64,
        is_active: bool,
        user_uuid: uuid::Uuid,
    }

    // Model with Option<T> fields to test fallback to filter value type inference
    // Option<T> fields serialize to null, resulting in FieldType::Unknown
    #[derive(Default, Serialize)]
    struct TestModelWithOptions {
        id: i64,
        name: String,
        // These Option fields will have FieldType::Unknown, triggering fallback
        optional_amount: Option<f64>,
        optional_datetime: Option<String>,
        optional_date: Option<String>,
        optional_time: Option<String>,
    }

    fn make_params_with_filter(filter: Filter) -> QueryParams<'static, TestModel> {
        QueryParams {
            filters: vec![filter],
            ..Default::default()
        }
    }

    fn make_option_params_with_filter(filter: Filter) -> QueryParams<'static, TestModelWithOptions> {
        QueryParams {
            filters: vec![filter],
            ..Default::default()
        }
    }

    // ========================================
    // Type Cast Tests for Comparison Operators
    // ========================================

    #[test]
    fn test_eq_filter_int_generates_bigint_cast() {
        let filter = Filter {
            field: "id".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Int(123),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        assert_eq!(result.conditions.len(), 1);
        assert!(
            result.conditions[0].contains("::bigint"),
            "Expected ::bigint cast, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_gt_filter_int_generates_bigint_cast() {
        let filter = Filter {
            field: "id".to_string(),
            operator: FilterOperator::Gt,
            value: FilterValue::Int(100),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::bigint"),
            "Expected ::bigint cast for Gt operator, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_lt_filter_float_generates_float8_cast() {
        // Use Option field to trigger fallback to filter value type inference
        let filter = Filter {
            field: "optional_amount".to_string(),
            operator: FilterOperator::Lt,
            value: FilterValue::Float(99.99),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::float8"),
            "Expected ::float8 cast for Float value on Option field, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_eq_filter_bool_true_generates_is_true() {
        let filter = Filter {
            field: "is_active".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Bool(true),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        // Boolean Eq uses IS TRUE/FALSE syntax for proper NULL handling
        assert!(
            result.conditions[0].contains("IS TRUE"),
            "Expected IS TRUE for boolean Eq filter, got: {}",
            result.conditions[0]
        );
        // Should NOT add a placeholder argument for boolean
        assert!(
            !result.conditions[0].contains("$"),
            "Boolean Eq should not use placeholder, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_eq_filter_bool_false_generates_is_false() {
        let filter = Filter {
            field: "is_active".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Bool(false),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("IS FALSE"),
            "Expected IS FALSE for boolean Eq filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_ne_filter_bool_true_generates_is_not_true() {
        let filter = Filter {
            field: "is_active".to_string(),
            operator: FilterOperator::Ne,
            value: FilterValue::Bool(true),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("IS NOT TRUE"),
            "Expected IS NOT TRUE for boolean Ne filter, got: {}",
            result.conditions[0]
        );
        // Should NOT add a placeholder argument for boolean
        assert!(
            !result.conditions[0].contains("$"),
            "Boolean Ne should not use placeholder, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_ne_filter_bool_false_generates_is_not_false() {
        let filter = Filter {
            field: "is_active".to_string(),
            operator: FilterOperator::Ne,
            value: FilterValue::Bool(false),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("IS NOT FALSE"),
            "Expected IS NOT FALSE for boolean Ne filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_bool_filter_does_not_consume_argument_slot() {
        // Verify that boolean filters don't add arguments, so subsequent filters
        // get correct placeholder numbers
        let filter1 = Filter {
            field: "is_active".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Bool(true),
        };
        let filter2 = Filter {
            field: "id".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Int(123),
        };
        let params: QueryParams<TestModel> = QueryParams {
            filters: vec![filter1, filter2],
            ..Default::default()
        };

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        // First condition should be IS TRUE (no placeholder)
        assert!(
            result.conditions[0].contains("IS TRUE"),
            "First condition should be IS TRUE, got: {}",
            result.conditions[0]
        );
        // Second condition should use $1 (not $2) since boolean didn't consume a slot
        assert!(
            result.conditions[1].contains("$1"),
            "Second condition should use $1 since boolean didn't consume argument slot, got: {}",
            result.conditions[1]
        );
        // Only one argument should be in the arguments list
        assert_eq!(
            result.arguments.len(),
            1,
            "Expected 1 argument (only for int filter), got {}",
            result.arguments.len()
        );
    }

    #[test]
    fn test_multiple_bool_filters_dont_affect_argument_count() {
        let filter1 = Filter {
            field: "is_active".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Bool(true),
        };
        let filter2 = Filter {
            field: "is_active".to_string(),
            operator: FilterOperator::Ne,
            value: FilterValue::Bool(false),
        };
        let filter3 = Filter {
            field: "name".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::String("test".to_string()),
        };
        let params: QueryParams<TestModel> = QueryParams {
            filters: vec![filter1, filter2, filter3],
            ..Default::default()
        };

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        // Two boolean filters should not add arguments
        // String filter should use $1
        assert!(result.conditions[0].contains("IS TRUE"));
        assert!(result.conditions[1].contains("IS NOT FALSE"));
        assert!(
            result.conditions[2].contains("$1"),
            "String filter should use $1, got: {}",
            result.conditions[2]
        );
        assert_eq!(
            result.arguments.len(),
            1,
            "Expected 1 argument (only for string filter)"
        );
    }

    #[test]
    fn test_eq_filter_uuid_generates_uuid_cast() {
        let filter = Filter {
            field: "user_uuid".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Uuid(uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::uuid"),
            "Expected ::uuid cast, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_eq_filter_string_no_cast() {
        let filter = Filter {
            field: "name".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::String("John".to_string()),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        // String values should not have type cast
        assert!(
            !result.conditions[0].contains("::"),
            "String filter should not have type cast, got: {}",
            result.conditions[0]
        );
    }

    // ========================================
    // DateTime/Date/Time Type Cast Tests
    // ========================================

    #[test]
    fn test_gt_filter_datetime_generates_timestamptz_cast() {
        // Use Option field to trigger fallback to filter value type inference
        let filter = Filter {
            field: "optional_datetime".to_string(),
            operator: FilterOperator::Gt,
            value: FilterValue::DateTime("2025-12-02T10:30:00Z".to_string()),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::timestamptz"),
            "Expected ::timestamptz cast for DateTime value on Option field, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_gte_filter_date_generates_date_cast() {
        // Use Option field to trigger fallback to filter value type inference
        let filter = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::Gte,
            value: FilterValue::Date("2025-12-02".to_string()),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::date"),
            "Expected ::date cast for Date value on Option field, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_eq_filter_time_generates_time_cast() {
        // Use Option field to trigger fallback to filter value type inference
        let filter = Filter {
            field: "optional_time".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Time("10:30:00".to_string()),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::time"),
            "Expected ::time cast for Time value on Option field, got: {}",
            result.conditions[0]
        );
    }

    // ========================================
    // In/NotIn/Between Operator Tests
    // ========================================

    #[test]
    fn test_in_filter_generates_cast_per_value() {
        let filter = Filter {
            field: "id".to_string(),
            operator: FilterOperator::In,
            value: FilterValue::Array(vec![
                FilterValue::Int(1),
                FilterValue::Int(2),
                FilterValue::Int(3),
            ]),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        // Each value in IN clause should have ::bigint cast
        let condition = &result.conditions[0];
        let bigint_count = condition.matches("::bigint").count();
        assert_eq!(
            bigint_count, 3,
            "Expected 3 ::bigint casts in IN clause, got {} in: {}",
            bigint_count, condition
        );
    }

    #[test]
    fn test_between_filter_generates_two_casts() {
        let filter = Filter {
            field: "optional_amount".to_string(),
            operator: FilterOperator::Between,
            value: FilterValue::Array(vec![
                FilterValue::Float(10.0),
                FilterValue::Float(100.0),
            ]),
        };
        let params: QueryParams<TestModelWithOptions> = QueryParams {
            filters: vec![filter],
            ..Default::default()
        };

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        let condition = &result.conditions[0];
        let float8_count = condition.matches("::float8").count();
        assert_eq!(
            float8_count, 2,
            "Expected 2 ::float8 casts in BETWEEN clause, got {} in: {}",
            float8_count, condition
        );
    }

    // ========================================
    // Like/ILike Operator Tests
    // ========================================

    #[test]
    fn test_like_on_int_field_casts_column_to_text() {
        let filter = Filter {
            field: "id".to_string(),
            operator: FilterOperator::Like,
            value: FilterValue::String("%123%".to_string()),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        // When using LIKE on non-string field, column should be cast to text
        assert!(
            result.conditions[0].contains("::text LIKE"),
            "Expected column::text LIKE for non-string field, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_like_on_string_field_no_column_cast() {
        let filter = Filter {
            field: "name".to_string(),
            operator: FilterOperator::Like,
            value: FilterValue::String("%John%".to_string()),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        // String field should not have column cast, just LIKE
        assert!(
            !result.conditions[0].contains("::text LIKE"),
            "String field should not have ::text cast, got: {}",
            result.conditions[0]
        );
        assert!(
            result.conditions[0].contains("LIKE"),
            "Should contain LIKE operator, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_ilike_on_bool_field_casts_column_to_text() {
        let filter = Filter {
            field: "is_active".to_string(),
            operator: FilterOperator::ILike,
            value: FilterValue::String("%true%".to_string()),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::text ILIKE"),
            "Expected column::text ILIKE for non-string field, got: {}",
            result.conditions[0]
        );
    }

    // ========================================
    // Date Column Casting Tests
    // ========================================
    // When filtering with a Date value, the column should be cast to ::date
    // to ensure timestamp columns match all records on that calendar day

    #[test]
    fn test_eq_date_filter_casts_column_to_date() {
        let filter = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Date("2025-12-22".to_string()),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        // Column should be cast to ::date for proper date comparison
        assert!(
            result.conditions[0].contains("::date ="),
            "Expected column::date = for Date filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_ne_date_filter_casts_column_to_date() {
        let filter = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::Ne,
            value: FilterValue::Date("2025-12-22".to_string()),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::date <>"),
            "Expected column::date <> for Date filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_gt_date_filter_casts_column_to_date() {
        let filter = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::Gt,
            value: FilterValue::Date("2025-12-22".to_string()),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::date >"),
            "Expected column::date > for Date filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_lt_date_filter_casts_column_to_date() {
        let filter = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::Lt,
            value: FilterValue::Date("2025-12-22".to_string()),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::date <"),
            "Expected column::date < for Date filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_gte_date_filter_casts_column_to_date() {
        let filter = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::Gte,
            value: FilterValue::Date("2025-12-22".to_string()),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::date >="),
            "Expected column::date >= for Date filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_lte_date_filter_casts_column_to_date() {
        let filter = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::Lte,
            value: FilterValue::Date("2025-12-22".to_string()),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::date <="),
            "Expected column::date <= for Date filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_in_date_filter_casts_column_to_date() {
        let filter = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::In,
            value: FilterValue::Array(vec![
                FilterValue::Date("2025-12-22".to_string()),
                FilterValue::Date("2025-12-23".to_string()),
            ]),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        // Column should be cast to ::date
        assert!(
            result.conditions[0].contains("::date IN"),
            "Expected column::date IN for Date filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_not_in_date_filter_casts_column_to_date() {
        let filter = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::NotIn,
            value: FilterValue::Array(vec![
                FilterValue::Date("2025-12-22".to_string()),
                FilterValue::Date("2025-12-23".to_string()),
            ]),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::date NOT IN"),
            "Expected column::date NOT IN for Date filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_between_date_filter_casts_column_to_date() {
        let filter = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::Between,
            value: FilterValue::Array(vec![
                FilterValue::Date("2025-12-01".to_string()),
                FilterValue::Date("2025-12-31".to_string()),
            ]),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::date BETWEEN"),
            "Expected column::date BETWEEN for Date filter, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_datetime_filter_does_not_cast_column() {
        // DateTime filter should NOT cast the column, only the value
        let filter = Filter {
            field: "optional_datetime".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::DateTime("2025-12-22T10:30:00Z".to_string()),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_filters(&params)
            .build();

        // Should have ::timestamptz for the value, but column should NOT be cast
        assert!(
            result.conditions[0].contains("::timestamptz"),
            "Expected ::timestamptz cast for value, got: {}",
            result.conditions[0]
        );
        // The column itself should not have ::date cast
        assert!(
            !result.conditions[0].contains("::date"),
            "DateTime filter should not cast column to date, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_int_filter_does_not_cast_column_to_date() {
        // Non-date filters should NOT cast the column to ::date
        let filter = Filter {
            field: "id".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Int(123),
        };
        let params = make_params_with_filter(filter);

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_filters(&params)
            .build();

        assert!(
            !result.conditions[0].contains("::date"),
            "Int filter should not cast column to date, got: {}",
            result.conditions[0]
        );
    }

    // ========================================
    // with_column_cast Override Tests
    // ========================================

    #[test]
    fn test_with_column_cast_overrides_filter_value_inference() {
        // Without override: numeric value "123" would be inferred as Int -> ::bigint
        // With override to String: no cast should be applied
        let filter = Filter {
            field: "optional_amount".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Int(123),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_column_cast("optional_amount", FieldType::String)
            .with_filters(&params)
            .build();

        // String type has no cast, so ::bigint should NOT be present
        assert!(
            !result.conditions[0].contains("::bigint"),
            "with_column_cast(String) should prevent bigint cast, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_with_column_cast_overrides_to_specific_type() {
        // Override to Float should produce ::float8 cast
        let filter = Filter {
            field: "optional_amount".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Int(100),
        };
        let params = make_option_params_with_filter(filter);

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_column_cast("optional_amount", FieldType::Float)
            .with_filters(&params)
            .build();

        assert!(
            result.conditions[0].contains("::float8"),
            "with_column_cast(Float) should produce ::float8 cast, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_with_column_cast_overrides_virtual_column() {
        // Virtual column has default FieldType::String
        // with_column_cast should override it
        let filter = Filter {
            field: "computed_field".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Int(42),
        };
        let params: QueryParams<TestModelWithOptions> = QueryParams {
            filters: vec![filter],
            ..Default::default()
        };

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_virtual_column("computed_field", |_vc| {
                "some_expression"
            })
            .with_column_cast("computed_field", FieldType::Int)
            .with_filters(&params)
            .build();

        // Should have ::bigint from the override, not default String (no cast)
        assert!(
            result.conditions[0].contains("::bigint"),
            "with_column_cast should override virtual_column type, got: {}",
            result.conditions[0]
        );
    }

    #[test]
    fn test_with_column_cast_multiple_columns() {
        let filter1 = Filter {
            field: "optional_amount".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Int(100),
        };
        let filter2 = Filter {
            field: "optional_date".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Int(20251222),
        };
        let params: QueryParams<TestModelWithOptions> = QueryParams {
            filters: vec![filter1, filter2],
            ..Default::default()
        };

        let result = QueryBuilder::<TestModelWithOptions, Postgres>::new()
            .with_column_cast("optional_amount", FieldType::Float)
            .with_column_cast("optional_date", FieldType::String)
            .with_filters(&params)
            .build();

        // First filter should have ::float8
        assert!(
            result.conditions[0].contains("::float8"),
            "optional_amount should have ::float8, got: {}",
            result.conditions[0]
        );
        // Second filter should have no cast (String)
        assert!(
            !result.conditions[1].contains("::bigint"),
            "optional_date should not have ::bigint, got: {}",
            result.conditions[1]
        );
    }

    #[test]
    fn test_with_column_cast_does_not_affect_other_columns() {
        // Override one column, other columns should still use normal inference
        let filter1 = Filter {
            field: "id".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Int(123),
        };
        let filter2 = Filter {
            field: "name".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::String("test".to_string()),
        };
        let params: QueryParams<TestModel> = QueryParams {
            filters: vec![filter1, filter2],
            ..Default::default()
        };

        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_column_cast("name", FieldType::Uuid)  // Override name to Uuid
            .with_filters(&params)
            .build();

        // id should still have ::bigint (from struct inference)
        assert!(
            result.conditions[0].contains("::bigint"),
            "id should still have ::bigint, got: {}",
            result.conditions[0]
        );
        // name should now have ::uuid (from override)
        assert!(
            result.conditions[1].contains("::uuid"),
            "name should have ::uuid from override, got: {}",
            result.conditions[1]
        );
    }
}
