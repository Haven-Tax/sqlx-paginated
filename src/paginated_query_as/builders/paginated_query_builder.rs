use crate::paginated_query_as::builders::QueryBuildResult;
use crate::paginated_query_as::examples::postgres_examples::build_query_with_safe_defaults;
use crate::paginated_query_as::internal::quote_identifier;
use crate::paginated_query_as::models::QuerySortDirection;
use crate::{PaginatedResponse, QueryParams};
use serde::Serialize;
use sqlx::{postgres::Postgres, query::QueryAs, Execute, FromRow, IntoArguments, Pool};

pub struct PaginatedQueryBuilder<'q, T, A>
where
    T: for<'r> FromRow<'r, <Postgres as sqlx::Database>::Row> + Send + Unpin,
{
    query: QueryAs<'q, Postgres, T, A>,
    params: QueryParams<'q, T>,
    totals_count_enabled: bool,
    build_query_fn: fn(&QueryParams<T>) -> QueryBuildResult<'static, Postgres>,
}

/// A builder for constructing and executing paginated queries.
///
/// This builder provides a fluent interface for creating paginated queries.
/// For more examples explore `examples/paginated_query_builder_advanced_examples.rs`
///
/// # Type Parameters
///
/// * `'q`: The lifetime of the query and its arguments
/// * `T`: The model type that the query will return
/// * `A`: The type of the query arguments
///
/// # Generic Constraints
///
/// * `T`: Must be deserializable from Postgres rows (`FromRow`), `Send`, and `Unpin`
/// * `A`: Must be compatible with Postgres arguments and `Send`
///
/// (Attention: Only `Pool<Postgres>` is supported at the moment)
impl<'q, T, A> PaginatedQueryBuilder<'q, T, A>
where
    T: for<'r> FromRow<'r, <Postgres as sqlx::Database>::Row> + Send + Unpin + Serialize + Default + 'static,
    A: 'q + IntoArguments<'q, Postgres> + Send,
{
    /// Creates a new `PaginatedQueryBuilder` with default settings.
    ///
    /// # Arguments
    ///
    /// * `query` - The base query to paginate
    ///
    /// # Default Settings
    ///
    /// - Totals calculation is enabled
    /// - Uses default query parameters
    /// - Uses safe default query building function
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sqlx::{FromRow, Postgres};
    /// use serde::{Serialize};
    /// use sqlx_paginated::PaginatedQueryBuilder;
    ///
    /// #[derive(Serialize, FromRow, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    /// let base_query = sqlx::query_as::<_, UserExample>("SELECT * FROM users");
    /// let builder = PaginatedQueryBuilder::new(base_query);
    /// ```
    pub fn new(query: QueryAs<'q, Postgres, T, A>) -> Self {
        Self {
            query,
            params: QueryParams::default(),
            totals_count_enabled: true,
            build_query_fn: |params| build_query_with_safe_defaults::<T>(params),
        }
    }

    pub fn with_query_builder(
        self,
        build_query_fn: fn(&QueryParams<T>) -> QueryBuildResult<'static, Postgres>,
    ) -> Self {
        Self {
            build_query_fn,
            ..self
        }
    }

    pub fn with_params(mut self, params: impl Into<QueryParams<'q, T>>) -> Self {
        self.params = params.into();
        self
    }

    /// Disables the calculation of total record count.
    ///
    /// When disabled, the response will not include total count or total pages.
    /// This can improve query performance for large datasets where the total
    /// count is not needed.
    ///
    /// # Returns
    ///
    /// Returns self for method chaining
    pub fn disable_totals_count(mut self) -> Self {
        self.totals_count_enabled = false;
        self
    }

    /// Executes the paginated query and returns the results.
    ///
    /// # Arguments
    ///
    /// * `pool` - Database connection pool (Attention: Only `Pool<Postgres>` is supported at the moment)
    ///
    /// # Returns
    ///
    /// Returns a Result containing a `PaginatedResponse<T>` with:
    /// - Records for the requested page
    /// - Optional Pagination information (if enabled)
    /// - Optional total count and total pages (if enabled)
    ///
    /// # Errors
    ///
    /// Returns `sqlx::Error` if the query execution fails
    pub async fn fetch_paginated(
        self,
        pool: &Pool<Postgres>,
    ) -> Result<PaginatedResponse<T>, sqlx::Error> {
        let base_sql = self.build_base_query();
        let main_result = (self.build_query_fn)(&self.params);
        let join_clause = self.build_join_clause(&main_result.joins);
        let where_clause = self.build_where_clause(&main_result.conditions);

        let (total, total_pages, pagination) = if self.totals_count_enabled {
            let count_result = (self.build_query_fn)(&self.params);

            let count_sql = format!(
                "{} SELECT COUNT(*) FROM base_query{}{}",
                base_sql, join_clause, where_clause
            );
            let count: i64 = sqlx::query_scalar_with(&count_sql, count_result.arguments)
                .fetch_one(pool)
                .await?;

            match &self.params.pagination {
                Some(p) => {
                    let available_pages = if count == 0 {
                        0
                    } else {
                        (count + p.page_size - 1) / p.page_size
                    };
                    (Some(count), Some(available_pages), Some(p.clone()))
                }
                None => (Some(count), None, None),
            }
        } else {
            (None, None, None)
        };

        let select_target = format!("{}.*", quote_identifier(&main_result.table_alias));
        
        let mut main_sql = format!(
            "{} SELECT {} FROM base_query{}{}",
            base_sql, select_target, join_clause, where_clause
        );

        main_sql.push_str(&self.build_order_clause());
        main_sql.push_str(&self.build_limit_offset_clause());

        let records = sqlx::query_as_with::<Postgres, T, _>(&main_sql, main_result.arguments)
            .fetch_all(pool)
            .await?;

        Ok(PaginatedResponse {
            records,
            pagination,
            total,
            total_pages,
        })
    }

    /// Builds the base query with CTE (Common Table Expression).
    ///
    /// # Returns
    ///
    /// Returns the SQL string for the base query wrapped in a CTE
    fn build_base_query(&self) -> String {
        format!("WITH base_query AS ({})", self.query.sql())
    }

    /// Builds the JOIN clause from the provided joins.
    ///
    /// # Arguments
    ///
    /// * `joins` - Vector of JOIN clause strings
    ///
    /// # Returns
    ///
    /// Returns the formatted JOIN clause or empty string if no joins
    fn build_join_clause(&self, joins: &[String]) -> String {
        if joins.is_empty() {
            String::new()
        } else {
            format!(" {}", joins.join(" "))
        }
    }

    /// Builds the WHERE clause from the provided conditions.
    ///
    /// # Arguments
    ///
    /// * `conditions` - Vector of condition strings to join with AND
    ///
    /// # Returns
    ///
    /// Returns the formatted WHERE clause or empty string if no conditions
    fn build_where_clause(&self, conditions: &[String]) -> String {
        if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        }
    }

    /// Builds the ORDER BY clause based on sort parameters.
    ///
    /// # Returns
    ///
    /// Returns the formatted ORDER BY clause with proper column quoting.
    /// If the column doesn't contain a '.', it's prefixed with "base_query." to avoid
    /// ambiguity when JOINs are present.
    fn build_order_clause(&self) -> String {
        let order = match self.params.sort.sort_direction {
            QuerySortDirection::Ascending => "ASC",
            QuerySortDirection::Descending => "DESC",
        };
        
        let sort_column = &self.params.sort.sort_column;
        let column_name = if sort_column.contains('.') {
            // Already qualified (e.g., "table.column" or "base_query.column")
            quote_identifier(sort_column)
        } else {
            // Prefix with base_query to avoid ambiguity with JOINed tables
            format!("\"base_query\".{}", quote_identifier(sort_column))
        };

        format!(" ORDER BY {} {}", column_name, order)
    }

    fn build_limit_offset_clause(&self) -> String {
        match &self.params.pagination {
            Some(p) => {
                let offset = (p.page - 1) * p.page_size;
                format!(" LIMIT {} OFFSET {}", p.page_size, offset)
            }
            None => String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paginated_query_as::builders::QueryBuilder;
    use serde::Serialize;

    #[derive(Default, Serialize, sqlx::FromRow)]
    struct TestModel {
        id: i64,
        name: String,
        created_at: String,
    }

    #[test]
    fn test_table_alias_in_select_clause() {
        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_table_alias("base_query")
            .build();
        
        assert_eq!(result.table_alias, "base_query");
    }

    #[test]
    fn test_default_table_alias() {
        let result = QueryBuilder::<TestModel, Postgres>::new().build();
        
        assert_eq!(result.table_alias, "base_query");
    }

    #[test]
    fn test_custom_table_alias() {
        let result = QueryBuilder::<TestModel, Postgres>::new()
            .with_table_alias("custom_cte")
            .build();
        
        assert_eq!(result.table_alias, "custom_cte");
    }
}
