use crate::paginated_query_as::builders::QueryBuildResult;
use crate::paginated_query_as::examples::postgres_examples::build_query_with_safe_defaults;
use crate::paginated_query_as::internal::quote_identifier;
use crate::paginated_query_as::models::{QuerySortDirection, SortEntry};
use crate::{PaginatedResponse, QueryParams};
use serde::Serialize;
use sqlx::{postgres::Postgres, query::QueryAs, Execute, FromRow, IntoArguments, Pool};
use std::sync::Arc;

type BuildQueryFn<T> =
    Arc<dyn for<'a> Fn(&QueryParams<'a, T>) -> QueryBuildResult<'static, Postgres> + Send + Sync>;

pub struct PaginatedQueryBuilder<'q, T, A>
where
    T: for<'r> FromRow<'r, <Postgres as sqlx::Database>::Row> + Send + Unpin,
{
    query: QueryAs<'q, Postgres, T, A>,
    params: QueryParams<'q, T>,
    totals_count_enabled: bool,
    build_query_fn: BuildQueryFn<T>,
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
    T: for<'r> FromRow<'r, <Postgres as sqlx::Database>::Row>
        + Send
        + Unpin
        + Serialize
        + Default
        + 'static,
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
            build_query_fn: Arc::new(|params| build_query_with_safe_defaults::<T>(params)),
        }
    }

    pub fn with_query_builder<F>(self, build_query_fn: F) -> Self
    where
        F: for<'a> Fn(&QueryParams<'a, T>) -> QueryBuildResult<'static, Postgres>
            + Send
            + Sync
            + 'static,
    {
        Self {
            build_query_fn: Arc::new(build_query_fn),
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
        let build_query_fn = &self.build_query_fn;
        let main_result = build_query_fn(&self.params);
        let join_clause = self.build_join_clause(&main_result.joins);
        let where_clause = self.build_where_clause(&main_result.conditions);
        let group_by_clause =
            build_group_by_clause(main_result.group_by_columns.as_deref().unwrap_or(&[]));
        let outer_query = main_result.outer_query.as_ref();
        let has_outer_conditions = outer_query.is_some_and(|oq| !oq.conditions.is_empty());
        let outer_where_clause = outer_query
            .map(|oq| self.build_where_clause(&oq.conditions))
            .unwrap_or_default();

        let select_target = match &main_result.select_columns {
            Some(cols) if !cols.is_empty() => cols.join(", "),
            _ => format!("{}.*", quote_identifier(&main_result.table_alias)),
        };

        let has_outer_aggregation = outer_query.is_some_and(|oq| !oq.group_by_columns.is_empty());
        let outer_group_by_clause_for_count = outer_query
            .map(|oq| build_group_by_clause(&oq.group_by_columns))
            .unwrap_or_default();
        let group_by_present = main_result
            .group_by_columns
            .as_ref()
            .is_some_and(|c| !c.is_empty());

        let distinct_clause = match &main_result.distinct_on_columns {
            Some(cols) if !cols.is_empty() => {
                let cols_sql = cols
                    .iter()
                    .map(|c| {
                        format!(
                            "{}.{}",
                            quote_identifier(&main_result.table_alias),
                            quote_identifier(c)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("DISTINCT ON ({}) ", cols_sql)
            }
            Some(_) | None => String::new(),
        };
        let (total, total_pages, pagination) = if self.totals_count_enabled {
            let count_result = build_query_fn(&self.params);
            let count_sql = build_count_sql(CountSqlInputs {
                base_sql: &base_sql,
                select_target: &select_target,
                distinct_clause: &distinct_clause,
                join_clause: &join_clause,
                where_clause: &where_clause,
                group_by_clause: &group_by_clause,
                outer_where_clause: &outer_where_clause,
                outer_group_by_clause_for_count: &outer_group_by_clause_for_count,
                has_outer_conditions,
                has_outer_aggregation,
                group_by_present,
            });
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

        let outer_select_target = outer_query
            .filter(|oq| !oq.select_columns.is_empty())
            .map(|oq| oq.select_columns.join(", "))
            .unwrap_or_else(|| "*".to_string());

        let outer_group_by_clause = outer_query
            .map(|oq| build_group_by_clause(&oq.group_by_columns))
            .unwrap_or_default();

        let mut main_sql = if has_outer_conditions {
            let inner_sql = build_inner_select(
                &format!("{}{}", distinct_clause, select_target),
                &join_clause,
                &where_clause,
                &group_by_clause,
            );
            format!(
                "{} SELECT {} FROM ({}) AS inner_query{}{}",
                base_sql, outer_select_target, inner_sql, outer_where_clause, outer_group_by_clause
            )
        } else {
            let mut sql = format!(
                "{} SELECT {}{} FROM base_query{}{}",
                base_sql, distinct_clause, select_target, join_clause, where_clause
            );
            sql.push_str(&group_by_clause);
            sql
        };

        let order_table_alias = if has_outer_conditions {
            "inner_query"
        } else {
            &main_result.table_alias
        };
        main_sql.push_str(&self.build_order_clause(&main_result.sort_entries, order_table_alias));
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
    /// # Arguments
    ///
    /// * `sort_entries` - Explicit sort entries from the query builder (takes precedence)
    /// * `table_alias` - The table alias to use for column references
    ///
    /// # Returns
    ///
    /// Returns the formatted ORDER BY clause with proper column quoting.
    /// Uses sort_entries if provided, otherwise falls back to params.sort.
    fn build_order_clause(&self, sort_entries: &[SortEntry], table_alias: &str) -> String {
        if !sort_entries.is_empty() {
            let order_parts: Vec<String> = sort_entries
                .iter()
                .map(|entry| {
                    let direction = match entry.direction {
                        QuerySortDirection::Ascending => "ASC",
                        QuerySortDirection::Descending => "DESC",
                    };
                    format!("{} {}", entry.item.to_sql(table_alias), direction)
                })
                .collect();
            return format!(" ORDER BY {}", order_parts.join(", "));
        }

        self.params
            .sort
            .as_ref()
            .and_then(|sort| {
                let sort_direction = sort.sort_direction.as_ref()?;
                let sort_column = sort.sort_column.as_ref()?;
                let order = match sort_direction {
                    QuerySortDirection::Ascending => "ASC",
                    QuerySortDirection::Descending => "DESC",
                };
                let column_name = if sort_column.contains('.') {
                    quote_identifier(sort_column)
                } else {
                    format!(
                        "{}.{}",
                        quote_identifier(table_alias),
                        quote_identifier(sort_column)
                    )
                };
                Some(format!(" ORDER BY {} {}", column_name, order))
            })
            .unwrap_or_default()
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

fn build_group_by_clause(group_by_columns: &[String]) -> String {
    if group_by_columns.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", group_by_columns.join(", "))
    }
}

struct CountSqlInputs<'a> {
    base_sql: &'a str,
    select_target: &'a str,
    distinct_clause: &'a str,
    join_clause: &'a str,
    where_clause: &'a str,
    group_by_clause: &'a str,
    outer_where_clause: &'a str,
    outer_group_by_clause_for_count: &'a str,
    has_outer_conditions: bool,
    has_outer_aggregation: bool,
    group_by_present: bool,
}

fn build_count_sql(i: CountSqlInputs<'_>) -> String {
    let has_distinct = !i.distinct_clause.is_empty();
    let inner_select_target = format!("{}{}", i.distinct_clause, i.select_target);

    match (
        i.has_outer_conditions,
        i.has_outer_aggregation,
        i.group_by_present,
    ) {
        (true, true, _) => {
            let inner_select = build_inner_select(
                &inner_select_target,
                i.join_clause,
                i.where_clause,
                i.group_by_clause,
            );
            let outer_select = format!(
                "SELECT 1 FROM ({}) AS inner_query{}{}",
                inner_select, i.outer_where_clause, i.outer_group_by_clause_for_count
            );
            format!(
                "{} SELECT COUNT(*) FROM ({}) AS aggregated",
                i.base_sql, outer_select
            )
        }
        (true, false, _) => {
            let inner_select = build_inner_select(
                &inner_select_target,
                i.join_clause,
                i.where_clause,
                i.group_by_clause,
            );
            format!(
                "{} SELECT COUNT(*) FROM ({}) AS inner_query{}",
                i.base_sql, inner_select, i.outer_where_clause
            )
        }
        (false, _, true) => format!(
            "{} SELECT COUNT(*) FROM (SELECT {} FROM base_query{}{}{}) AS grouped",
            i.base_sql,
            if has_distinct { inner_select_target.as_str() } else { "1" },
            i.join_clause,
            i.where_clause,
            i.group_by_clause
        ),
        (false, _, false) if has_distinct => format!(
            "{} SELECT COUNT(*) FROM (SELECT {} FROM base_query{}{}) AS distinct_query",
            i.base_sql, inner_select_target, i.join_clause, i.where_clause
        ),
        (false, _, false) => format!(
            "{} SELECT COUNT(*) FROM base_query{}{}",
            i.base_sql, i.join_clause, i.where_clause
        ),
    }
}

fn build_inner_select(
    select_target: &str,
    join_clause: &str,
    where_clause: &str,
    group_by_clause: &str,
) -> String {
    format!(
        "SELECT {} FROM base_query{}{}{}",
        select_target, join_clause, where_clause, group_by_clause
    )
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

    fn default_count_inputs<'a>(base_sql: &'a str, distinct_clause: &'a str) -> CountSqlInputs<'a> {
        CountSqlInputs {
            base_sql,
            select_target: "\"base_query\".*",
            distinct_clause,
            join_clause: "",
            where_clause: "",
            group_by_clause: "",
            outer_where_clause: "",
            outer_group_by_clause_for_count: "",
            has_outer_conditions: false,
            has_outer_aggregation: false,
            group_by_present: false,
        }
    }

    #[test]
    fn test_count_sql_without_distinct_uses_direct_count() {
        let sql = build_count_sql(default_count_inputs("WITH base_query AS (SELECT 1)", ""));
        assert!(
            sql.ends_with("SELECT COUNT(*) FROM base_query"),
            "plain count should hit fast path, got: {sql}"
        );
        assert!(!sql.contains("DISTINCT ON"));
    }

    #[test]
    fn test_count_sql_with_distinct_wraps_subquery() {
        let sql = build_count_sql(default_count_inputs(
            "WITH base_query AS (SELECT 1)",
            "DISTINCT ON (\"base_query\".\"id\") ",
        ));
        assert!(
            sql.contains("DISTINCT ON (\"base_query\".\"id\")"),
            "distinct count must carry DISTINCT ON into the subquery, got: {sql}"
        );
        assert!(
            sql.contains("AS distinct_query"),
            "distinct count must wrap in a subquery, got: {sql}"
        );
        assert!(sql.contains("SELECT COUNT(*) FROM ("));
    }

    #[test]
    fn test_count_sql_with_distinct_and_outer_conditions_wraps_inner() {
        let mut inputs = default_count_inputs(
            "WITH base_query AS (SELECT 1)",
            "DISTINCT ON (\"base_query\".\"id\") ",
        );
        inputs.has_outer_conditions = true;
        inputs.outer_where_clause = " WHERE rn = 1";
        let sql = build_count_sql(inputs);
        assert!(sql.contains("DISTINCT ON (\"base_query\".\"id\")"));
        assert!(sql.contains("AS inner_query"));
        assert!(sql.contains(" WHERE rn = 1"));
    }

    #[test]
    fn test_count_sql_with_distinct_and_group_by_uses_columns_not_one() {
        let mut inputs = default_count_inputs(
            "WITH base_query AS (SELECT 1)",
            "DISTINCT ON (\"base_query\".\"id\") ",
        );
        inputs.group_by_present = true;
        inputs.group_by_clause = " GROUP BY \"base_query\".\"id\"";
        let sql = build_count_sql(inputs);
        assert!(
            sql.contains("DISTINCT ON (\"base_query\".\"id\") \"base_query\".*"),
            "group-by + distinct must select real columns (not 1), got: {sql}"
        );
        assert!(sql.contains("AS grouped"));
    }

    #[test]
    fn test_count_sql_group_by_without_distinct_uses_select_one() {
        let mut inputs = default_count_inputs("WITH base_query AS (SELECT 1)", "");
        inputs.group_by_present = true;
        inputs.group_by_clause = " GROUP BY \"base_query\".\"id\"";
        let sql = build_count_sql(inputs);
        assert!(
            sql.contains("SELECT 1 FROM base_query"),
            "group-by-only fast path should SELECT 1, got: {sql}"
        );
        assert!(!sql.contains("DISTINCT ON"));
    }
}
