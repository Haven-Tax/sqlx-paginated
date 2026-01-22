use crate::paginated_query_as::internal::{
    get_struct_field_meta, FilterParseError, QueryPaginationParams, QuerySearchParams,
    QuerySortParams,
};
use crate::paginated_query_as::models::{Filter, FilterOperator, FilterValue, QuerySortDirection};
use crate::QueryParams;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct QueryParamsBuilder<'q, T> {
    query: QueryParams<'q, T>,
}

impl<T: Default + Serialize> Default for QueryParamsBuilder<'_, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'q, T: Default + Serialize> QueryParamsBuilder<'q, T> {
    /// Creates a new `QueryParamsBuilder` with default values.
    ///
    /// Default values include:
    /// - Pagination: None (no LIMIT/OFFSET)
    /// - Sort column: "created_at"
    /// - Sort direction: Descending
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryParamsBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    /// let builder = QueryParamsBuilder::<UserExample>::new();
    /// ```
    pub fn new() -> Self {
        Self {
            query: QueryParams::default(),
        }
    }

    /// Sets pagination parameters.
    ///
    /// # Arguments
    ///
    /// * `page` - Page number (1-indexed, must be > 0)
    /// * `page_size` - Number of items per page (must be > 0)
    ///
    /// # Errors
    ///
    /// Returns `FilterParseError::InvalidPageNumber` if page < 1.
    /// Returns `FilterParseError::InvalidPageSize` if page_size < 1.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryParamsBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    /// let builder = QueryParamsBuilder::<UserExample>::new()
    ///     .with_pagination(1, 20)
    ///     .expect("valid pagination");
    /// ```
    pub fn with_pagination(mut self, page: i64, page_size: i64) -> Result<Self, FilterParseError> {
        if page < 1 {
            return Err(FilterParseError::InvalidPageNumber {
                value: page.to_string(),
            });
        }
        if page_size < 1 {
            return Err(FilterParseError::InvalidPageSize {
                value: page_size.to_string(),
            });
        }
        self.query.pagination = Some(QueryPaginationParams { page, page_size });
        Ok(self)
    }

    /// Explicitly disables pagination (returns all results without LIMIT/OFFSET).
    ///
    /// This is the default state, but can be useful to explicitly clear pagination
    /// that was previously set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryParamsBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    /// let builder = QueryParamsBuilder::<UserExample>::new()
    ///     .without_pagination();
    /// ```
    pub fn without_pagination(mut self) -> Self {
        self.query.pagination = None;
        self
    }

    /// Sets sorting parameters.
    ///
    /// # Arguments
    ///
    /// * `sort_column` - Column name to sort by
    /// * `sort_direction` - Direction of sort (Ascending or Descending)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryParamsBuilder, QuerySortDirection};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    ///
    /// let params = QueryParamsBuilder::<UserExample>::new()
    ///     .with_sort("updated_at", QuerySortDirection::Ascending)
    ///     .build();
    /// ```
    pub fn with_sort(
        mut self,
        sort_column: impl Into<String>,
        sort_direction: QuerySortDirection,
    ) -> Self {
        self.query.sort = Some(QuerySortParams {
            sort_column: Some(sort_column.into()),
            sort_direction: Some(sort_direction),
        });
        self
    }

    /// Sets search parameters with multiple columns support.
    ///
    /// # Arguments
    ///
    /// * `search` - Search term to look for
    /// * `search_columns` - Vector of column names to search in
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryParamsBuilder, QuerySortDirection};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String
    /// }
    ///
    /// let params = QueryParamsBuilder::<UserExample>::new()
    ///     .with_search("john", vec!["name", "email", "username"])
    ///     .build();
    /// ```
    pub fn with_search(
        mut self,
        search: impl Into<String>,
        search_columns: Vec<impl Into<String>>,
    ) -> Self {
        self.query.search = QuerySearchParams {
            search: Some(search.into()),
            search_columns: Some(search_columns.into_iter().map(Into::into).collect()),
        };
        self
    }

    /// Adds a filter with the specified field, operator, and value.
    ///
    /// # Arguments
    ///
    /// * `field` - Column name to filter on
    /// * `operator` - Filter operator (Eq, Ne, Gt, Lt, etc.)
    /// * `value` - Filter value
    ///
    /// # Details
    ///
    /// Only adds the filter if the column exists in the model struct.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryParamsBuilder, FilterOperator, FilterValue};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String,
    ///     status: String,
    /// }
    ///
    /// let params = QueryParamsBuilder::<UserExample>::new()
    ///     .with_filter("status", FilterOperator::Eq, FilterValue::String("active".to_string()))
    ///     .build();
    /// ```
    pub fn with_filter(
        mut self,
        field: impl Into<String>,
        operator: FilterOperator,
        value: FilterValue,
    ) -> Self {
        let field = field.into();
        let valid_fields: Vec<String> = get_struct_field_meta::<T>().keys().cloned().collect();

        if valid_fields.contains(&field) {
            self.query.filters.push(Filter {
                field,
                operator,
                value,
            });
        } else {
            #[cfg(feature = "tracing")]
            tracing::warn!(column = %field, "Skipping invalid filter column");
        }
        self
    }

    /// Adds a simple equality filter (shorthand for with_filter with Eq operator).
    ///
    /// # Arguments
    ///
    /// * `field` - Column name to filter on
    /// * `value` - Value to filter by (will be converted to FilterValue::String)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryParamsBuilder};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String,
    ///     status: String,
    /// }
    ///
    /// let params = QueryParamsBuilder::<UserExample>::new()
    ///     .with_eq_filter("status", "active")
    ///     .build();
    /// ```
    pub fn with_eq_filter(self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.with_filter(
            field,
            FilterOperator::Eq,
            FilterValue::String(value.into()),
        )
    }

    /// Adds multiple filters.
    ///
    /// # Arguments
    ///
    /// * `filters` - Vector of Filter structs
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::{Serialize};
    /// use sqlx_paginated::{QueryParamsBuilder, Filter, FilterOperator, FilterValue};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String,
    ///     status: String,
    /// }
    ///
    /// let filters = vec![
    ///     Filter {
    ///         field: "status".to_string(),
    ///         operator: FilterOperator::Eq,
    ///         value: FilterValue::String("active".to_string()),
    ///     },
    /// ];
    ///
    /// let params = QueryParamsBuilder::<UserExample>::new()
    ///     .with_filters(filters)
    ///     .build();
    /// ```
    pub fn with_filters(mut self, filters: Vec<Filter>) -> Self {
        let valid_fields: Vec<String> = get_struct_field_meta::<T>().keys().cloned().collect();

        for filter in filters {
            if valid_fields.contains(&filter.field) {
                self.query.filters.push(filter);
            } else {
                #[cfg(feature = "tracing")]
                tracing::warn!(column = %filter.field, "Skipping invalid filter column");
            }
        }
        self
    }

    /// Builds and returns the final QueryParams.
    ///
    /// # Returns
    ///
    /// Returns the constructed `QueryParams<T>` with all the configured parameters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sqlx_paginated::{QueryParamsBuilder, QuerySortDirection, FilterOperator, FilterValue};
    /// use serde::{Serialize};
    ///
    /// #[derive(Serialize, Default)]
    /// struct UserExample {
    ///     name: String,
    ///     status: String,
    ///     email: String,
    /// }
    ///
    /// let params = QueryParamsBuilder::<UserExample>::new()
    ///     .with_pagination(1, 20)
    ///     .expect("valid pagination")
    ///     .with_sort("created_at", QuerySortDirection::Descending)
    ///     .with_search("john", vec!["name", "email"])
    ///     .with_filter("status", FilterOperator::Eq, FilterValue::String("active".to_string()))
    ///     .build();
    /// ```
    pub fn build(self) -> QueryParams<'q, T> {
        self.query
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paginated_query_as::internal::{
        DEFAULT_SEARCH_COLUMN_NAMES,
    };

    #[derive(Debug, Default, Serialize)]
    struct TestModel {
        name: String,
        title: String,
        description: String,
        status: String,
        category: String,
    }

    #[test]
    fn test_pagination_defaults_to_none() {
        let params = QueryParamsBuilder::<TestModel>::new().build();

        assert!(
            params.pagination.is_none(),
            "Default pagination should be None"
        );
    }

    #[test]
    fn test_default_sort_column() {
        let params = QueryParamsBuilder::<TestModel>::new().build();

        assert_eq!(
            params.sort.clone().and_then(|s| s.sort_column), None,
            "Default sort column should be None"
        );
    }

    #[test]
    fn test_search_defaults() {
        let params = QueryParamsBuilder::<TestModel>::new().build();

        assert_eq!(
            params.search.search_columns,
            Some(
                DEFAULT_SEARCH_COLUMN_NAMES
                    .iter()
                    .map(|&s| s.to_string())
                    .collect()
            ),
            "Default search columns should be {:?}",
            DEFAULT_SEARCH_COLUMN_NAMES
        );
        assert!(
            params.search.search.is_none(),
            "Default search term should be None"
        );
    }

    #[test]
    fn test_combined_defaults() {
        let params = QueryParamsBuilder::<TestModel>::new().build();

        assert!(params.pagination.is_none());
        assert!(params.sort.is_none());
        assert!(params.search.search_columns.is_none());
        assert!(params.search.search.is_none());
    }

    #[test]
    fn test_empty_params() {
        let params = QueryParamsBuilder::<TestModel>::new().build();

        assert!(params.pagination.is_none());
        assert!(params.sort.is_none());
    }

    #[test]
    fn test_partial_params() {
        let params = QueryParamsBuilder::<TestModel>::new()
            .with_pagination(2, 10)
            .unwrap()
            .with_search("test".to_string(), vec!["name".to_string()])
            .build();

        let pagination = params.pagination.unwrap();
        assert_eq!(pagination.page, 2);
        assert_eq!(pagination.page_size, 10);
        assert_eq!(params.search.search, Some("test".to_string()));
        assert!(params.sort.is_none());
    }

    #[test]
    fn test_invalid_page_returns_error() {
        let result = QueryParamsBuilder::<TestModel>::new().with_pagination(0, 10);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FilterParseError::InvalidPageNumber { .. }
        ));
    }

    #[test]
    fn test_invalid_page_size_returns_error() {
        let result = QueryParamsBuilder::<TestModel>::new().with_pagination(1, 0);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FilterParseError::InvalidPageSize { .. }
        ));
    }

    #[test]
    fn test_negative_page_returns_error() {
        let result = QueryParamsBuilder::<TestModel>::new().with_pagination(-1, 10);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FilterParseError::InvalidPageNumber { .. }
        ));
    }

    #[test]
    fn test_without_pagination() {
        let params = QueryParamsBuilder::<TestModel>::new()
            .with_pagination(2, 10)
            .unwrap()
            .without_pagination()
            .build();

        assert!(params.pagination.is_none());
    }

    #[test]
    fn test_filters() {
        let params = QueryParamsBuilder::<TestModel>::new()
            .with_filter(
                "status",
                FilterOperator::Eq,
                FilterValue::String("active".to_string()),
            )
            .with_filter(
                "category",
                FilterOperator::Eq,
                FilterValue::String("test".to_string()),
            )
            .build();

        assert_eq!(params.filters.len(), 2);
        assert_eq!(params.filters[0].field, "status");
        assert_eq!(params.filters[0].operator, FilterOperator::Eq);
        assert_eq!(
            params.filters[0].value,
            FilterValue::String("active".to_string())
        );
    }

    #[test]
    fn test_eq_filter_shorthand() {
        let params = QueryParamsBuilder::<TestModel>::new()
            .with_eq_filter("status", "active")
            .build();

        assert_eq!(params.filters.len(), 1);
        assert_eq!(params.filters[0].field, "status");
        assert_eq!(params.filters[0].operator, FilterOperator::Eq);
        assert_eq!(
            params.filters[0].value,
            FilterValue::String("active".to_string())
        );
    }

    #[test]
    fn test_search_with_columns() {
        let params = QueryParamsBuilder::<TestModel>::new()
            .with_search(
                "test".to_string(),
                vec!["title".to_string(), "description".to_string()],
            )
            .build();

        assert_eq!(params.search.search, Some("test".to_string()));
        assert_eq!(
            params.search.search_columns,
            Some(vec!["title".to_string(), "description".to_string()])
        );
    }

    #[test]
    fn test_full_params() {
        let params = QueryParamsBuilder::<TestModel>::new()
            .with_pagination(2, 20)
            .unwrap()
            .with_sort("name".to_string(), QuerySortDirection::Ascending)
            .with_search(
                "test".to_string(),
                vec!["title".to_string(), "description".to_string()],
            )
            .with_eq_filter("status", "active")
            .build();

        let pagination = params.pagination.unwrap();
        assert_eq!(pagination.page, 2);
        assert_eq!(pagination.page_size, 20);
        assert_eq!(params.sort.clone().and_then(|s| s.sort_column), Some("name".to_string()));
        assert_eq!(params.sort.clone().and_then(|s| s.sort_direction), Some(QuerySortDirection::Ascending));
        assert_eq!(params.search.search, Some("test".to_string()));
        assert_eq!(
            params.search.search_columns,
            Some(vec!["title".to_string(), "description".to_string()])
        );
        assert_eq!(params.filters.len(), 1);
    }

    #[test]
    fn test_filter_chain() {
        let params = QueryParamsBuilder::<TestModel>::new()
            .with_eq_filter("status", "active")
            .with_eq_filter("category", "test")
            .build();

        assert_eq!(params.filters.len(), 2);
        assert_eq!(params.filters[0].field, "status");
        assert_eq!(params.filters[1].field, "category");
    }

    #[test]
    fn test_mixed_pagination() {
        let params = QueryParamsBuilder::<TestModel>::new()
            .with_pagination(2, 10)
            .unwrap()
            .with_search("test".to_string(), vec!["title".to_string()])
            .with_eq_filter("status", "active")
            .build();

        let pagination = params.pagination.unwrap();
        assert_eq!(pagination.page, 2);
        assert_eq!(pagination.page_size, 10);
        assert_eq!(params.search.search, Some("test".to_string()));
        assert_eq!(params.filters.len(), 1);
    }

    #[test]
    fn test_invalid_filter_column() {
        let params = QueryParamsBuilder::<TestModel>::new()
            .with_eq_filter("invalid_column", "value")
            .build();

        assert!(params.filters.is_empty(), "Invalid column should be skipped");
    }

    #[test]
    fn test_various_operators() {
        let params = QueryParamsBuilder::<TestModel>::new()
            .with_filter("status", FilterOperator::Ne, FilterValue::String("deleted".to_string()))
            .with_filter("name", FilterOperator::Like, FilterValue::String("%john%".to_string()))
            .build();

        assert_eq!(params.filters.len(), 2);
        assert_eq!(params.filters[0].operator, FilterOperator::Ne);
        assert_eq!(params.filters[1].operator, FilterOperator::Like);
    }
}
