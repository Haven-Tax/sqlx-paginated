# sqlx-paginated

[![Rust](https://github.com/alexandrughinea/sqlx-paginated/actions/workflows/rust.yml/badge.svg?branch=main)](https://github.com/alexandrughinea/sqlx-paginated/actions/workflows/rust.yml)
[![crates.io](https://img.shields.io/crates/v/sqlx-paginated.svg)](https://crates.io/crates/sqlx-paginated)
[![docs](https://docs.rs/sqlx-paginated/badge.svg)](https://docs.rs/sqlx-paginated/latest/sqlx_paginated/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Type-safe SQLx helpers for paginated API endpoints. Build dynamic search, sorting, and typed filters from query parameters, then execute them through SQLx with parameter binding and column validation.

## Features

- Paginated `sqlx::query_as` execution for PostgreSQL.
- Optional pagination. If `page` and `page_size` are absent, no `LIMIT/OFFSET` is added.
- Optional total counts via `disable_totals_count()`.
- URL-driven search across explicit column lists.
- URL-driven sorting by real columns or registered virtual columns.
- Typed filters with operators such as `Eq`, `Gt`, `In`, `Between`, `IsNull`, and `Contains`.
- Automatic filter value parsing for bools, UUIDs, numbers, dates, times, and datetimes.
- Virtual columns backed by SQL expressions and lazily activated joins.
- Query builder hooks for custom conditions, custom selects, grouping, `DISTINCT ON`, raw sort expressions, and outer-query filtering.
- Identifier quoting, parameter binding, and column validation against serializable model fields.

## Database Support

| Area | Status |
| --- | --- |
| PostgreSQL paginated execution | Supported |
| PostgreSQL query builder | Supported |
| SQLite query builder | Partial builder support behind `sqlite` feature |
| MySQL | Feature flag exists, query builder support not implemented |

`PaginatedQueryBuilder::fetch_paginated` currently accepts `Pool<Postgres>`.

## Installation

```toml
[dependencies]
sqlx-paginated = { version = "0.2.33", features = ["postgres"] }
```

Rust imports use crate name `sqlx_paginated`:

```rust
use sqlx_paginated::{paginated_query_as, QueryParamsBuilder};
```

## Quick Start

```rust
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{FromRow, PgPool};
use sqlx_paginated::{
    paginated_query_as, PaginatedResponse, QueryParamsBuilder, QuerySortDirection,
};

#[derive(Default, FromRow, Serialize)]
struct User {
    id: i64,
    first_name: String,
    last_name: String,
    email: String,
    confirmed: bool,
    created_at: Option<DateTime<Utc>>,
}

async fn get_users(pool: &PgPool) -> Result<PaginatedResponse<User>, sqlx::Error> {
    let params = QueryParamsBuilder::<User>::new()
        .with_pagination(1, 25)
        .expect("valid pagination")
        .with_search("john", vec!["first_name", "last_name", "email"])
        .with_sort("created_at", QuerySortDirection::Descending)
        .build();

    paginated_query_as!(User, "SELECT * FROM users")
        .with_params(params)
        .fetch_paginated(pool)
        .await
}
```

Function syntax is also available:

```rust
use sqlx_paginated::paginated_query_as;

let users = paginated_query_as::<User>("SELECT * FROM users")
    .with_params(params)
    .fetch_paginated(pool)
    .await?;
```

## Web Query Parameters

`FlatQueryParams` can deserialize query strings from web frameworks. Convert it into `QueryParams<T>` before passing it to the paginated query.

```rust
use sqlx_paginated::{FlatQueryParams, PaginatedResponse, QueryParams};

async fn list_users(flat: FlatQueryParams, pool: &sqlx::PgPool) -> Result<PaginatedResponse<User>, sqlx::Error> {
    let params: QueryParams<User> = flat.try_into().expect("valid filters");

    sqlx_paginated::paginated_query_as!(User, "SELECT * FROM users")
        .with_params(params)
        .fetch_paginated(pool)
        .await
}
```

Supported reserved query parameters:

| Parameter | Format | Notes |
| --- | --- | --- |
| `page` | integer, `>= 1` | Must be paired with `page_size`. |
| `page_size` | integer, `>= 1` | Must be paired with `page`. |
| `sort_column` | string | Real column or virtual column registered in custom builder. |
| `sort_direction` | `ascending` or `descending` | Deserialized as `QuerySortDirection`. |
| `search` | string | Empty or whitespace-only search is ignored. `%` and `_` are escaped, whitespace is normalized, and input is capped at 100 chars. |
| `search_columns` | comma-separated columns | Only valid columns are used. Defaults to `name,description` when deserialized through `FlatQueryParams`. |

Filters use `field=Operator:value`.

```text
GET /users?page=1&page_size=25
    &search=john
    &search_columns=first_name,last_name,email
    &sort_column=created_at
    &sort_direction=descending
    &confirmed=Eq:true
    &role=In:admin,manager
    &created_at=Gte:2026-01-01
```

All non-reserved query parameters are treated as filters. Unknown extra parameters must still use `Operator:value` format or deserialization returns `FilterParseError::InvalidFilterFormat`.

Logical groups use `$and` and `$or` with bracket notation.

```text
GET /users?$and[0][$or][0][username]=Eq:phiberber
    &$and[0][$or][1][age]=Gte:18
    &$and[1][organizationId]=Eq:550e8400-e29b-41d4-a716-446655440000
```

This becomes one root `AND` filter expression. Bracket leaf values encode operator in value.

Supported filter operators:

| Operator | SQL shape |
| --- | --- |
| `Eq`, `Ne` | `=`, `<>`; bools use `IS TRUE/FALSE` |
| `Gt`, `Lt`, `Gte`, `Lte` | comparison |
| `Like`, `ILike` | pattern match; non-strings cast to text |
| `In`, `NotIn` | comma-separated values |
| `Between` | two comma-separated values |
| `IsNull`, `IsNotNull` | null checks; bracket form uses bare operator value |
| `Contains` | PostgreSQL `@>` |

Filter values are parsed in this order: bool, UUID, RFC3339 datetime, naive datetime, date, time, integer, float, string. Date filters cast timestamp columns to `::date` so calendar-day comparisons work as expected.

## QueryParamsBuilder

Use `QueryParamsBuilder` when parameters come from application code instead of a query string.

```rust
use sqlx_paginated::{FilterOperator, FilterValue, QueryParamsBuilder, QuerySortDirection};

let params = QueryParamsBuilder::<User>::new()
    .with_pagination(1, 50)
    .expect("valid pagination")
    .with_search("smith", vec!["first_name", "last_name", "email"])
    .with_sort("created_at", QuerySortDirection::Descending)
    .with_filter("confirmed", FilterOperator::Eq, FilterValue::Bool(true))
    .with_filter(
        "role",
        FilterOperator::In,
        FilterValue::Array(vec![
            FilterValue::String("admin".to_string()),
            FilterValue::String("manager".to_string()),
        ]),
    )
    .build();
```

`with_pagination` returns `Result` and rejects page or page size values below `1`. Use `without_pagination()` to clear pagination explicitly.

## Custom QueryBuilder

`PaginatedQueryBuilder::with_query_builder` lets you replace default query construction. Default behavior is equivalent to:

```rust
use sqlx::Postgres;
use sqlx_paginated::QueryBuilder;

QueryBuilder::<User, Postgres>::new()
    .with_search(params)
    .with_filters(params)
    .with_sorting(params)
    .build()
```

### Virtual Columns And Lazy Joins

Virtual columns expose joined or computed SQL expressions to search, filters, and sorting. Joins are added only when the virtual column is used.

Use `with_column_type` inside the virtual-column closure to tell filter generation which SQL cast to use for that expression. It defaults to `FieldType::String`, which is correct for text expressions. Set it explicitly for numeric, UUID, date/time, or boolean expressions so operators such as `Eq`, `Gt`, `In`, and `Between` bind values with the right PostgreSQL type cast.

```rust
use sqlx::Postgres;
use sqlx_paginated::{FieldType, QueryBuilder};

let users = sqlx_paginated::paginated_query_as!(User, "SELECT * FROM users")
    .with_params(params)
    .with_query_builder(|params| {
        QueryBuilder::<User, Postgres>::new()
            .with_virtual_column("organization_name", |vc| {
                vc.with_join("LEFT JOIN organizations ON organizations.id = base_query.organization_id")
                    .with_column_type(FieldType::String);
                "organizations.name"
            })
            .with_virtual_column("last_payment_at", |vc| {
                vc.with_join("LEFT JOIN payments ON payments.user_id = base_query.id")
                    .with_column_type(FieldType::DateTime);
                "payments.created_at"
            })
            .with_search(params)
            .with_filters(params)
            .with_sorting(params)
            .build()
    })
    .fetch_paginated(pool)
    .await?;
```

When used with `PaginatedQueryBuilder`, base SQL is wrapped as `WITH base_query AS (...)`, so virtual-column joins should reference `base_query`.

`with_column_type` belongs to virtual columns only. For real model columns, use `with_column_cast` on `QueryBuilder`.

### Custom Sorting

`QueryBuilder` supports three sort forms. Sort items are quoted when they are plain columns, but real-column sorts are not model-validated by `with_sort_column` or `with_sorting`; validate/allowlist user-facing sort options before passing them through if the API accepts arbitrary `sort_column` values.

```rust
use sqlx_paginated::{QuerySortDirection, SortItem};

QueryBuilder::<User, Postgres>::new()
    .with_sort_column("created_at", QuerySortDirection::Descending)
    .with_sort_expression("LOWER(email)", QuerySortDirection::Ascending)
    .with_sort(SortItem::column("id"), QuerySortDirection::Ascending)
    .build();
```

`with_sorting(params)` applies URL-driven sorting and resolves virtual columns to their SQL expressions.

### Custom Selects, Grouping, And DISTINCT ON

```rust
QueryBuilder::<User, Postgres>::new()
    .with_select("base_query.organization_id")
    .with_select("COUNT(*) as user_count")
    .with_group_by("base_query.organization_id")
    .with_distinct_on(vec!["organization_id"])
    .with_sort_column("organization_id", QuerySortDirection::Ascending)
    .build();
```

Counts are generated through subqueries when grouping or `DISTINCT ON` requires it.

### Outer Query Filtering

Use `with_outer` when a window function or derived column must be filtered after the inner query runs.

```rust
QueryBuilder::<User, Postgres>::new()
    .with_select("base_query.*")
    .with_select("ROW_NUMBER() OVER (PARTITION BY organization_id ORDER BY created_at DESC) AS row_num")
    .with_outer(|outer| {
        outer
            .condition("row_num = 1")
            .select("organization_id")
            .select("COUNT(*) as count")
            .group_by("organization_id");
    })
    .build();
```

### Column Types And Validation

`QueryBuilder` infers columns from `T: Default + Serialize`. Optional fields often serialize as `null`, so type inference can fall back to filter values. Override casts when needed with `with_column_cast`.

Cast selection order:

1. `with_column_cast(column, field_type)` override.
2. Field metadata inferred from the serialized model, or `with_column_type` for a virtual column.
3. Filter value type inferred from query parameter value.

`with_column_cast` applies to both real columns and virtual columns, and wins over `with_column_type`. Use it when model defaults are ambiguous, when an `Option<T>` serializes as `null`, or when a public filter value looks numeric but the database column should be treated as text.

```rust
QueryBuilder::<User, Postgres>::new()
    .with_column_cast("external_id", FieldType::String)
    .with_filters(params)
    .build();
```

Example: `external_id=Eq:123` would otherwise infer `123` as an integer and emit a bigint cast. With `FieldType::String`, it binds as text and compares against the column without bigint casting.

Available field types:

| Field type | PostgreSQL cast behavior |
| --- | --- |
| `FieldType::String` | No value cast |
| `FieldType::Uuid` | `::uuid` |
| `FieldType::Int` | `::bigint` |
| `FieldType::Float` | `::float8` |
| `FieldType::Bool` | `Eq`/`Ne` use `IS TRUE/FALSE`; other operators cast values with `::boolean` |
| `FieldType::DateTime` | `::timestamptz` |
| `FieldType::Date` | `::date`; column is also cast to `::date` for date comparisons |
| `FieldType::Time` | `::time` |
| `FieldType::Unknown` | No cast |

Useful builder methods:

| Method | Purpose |
| --- | --- |
| `with_column_cast(column, field_type)` | Override filter cast type for a real or virtual column. |
| `with_fields_from::<U>()` | Add columns from another serializable type to validation/type metadata. |
| `map_column(column, mapper)` | Map a public column name to custom SQL and optional placeholder. |
| `with_condition(column, op, value)` | Add checked condition with bound value. |
| `with_raw_condition(sql)` | Add raw SQL condition. Use only with trusted SQL. |
| `with_combined_conditions(fn)` | Mutate builder for custom condition groups. |
| `disable_protection()` | Disable column protection checks. |
| `disable_column_validation()` | Allow columns not present on serialized model. |

## Response Shape

With pagination and total counts enabled:

```json
{
  "records": [
    {
      "id": 1,
      "first_name": "John",
      "last_name": "Smith",
      "email": "john@example.com",
      "confirmed": true,
      "created_at": "2026-01-01T00:00:00Z"
    }
  ],
  "page": 1,
  "page_size": 25,
  "total": 1,
  "total_pages": 1
}
```

If pagination is omitted, `page`, `page_size`, and `total_pages` are omitted. If `disable_totals_count()` is used, `total` and `total_pages` are omitted.

## Performance Notes

- Add indexes for common search, filter, and sort columns.
- Prefer explicit selected columns for aggregate/reporting queries.
- Use `disable_totals_count()` for large datasets where UI does not need total pages.
- Keep `search_columns` narrow. Search conditions are OR-ed across supplied columns.
- Virtual-column joins are lazy, but once activated they apply to main query and count query.

## Security Notes

- Values are bound as SQLx parameters.
- Identifiers are quoted before SQL generation.
- Search and filters validate columns against known model fields or registered virtual columns.
- URL-driven sort columns are quoted, but not model-validated by default. Prefer mapping public sort options to known columns or virtual columns before calling `with_sorting`.
- Raw SQL methods and disabled protection modes are developer-trusted escape hatches.
- System-schema and dangerous identifier patterns are blocked by default column protection.

## Contributing

Issues and pull requests are welcome. Please include tests for query generation changes, especially around filters, sorting, counts, and virtual columns.

## License

This project is licensed under the MIT License. See [LICENSE.md](LICENSE.md).
