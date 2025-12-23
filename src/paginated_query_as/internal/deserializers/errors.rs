use std::fmt;

/// Errors that can occur when parsing filter parameters.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterParseError {
    /// Invalid filter operator (e.g., "Foo" instead of "Eq")
    InvalidOperator { field: String, raw_operator: String },

    /// Invalid filter format (missing ":" separator)
    InvalidFilterFormat { field: String, raw_value: String },

    /// Invalid page number (non-numeric, negative, zero)
    InvalidPageNumber { value: String },

    /// Invalid page size (non-numeric, negative, zero)
    InvalidPageSize { value: String },

    /// Only one of page/page_size provided (must be both or neither)
    IncompletePagination { provided: String },
}

impl std::error::Error for FilterParseError {}

impl fmt::Display for FilterParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOperator { field, raw_operator } => {
                write!(
                    f,
                    "invalid operator '{}' for field '{}'",
                    raw_operator, field
                )
            }
            Self::InvalidFilterFormat { field, raw_value } => {
                write!(
                    f,
                    "invalid filter format '{}' for field '{}', expected 'Operator:value'",
                    raw_value, field
                )
            }
            Self::InvalidPageNumber { value } => {
                write!(f, "invalid page number '{}'", value)
            }
            Self::InvalidPageSize { value } => {
                write!(f, "invalid page size '{}'", value)
            }
            Self::IncompletePagination { provided } => {
                write!(
                    f,
                    "incomplete pagination: '{}' provided without its counterpart",
                    provided
                )
            }
        }
    }
}
