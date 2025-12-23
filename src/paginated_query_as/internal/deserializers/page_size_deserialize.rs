use super::FilterParseError;
use serde::{de::Error, Deserialize, Deserializer};

/// Deserializes a page size from query parameters.
///
/// Returns:
/// - `Ok(None)` if the value is `None` or empty (pagination not requested)
/// - `Ok(Some(n))` if the value is a valid positive integer
/// - `Err(FilterParseError::InvalidPageSize)` if the value is invalid
pub fn page_size_deserialize<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;

    match value {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => Ok(None),
        Some(s) => {
            let trimmed = s.trim();
            match trimmed.parse::<i64>() {
                Ok(n) if n > 0 => Ok(Some(n)),
                _ => Err(D::Error::custom(FilterParseError::InvalidPageSize {
                    value: s,
                })),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn deserialize_test<T, F>(json: &str, deserialize_fn: F) -> Result<T, serde_json::Error>
    where
        F: FnOnce(Value) -> Result<T, serde_json::Error>,
    {
        let value: Value = serde_json::from_str(json)?;
        deserialize_fn(value)
    }

    #[test]
    fn test_page_size_deserialize_none() {
        assert_eq!(
            deserialize_test(r#"null"#, page_size_deserialize).unwrap(),
            None
        );
    }

    #[test]
    fn test_page_size_deserialize_empty() {
        assert_eq!(
            deserialize_test(r#""""#, page_size_deserialize).unwrap(),
            None
        );
        assert_eq!(
            deserialize_test(r#""  ""#, page_size_deserialize).unwrap(),
            None
        );
    }

    #[test]
    fn test_page_size_deserialize_valid() {
        assert_eq!(
            deserialize_test(r#""1""#, page_size_deserialize).unwrap(),
            Some(1)
        );
        assert_eq!(
            deserialize_test(r#""10""#, page_size_deserialize).unwrap(),
            Some(10)
        );
        assert_eq!(
            deserialize_test(r#""20""#, page_size_deserialize).unwrap(),
            Some(20)
        );
        assert_eq!(
            deserialize_test(r#""100""#, page_size_deserialize).unwrap(),
            Some(100)
        );
    }

    #[test]
    fn test_page_size_deserialize_zero_is_invalid() {
        let result = deserialize_test(r#""0""#, page_size_deserialize);
        assert!(result.is_err());
    }

    #[test]
    fn test_page_size_deserialize_negative_is_invalid() {
        let result = deserialize_test(r#""-1""#, page_size_deserialize);
        assert!(result.is_err());

        let result = deserialize_test(r#""-50""#, page_size_deserialize);
        assert!(result.is_err());
    }

    #[test]
    fn test_page_size_deserialize_non_numeric_is_invalid() {
        let result = deserialize_test(r#""abc""#, page_size_deserialize);
        assert!(result.is_err());

        let result = deserialize_test(r#""size25""#, page_size_deserialize);
        assert!(result.is_err());
    }
}
