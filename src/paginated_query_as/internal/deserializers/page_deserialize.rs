use super::FilterParseError;
use serde::{de::Error, Deserialize, Deserializer};

/// Deserializes a page number from query parameters.
///
/// Returns:
/// - `Ok(None)` if the value is `None` or empty (pagination not requested)
/// - `Ok(Some(n))` if the value is a valid positive integer
/// - `Err(FilterParseError::InvalidPageNumber)` if the value is invalid
pub fn page_deserialize<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
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
                _ => Err(D::Error::custom(FilterParseError::InvalidPageNumber {
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
    fn test_page_deserialize_none() {
        assert_eq!(deserialize_test(r#"null"#, page_deserialize).unwrap(), None);
    }

    #[test]
    fn test_page_deserialize_empty() {
        assert_eq!(deserialize_test(r#""""#, page_deserialize).unwrap(), None);
        assert_eq!(
            deserialize_test(r#""  ""#, page_deserialize).unwrap(),
            None
        );
    }

    #[test]
    fn test_page_deserialize_valid() {
        assert_eq!(
            deserialize_test(r#""1""#, page_deserialize).unwrap(),
            Some(1)
        );
        assert_eq!(
            deserialize_test(r#""2""#, page_deserialize).unwrap(),
            Some(2)
        );
        assert_eq!(
            deserialize_test(r#""10""#, page_deserialize).unwrap(),
            Some(10)
        );
        assert_eq!(
            deserialize_test(r#""100""#, page_deserialize).unwrap(),
            Some(100)
        );
    }

    #[test]
    fn test_page_deserialize_zero_is_invalid() {
        let result = deserialize_test(r#""0""#, page_deserialize);
        assert!(result.is_err());
    }

    #[test]
    fn test_page_deserialize_negative_is_invalid() {
        let result = deserialize_test(r#""-1""#, page_deserialize);
        assert!(result.is_err());

        let result = deserialize_test(r#""-100""#, page_deserialize);
        assert!(result.is_err());
    }

    #[test]
    fn test_page_deserialize_non_numeric_is_invalid() {
        let result = deserialize_test(r#""abc""#, page_deserialize);
        assert!(result.is_err());

        let result = deserialize_test(r#""page 5""#, page_deserialize);
        assert!(result.is_err());

        let result = deserialize_test(r#""abc123""#, page_deserialize);
        assert!(result.is_err());
    }
}
