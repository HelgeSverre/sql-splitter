//! Mask strategy - partial masking with pattern.

use super::{RedactValue, Strategy, StrategyKind};
use rand::Rng;

/// Strategy that partially masks values using a pattern.
///
/// Pattern syntax:
/// - `*` = replace with asterisk
/// - `X` = keep original character
/// - `#` = replace with random digit
/// - Any other character = literal (e.g., `-`, `.`, `@`)
#[derive(Debug, Clone)]
pub struct MaskStrategy {
    pattern: String,
}

impl MaskStrategy {
    pub fn new(pattern: String) -> Self {
        Self { pattern }
    }

    /// Apply the mask pattern to a value
    fn mask_value(&self, value: &str, rng: &mut dyn rand::RngCore) -> String {
        let chars: Vec<char> = value.chars().collect();
        let mut result = String::with_capacity(self.pattern.len());

        let mut value_idx = 0;

        for pattern_char in self.pattern.chars() {
            match pattern_char {
                '*' => {
                    result.push('*');
                    value_idx += 1;
                }
                'X' => {
                    if value_idx < chars.len() {
                        result.push(chars[value_idx]);
                    }
                    value_idx += 1;
                }
                '#' => {
                    result.push(char::from_digit(rng.random_range(0..10), 10).unwrap());
                    value_idx += 1;
                }
                c => {
                    // Literal character (separator like -, space, etc.)
                    result.push(c);
                    // Advance value index if the original char matches
                    if value_idx < chars.len() && chars[value_idx] == c {
                        value_idx += 1;
                    }
                }
            }
        }

        result
    }
}

impl Strategy for MaskStrategy {
    fn apply(&self, value: &RedactValue, rng: &mut dyn rand::RngCore) -> RedactValue {
        match value {
            RedactValue::Null => RedactValue::Null,
            RedactValue::String(s) => RedactValue::String(self.mask_value(s, rng)),
            RedactValue::Integer(i) => RedactValue::String(self.mask_value(&i.to_string(), rng)),
            RedactValue::Bytes(b) => {
                let s = String::from_utf8_lossy(b);
                RedactValue::String(self.mask_value(&s, rng))
            }
        }
    }

    fn kind(&self) -> StrategyKind {
        StrategyKind::Mask {
            pattern: self.pattern.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn test_mask_credit_card() {
        // Keep last 4 digits
        let strategy = MaskStrategy::new("****-****-****-XXXX".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(
            &RedactValue::String("4532-0151-1283-0366".to_string()),
            &mut rng,
        );
        match result {
            RedactValue::String(s) => {
                assert!(s.starts_with("****-****-****-"));
                assert!(s.ends_with("0366"));
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_mask_email_first_char() {
        // Keep first character
        let strategy = MaskStrategy::new("X***@*****".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(
            &RedactValue::String("john@example.com".to_string()),
            &mut rng,
        );
        match result {
            RedactValue::String(s) => {
                assert!(s.starts_with('j'));
                assert!(s.contains('@'));
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_mask_random_digits() {
        let strategy = MaskStrategy::new("###-##-####".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(&RedactValue::String("123-45-6789".to_string()), &mut rng);
        match result {
            RedactValue::String(s) => {
                assert_eq!(s.len(), 11);
                assert!(s.chars().nth(3) == Some('-'));
                assert!(s.chars().nth(6) == Some('-'));
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_mask_null() {
        let strategy = MaskStrategy::new("****".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(&RedactValue::Null, &mut rng);
        assert!(matches!(result, RedactValue::Null));
    }
}
