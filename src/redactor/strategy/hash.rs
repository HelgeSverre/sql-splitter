//! Hash strategy - one-way SHA256 hash.

use super::{RedactValue, Strategy, StrategyKind};
use sha2::{Digest, Sha256};

/// Strategy that hashes values with SHA256
#[derive(Debug, Clone)]
pub struct HashStrategy {
    /// Whether to preserve email domain
    preserve_domain: bool,
}

impl HashStrategy {
    pub fn new(preserve_domain: bool) -> Self {
        Self { preserve_domain }
    }

    /// Hash a string value
    fn hash_value(&self, value: &str) -> String {
        if self.preserve_domain {
            // Email: preserve domain
            if let Some((local, domain)) = value.rsplit_once('@') {
                let mut result = String::with_capacity(8 + 1 + domain.len());
                Self::write_hash_prefix(local, 8, &mut result);
                result.push('@');
                result.push_str(domain);
                return result;
            }
        }

        // Regular hash: take first 16 chars of hex
        let mut result = String::with_capacity(16);
        Self::write_hash_prefix(value, 16, &mut result);
        result
    }

    /// Hash `value` and append the requested leading hexadecimal digits.
    ///
    /// Redaction only exposes 8 or 16 hex digits. Encoding all 64 digits and
    /// then slicing them allocates and writes bytes that the caller discards.
    fn write_hash_prefix(value: &str, digits: usize, output: &mut String) {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let mut hasher = Sha256::new();
        hasher.update(value.as_bytes());
        let digest = hasher.finalize();
        let initial_len = output.len();

        for byte in digest.iter().take(digits.div_ceil(2)) {
            output.push(HEX[(byte >> 4) as usize] as char);
            if output.len() - initial_len == digits {
                break;
            }
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }
}

impl Strategy for HashStrategy {
    fn apply(&self, value: &RedactValue, _rng: &mut dyn rand::Rng) -> RedactValue {
        match value {
            RedactValue::Null => RedactValue::Null,
            RedactValue::String(s) => RedactValue::String(self.hash_value(s)),
            RedactValue::Integer(i) => RedactValue::String(self.hash_value(&i.to_string())),
            RedactValue::Bytes(b) => {
                let s = String::from_utf8_lossy(b);
                RedactValue::String(self.hash_value(&s))
            }
        }
    }

    fn kind(&self) -> StrategyKind {
        StrategyKind::Hash {
            preserve_domain: self.preserve_domain,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn test_hash_strategy() {
        let strategy = HashStrategy::new(false);
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(&RedactValue::String("secret".to_string()), &mut rng);
        match result {
            RedactValue::String(s) => {
                assert_eq!(s, "2bb80d537b1da3e3");
                // Hash is deterministic
                let result2 = strategy.apply(&RedactValue::String("secret".to_string()), &mut rng);
                match result2 {
                    RedactValue::String(s2) => assert_eq!(s, s2),
                    _ => panic!("Expected String"),
                }
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_hash_preserve_domain() {
        let strategy = HashStrategy::new(true);
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(
            &RedactValue::String("john.doe@example.com".to_string()),
            &mut rng,
        );
        match result {
            RedactValue::String(s) => {
                assert_eq!(s, "30f69670@example.com");
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_hash_deterministic() {
        let strategy = HashStrategy::new(false);
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        // Same input = same output (for referential integrity)
        let result1 = strategy.apply(
            &RedactValue::String("test@example.com".to_string()),
            &mut rng,
        );
        let result2 = strategy.apply(
            &RedactValue::String("test@example.com".to_string()),
            &mut rng,
        );

        match (result1, result2) {
            (RedactValue::String(s1), RedactValue::String(s2)) => assert_eq!(s1, s2),
            _ => panic!("Expected Strings"),
        }
    }

    #[test]
    fn test_hash_null() {
        let strategy = HashStrategy::new(false);
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(&RedactValue::Null, &mut rng);
        assert!(matches!(result, RedactValue::Null));
    }
}
