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
        if self.preserve_domain && value.contains('@') {
            // Email: preserve domain
            if let Some((local, domain)) = value.rsplit_once('@') {
                let hash = self.compute_hash(local);
                return format!("{}@{}", &hash[..8], domain);
            }
        }

        // Regular hash: take first 16 chars of hex
        let hash = self.compute_hash(value);
        hash[..16].to_string()
    }

    /// Compute SHA256 hash and return hex string
    fn compute_hash(&self, value: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(value.as_bytes());
        let result = hasher.finalize();
        hex::encode(result)
    }
}

impl Strategy for HashStrategy {
    fn apply(&self, value: &RedactValue, _rng: &mut dyn rand::RngCore) -> RedactValue {
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
                assert_eq!(s.len(), 16);
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
                assert!(s.ends_with("@example.com"));
                assert!(s.len() > "@example.com".len());
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_hash_deterministic() {
        let strategy = HashStrategy::new(false);
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        // Same input = same output (for referential integrity)
        let result1 = strategy.apply(&RedactValue::String("test@example.com".to_string()), &mut rng);
        let result2 = strategy.apply(&RedactValue::String("test@example.com".to_string()), &mut rng);

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
