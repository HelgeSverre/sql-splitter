//! Constant strategy - replace values with a fixed constant.

use super::{RedactValue, Strategy, StrategyKind};

/// Strategy that replaces all non-NULL values with a constant
#[derive(Debug, Clone)]
pub struct ConstantStrategy {
    value: String,
}

impl ConstantStrategy {
    pub fn new(value: String) -> Self {
        Self { value }
    }
}

impl Strategy for ConstantStrategy {
    fn apply(&self, value: &RedactValue, _rng: &mut dyn rand::RngCore) -> RedactValue {
        // Preserve NULL values, replace everything else with constant
        if value.is_null() {
            RedactValue::Null
        } else {
            RedactValue::String(self.value.clone())
        }
    }

    fn kind(&self) -> StrategyKind {
        StrategyKind::Constant {
            value: self.value.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn test_constant_strategy() {
        let strategy = ConstantStrategy::new("REDACTED".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        // String becomes constant
        let result = strategy.apply(&RedactValue::String("secret".to_string()), &mut rng);
        match result {
            RedactValue::String(s) => assert_eq!(s, "REDACTED"),
            _ => panic!("Expected String"),
        }

        // Integer becomes constant
        let result = strategy.apply(&RedactValue::Integer(123), &mut rng);
        match result {
            RedactValue::String(s) => assert_eq!(s, "REDACTED"),
            _ => panic!("Expected String"),
        }

        // NULL stays NULL
        let result = strategy.apply(&RedactValue::Null, &mut rng);
        assert!(matches!(result, RedactValue::Null));
    }

    #[test]
    fn test_password_hash_constant() {
        let strategy = ConstantStrategy::new("$2b$10$REDACTED_PASSWORD_HASH".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(
            &RedactValue::String("$2b$10$real_hash_here".to_string()),
            &mut rng,
        );
        match result {
            RedactValue::String(s) => assert_eq!(s, "$2b$10$REDACTED_PASSWORD_HASH"),
            _ => panic!("Expected String"),
        }
    }
}
