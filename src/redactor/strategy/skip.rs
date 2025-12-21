//! Skip strategy - no redaction, passthrough.

use super::{RedactValue, Strategy, StrategyKind};

/// Strategy that passes values through unchanged
#[derive(Debug, Clone, Default)]
pub struct SkipStrategy;

impl SkipStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl Strategy for SkipStrategy {
    fn apply(&self, value: &RedactValue, _rng: &mut dyn rand::RngCore) -> RedactValue {
        // Return value unchanged
        value.clone()
    }

    fn kind(&self) -> StrategyKind {
        StrategyKind::Skip
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn test_skip_strategy() {
        let strategy = SkipStrategy::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        // String passes through
        let result = strategy.apply(&RedactValue::String("test".to_string()), &mut rng);
        match result {
            RedactValue::String(s) => assert_eq!(s, "test"),
            _ => panic!("Expected String"),
        }

        // Integer passes through
        let result = strategy.apply(&RedactValue::Integer(123), &mut rng);
        match result {
            RedactValue::Integer(i) => assert_eq!(i, 123),
            _ => panic!("Expected Integer"),
        }

        // NULL passes through
        let result = strategy.apply(&RedactValue::Null, &mut rng);
        assert!(matches!(result, RedactValue::Null));
    }
}
