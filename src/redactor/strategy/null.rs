//! Null strategy - replace values with NULL.

use super::{RedactValue, Strategy, StrategyKind};

/// Strategy that replaces all values with NULL
#[derive(Debug, Clone, Default)]
pub struct NullStrategy;

impl NullStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl Strategy for NullStrategy {
    fn apply(&self, _value: &RedactValue, _rng: &mut dyn rand::RngCore) -> RedactValue {
        // Always return NULL regardless of input
        RedactValue::Null
    }

    fn kind(&self) -> StrategyKind {
        StrategyKind::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn test_null_strategy() {
        let strategy = NullStrategy::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        // String becomes NULL
        let result = strategy.apply(&RedactValue::String("test".to_string()), &mut rng);
        assert!(matches!(result, RedactValue::Null));

        // Integer becomes NULL
        let result = strategy.apply(&RedactValue::Integer(123), &mut rng);
        assert!(matches!(result, RedactValue::Null));

        // NULL stays NULL
        let result = strategy.apply(&RedactValue::Null, &mut rng);
        assert!(matches!(result, RedactValue::Null));
    }
}
