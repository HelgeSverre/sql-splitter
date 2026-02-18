//! Shuffle strategy - redistribute values within a column.
//!
//! This strategy collects all values for a column and randomly redistributes them.
//! This preserves the statistical distribution while breaking the row correlation.
//!
//! Note: This requires collecting all values in memory, so it may not be suitable
//! for very large tables. Consider using hash strategy for large datasets.

use super::{RedactValue, Strategy, StrategyKind};
use rand::seq::SliceRandom;

/// Strategy that shuffles values within a column
#[derive(Debug, Clone, Default)]
pub struct ShuffleStrategy {
    /// Collected values for shuffling
    values: Vec<RedactValue>,
    /// Shuffled values (populated after shuffle())
    shuffled: Vec<RedactValue>,
    /// Current index into shuffled values
    index: usize,
}

impl ShuffleStrategy {
    pub fn new() -> Self {
        Self::default()
    }

    /// Collect a value for later shuffling
    pub fn collect(&mut self, value: RedactValue) {
        self.values.push(value);
    }

    /// Shuffle the collected values
    pub fn shuffle(&mut self, rng: &mut impl rand::Rng) {
        self.shuffled = self.values.clone();
        self.shuffled.shuffle(rng);
        self.index = 0;
    }

    /// Get the next shuffled value
    pub fn next_value(&mut self) -> Option<RedactValue> {
        if self.index < self.shuffled.len() {
            let value = self.shuffled[self.index].clone();
            self.index += 1;
            Some(value)
        } else {
            None
        }
    }

    /// Get the number of collected values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the strategy is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl Strategy for ShuffleStrategy {
    fn apply(&self, value: &RedactValue, _rng: &mut dyn rand::RngCore) -> RedactValue {
        // Note: For shuffle strategy, we need a two-pass approach:
        // 1. First pass: collect all values
        // 2. Shuffle
        // 3. Second pass: return shuffled values in order
        //
        // This trait-based apply() is called during the second pass.
        // The actual shuffling logic is handled by the Redactor.
        //
        // For now, we just return the value unchanged - the Redactor
        // will handle the shuffle logic specially.
        value.clone()
    }

    fn kind(&self) -> StrategyKind {
        StrategyKind::Shuffle
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn test_shuffle_strategy() {
        let mut strategy = ShuffleStrategy::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        // Collect values
        strategy.collect(RedactValue::Integer(1));
        strategy.collect(RedactValue::Integer(2));
        strategy.collect(RedactValue::Integer(3));
        strategy.collect(RedactValue::Integer(4));
        strategy.collect(RedactValue::Integer(5));

        assert_eq!(strategy.len(), 5);

        // Shuffle
        strategy.shuffle(&mut rng);

        // Get shuffled values
        let mut results = Vec::new();
        while let Some(v) = strategy.next_value() {
            results.push(v);
        }

        assert_eq!(results.len(), 5);

        // Verify all original values are present (just reordered)
        let mut ints: Vec<i64> = results
            .iter()
            .map(|v| match v {
                RedactValue::Integer(i) => *i,
                _ => panic!("Expected Integer"),
            })
            .collect();
        ints.sort();
        assert_eq!(ints, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_shuffle_with_nulls() {
        let mut strategy = ShuffleStrategy::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        strategy.collect(RedactValue::String("a".to_string()));
        strategy.collect(RedactValue::Null);
        strategy.collect(RedactValue::String("b".to_string()));

        strategy.shuffle(&mut rng);

        let mut null_count = 0;
        let mut string_count = 0;

        while let Some(v) = strategy.next_value() {
            match v {
                RedactValue::Null => null_count += 1,
                RedactValue::String(_) => string_count += 1,
                _ => {}
            }
        }

        assert_eq!(null_count, 1);
        assert_eq!(string_count, 2);
    }
}
