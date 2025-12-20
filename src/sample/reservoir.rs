//! Reservoir sampling implementation (Algorithm R).
//!
//! Provides uniform random sampling over a stream of items
//! with a fixed-size reservoir.

use rand::rngs::StdRng;
use rand::Rng;

/// Reservoir sampler using Algorithm R.
///
/// Maintains a fixed-size sample of items seen so far,
/// with each item having equal probability of being in the sample.
#[derive(Debug)]
pub struct Reservoir<T> {
    /// Maximum capacity of the reservoir
    capacity: usize,
    /// Total count of items seen
    count: usize,
    /// Current items in the reservoir
    items: Vec<T>,
    /// Random number generator
    rng: StdRng,
}

impl<T> Reservoir<T> {
    /// Create a new reservoir with the given capacity
    pub fn new(capacity: usize, rng: StdRng) -> Self {
        Self {
            capacity,
            count: 0,
            items: Vec::with_capacity(capacity),
            rng,
        }
    }

    /// Consider an item for inclusion in the reservoir
    pub fn consider(&mut self, item: T) {
        self.count += 1;

        if self.items.len() < self.capacity {
            // Reservoir not full yet - add item
            self.items.push(item);
        } else {
            // Reservoir full - randomly replace
            let j = self.rng.gen_range(0..self.count);
            if j < self.capacity {
                self.items[j] = item;
            }
        }
    }

    /// Get the number of items seen so far
    pub fn total_seen(&self) -> usize {
        self.count
    }

    /// Get the current size of the reservoir
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Check if the reservoir is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get the capacity of the reservoir
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Consume the reservoir and return the sampled items
    pub fn into_items(self) -> Vec<T> {
        self.items
    }

    /// Get a reference to the current items
    pub fn items(&self) -> &[T] {
        &self.items
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn test_reservoir_underfilled() {
        let rng = StdRng::seed_from_u64(42);
        let mut reservoir: Reservoir<i32> = Reservoir::new(10, rng);

        for i in 0..5 {
            reservoir.consider(i);
        }

        assert_eq!(reservoir.len(), 5);
        assert_eq!(reservoir.total_seen(), 5);

        let items = reservoir.into_items();
        assert_eq!(items, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_reservoir_overfilled() {
        let rng = StdRng::seed_from_u64(42);
        let mut reservoir: Reservoir<i32> = Reservoir::new(5, rng);

        for i in 0..100 {
            reservoir.consider(i);
        }

        assert_eq!(reservoir.len(), 5);
        assert_eq!(reservoir.total_seen(), 100);

        let items = reservoir.into_items();
        assert_eq!(items.len(), 5);
    }

    #[test]
    fn test_reservoir_deterministic_with_seed() {
        let rng1 = StdRng::seed_from_u64(42);
        let rng2 = StdRng::seed_from_u64(42);

        let mut reservoir1: Reservoir<i32> = Reservoir::new(5, rng1);
        let mut reservoir2: Reservoir<i32> = Reservoir::new(5, rng2);

        for i in 0..100 {
            reservoir1.consider(i);
            reservoir2.consider(i);
        }

        let items1 = reservoir1.into_items();
        let items2 = reservoir2.into_items();

        assert_eq!(items1, items2);
    }

    #[test]
    fn test_reservoir_uniform_distribution() {
        // Statistical test: with many runs, each item should appear roughly equally
        let trials = 10000;
        let capacity = 10;
        let stream_size = 100;
        let mut counts = vec![0usize; stream_size];

        for seed in 0..trials {
            let rng = StdRng::seed_from_u64(seed);
            let mut reservoir: Reservoir<usize> = Reservoir::new(capacity, rng);

            for i in 0..stream_size {
                reservoir.consider(i);
            }

            for item in reservoir.into_items() {
                counts[item] += 1;
            }
        }

        // Expected count per item: trials * capacity / stream_size = 1000
        let expected = (trials as usize * capacity) / stream_size;
        let tolerance = expected / 5; // 20% tolerance

        for (i, &count) in counts.iter().enumerate() {
            assert!(
                count > expected.saturating_sub(tolerance) && count < expected + tolerance,
                "Item {} count {} is outside expected range {} ± {}",
                i,
                count,
                expected,
                tolerance
            );
        }
    }
}
