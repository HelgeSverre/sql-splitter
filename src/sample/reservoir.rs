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


