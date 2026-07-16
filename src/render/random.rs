//! Block-buffered, unbiased sampling from a fixed alphanumeric alphabet.
//!
//! Drawing one `ChaCha8Rng` value per character is the dominant cost of
//! simple-mode generation. `RandomBlock` instead fills a fixed-size buffer in
//! one call and hands out bytes from it, refilling only once the buffer is
//! exhausted.

use rand::Rng;
use rand_chacha::ChaCha8Rng;

/// The 63-character alphabet random strings are drawn from: lowercase,
/// uppercase, digits, and a trailing space. This matches the alphabet used
/// by `test_data_gen`'s streaming generator so seeded output stays
/// comparable across the two implementations.
const ALPHABET: &[u8; 63] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";

/// Size of the buffer refilled from the underlying RNG in one call.
const BLOCK_SIZE: usize = 4096;

/// A block-buffered source of unbiased, alphabet-restricted random bytes.
///
/// Each buffered byte's low six bits select one of 64 possible values; the
/// alphabet has only 63 characters, so a byte whose low six bits equal 63 is
/// rejected and redrawn. Rejecting rather than reducing modulo 63 keeps every
/// alphabet character equally likely.
pub struct RandomBlock {
    rng: ChaCha8Rng,
    buf: [u8; BLOCK_SIZE],
    pos: usize,
}

impl RandomBlock {
    /// Create a block sampler backed by `rng`, filling the first block
    /// immediately.
    pub fn new(mut rng: ChaCha8Rng) -> Self {
        let mut buf = [0u8; BLOCK_SIZE];
        rng.fill_bytes(&mut buf);
        Self { rng, buf, pos: 0 }
    }

    /// Return the next raw buffered byte, refilling the block first if it is
    /// exhausted.
    fn next_byte(&mut self) -> u8 {
        if self.pos >= self.buf.len() {
            self.rng.fill_bytes(&mut self.buf);
            self.pos = 0;
        }
        let byte = self.buf[self.pos];
        self.pos += 1;
        byte
    }

    /// Sample a single character from the 63-character alphanumeric
    /// alphabet, uniformly at random.
    pub fn next_alphanumeric(&mut self) -> u8 {
        loop {
            let idx = self.next_byte() & 0b0011_1111;
            if idx != 63 {
                return ALPHABET[idx as usize];
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_chacha::rand_core::SeedableRng;

    #[test]
    fn samples_stay_in_alphabet_and_are_seed_reproducible() {
        let mut a = RandomBlock::new(ChaCha8Rng::from_seed([7u8; 32]));
        let mut b = RandomBlock::new(ChaCha8Rng::from_seed([7u8; 32]));

        let bytes_a: Vec<u8> = (0..10_000).map(|_| a.next_alphanumeric()).collect();
        let bytes_b: Vec<u8> = (0..10_000).map(|_| b.next_alphanumeric()).collect();

        assert!(bytes_a.iter().all(|byte| ALPHABET.contains(byte)));
        assert_eq!(bytes_a, bytes_b);
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = RandomBlock::new(ChaCha8Rng::from_seed([1u8; 32]));
        let mut b = RandomBlock::new(ChaCha8Rng::from_seed([2u8; 32]));
        assert_ne!(a.next_alphanumeric(), b.next_alphanumeric());
    }
}
