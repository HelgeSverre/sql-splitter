//! Stable, deterministic seed derivation for synthetic data generation.
//!
//! Every value the generator produces must be reproducible from a single
//! root seed, independent of *when* or in what order it is requested during a
//! traversal. We get that by deriving a dedicated RNG stream per logical
//! identity (a table, a column, a planner, or an operator invocation) via
//! SHA-256 over a fixed, length-prefixed byte layout.
//!
//! [`std::collections::hash_map::DefaultHasher`] and `ahash` are deliberately
//! not used here: neither publishes a stability guarantee across Rust or
//! crate versions, so a toolchain upgrade could silently reshuffle every
//! generated dataset. SHA-256 is a fixed, versioned algorithm we control.

use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use sha2::{Digest, Sha256};

/// Domain-separation prefix mixed into every derived seed. Bumping the
/// trailing version number is the only sanctioned way to change the
/// derivation algorithm, since it invalidates every previously generated
/// dataset's reproducibility.
const SEED_DOMAIN: &[u8] = b"sql-splitter.generate.seed.v1\0";

/// Derive a 32-byte seed from a root value and an ordered list of identity
/// parts.
///
/// The hash is over the domain prefix, the little-endian root, then each
/// part's length (as a little-endian `u64`) followed by its bytes. Length
/// prefixing prevents ambiguity between e.g. `["ab", "c"]` and `["a", "bc"]`.
pub fn derive_seed(root: u64, parts: &[&str]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(SEED_DOMAIN);
    hash.update(root.to_le_bytes());
    for part in parts {
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    hash.finalize().into()
}

/// Identity of a single deterministic RNG stream.
///
/// Stream identity is built from normalized table, column/planner, and
/// operator names only. A traversal index (row number, retry count, visit
/// order) must never enter the hash: the same logical identity has to
/// rederive the same stream no matter how many other streams were derived
/// before it in a given run.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId {
    kind: &'static str,
    parts: Vec<String>,
}

impl StreamId {
    /// Stream for table-level decisions (e.g. row count planning).
    pub fn table(table: impl Into<String>) -> Self {
        Self {
            kind: "table",
            parts: vec![table.into()],
        }
    }

    /// Stream for a single column's value generation.
    pub fn column(
        table: impl Into<String>,
        column: impl Into<String>,
        operator: impl Into<String>,
    ) -> Self {
        Self {
            kind: "column",
            parts: vec![table.into(), column.into(), operator.into()],
        }
    }

    /// Stream for planner decisions scoped to a table (row counts, child
    /// distributions, and similar structural choices).
    pub fn planner(table: impl Into<String>) -> Self {
        Self {
            kind: "planner",
            parts: vec![table.into()],
        }
    }

    /// Stream for a table's stochastic row-count rounding decision.
    ///
    /// The row compiler resolves an exact integer count per table; when the
    /// resolved target is fractional (e.g. a `--scale` that lands on 1.5), the
    /// remainder is rounded up with probability equal to the fraction. Keying
    /// the stream on the table name (behind the `rows.rounding` prefix) keeps
    /// that decision stable and independent of every other stream in the run.
    pub fn rounding(table: impl Into<String>) -> Self {
        Self {
            kind: "rows.rounding",
            parts: vec![table.into()],
        }
    }

    /// Stream for a specific generator operator invocation, independent of
    /// any particular column (e.g. a shared lookup pool).
    pub fn operator(
        table: impl Into<String>,
        column: impl Into<String>,
        operator: impl Into<String>,
    ) -> Self {
        Self {
            kind: "operator",
            parts: vec![table.into(), column.into(), operator.into()],
        }
    }

    /// The byte-string parts hashed for this stream, in order, prefixed by
    /// the stream kind so different constructors never collide even when
    /// given identical string arguments.
    fn hash_parts(&self) -> Vec<&str> {
        let mut parts = Vec::with_capacity(self.parts.len() + 1);
        parts.push(self.kind);
        parts.extend(self.parts.iter().map(String::as_str));
        parts
    }
}

/// A run's single root seed, from which every deterministic stream is
/// derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedRoot {
    root: u64,
}

impl SeedRoot {
    /// Create a new root from a plain `u64` seed value.
    pub fn new(root: u64) -> Self {
        Self { root }
    }

    /// Derive a fresh, deterministic RNG for the given stream identity.
    pub fn stream(&self, id: StreamId) -> ChaCha8Rng {
        let seed = derive_seed(self.root, &id.hash_parts());
        ChaCha8Rng::from_seed(seed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_seed_is_deterministic_for_the_same_inputs() {
        let a = derive_seed(1, &["a", "b"]);
        let b = derive_seed(1, &["a", "b"]);
        assert_eq!(a, b);
    }

    #[test]
    fn derive_seed_distinguishes_part_boundaries() {
        // Without length prefixing, ["ab", "c"] and ["a", "bc"] would hash
        // identically once concatenated.
        let a = derive_seed(1, &["ab", "c"]);
        let b = derive_seed(1, &["a", "bc"]);
        assert_ne!(a, b);
    }

    #[test]
    fn derive_seed_distinguishes_roots() {
        let a = derive_seed(1, &["same"]);
        let b = derive_seed(2, &["same"]);
        assert_ne!(a, b);
    }

    /// Checked-in golden for `derive_seed(42, ["users", "email", "internet.email"])`.
    /// If this ever changes, the derivation algorithm changed and every
    /// previously generated dataset would silently diverge - bump
    /// [`SEED_DOMAIN`]'s version and update this golden deliberately.
    #[test]
    fn seed_golden_hex_matches_checked_in_value() {
        let golden = "122016424915fd14795335972fba7c8cc3f69d38deffd59c49e22a4f477ce584";
        let actual = hex::encode(derive_seed(42, &["users", "email", "internet.email"]));
        assert_eq!(actual, golden);
    }
}
