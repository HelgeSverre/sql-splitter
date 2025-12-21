//! Redaction strategies for anonymizing data.
//!
//! Each strategy implements a different approach to redacting values:
//! - `null`: Replace with NULL
//! - `constant`: Replace with a fixed value
//! - `hash`: One-way hash (deterministic)
//! - `mask`: Partial masking with pattern
//! - `shuffle`: Redistribute values within column
//! - `fake`: Generate realistic fake data
//! - `skip`: No redaction (passthrough)

mod constant;
mod fake;
mod hash;
mod mask;
mod null;
mod shuffle;
mod skip;

// Strategy structs - will be used in Phase 3 when INSERT/COPY rewriting is implemented
#[allow(unused_imports)]
pub use constant::ConstantStrategy;
#[allow(unused_imports)]
pub use fake::FakeStrategy;
#[allow(unused_imports)]
pub use hash::HashStrategy;
#[allow(unused_imports)]
pub use mask::MaskStrategy;
#[allow(unused_imports)]
pub use null::NullStrategy;
#[allow(unused_imports)]
pub use shuffle::ShuffleStrategy;
#[allow(unused_imports)]
pub use skip::SkipStrategy;

use serde::{Deserialize, Serialize};

/// Redaction strategy kind with associated configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum StrategyKind {
    /// Replace value with NULL
    Null,

    /// Replace with a constant value
    Constant {
        /// The constant value to use
        value: String,
    },

    /// One-way hash (SHA256)
    Hash {
        /// Preserve email domain (user@domain.com → hash@domain.com)
        #[serde(default)]
        preserve_domain: bool,
    },

    /// Partial masking with pattern
    Mask {
        /// Pattern: * = asterisk, X = keep, # = random digit
        pattern: String,
    },

    /// Shuffle values within the column
    Shuffle,

    /// Generate fake data
    Fake {
        /// Generator name: email, name, phone, address, etc.
        generator: String,
    },

    /// No redaction (passthrough)
    #[default]
    Skip,
}

impl StrategyKind {
    /// Validate the strategy configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            StrategyKind::Null => Ok(()),
            StrategyKind::Constant { value } => {
                if value.is_empty() {
                    anyhow::bail!("Constant strategy requires a non-empty value");
                }
                Ok(())
            }
            StrategyKind::Hash { .. } => Ok(()),
            StrategyKind::Mask { pattern } => {
                if pattern.is_empty() {
                    anyhow::bail!("Mask strategy requires a non-empty pattern");
                }
                // Validate pattern characters
                for c in pattern.chars() {
                    if !matches!(c, '*' | 'X' | '#' | '-' | ' ' | '.' | '@' | '(' | ')') {
                        // Allow common separator chars
                    }
                }
                Ok(())
            }
            StrategyKind::Shuffle => Ok(()),
            StrategyKind::Fake { generator } => {
                if !is_valid_generator(generator) {
                    anyhow::bail!("Unknown fake generator: {}. Use: email, name, first_name, last_name, phone, address, city, zip, company, ip, uuid, date, etc.", generator);
                }
                Ok(())
            }
            StrategyKind::Skip => Ok(()),
        }
    }

    /// Get the YAML string representation of this strategy
    pub fn to_yaml_str(&self) -> &'static str {
        match self {
            StrategyKind::Null => "null",
            StrategyKind::Constant { .. } => "constant",
            StrategyKind::Hash { .. } => "hash",
            StrategyKind::Mask { .. } => "mask",
            StrategyKind::Shuffle => "shuffle",
            StrategyKind::Fake { .. } => "fake",
            StrategyKind::Skip => "skip",
        }
    }
}

/// Check if a fake generator name is valid
fn is_valid_generator(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "email"
            | "safe_email"
            | "name"
            | "first_name"
            | "last_name"
            | "full_name"
            | "phone"
            | "phone_number"
            | "address"
            | "street_address"
            | "city"
            | "state"
            | "zip"
            | "zip_code"
            | "postal_code"
            | "country"
            | "company"
            | "company_name"
            | "job_title"
            | "username"
            | "user_name"
            | "url"
            | "ip"
            | "ip_address"
            | "ipv4"
            | "ipv6"
            | "uuid"
            | "date"
            | "date_time"
            | "datetime"
            | "time"
            | "credit_card"
            | "iban"
            | "lorem"
            | "paragraph"
            | "sentence"
            | "word"
            | "ssn"
    )
}

/// Value representation for redaction
#[derive(Debug, Clone)]
pub enum RedactValue {
    /// NULL value
    Null,
    /// String value (may contain SQL escaping)
    String(String),
    /// Integer value
    Integer(i64),
    /// Raw bytes (for binary data)
    Bytes(Vec<u8>),
}

impl RedactValue {
    /// Check if this is a NULL value
    pub fn is_null(&self) -> bool {
        matches!(self, RedactValue::Null)
    }

    /// Get as string, or None if NULL
    pub fn as_str(&self) -> Option<&str> {
        match self {
            RedactValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Convert to string representation
    pub fn to_string_value(&self) -> String {
        match self {
            RedactValue::Null => "NULL".to_string(),
            RedactValue::String(s) => s.clone(),
            RedactValue::Integer(i) => i.to_string(),
            RedactValue::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        }
    }
}

/// Trait for redaction strategies
pub trait Strategy: Send + Sync {
    /// Apply the strategy to redact a value
    fn apply(&self, value: &RedactValue, rng: &mut dyn rand::RngCore) -> RedactValue;

    /// Get the strategy kind
    fn kind(&self) -> StrategyKind;
}
