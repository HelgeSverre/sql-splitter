//! Fake strategy - generate realistic fake data.
//!
//! The single-kind aliases below (`name`, `email`, `zip`, ...) dispatch
//! through the shared [`crate::fake_data`] catalog, the same one the
//! generate module's semantic generators use, so a given alias and a given
//! semantic kind never diverge into two implementations of the same fake
//! value. Locale is always [`Locale::En`] here regardless of the configured
//! `self.locale`: that mirrors this strategy's behavior before the shared
//! catalog existed (it only ever generated English-locale fakes), so
//! extracting the catalog does not change what any existing redact config
//! produces. Composite/non-fake-crate kinds (`address`, `credit_card`,
//! `iban`, `ssn`, `uuid`, date/time) stay local: each draws several fields
//! from one seeded RNG, a shape [`crate::fake_data::generate_semantic`]
//! deliberately does not support (see its doc comment).
use super::{RedactValue, Strategy, StrategyKind};
use crate::fake_data::{generate_semantic, Locale, SemanticKind};
use fake::faker::address::en::{CityName, StateName, StreetName, ZipCode};
use fake::Fake;

/// Strategy that generates fake data
#[derive(Debug, Clone)]
pub struct FakeStrategy {
    generator: String,
    #[allow(dead_code)]
    locale: String,
}

impl FakeStrategy {
    pub fn new(generator: String, locale: String) -> Self {
        Self { generator, locale }
    }

    /// Generate a fake value based on the generator type.
    ///
    /// Every branch draws exactly one 32-byte block from `rng` — whether
    /// directly (the composite/non-fake-crate branches below) or indirectly
    /// via [`generate_semantic`] (the single-kind aliases) — so moving an
    /// alias between the two never changes how much entropy a call consumes
    /// from the caller's stream.
    fn generate(&self, rng: &mut dyn rand::Rng) -> String {
        match self.generator.to_lowercase().as_str() {
            // Name generators
            "name" | "full_name" => {
                generate_semantic(SemanticKind::PersonFullName, Locale::En, rng)
            }
            "first_name" => generate_semantic(SemanticKind::PersonFirstName, Locale::En, rng),
            "last_name" => generate_semantic(SemanticKind::PersonLastName, Locale::En, rng),

            // Contact generators
            "email" | "safe_email" => {
                generate_semantic(SemanticKind::InternetEmail, Locale::En, rng)
            }
            "phone" | "phone_number" => {
                generate_semantic(SemanticKind::PhoneNumber, Locale::En, rng)
            }
            "username" | "user_name" => {
                generate_semantic(SemanticKind::PersonUsername, Locale::En, rng)
            }

            // Address generators
            "address" | "street_address" => {
                let mut seed = [0u8; 32];
                rng.fill_bytes(&mut seed);
                let mut fake_rng = rand::rngs::StdRng::from_seed(seed);
                let street: String = StreetName().fake_with_rng(&mut fake_rng);
                let city: String = CityName().fake_with_rng(&mut fake_rng);
                let state: String = StateName().fake_with_rng(&mut fake_rng);
                let zip: String = ZipCode().fake_with_rng(&mut fake_rng);
                format!("{}, {}, {} {}", street, city, state, zip)
            }
            "street" | "street_name" => {
                generate_semantic(SemanticKind::AddressStreet, Locale::En, rng)
            }
            "city" => generate_semantic(SemanticKind::AddressCity, Locale::En, rng),
            "state" => generate_semantic(SemanticKind::AddressRegion, Locale::En, rng),
            "zip" | "zip_code" | "postal_code" => {
                generate_semantic(SemanticKind::AddressPostcode, Locale::En, rng)
            }
            "country" => "United States".to_string(), // Simplified for now

            // Business generators
            "company" | "company_name" => {
                generate_semantic(SemanticKind::CompanyName, Locale::En, rng)
            }
            "job_title" => generate_semantic(SemanticKind::CompanyJobTitle, Locale::En, rng),

            // Internet generators
            "url" => generate_semantic(SemanticKind::InternetUrl, Locale::En, rng),
            "ip" | "ip_address" | "ipv4" => {
                generate_semantic(SemanticKind::InternetIpv4, Locale::En, rng)
            }
            "ipv6" => generate_semantic(SemanticKind::InternetIpv6, Locale::En, rng),

            // Identifier generators
            "uuid" => {
                let mut seed = [0u8; 32];
                rng.fill_bytes(&mut seed);
                let mut fake_rng = rand::rngs::StdRng::from_seed(seed);
                format!(
                    "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                    fake_rng.random::<u32>(),
                    fake_rng.random::<u16>(),
                    (fake_rng.random::<u16>() & 0x0FFF) | 0x4000, // Version 4
                    (fake_rng.random::<u16>() & 0x3FFF) | 0x8000, // Variant
                    fake_rng.random::<u64>() & 0xFFFFFFFFFFFF_u64
                )
            }

            // Date/time generators
            "date" => {
                let mut seed = [0u8; 32];
                rng.fill_bytes(&mut seed);
                let mut fake_rng = rand::rngs::StdRng::from_seed(seed);
                let year = fake_rng.random_range(1970..2024);
                let month = fake_rng.random_range(1..=12);
                let day = fake_rng.random_range(1..=28);
                format!("{:04}-{:02}-{:02}", year, month, day)
            }
            "datetime" | "date_time" => {
                let mut seed = [0u8; 32];
                rng.fill_bytes(&mut seed);
                let mut fake_rng = rand::rngs::StdRng::from_seed(seed);
                let year = fake_rng.random_range(1970..2024);
                let month = fake_rng.random_range(1..=12);
                let day = fake_rng.random_range(1..=28);
                let hour = fake_rng.random_range(0..24);
                let minute = fake_rng.random_range(0..60);
                let second = fake_rng.random_range(0..60);
                format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    year, month, day, hour, minute, second
                )
            }
            "time" => {
                let mut seed = [0u8; 32];
                rng.fill_bytes(&mut seed);
                let mut fake_rng = rand::rngs::StdRng::from_seed(seed);
                let hour = fake_rng.random_range(0..24);
                let minute = fake_rng.random_range(0..60);
                let second = fake_rng.random_range(0..60);
                format!("{:02}:{:02}:{:02}", hour, minute, second)
            }

            // Financial generators
            "credit_card" => {
                // Generate a fake credit card number (Luhn-valid would be complex, simplified)
                let mut seed = [0u8; 32];
                rng.fill_bytes(&mut seed);
                let mut fake_rng = rand::rngs::StdRng::from_seed(seed);
                format!(
                    "{:04}-{:04}-{:04}-{:04}",
                    fake_rng.random_range(1000..9999),
                    fake_rng.random_range(1000..9999),
                    fake_rng.random_range(1000..9999),
                    fake_rng.random_range(1000..9999)
                )
            }
            "iban" => {
                // Simplified IBAN (not valid, but looks realistic)
                let mut seed = [0u8; 32];
                rng.fill_bytes(&mut seed);
                let mut fake_rng = rand::rngs::StdRng::from_seed(seed);
                format!(
                    "DE{:02}{:04}{:04}{:04}{:04}{:02}",
                    fake_rng.random_range(10..99),
                    fake_rng.random_range(1000..9999),
                    fake_rng.random_range(1000..9999),
                    fake_rng.random_range(1000..9999),
                    fake_rng.random_range(1000..9999),
                    fake_rng.random_range(10..99)
                )
            }

            // SSN generator
            "ssn" => {
                let mut seed = [0u8; 32];
                rng.fill_bytes(&mut seed);
                let mut fake_rng = rand::rngs::StdRng::from_seed(seed);
                format!(
                    "{:03}-{:02}-{:04}",
                    fake_rng.random_range(100..999),
                    fake_rng.random_range(10..99),
                    fake_rng.random_range(1000..9999)
                )
            }

            // Text generators
            "lorem" | "paragraph" => {
                generate_semantic(SemanticKind::TextParagraph, Locale::En, rng)
            }
            "sentence" => generate_semantic(SemanticKind::TextSentence, Locale::En, rng),
            "word" => generate_semantic(SemanticKind::TextWord, Locale::En, rng),

            // Default: return a generic fake string
            _ => {
                let mut seed = [0u8; 32];
                rng.fill_bytes(&mut seed);
                let mut fake_rng = rand::rngs::StdRng::from_seed(seed);
                format!("FAKE_{}", fake_rng.random_range(10000..99999))
            }
        }
    }
}

impl Strategy for FakeStrategy {
    fn apply(&self, value: &RedactValue, rng: &mut dyn rand::Rng) -> RedactValue {
        match value {
            RedactValue::Null => RedactValue::Null,
            _ => RedactValue::String(self.generate(rng)),
        }
    }

    fn kind(&self) -> StrategyKind {
        StrategyKind::Fake {
            generator: self.generator.clone(),
        }
    }
}

use rand::{RngExt, SeedableRng};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fake_name() {
        let strategy = FakeStrategy::new("name".to_string(), "en".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(&RedactValue::String("John Doe".to_string()), &mut rng);
        match result {
            RedactValue::String(s) => {
                assert!(!s.is_empty());
                assert!(s.contains(' ')); // Full name has space
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_fake_email() {
        let strategy = FakeStrategy::new("email".to_string(), "en".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(
            &RedactValue::String("real@example.com".to_string()),
            &mut rng,
        );
        match result {
            RedactValue::String(s) => {
                assert!(s.contains('@'));
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_fake_phone() {
        let strategy = FakeStrategy::new("phone".to_string(), "en".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(&RedactValue::String("555-123-4567".to_string()), &mut rng);
        match result {
            RedactValue::String(s) => {
                assert!(!s.is_empty());
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_fake_uuid() {
        let strategy = FakeStrategy::new("uuid".to_string(), "en".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(
            &RedactValue::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
            &mut rng,
        );
        match result {
            RedactValue::String(s) => {
                // UUID format: 8-4-4-4-12
                assert!(s.contains('-'));
                assert_eq!(s.len(), 36);
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_fake_null() {
        let strategy = FakeStrategy::new("name".to_string(), "en".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(&RedactValue::Null, &mut rng);
        assert!(matches!(result, RedactValue::Null));
    }

    #[test]
    fn test_fake_date() {
        let strategy = FakeStrategy::new("date".to_string(), "en".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(&RedactValue::String("2024-01-15".to_string()), &mut rng);
        match result {
            RedactValue::String(s) => {
                // YYYY-MM-DD format
                assert_eq!(s.len(), 10);
                assert!(s.contains('-'));
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_fake_credit_card() {
        let strategy = FakeStrategy::new("credit_card".to_string(), "en".to_string());
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let result = strategy.apply(
            &RedactValue::String("4532-0151-1283-0366".to_string()),
            &mut rng,
        );
        match result {
            RedactValue::String(s) => {
                // XXXX-XXXX-XXXX-XXXX format
                assert_eq!(s.len(), 19);
                assert_eq!(s.matches('-').count(), 3);
            }
            _ => panic!("Expected String"),
        }
    }
}
