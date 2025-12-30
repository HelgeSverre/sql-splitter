//! Fake strategy - generate realistic fake data.

use super::{RedactValue, Strategy, StrategyKind};
use fake::faker::address::en::{CityName, StateName, StreetName, ZipCode};
use fake::faker::company::en::CompanyName;
use fake::faker::internet::en::{SafeEmail, Username};
use fake::faker::lorem::en::{Paragraph, Sentence, Word};
use fake::faker::name::en::{FirstName, LastName, Name};
use fake::faker::phone_number::en::PhoneNumber;
use fake::Fake;

/// Strategy that generates fake data
#[derive(Debug, Clone)]
pub struct FakeStrategy {
    generator: String,
    locale: String,
}

impl FakeStrategy {
    pub fn new(generator: String, locale: String) -> Self {
        Self { generator, locale }
    }

    /// Generate a fake value based on the generator type
    fn generate(&self, rng: &mut dyn rand::RngCore) -> String {
        // Convert rng to StdRng for fake crate compatibility
        let mut seed = [0u8; 32];
        rng.fill_bytes(&mut seed);
        let mut fake_rng = rand::rngs::StdRng::from_seed(seed);

        match self.generator.to_lowercase().as_str() {
            // Name generators
            "name" | "full_name" => Name().fake_with_rng(&mut fake_rng),
            "first_name" => FirstName().fake_with_rng(&mut fake_rng),
            "last_name" => LastName().fake_with_rng(&mut fake_rng),

            // Contact generators
            "email" | "safe_email" => SafeEmail().fake_with_rng(&mut fake_rng),
            "phone" | "phone_number" => PhoneNumber().fake_with_rng(&mut fake_rng),
            "username" | "user_name" => Username().fake_with_rng(&mut fake_rng),

            // Address generators
            "address" | "street_address" => {
                let street: String = StreetName().fake_with_rng(&mut fake_rng);
                let city: String = CityName().fake_with_rng(&mut fake_rng);
                let state: String = StateName().fake_with_rng(&mut fake_rng);
                let zip: String = ZipCode().fake_with_rng(&mut fake_rng);
                format!("{}, {}, {} {}", street, city, state, zip)
            }
            "street" | "street_name" => StreetName().fake_with_rng(&mut fake_rng),
            "city" => CityName().fake_with_rng(&mut fake_rng),
            "state" => StateName().fake_with_rng(&mut fake_rng),
            "zip" | "zip_code" | "postal_code" => ZipCode().fake_with_rng(&mut fake_rng),
            "country" => "United States".to_string(), // Simplified for now

            // Business generators
            "company" | "company_name" => CompanyName().fake_with_rng(&mut fake_rng),
            "job_title" => {
                // Simplified job title generator
                let titles = [
                    "Software Engineer",
                    "Product Manager",
                    "Data Analyst",
                    "Designer",
                    "Marketing Manager",
                    "Sales Representative",
                    "Customer Support",
                    "Operations Manager",
                ];
                let idx = fake_rng.random_range(0..titles.len());
                titles[idx].to_string()
            }

            // Internet generators
            "url" => format!(
                "https://example{}.com/{}",
                fake_rng.random_range(1..1000),
                Word().fake_with_rng::<String, _>(&mut fake_rng)
            ),
            "ip" | "ip_address" | "ipv4" => {
                format!(
                    "{}.{}.{}.{}",
                    fake_rng.random_range(1..255),
                    fake_rng.random_range(0..255),
                    fake_rng.random_range(0..255),
                    fake_rng.random_range(1..255)
                )
            }
            "ipv6" => {
                format!(
                    "{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}",
                    fake_rng.random_range(0..0xFFFF_u16),
                    fake_rng.random_range(0..0xFFFF_u16),
                    fake_rng.random_range(0..0xFFFF_u16),
                    fake_rng.random_range(0..0xFFFF_u16),
                    fake_rng.random_range(0..0xFFFF_u16),
                    fake_rng.random_range(0..0xFFFF_u16),
                    fake_rng.random_range(0..0xFFFF_u16),
                    fake_rng.random_range(0..0xFFFF_u16)
                )
            }

            // Identifier generators
            "uuid" => {
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
                let year = fake_rng.random_range(1970..2024);
                let month = fake_rng.random_range(1..=12);
                let day = fake_rng.random_range(1..=28);
                format!("{:04}-{:02}-{:02}", year, month, day)
            }
            "datetime" | "date_time" => {
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
                let hour = fake_rng.random_range(0..24);
                let minute = fake_rng.random_range(0..60);
                let second = fake_rng.random_range(0..60);
                format!("{:02}:{:02}:{:02}", hour, minute, second)
            }

            // Financial generators
            "credit_card" => {
                // Generate a fake credit card number (Luhn-valid would be complex, simplified)
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
                format!(
                    "{:03}-{:02}-{:04}",
                    fake_rng.random_range(100..999),
                    fake_rng.random_range(10..99),
                    fake_rng.random_range(1000..9999)
                )
            }

            // Text generators
            "lorem" | "paragraph" => Paragraph(3..5).fake_with_rng(&mut fake_rng),
            "sentence" => Sentence(5..10).fake_with_rng(&mut fake_rng),
            "word" => Word().fake_with_rng(&mut fake_rng),

            // Default: return a generic fake string
            _ => format!("FAKE_{}", fake_rng.random_range(10000..99999)),
        }
    }
}

impl Strategy for FakeStrategy {
    fn apply(&self, value: &RedactValue, rng: &mut dyn rand::RngCore) -> RedactValue {
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

use rand::{Rng, SeedableRng};

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
