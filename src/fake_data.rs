//! Neutral fake-data catalog shared between the generate module's semantic
//! generators ([`crate::generate::generators::semantic`]) and the
//! redactor's `fake` strategy ([`crate::redactor::strategy::fake`]).
//!
//! Neither consumer depends on the other's module; both depend on this one
//! instead, so a given [`SemanticKind`] is dispatched to the `fake` crate by
//! exactly one code path regardless of which caller asked for it.
//!
//! Only shapes both consumers can use identically live here (a single
//! locale-scoped draw producing one string). Kinds unique to one consumer —
//! the redactor's composite `address` string, its non-fake-crate
//! `uuid`/`credit_card`/`iban`/`ssn`/date-time formats, or the semantic
//! generators' identifiers/credentials/temporal values — stay local to that
//! consumer.

use fake::faker::address::en::{
    CityName, CountryName, SecondaryAddress, StateName, StreetName, ZipCode,
};
use fake::faker::company::en::CompanyName;
use fake::faker::currency::en::CurrencyCode;
use fake::faker::filesystem::en::{FileExtension, FileName, MimeType};
use fake::faker::internet::en::{MACAddress, SafeEmail, UserAgent, Username};
use fake::faker::lorem::en::{Paragraph, Sentence, Word};
use fake::faker::name::en::{FirstName, LastName, Name, Title};
use fake::faker::phone_number::en::PhoneNumber;
use fake::Fake;
use rand::{RngExt, SeedableRng};

/// The locales the shared catalog supports. A closed enum: naming an
/// unsupported locale is a compile error (no such variant exists), rather
/// than a runtime validation failure discovered mid-run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Locale {
    En,
}

/// Fixed international calling codes for [`SemanticKind::PhoneCountryCode`].
/// Not part of the `fake` crate's catalog, so drawn from a small fixed list
/// instead.
const COUNTRY_CALLING_CODES: &[&str] = &[
    "+1", "+44", "+49", "+33", "+34", "+39", "+31", "+46", "+47", "+45", "+41", "+43", "+351",
    "+352", "+353", "+61", "+64", "+81", "+82", "+86", "+91", "+55", "+52", "+27",
];

/// Fixed department names for [`SemanticKind::CompanyDepartment`]. Not part
/// of the `fake` crate's catalog, so drawn from a small fixed list instead.
const DEPARTMENTS: &[&str] = &[
    "Engineering",
    "Sales",
    "Marketing",
    "Finance",
    "Human Resources",
    "Customer Support",
    "Operations",
    "Legal",
    "Product",
    "Research",
];

/// Job titles for [`SemanticKind::CompanyJobTitle`]. Moved verbatim from the
/// redactor's prior `job_title` match arm — same list, same draw — so the
/// alias keeps producing the same values it always has.
const JOB_TITLES: &[&str] = &[
    "Software Engineer",
    "Product Manager",
    "Data Analyst",
    "Designer",
    "Marketing Manager",
    "Sales Representative",
    "Customer Support",
    "Operations Manager",
];

/// One shape in the shared fake-data catalog, named for what it produces
/// rather than for any one consumer's column-kind or alias string. Both
/// `semantic.rs`'s catalog and the redactor's alias table map their own
/// names onto these variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SemanticKind {
    PersonFirstName,
    PersonLastName,
    PersonFullName,
    PersonUsername,
    PersonTitle,
    InternetEmail,
    InternetDomain,
    InternetUrl,
    InternetIpv4,
    InternetIpv6,
    InternetUserAgent,
    PhoneNumber,
    PhoneCountryCode,
    CompanyName,
    CompanyDepartment,
    CompanyJobTitle,
    AddressStreet,
    AddressLine2,
    AddressCity,
    AddressRegion,
    AddressPostcode,
    AddressCountry,
    CommerceCurrency,
    TextWord,
    TextSentence,
    TextParagraph,
    TextSlug,
    FileName,
    FileExtension,
    FileMimeType,
    NetworkMac,
}

/// Generate one fake-data value of `kind` in `locale`.
///
/// Draws exactly one block of entropy from `rng` (via a seeded `StdRng`
/// handed to the `fake` crate), regardless of how many internal draws the
/// chosen kind makes from that block — matching the single-fill-then-draw
/// shape the redactor's fake strategy has always used, so moving a call site
/// over to this function does not change how much entropy it consumes from
/// the caller's stream.
pub(crate) fn generate_semantic(
    kind: SemanticKind,
    locale: Locale,
    rng: &mut dyn rand::Rng,
) -> String {
    let Locale::En = locale;

    let mut seed = [0u8; 32];
    rng.fill_bytes(&mut seed);
    let mut fake_rng = rand::rngs::StdRng::from_seed(seed);

    match kind {
        SemanticKind::PersonFirstName => FirstName().fake_with_rng(&mut fake_rng),
        SemanticKind::PersonLastName => LastName().fake_with_rng(&mut fake_rng),
        SemanticKind::PersonFullName => Name().fake_with_rng(&mut fake_rng),
        SemanticKind::PersonUsername => Username().fake_with_rng(&mut fake_rng),
        SemanticKind::PersonTitle => Title().fake_with_rng(&mut fake_rng),

        SemanticKind::InternetEmail => SafeEmail().fake_with_rng(&mut fake_rng),
        SemanticKind::InternetDomain => {
            const TLDS: &[&str] = &["com", "net", "org", "io"];
            let word: String = Word().fake_with_rng(&mut fake_rng);
            let tld = TLDS[fake_rng.random_range(0..TLDS.len())];
            format!("{}.{tld}", word.to_lowercase())
        }
        SemanticKind::InternetUrl => format!(
            "https://example{}.com/{}",
            fake_rng.random_range(1..1000),
            Word().fake_with_rng::<String, _>(&mut fake_rng)
        ),
        SemanticKind::InternetIpv4 => format!(
            "{}.{}.{}.{}",
            fake_rng.random_range(1..255),
            fake_rng.random_range(0..255),
            fake_rng.random_range(0..255),
            fake_rng.random_range(1..255)
        ),
        SemanticKind::InternetIpv6 => format!(
            "{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}",
            fake_rng.random_range(0..0xFFFF_u16),
            fake_rng.random_range(0..0xFFFF_u16),
            fake_rng.random_range(0..0xFFFF_u16),
            fake_rng.random_range(0..0xFFFF_u16),
            fake_rng.random_range(0..0xFFFF_u16),
            fake_rng.random_range(0..0xFFFF_u16),
            fake_rng.random_range(0..0xFFFF_u16),
            fake_rng.random_range(0..0xFFFF_u16)
        ),
        SemanticKind::InternetUserAgent => UserAgent().fake_with_rng(&mut fake_rng),

        SemanticKind::PhoneNumber => PhoneNumber().fake_with_rng(&mut fake_rng),
        SemanticKind::PhoneCountryCode => {
            COUNTRY_CALLING_CODES[fake_rng.random_range(0..COUNTRY_CALLING_CODES.len())].to_string()
        }

        SemanticKind::CompanyName => CompanyName().fake_with_rng(&mut fake_rng),
        SemanticKind::CompanyDepartment => {
            DEPARTMENTS[fake_rng.random_range(0..DEPARTMENTS.len())].to_string()
        }
        SemanticKind::CompanyJobTitle => {
            JOB_TITLES[fake_rng.random_range(0..JOB_TITLES.len())].to_string()
        }

        SemanticKind::AddressStreet => StreetName().fake_with_rng(&mut fake_rng),
        SemanticKind::AddressLine2 => SecondaryAddress().fake_with_rng(&mut fake_rng),
        SemanticKind::AddressCity => CityName().fake_with_rng(&mut fake_rng),
        SemanticKind::AddressRegion => StateName().fake_with_rng(&mut fake_rng),
        SemanticKind::AddressPostcode => ZipCode().fake_with_rng(&mut fake_rng),
        SemanticKind::AddressCountry => CountryName().fake_with_rng(&mut fake_rng),

        SemanticKind::CommerceCurrency => CurrencyCode().fake_with_rng(&mut fake_rng),

        SemanticKind::TextWord => Word().fake_with_rng(&mut fake_rng),
        SemanticKind::TextSentence => Sentence(5..10).fake_with_rng(&mut fake_rng),
        SemanticKind::TextParagraph => Paragraph(3..5).fake_with_rng(&mut fake_rng),
        SemanticKind::TextSlug => {
            let words: [String; 3] = [
                Word().fake_with_rng(&mut fake_rng),
                Word().fake_with_rng(&mut fake_rng),
                Word().fake_with_rng(&mut fake_rng),
            ];
            words
                .iter()
                .map(|w| w.to_lowercase())
                .collect::<Vec<_>>()
                .join("-")
        }

        SemanticKind::FileName => FileName().fake_with_rng(&mut fake_rng),
        SemanticKind::FileExtension => FileExtension().fake_with_rng(&mut fake_rng),
        SemanticKind::FileMimeType => MimeType().fake_with_rng(&mut fake_rng),

        SemanticKind::NetworkMac => MACAddress().fake_with_rng(&mut fake_rng),
    }
}
