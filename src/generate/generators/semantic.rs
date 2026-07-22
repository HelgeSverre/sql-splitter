//! The semantic, credential, and temporal generator catalog.
//!
//! Most of this catalog (`person.*`, `internet.*` minus a few numeric-ish
//! fields, `phone.*`, `company.*`, most of `address.*`, `commerce.currency`,
//! `text.*`, `file.name`/`extension`/`mime_type`, `network.mac`) is a single
//! locale-scoped draw from the shared [`crate::fake_data`] catalog — the
//! same catalog the redactor's `fake` strategy uses — wrapped by
//! [`FakeTextFactory`]/[`CompiledFakeText`], the one factory/compiled pair
//! shared by every such kind.
//!
//! Everything else needs its own shape: numeric ranges
//! (`address.latitude`/`longitude`, `commerce.money`/`quantity`,
//! `file.size`, `network.port`, `duration`), configurable random strings
//! (`identifier.*`, `credential.token`/`api_key`/`secret`), fixed synthetic
//! formats (`credential.password_hash`, `credential.placeholder`), and
//! timestamp arithmetic (`date`, `time`, `datetime`, `before`, `after`).

use rand::RngExt;
use rand_chacha::ChaCha8Rng;

use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime};

use crate::diagnostic::DiagnosticBag;
use crate::fake_data::{self, Locale};
use crate::synthetic::model::GeneratorConfig;
use crate::synthetic::schema::{PortableColumn, PortableTable, SqlTypeFamily};

use crate::generate::registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledGenerator, Determinism,
    ExtensionRegistry, GeneratorDescriptor, GeneratorFactory, RowContext, Verification,
    INHERENT_UNIQUENESS_ENTROPY_BITS,
};
use crate::generate::seed::StreamId;
use crate::generate::value::{GenerateError, GeneratedValue};

// --- Shared helpers (mirrors core.rs's; kept local since core.rs's are
// module-private and this catalog's argument surface is small enough that
// duplicating a handful of parsers is cheaper than widening core.rs's API)
// ---------------------------------------------------------------------------

fn column<'a>(context: &CompileContext<'a>) -> &'a PortableColumn {
    context
        .column()
        .expect("semantic generators are column-scoped")
}

fn stream(context: &CompileContext<'_>, kind: &str) -> ChaCha8Rng {
    let table = context.table().name.clone();
    let col = column(context).name.clone();
    context.rng(StreamId::column(table, col, kind.to_string()))
}

fn find_column<'a>(table: &'a PortableTable, name: &str) -> Option<&'a PortableColumn> {
    table.columns.iter().find(|c| c.name == name)
}

fn parse_i128(value: &serde_yaml_ng::Value) -> Option<i128> {
    match value {
        serde_yaml_ng::Value::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_f64().map(|f| f as i128)),
        serde_yaml_ng::Value::String(s) => s.trim().parse::<i128>().ok(),
        _ => None,
    }
}

fn parse_usize(value: &serde_yaml_ng::Value) -> Option<usize> {
    parse_i128(value).and_then(|n| usize::try_from(n).ok())
}

fn parse_str(value: &serde_yaml_ng::Value) -> Option<&str> {
    value.as_str()
}

/// Render `minor` units at `scale` decimal places as a fixed-point string.
fn format_decimal(minor: i128, scale: u32) -> String {
    if scale == 0 {
        return minor.to_string();
    }
    let negative = minor < 0;
    let magnitude = minor.unsigned_abs();
    let factor = 10u128.pow(scale);
    let whole = magnitude / factor;
    let frac = magnitude % factor;
    let sign = if negative { "-" } else { "" };
    format!("{sign}{whole}.{frac:0width$}", width = scale as usize)
}

/// Emit a decimal-shaped value in whichever representation `family` expects.
fn decimal_value(family: &SqlTypeFamily, minor: i128, scale: u32) -> GeneratedValue {
    match family {
        SqlTypeFamily::Decimal => GeneratedValue::Decimal { minor, scale },
        _ => GeneratedValue::Text(format_decimal(minor, scale)),
    }
}

/// Emit a timestamp-shaped value in whichever representation `family` expects.
fn temporal_value(family: &SqlTypeFamily, formatted: String) -> GeneratedValue {
    match family {
        SqlTypeFamily::DateTime => GeneratedValue::DateTime(formatted),
        _ => GeneratedValue::Text(formatted),
    }
}

const TEXT_ONLY: &[SqlTypeFamily] = &[SqlTypeFamily::Text];
const DECIMAL_OR_TEXT: &[SqlTypeFamily] = &[SqlTypeFamily::Decimal, SqlTypeFamily::Text];
const INTEGER_FAMILIES: &[SqlTypeFamily] = &[SqlTypeFamily::Integer, SqlTypeFamily::BigInteger];
const TEMPORAL_FAMILIES: &[SqlTypeFamily] = &[SqlTypeFamily::DateTime, SqlTypeFamily::Text];

// --- Fake-data-backed text generators ---------------------------------------

/// One static description of a [`fake_data::SemanticKind`]-backed generator:
/// its registry descriptor plus which shared catalog entry it draws from.
struct FakeTextSpec {
    descriptor: GeneratorDescriptor,
    kind: fake_data::SemanticKind,
}

/// Factory for every catalog kind that is a single draw from the shared
/// [`fake_data`] catalog: reads no config, always emits `Text`.
struct FakeTextFactory(&'static FakeTextSpec);

impl GeneratorFactory for FakeTextFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &self.0.descriptor
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let rng = stream(context, self.0.descriptor.kind);
        Ok(Box::new(CompiledFakeText {
            kind: self.0.kind,
            rng,
        }))
    }
}

struct CompiledFakeText {
    kind: fake_data::SemanticKind,
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledFakeText {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let text = fake_data::generate_semantic(self.kind, Locale::En, &mut self.rng);
        *output = GeneratedValue::Text(text);
        Ok(())
    }
}

macro_rules! fake_text_spec {
    ($static_name:ident, $kind_str:literal, $summary:literal, $fake_kind:expr) => {
        static $static_name: FakeTextSpec = FakeTextSpec {
            descriptor: GeneratorDescriptor {
                kind: $kind_str,
                aliases: &[],
                summary: $summary,
                arguments: &[],
                accepts: TEXT_ONLY,
                writes: ColumnScope::OwnColumn,
                reads: ColumnScope::None,
                determinism: Determinism::Deterministic,
                buffering: Buffering::Streaming,
                verification: Verification::Unsupported,
            },
            kind: $fake_kind,
        };
    };
}

fake_text_spec!(
    PERSON_FIRST_NAME,
    "person.first_name",
    "A random first name.",
    fake_data::SemanticKind::PersonFirstName
);
fake_text_spec!(
    PERSON_LAST_NAME,
    "person.last_name",
    "A random last name.",
    fake_data::SemanticKind::PersonLastName
);
fake_text_spec!(
    PERSON_FULL_NAME,
    "person.full_name",
    "A random full name.",
    fake_data::SemanticKind::PersonFullName
);
fake_text_spec!(
    PERSON_USERNAME,
    "person.username",
    "A random username.",
    fake_data::SemanticKind::PersonUsername
);
fake_text_spec!(
    PERSON_TITLE,
    "person.title",
    "A random honorific title (Mr., Dr., ...).",
    fake_data::SemanticKind::PersonTitle
);
fake_text_spec!(
    INTERNET_EMAIL,
    "internet.email",
    "A random, safely-namespaced email address.",
    fake_data::SemanticKind::InternetEmail
);
fake_text_spec!(
    INTERNET_DOMAIN,
    "internet.domain",
    "A random domain name.",
    fake_data::SemanticKind::InternetDomain
);
fake_text_spec!(
    INTERNET_URL,
    "internet.url",
    "A random URL.",
    fake_data::SemanticKind::InternetUrl
);
fake_text_spec!(
    INTERNET_IPV4,
    "internet.ipv4",
    "A random IPv4 address.",
    fake_data::SemanticKind::InternetIpv4
);
fake_text_spec!(
    INTERNET_IPV6,
    "internet.ipv6",
    "A random IPv6 address.",
    fake_data::SemanticKind::InternetIpv6
);
fake_text_spec!(
    INTERNET_USER_AGENT,
    "internet.user_agent",
    "A random browser User-Agent string.",
    fake_data::SemanticKind::InternetUserAgent
);
fake_text_spec!(
    PHONE_NUMBER,
    "phone.number",
    "A random phone number.",
    fake_data::SemanticKind::PhoneNumber
);
fake_text_spec!(
    PHONE_COUNTRY_CODE,
    "phone.country_code",
    "A random international calling code.",
    fake_data::SemanticKind::PhoneCountryCode
);
fake_text_spec!(
    COMPANY_NAME,
    "company.name",
    "A random company name.",
    fake_data::SemanticKind::CompanyName
);
fake_text_spec!(
    COMPANY_DEPARTMENT,
    "company.department",
    "A random department name.",
    fake_data::SemanticKind::CompanyDepartment
);
fake_text_spec!(
    COMPANY_JOB_TITLE,
    "company.job_title",
    "A random job title.",
    fake_data::SemanticKind::CompanyJobTitle
);
fake_text_spec!(
    ADDRESS_LINE1,
    "address.line1",
    "A random street address line.",
    fake_data::SemanticKind::AddressStreet
);
fake_text_spec!(
    ADDRESS_LINE2,
    "address.line2",
    "A random secondary address line (apartment/suite).",
    fake_data::SemanticKind::AddressLine2
);
fake_text_spec!(
    ADDRESS_CITY,
    "address.city",
    "A random city name.",
    fake_data::SemanticKind::AddressCity
);
fake_text_spec!(
    ADDRESS_REGION,
    "address.region",
    "A random region/state name.",
    fake_data::SemanticKind::AddressRegion
);
fake_text_spec!(
    ADDRESS_POSTCODE,
    "address.postcode",
    "A random postal code.",
    fake_data::SemanticKind::AddressPostcode
);
fake_text_spec!(
    ADDRESS_COUNTRY,
    "address.country",
    "A random country name.",
    fake_data::SemanticKind::AddressCountry
);
fake_text_spec!(
    COMMERCE_CURRENCY,
    "commerce.currency",
    "A random ISO 4217 currency code.",
    fake_data::SemanticKind::CommerceCurrency
);
fake_text_spec!(
    TEXT_WORD,
    "text.word",
    "A random single word.",
    fake_data::SemanticKind::TextWord
);
fake_text_spec!(
    TEXT_SENTENCE,
    "text.sentence",
    "A random sentence.",
    fake_data::SemanticKind::TextSentence
);
fake_text_spec!(
    TEXT_PARAGRAPH,
    "text.paragraph",
    "A random paragraph.",
    fake_data::SemanticKind::TextParagraph
);
fake_text_spec!(
    TEXT_SLUG,
    "text.slug",
    "A random hyphenated slug.",
    fake_data::SemanticKind::TextSlug
);
fake_text_spec!(
    FILE_NAME,
    "file.name",
    "A random file name.",
    fake_data::SemanticKind::FileName
);
fake_text_spec!(
    FILE_EXTENSION,
    "file.extension",
    "A random file extension.",
    fake_data::SemanticKind::FileExtension
);
fake_text_spec!(
    FILE_MIME_TYPE,
    "file.mime_type",
    "A random MIME type.",
    fake_data::SemanticKind::FileMimeType
);
fake_text_spec!(
    NETWORK_MAC,
    "network.mac",
    "A random MAC address.",
    fake_data::SemanticKind::NetworkMac
);

const FAKE_TEXT_SPECS: &[&FakeTextSpec] = &[
    &PERSON_FIRST_NAME,
    &PERSON_LAST_NAME,
    &PERSON_FULL_NAME,
    &PERSON_USERNAME,
    &PERSON_TITLE,
    &INTERNET_EMAIL,
    &INTERNET_DOMAIN,
    &INTERNET_URL,
    &INTERNET_IPV4,
    &INTERNET_IPV6,
    &INTERNET_USER_AGENT,
    &PHONE_NUMBER,
    &PHONE_COUNTRY_CODE,
    &COMPANY_NAME,
    &COMPANY_DEPARTMENT,
    &COMPANY_JOB_TITLE,
    &ADDRESS_LINE1,
    &ADDRESS_LINE2,
    &ADDRESS_CITY,
    &ADDRESS_REGION,
    &ADDRESS_POSTCODE,
    &ADDRESS_COUNTRY,
    &COMMERCE_CURRENCY,
    &TEXT_WORD,
    &TEXT_SENTENCE,
    &TEXT_PARAGRAPH,
    &TEXT_SLUG,
    &FILE_NAME,
    &FILE_EXTENSION,
    &FILE_MIME_TYPE,
    &NETWORK_MAC,
];

// --- Coordinate generators (address.latitude / address.longitude) ----------

/// The `address.latitude`/`address.longitude` generator: a uniformly random
/// coordinate, represented as an integer-scaled decimal (6 decimal places).
struct CoordinateFactory {
    descriptor: &'static GeneratorDescriptor,
    /// Inclusive bound in whole degrees (e.g. 90 for latitude, 180 for
    /// longitude); the generated range is `[-bound, bound]` at scale 6.
    bound_degrees: i128,
}

static ADDRESS_LATITUDE_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "address.latitude",
    aliases: &[],
    summary: "A uniformly random latitude in [-90, 90], 6 decimal places.",
    arguments: &[],
    accepts: DECIMAL_OR_TEXT,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

static ADDRESS_LONGITUDE_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "address.longitude",
    aliases: &[],
    summary: "A uniformly random longitude in [-180, 180], 6 decimal places.",
    arguments: &[],
    accepts: DECIMAL_OR_TEXT,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for CoordinateFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        self.descriptor
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let rng = stream(context, self.descriptor.kind);
        let family = column(context).family.clone();
        let bound_minor = self.bound_degrees * 1_000_000;
        Ok(Box::new(CompiledCoordinate {
            family,
            bound_minor,
            rng,
        }))
    }
}

struct CompiledCoordinate {
    family: SqlTypeFamily,
    bound_minor: i128,
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledCoordinate {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let minor = self.rng.random_range(-self.bound_minor..=self.bound_minor);
        *output = decimal_value(&self.family, minor, 6);
        Ok(())
    }
}

// --- commerce.product_name ---------------------------------------------------

static COMMERCE_PRODUCT_NAME_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "commerce.product_name",
    aliases: &[],
    summary: "A random two-word product name.",
    arguments: &[],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct CommerceProductNameFactory;

impl GeneratorFactory for CommerceProductNameFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &COMMERCE_PRODUCT_NAME_DESCRIPTOR
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let rng = stream(context, "commerce.product_name");
        Ok(Box::new(CompiledCommerceProductName { rng }))
    }
}

struct CompiledCommerceProductName {
    rng: ChaCha8Rng,
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

impl CompiledGenerator for CompiledCommerceProductName {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let first = fake_data::generate_semantic(
            fake_data::SemanticKind::TextWord,
            Locale::En,
            &mut self.rng,
        );
        let second = fake_data::generate_semantic(
            fake_data::SemanticKind::TextWord,
            Locale::En,
            &mut self.rng,
        );
        *output = GeneratedValue::Text(format!("{} {}", capitalize(&first), capitalize(&second)));
        Ok(())
    }
}

// --- Random-string generators (identifier.*, credential.token/api_key/secret)

/// The alphabets a [`RandomStringFactory`] can draw from, selected by its
/// `alphabet` config argument.
#[derive(Debug, Clone, Copy)]
enum Alphabet {
    Alphanumeric,
    Hex,
    Numeric,
    Alpha,
    UrlSafe,
}

impl Alphabet {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "alphanumeric" => Some(Self::Alphanumeric),
            "hex" => Some(Self::Hex),
            "numeric" => Some(Self::Numeric),
            "alpha" => Some(Self::Alpha),
            "url_safe" | "urlsafe" => Some(Self::UrlSafe),
            _ => None,
        }
    }

    fn chars(self) -> &'static [u8] {
        match self {
            Self::Alphanumeric => b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
            Self::Hex => b"0123456789abcdef",
            Self::Numeric => b"0123456789",
            Self::Alpha => b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
            Self::UrlSafe => b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-",
        }
    }
}

/// Static configuration for a [`RandomStringFactory`]: its registry
/// descriptor plus the defaults it falls back to when its own `length`,
/// `alphabet`, or `prefix` config arguments are omitted.
struct RandomStringSpec {
    descriptor: GeneratorDescriptor,
    default_length: usize,
    default_alphabet: Alphabet,
    default_prefix: &'static str,
}

/// Factory for every catalog kind that is a configurable random string:
/// `identifier.token`, `identifier.nanoid`, `credential.token`,
/// `credential.api_key`, `credential.secret`. `length` sets the length of
/// the random suffix (not counting `prefix`), so a configured `length`
/// always matches the number of drawn characters exactly.
struct RandomStringFactory(&'static RandomStringSpec);

impl GeneratorFactory for RandomStringFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &self.0.descriptor
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let length = config
            .args
            .get("length")
            .and_then(parse_usize)
            .unwrap_or(self.0.default_length);
        let alphabet = match config.args.get("alphabet").and_then(parse_str) {
            Some(name) => match Alphabet::parse(name) {
                Some(alphabet) => alphabet,
                None => {
                    bag.error(
                        crate::diagnostic::codes::RANDOM_STRING_INVALID_ALPHABET.code,
                        context.path(),
                        format!("unknown alphabet `{name}`"),
                    );
                    self.0.default_alphabet
                }
            },
            None => self.0.default_alphabet,
        };
        let prefix = config
            .args
            .get("prefix")
            .and_then(parse_str)
            .unwrap_or(self.0.default_prefix)
            .to_string();
        let rng = stream(context, self.0.descriptor.kind);
        bag.into_result(Box::new(CompiledRandomString {
            length,
            alphabet,
            prefix,
            rng,
        }) as Box<dyn CompiledGenerator>)
    }
}

struct CompiledRandomString {
    length: usize,
    alphabet: Alphabet,
    prefix: String,
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledRandomString {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let alphabet = self.alphabet.chars();
        let mut text = String::with_capacity(self.prefix.len() + self.length);
        text.push_str(&self.prefix);
        for _ in 0..self.length {
            let index = self.rng.random_range(0..alphabet.len());
            text.push(alphabet[index] as char);
        }
        *output = GeneratedValue::Text(text);
        Ok(())
    }

    fn is_inherently_unique(&self) -> bool {
        // Entropy is the random suffix only; the constant prefix adds none.
        let symbols = self.alphabet.chars().len() as f64;
        let entropy = self.length as f64 * symbols.log2();
        entropy >= INHERENT_UNIQUENESS_ENTROPY_BITS
    }
}

static IDENTIFIER_TOKEN_SPEC: RandomStringSpec = RandomStringSpec {
    descriptor: GeneratorDescriptor {
        kind: "identifier.token",
        aliases: &[],
        summary: "A random string with a configurable length and alphabet.",
        arguments: &[
            ArgumentSpec {
                name: "length",
                required: false,
                summary: "Number of characters drawn; defaults to 32.",
            },
            ArgumentSpec {
                name: "alphabet",
                required: false,
                summary: "`alphanumeric` | `hex` | `numeric` | `alpha` | `url_safe`; \
                          defaults to `alphanumeric`.",
            },
            ArgumentSpec {
                name: "prefix",
                required: false,
                summary: "Literal text prepended before the random characters.",
            },
        ],
        accepts: TEXT_ONLY,
        writes: ColumnScope::OwnColumn,
        reads: ColumnScope::None,
        determinism: Determinism::Deterministic,
        buffering: Buffering::Streaming,
        verification: Verification::Unsupported,
    },
    default_length: 32,
    default_alphabet: Alphabet::Alphanumeric,
    default_prefix: "",
};

static IDENTIFIER_NANOID_SPEC: RandomStringSpec = RandomStringSpec {
    descriptor: GeneratorDescriptor {
        kind: "identifier.nanoid",
        aliases: &[],
        summary: "A random Nano ID (URL-safe alphabet, 21 characters by default).",
        arguments: &[
            ArgumentSpec {
                name: "length",
                required: false,
                summary: "Number of characters drawn; defaults to 21.",
            },
            ArgumentSpec {
                name: "alphabet",
                required: false,
                summary: "`alphanumeric` | `hex` | `numeric` | `alpha` | `url_safe`; \
                          defaults to `url_safe`.",
            },
            ArgumentSpec {
                name: "prefix",
                required: false,
                summary: "Literal text prepended before the random characters.",
            },
        ],
        accepts: TEXT_ONLY,
        writes: ColumnScope::OwnColumn,
        reads: ColumnScope::None,
        determinism: Determinism::Deterministic,
        buffering: Buffering::Streaming,
        verification: Verification::Unsupported,
    },
    default_length: 21,
    default_alphabet: Alphabet::UrlSafe,
    default_prefix: "",
};

static CREDENTIAL_TOKEN_SPEC: RandomStringSpec = RandomStringSpec {
    descriptor: GeneratorDescriptor {
        kind: "credential.token",
        aliases: &[],
        summary: "A synthetic bearer-token-shaped random string.",
        arguments: &[
            ArgumentSpec {
                name: "length",
                required: false,
                summary: "Number of characters drawn; defaults to 32.",
            },
            ArgumentSpec {
                name: "alphabet",
                required: false,
                summary: "`alphanumeric` | `hex` | `numeric` | `alpha` | `url_safe`; \
                          defaults to `alphanumeric`.",
            },
            ArgumentSpec {
                name: "prefix",
                required: false,
                summary: "Literal text prepended before the random characters.",
            },
        ],
        accepts: TEXT_ONLY,
        writes: ColumnScope::OwnColumn,
        reads: ColumnScope::None,
        determinism: Determinism::Deterministic,
        buffering: Buffering::Streaming,
        verification: Verification::Unsupported,
    },
    default_length: 32,
    default_alphabet: Alphabet::Alphanumeric,
    default_prefix: "",
};

static CREDENTIAL_API_KEY_SPEC: RandomStringSpec = RandomStringSpec {
    descriptor: GeneratorDescriptor {
        kind: "credential.api_key",
        aliases: &[],
        summary: "A synthetic API-key-shaped random string (`sk_`-prefixed by default).",
        arguments: &[
            ArgumentSpec {
                name: "length",
                required: false,
                summary: "Number of characters drawn after the prefix; defaults to 32.",
            },
            ArgumentSpec {
                name: "alphabet",
                required: false,
                summary: "`alphanumeric` | `hex` | `numeric` | `alpha` | `url_safe`; \
                          defaults to `alphanumeric`.",
            },
            ArgumentSpec {
                name: "prefix",
                required: false,
                summary: "Literal text prepended before the random characters; defaults to `sk_`.",
            },
        ],
        accepts: TEXT_ONLY,
        writes: ColumnScope::OwnColumn,
        reads: ColumnScope::None,
        determinism: Determinism::Deterministic,
        buffering: Buffering::Streaming,
        verification: Verification::Unsupported,
    },
    default_length: 32,
    default_alphabet: Alphabet::Alphanumeric,
    default_prefix: "sk_",
};

static CREDENTIAL_SECRET_SPEC: RandomStringSpec = RandomStringSpec {
    descriptor: GeneratorDescriptor {
        kind: "credential.secret",
        aliases: &[],
        summary: "A synthetic long-form secret random string.",
        arguments: &[
            ArgumentSpec {
                name: "length",
                required: false,
                summary: "Number of characters drawn; defaults to 48.",
            },
            ArgumentSpec {
                name: "alphabet",
                required: false,
                summary: "`alphanumeric` | `hex` | `numeric` | `alpha` | `url_safe`; \
                          defaults to `alphanumeric`.",
            },
            ArgumentSpec {
                name: "prefix",
                required: false,
                summary: "Literal text prepended before the random characters.",
            },
        ],
        accepts: TEXT_ONLY,
        writes: ColumnScope::OwnColumn,
        reads: ColumnScope::None,
        determinism: Determinism::Deterministic,
        buffering: Buffering::Streaming,
        verification: Verification::Unsupported,
    },
    default_length: 48,
    default_alphabet: Alphabet::Alphanumeric,
    default_prefix: "",
};

// --- identifier.ulid ----------------------------------------------------------

const CROCKFORD_BASE32: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

static IDENTIFIER_ULID_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "identifier.ulid",
    aliases: &[],
    summary: "A random 26-character Crockford-base32 ULID-shaped identifier.",
    arguments: &[],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct IdentifierUlidFactory;

impl GeneratorFactory for IdentifierUlidFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &IDENTIFIER_ULID_DESCRIPTOR
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let rng = stream(context, "identifier.ulid");
        Ok(Box::new(CompiledUlid { rng }))
    }
}

struct CompiledUlid {
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledUlid {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let text: String = (0..26)
            .map(|_| {
                let index = self.rng.random_range(0..CROCKFORD_BASE32.len());
                CROCKFORD_BASE32[index] as char
            })
            .collect();
        *output = GeneratedValue::Text(text);
        Ok(())
    }

    fn is_inherently_unique(&self) -> bool {
        // 26 Crockford base32 characters ≈ 130 bits of entropy: negligible
        // collision at any table scale.
        true
    }
}

// --- identifier.hash ----------------------------------------------------------

static IDENTIFIER_HASH_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "identifier.hash",
    aliases: &[],
    summary: "A random lowercase-hex, digest-shaped string.",
    arguments: &[ArgumentSpec {
        name: "length",
        required: false,
        summary: "Number of hex characters; defaults to 64 (SHA-256-shaped).",
    }],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct IdentifierHashFactory;

impl GeneratorFactory for IdentifierHashFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &IDENTIFIER_HASH_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let length = config
            .args
            .get("length")
            .and_then(parse_usize)
            .unwrap_or(64);
        let rng = stream(context, "identifier.hash");
        Ok(Box::new(CompiledRandomString {
            length,
            alphabet: Alphabet::Hex,
            prefix: String::new(),
            rng,
        }))
    }
}

// --- credential.password_hash ------------------------------------------------

static CREDENTIAL_PASSWORD_HASH_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "credential.password_hash",
    aliases: &[],
    summary: "A syntactically hash-shaped, unmistakably synthetic password hash \
              (`$synthetic$<64 hex chars>`).",
    arguments: &[],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct CredentialPasswordHashFactory;

impl GeneratorFactory for CredentialPasswordHashFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &CREDENTIAL_PASSWORD_HASH_DESCRIPTOR
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let rng = stream(context, "credential.password_hash");
        Ok(Box::new(CompiledPasswordHash { rng }))
    }
}

struct CompiledPasswordHash {
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledPasswordHash {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let digest: String = (0..64)
            .map(|_| {
                let index = self.rng.random_range(0..Alphabet::Hex.chars().len());
                Alphabet::Hex.chars()[index] as char
            })
            .collect();
        *output = GeneratedValue::Text(format!("$synthetic${digest}"));
        Ok(())
    }
}

// --- credential.placeholder ---------------------------------------------------

/// The fixed placeholder [`CredentialPlaceholderFactory`] always emits. Not a
/// PEM header/footer pair and not any real credential format — deliberately
/// unparsable as a private key, certificate, or any other credential shape,
/// so it can never be mistaken for real secret material.
const INVALID_CREDENTIAL_PLACEHOLDER: &str = "SYNTHETIC_PLACEHOLDER_NOT_A_REAL_CREDENTIAL";

static CREDENTIAL_PLACEHOLDER_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "credential.placeholder",
    aliases: &[],
    summary: "A fixed, unmistakably invalid placeholder for private-key-shaped columns.",
    arguments: &[],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct CredentialPlaceholderFactory;

impl GeneratorFactory for CredentialPlaceholderFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &CREDENTIAL_PLACEHOLDER_DESCRIPTOR
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        _context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        Ok(Box::new(CompiledPlaceholder))
    }
}

struct CompiledPlaceholder;

impl CompiledGenerator for CompiledPlaceholder {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        *output = GeneratedValue::Text(INVALID_CREDENTIAL_PLACEHOLDER.to_string());
        Ok(())
    }
}

// --- Ranged integer generators (commerce.quantity, file.size,
// network.port, duration) ----------------------------------------------------

struct RangedIntegerSpec {
    descriptor: GeneratorDescriptor,
    default_min: i128,
    default_max: i128,
}

struct RangedIntegerFactory(&'static RangedIntegerSpec);

impl GeneratorFactory for RangedIntegerFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &self.0.descriptor
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let min = config
            .args
            .get("min")
            .and_then(parse_i128)
            .unwrap_or(self.0.default_min);
        let max = config
            .args
            .get("max")
            .and_then(parse_i128)
            .unwrap_or(self.0.default_max);
        if min > max {
            bag.error(
                crate::diagnostic::codes::RANGED_INTEGER_RANGE.code,
                context.path(),
                format!(
                    "`{}.min` ({min}) must not exceed `{}.max` ({max})",
                    self.0.descriptor.kind, self.0.descriptor.kind
                ),
            );
        }
        let rng = stream(context, self.0.descriptor.kind);
        bag.into_result(
            Box::new(CompiledRangedInteger { min, max, rng }) as Box<dyn CompiledGenerator>
        )
    }
}

struct CompiledRangedInteger {
    min: i128,
    max: i128,
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledRangedInteger {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        *output = GeneratedValue::Integer(self.rng.random_range(self.min..=self.max));
        Ok(())
    }
}

static COMMERCE_QUANTITY_SPEC: RangedIntegerSpec = RangedIntegerSpec {
    descriptor: GeneratorDescriptor {
        kind: "commerce.quantity",
        aliases: &[],
        summary: "A uniformly random quantity in [min, max].",
        arguments: &[
            ArgumentSpec {
                name: "min",
                required: false,
                summary: "Inclusive lower bound; defaults to 1.",
            },
            ArgumentSpec {
                name: "max",
                required: false,
                summary: "Inclusive upper bound; defaults to 100.",
            },
        ],
        accepts: INTEGER_FAMILIES,
        writes: ColumnScope::OwnColumn,
        reads: ColumnScope::None,
        determinism: Determinism::Deterministic,
        buffering: Buffering::Streaming,
        verification: Verification::Unsupported,
    },
    default_min: 1,
    default_max: 100,
};

static FILE_SIZE_SPEC: RangedIntegerSpec = RangedIntegerSpec {
    descriptor: GeneratorDescriptor {
        kind: "file.size",
        aliases: &[],
        summary: "A uniformly random file size in bytes, in [min, max].",
        arguments: &[
            ArgumentSpec {
                name: "min",
                required: false,
                summary: "Inclusive lower bound; defaults to 1.",
            },
            ArgumentSpec {
                name: "max",
                required: false,
                summary: "Inclusive upper bound; defaults to 10,000,000.",
            },
        ],
        accepts: INTEGER_FAMILIES,
        writes: ColumnScope::OwnColumn,
        reads: ColumnScope::None,
        determinism: Determinism::Deterministic,
        buffering: Buffering::Streaming,
        verification: Verification::Unsupported,
    },
    default_min: 1,
    default_max: 10_000_000,
};

static NETWORK_PORT_SPEC: RangedIntegerSpec = RangedIntegerSpec {
    descriptor: GeneratorDescriptor {
        kind: "network.port",
        aliases: &[],
        summary: "A uniformly random TCP/UDP port in [min, max].",
        arguments: &[
            ArgumentSpec {
                name: "min",
                required: false,
                summary: "Inclusive lower bound; defaults to 1.",
            },
            ArgumentSpec {
                name: "max",
                required: false,
                summary: "Inclusive upper bound; defaults to 65535.",
            },
        ],
        accepts: INTEGER_FAMILIES,
        writes: ColumnScope::OwnColumn,
        reads: ColumnScope::None,
        determinism: Determinism::Deterministic,
        buffering: Buffering::Streaming,
        verification: Verification::Unsupported,
    },
    default_min: 1,
    default_max: 65535,
};

static DURATION_SPEC: RangedIntegerSpec = RangedIntegerSpec {
    descriptor: GeneratorDescriptor {
        kind: "duration",
        aliases: &[],
        summary: "A uniformly random duration in seconds, in [min, max].",
        arguments: &[
            ArgumentSpec {
                name: "min",
                required: false,
                summary: "Inclusive lower bound in seconds; defaults to 0.",
            },
            ArgumentSpec {
                name: "max",
                required: false,
                summary: "Inclusive upper bound in seconds; defaults to 86400 (one day).",
            },
        ],
        accepts: INTEGER_FAMILIES,
        writes: ColumnScope::OwnColumn,
        reads: ColumnScope::None,
        determinism: Determinism::Deterministic,
        buffering: Buffering::Streaming,
        verification: Verification::Unsupported,
    },
    default_min: 0,
    default_max: 86_400,
};

// --- commerce.money -----------------------------------------------------------

static COMMERCE_MONEY_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "commerce.money",
    aliases: &[],
    summary: "A uniformly random decimal amount in [min, max] at a fixed scale.",
    arguments: &[
        ArgumentSpec {
            name: "min",
            required: false,
            summary: "Inclusive lower bound; defaults to 0.",
        },
        ArgumentSpec {
            name: "max",
            required: false,
            summary: "Inclusive upper bound; defaults to 1000.",
        },
        ArgumentSpec {
            name: "scale",
            required: false,
            summary: "Decimal places; defaults to 2.",
        },
    ],
    accepts: DECIMAL_OR_TEXT,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct CommerceMoneyFactory;

impl GeneratorFactory for CommerceMoneyFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &COMMERCE_MONEY_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let scale = config.args.get("scale").and_then(parse_usize).unwrap_or(2);
        let scale = match u32::try_from(scale) {
            Ok(scale) if scale <= 18 => scale,
            _ => {
                bag.error(
                    crate::diagnostic::codes::COMMERCE_MONEY_SCALE.code,
                    context.path(),
                    "`commerce.money.scale` must be between 0 and 18",
                );
                2
            }
        };
        let factor = 10i128.pow(scale);
        let raw_min = config.args.get("min").and_then(parse_i128).unwrap_or(0);
        let raw_max = config.args.get("max").and_then(parse_i128).unwrap_or(1000);
        // Scale bounds to minor units with a checked multiply: a large min/max
        // times `factor` (up to 10^18) can overflow i128 (panic in debug, wrap
        // in release into a bogus range).
        let (min_minor, max_minor) = match (raw_min.checked_mul(factor), raw_max.checked_mul(factor))
        {
            (Some(min), Some(max)) => (min, max),
            _ => {
                bag.error(
                    crate::diagnostic::codes::COMMERCE_MONEY_RANGE.code,
                    context.path(),
                    "`commerce.money` min/max scaled to minor units exceeds the representable range",
                );
                (0, 0)
            }
        };
        if min_minor > max_minor {
            bag.error(
                crate::diagnostic::codes::COMMERCE_MONEY_RANGE.code,
                context.path(),
                format!(
                    "`commerce.money.min` ({}) must not exceed `commerce.money.max` ({})",
                    format_decimal(min_minor, scale),
                    format_decimal(max_minor, scale)
                ),
            );
        }
        let rng = stream(context, "commerce.money");
        let family = column(context).family.clone();
        bag.into_result(Box::new(CompiledCommerceMoney {
            family,
            min_minor,
            max_minor,
            scale,
            rng,
        }) as Box<dyn CompiledGenerator>)
    }
}

struct CompiledCommerceMoney {
    family: SqlTypeFamily,
    min_minor: i128,
    max_minor: i128,
    scale: u32,
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledCommerceMoney {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let minor = self.rng.random_range(self.min_minor..=self.max_minor);
        *output = decimal_value(&self.family, minor, self.scale);
        Ok(())
    }
}

// --- commerce.sku ---------------------------------------------------------

static COMMERCE_SKU_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "commerce.sku",
    aliases: &[],
    summary: "A random `AAA-###-####`-shaped SKU code.",
    arguments: &[],
    accepts: TEXT_ONLY,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct CommerceSkuFactory;

impl GeneratorFactory for CommerceSkuFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &COMMERCE_SKU_DESCRIPTOR
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let rng = stream(context, "commerce.sku");
        Ok(Box::new(CompiledCommerceSku { rng }))
    }
}

struct CompiledCommerceSku {
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledCommerceSku {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let letters: String = (0..3)
            .map(|_| {
                let index = self.rng.random_range(0..Alphabet::Alpha.chars().len());
                (Alphabet::Alpha.chars()[index] as char).to_ascii_uppercase()
            })
            .collect();
        let block_a = self.rng.random_range(0..1000);
        let block_b = self.rng.random_range(0..10000);
        *output = GeneratedValue::Text(format!("{letters}-{block_a:03}-{block_b:04}"));
        Ok(())
    }
}

// --- Temporal generators: date, time, datetime ------------------------------

/// Which of the three timestamp shapes a [`TemporalFactory`] produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemporalShape {
    Date,
    Time,
    DateTime,
}

impl TemporalShape {
    fn format(self) -> &'static str {
        match self {
            Self::Date => "%Y-%m-%d",
            Self::Time => "%H:%M:%S",
            Self::DateTime => "%Y-%m-%d %H:%M:%S",
        }
    }

    /// Classify a column's declared SQL type into a temporal granularity.
    /// Full-precision types (`datetime`/`timestamp`) are checked first since
    /// their names contain both `date` and `time`.
    fn from_source_type(source_type: &str) -> Self {
        let lower = source_type.to_ascii_lowercase();
        if lower.contains("datetime") || lower.contains("timestamp") {
            Self::DateTime
        } else if lower.starts_with("date") {
            Self::Date
        } else if lower.starts_with("time") {
            Self::Time
        } else {
            Self::DateTime
        }
    }

    /// The default `[min, max]` bound, as epoch seconds.
    fn default_bounds(self) -> (i64, i64) {
        match self {
            Self::Time => (0, 86_399),
            Self::Date | Self::DateTime => {
                let min = epoch_seconds_of(1970, 1, 1).unwrap_or(0);
                let max = epoch_seconds_of(2035, 12, 31).unwrap_or(i64::MAX);
                (min, max)
            }
        }
    }
}

fn epoch_seconds_of(year: i32, month: u32, day: u32) -> Option<i64> {
    NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp())
}

static DATE_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "date",
    aliases: &[],
    summary: "A uniformly random calendar date.",
    arguments: &[],
    accepts: TEMPORAL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

static TIME_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "time",
    aliases: &[],
    summary: "A uniformly random time of day.",
    arguments: &[],
    accepts: TEMPORAL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

static DATETIME_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "datetime",
    aliases: &[],
    summary: "A uniformly random date and time.",
    arguments: &[],
    accepts: TEMPORAL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct TemporalFactory {
    descriptor: &'static GeneratorDescriptor,
    shape: TemporalShape,
}

impl GeneratorFactory for TemporalFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        self.descriptor
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let (min, max) = self.shape.default_bounds();
        let rng = stream(context, self.descriptor.kind);
        let family = column(context).family.clone();
        Ok(Box::new(CompiledTemporal {
            family,
            shape: self.shape,
            min,
            max,
            rng,
        }))
    }
}

struct CompiledTemporal {
    family: SqlTypeFamily,
    shape: TemporalShape,
    min: i64,
    max: i64,
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledTemporal {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let seconds = self.rng.random_range(self.min..=self.max);
        let formatted = match self.shape {
            TemporalShape::Time => {
                let hour = seconds / 3600;
                let minute = (seconds % 3600) / 60;
                let second = seconds % 60;
                format!("{hour:02}:{minute:02}:{second:02}")
            }
            _ => chrono::DateTime::from_timestamp(seconds, 0)
                .map(|dt| dt.format(self.shape.format()).to_string())
                .unwrap_or_default(),
        };
        *output = temporal_value(&self.family, formatted);
        Ok(())
    }
}

// --- before / after -----------------------------------------------------------

/// Parse a `date` or `datetime`-shaped literal (as produced by this
/// catalog's own `date`/`datetime` generators, or any `%Y-%m-%d[ %H:%M:%S]`
/// text) into a naive timestamp.
fn parse_timestamp(text: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
        .ok()
        .or_else(|| {
            NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
        })
}

fn source_text(value: &GeneratedValue) -> Option<&str> {
    match value {
        GeneratedValue::Text(s) | GeneratedValue::DateTime(s) => Some(s.as_str()),
        _ => None,
    }
}

static BEFORE_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "before",
    aliases: &[],
    summary: "A timestamp strictly at or before a declared source column.",
    arguments: &[
        ArgumentSpec {
            name: "source",
            required: true,
            summary: "The sibling column to generate relative to.",
        },
        ArgumentSpec {
            name: "min_seconds",
            required: false,
            summary: "Minimum offset before the source, in seconds; defaults to 1.",
        },
        ArgumentSpec {
            name: "max_seconds",
            required: false,
            summary: "Maximum offset before the source, in seconds; defaults to 86400.",
        },
    ],
    accepts: TEMPORAL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::Configured,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

static AFTER_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "after",
    aliases: &[],
    summary: "A timestamp strictly at or after a declared source column.",
    arguments: &[
        ArgumentSpec {
            name: "source",
            required: true,
            summary: "The sibling column to generate relative to.",
        },
        ArgumentSpec {
            name: "min_seconds",
            required: false,
            summary: "Minimum offset after the source, in seconds; defaults to 1.",
        },
        ArgumentSpec {
            name: "max_seconds",
            required: false,
            summary: "Maximum offset after the source, in seconds; defaults to 86400.",
        },
    ],
    accepts: TEMPORAL_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::Configured,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct RelativeFactory {
    descriptor: &'static GeneratorDescriptor,
    /// `true` for `after` (add the offset), `false` for `before` (subtract).
    forward: bool,
}

impl GeneratorFactory for RelativeFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        self.descriptor
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();
        let Some(source) = config
            .args
            .get("source")
            .and_then(parse_str)
            .map(str::to_string)
        else {
            bag.error(
                crate::diagnostic::codes::RELATIVE_MISSING_SOURCE.code,
                context.path(),
                format!("`{}` requires a `source` column name", self.descriptor.kind),
            );
            return Err(bag);
        };
        if find_column(context.table(), &source).is_none() {
            bag.error(
                crate::diagnostic::codes::RELATIVE_UNKNOWN_SOURCE.code,
                context.path(),
                format!(
                    "`{}.source` references unknown column `{source}` on table `{}`",
                    self.descriptor.kind,
                    context.table().name
                ),
            );
        }
        let min_seconds = config
            .args
            .get("min_seconds")
            .and_then(parse_i128)
            .unwrap_or(1);
        let max_seconds = config
            .args
            .get("max_seconds")
            .and_then(parse_i128)
            .unwrap_or(86_400);
        if min_seconds > max_seconds {
            bag.error(
                crate::diagnostic::codes::RELATIVE_RANGE.code,
                context.path(),
                format!(
                    "`{}.min_seconds` ({min_seconds}) must not exceed `{}.max_seconds` ({max_seconds})",
                    self.descriptor.kind, self.descriptor.kind
                ),
            );
        }
        bag.into_result(())?;

        let rng = stream(context, self.descriptor.kind);
        let target = column(context);
        let family = target.family.clone();
        let shape = TemporalShape::from_source_type(&target.source_type);
        Ok(Box::new(CompiledRelative {
            family,
            source,
            forward: self.forward,
            shape,
            min_seconds,
            max_seconds,
            rng,
        }))
    }
}

struct CompiledRelative {
    family: SqlTypeFamily,
    source: String,
    forward: bool,
    shape: TemporalShape,
    min_seconds: i128,
    max_seconds: i128,
    rng: ChaCha8Rng,
}

impl CompiledGenerator for CompiledRelative {
    fn generate(
        &mut self,
        context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        let source_value = context.column(&self.source).ok_or_else(|| {
            GenerateError::InvalidInput(format!(
                "`{}` has not been generated yet for sibling column `{}`",
                self.source, self.source
            ))
        })?;
        let text = source_text(source_value).ok_or_else(|| {
            GenerateError::InvalidInput(format!(
                "column `{}` did not produce a text/datetime value to read from",
                self.source
            ))
        })?;
        let base = parse_timestamp(text).ok_or_else(|| {
            GenerateError::InvalidInput(format!(
                "column `{}` value `{text}` is not a recognized date/datetime",
                self.source
            ))
        })?;

        let delta_seconds = self.rng.random_range(self.min_seconds..=self.max_seconds);
        let delta_seconds = i64::try_from(delta_seconds)
            .map_err(|_| GenerateError::Overflow("relative offset out of range".to_string()))?;
        let delta = ChronoDuration::seconds(delta_seconds);
        let result = if self.forward {
            base.checked_add_signed(delta)
        } else {
            base.checked_sub_signed(delta)
        }
        .ok_or_else(|| {
            GenerateError::Overflow(format!(
                "`{}` could not offset `{}` by {delta_seconds}s without overflowing",
                if self.forward { "after" } else { "before" },
                self.source
            ))
        })?;

        let formatted = result.format(self.shape.format()).to_string();
        *output = temporal_value(&self.family, formatted);
        Ok(())
    }
}

// --- Registration --------------------------------------------------------------

/// Register the complete semantic, credential, and temporal catalog.
pub(crate) fn register_all(registry: &mut ExtensionRegistry) {
    for spec in FAKE_TEXT_SPECS {
        registry
            .register_generator(Box::new(FakeTextFactory(spec)))
            .expect("built-in semantic generator kinds are collision-free");
    }

    registry
        .register_generator(Box::new(CoordinateFactory {
            descriptor: &ADDRESS_LATITUDE_DESCRIPTOR,
            bound_degrees: 90,
        }))
        .expect("built-in semantic generator kinds are collision-free");
    registry
        .register_generator(Box::new(CoordinateFactory {
            descriptor: &ADDRESS_LONGITUDE_DESCRIPTOR,
            bound_degrees: 180,
        }))
        .expect("built-in semantic generator kinds are collision-free");

    registry
        .register_generator(Box::new(CommerceProductNameFactory))
        .expect("built-in semantic generator kinds are collision-free");
    registry
        .register_generator(Box::new(CommerceSkuFactory))
        .expect("built-in semantic generator kinds are collision-free");
    registry
        .register_generator(Box::new(CommerceMoneyFactory))
        .expect("built-in semantic generator kinds are collision-free");

    for spec in [
        &IDENTIFIER_TOKEN_SPEC,
        &IDENTIFIER_NANOID_SPEC,
        &CREDENTIAL_TOKEN_SPEC,
        &CREDENTIAL_API_KEY_SPEC,
        &CREDENTIAL_SECRET_SPEC,
    ] {
        registry
            .register_generator(Box::new(RandomStringFactory(spec)))
            .expect("built-in semantic generator kinds are collision-free");
    }

    registry
        .register_generator(Box::new(IdentifierUlidFactory))
        .expect("built-in semantic generator kinds are collision-free");
    registry
        .register_generator(Box::new(IdentifierHashFactory))
        .expect("built-in semantic generator kinds are collision-free");

    registry
        .register_generator(Box::new(CredentialPasswordHashFactory))
        .expect("built-in semantic generator kinds are collision-free");
    registry
        .register_generator(Box::new(CredentialPlaceholderFactory))
        .expect("built-in semantic generator kinds are collision-free");

    for spec in [
        &COMMERCE_QUANTITY_SPEC,
        &FILE_SIZE_SPEC,
        &NETWORK_PORT_SPEC,
        &DURATION_SPEC,
    ] {
        registry
            .register_generator(Box::new(RangedIntegerFactory(spec)))
            .expect("built-in semantic generator kinds are collision-free");
    }

    registry
        .register_generator(Box::new(TemporalFactory {
            descriptor: &DATE_DESCRIPTOR,
            shape: TemporalShape::Date,
        }))
        .expect("built-in semantic generator kinds are collision-free");
    registry
        .register_generator(Box::new(TemporalFactory {
            descriptor: &TIME_DESCRIPTOR,
            shape: TemporalShape::Time,
        }))
        .expect("built-in semantic generator kinds are collision-free");
    registry
        .register_generator(Box::new(TemporalFactory {
            descriptor: &DATETIME_DESCRIPTOR,
            shape: TemporalShape::DateTime,
        }))
        .expect("built-in semantic generator kinds are collision-free");

    registry
        .register_generator(Box::new(RelativeFactory {
            descriptor: &BEFORE_DESCRIPTOR,
            forward: false,
        }))
        .expect("built-in semantic generator kinds are collision-free");
    registry
        .register_generator(Box::new(RelativeFactory {
            descriptor: &AFTER_DESCRIPTOR,
            forward: true,
        }))
        .expect("built-in semantic generator kinds are collision-free");
}
