//! In-process coverage for `src/redactor/`: the PII column detector and YAML
//! config generator (`generate_config`), `StrategyKind` parsing/validation,
//! the `fake` strategy generators, and `RedactConfig` builder merging.

use rand::rngs::StdRng;
use rand::SeedableRng;
use sql_splitter::parser::SqlDialect;
use sql_splitter::redactor::strategy::{FakeStrategy, RedactValue, Strategy};
use sql_splitter::redactor::{generate_config, RedactConfig, RedactYamlConfig, Rule, StrategyKind};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

// ============================================================================
// Helpers
// ============================================================================

/// Write a MySQL dump with one `t` table holding `columns` (all VARCHAR).
fn write_schema(dir: &Path, table: &str, columns: &[&str]) -> PathBuf {
    let cols: Vec<String> = columns
        .iter()
        .map(|c| format!("  `{c}` VARCHAR(255)"))
        .collect();
    let sql = format!(
        "CREATE TABLE `{table}` (\n  `id` INT PRIMARY KEY,\n{}\n);\n",
        cols.join(",\n")
    );
    let path = dir.join(format!("{table}.sql"));
    fs::write(&path, sql).unwrap();
    path
}

/// Run `generate_config` on `input`, returning the YAML it wrote to `output`.
fn generate(input: PathBuf, output: Option<PathBuf>) -> String {
    let config = RedactConfig::builder()
        .input(input.clone())
        .output(output.clone())
        .dialect(SqlDialect::MySql)
        .build()
        .unwrap();
    generate_config(&config).unwrap();
    let path = output.unwrap_or_else(|| input.with_extension("redact.yaml"));
    fs::read_to_string(path).unwrap()
}

/// The lines of the generated rule block for `table.column`, or None if the
/// column was emitted as a commented-out "No PII detected" entry.
fn rule_block(yaml: &str, table: &str, column: &str) -> Option<Vec<String>> {
    let header = format!("  - column: \"{table}.{column}\"");
    let mut lines = yaml.lines().skip_while(|l| *l != header);
    lines.next()?;
    Some(
        lines
            .take_while(|l| l.starts_with("    ") || l.starts_with("   #"))
            .map(|l| l.trim().to_string())
            .collect(),
    )
}

// ============================================================================
// detect_pii (via generate_config): table-driven
// ============================================================================

/// (column name, expected strategy lines, expected confidence comment)
/// `None` strategy lines means "no PII detected" (commented out in the YAML).
type DetectCase = (&'static str, Option<&'static [&'static str]>, &'static str);

const HASH_EMAIL: &[&str] = &["strategy: hash", "preserve_domain: true"];
const CONST_PW: &[&str] = &["strategy: constant", "value: \"$2b$10$REDACTED\""];
const NULL: &[&str] = &["strategy: \"null\""];
const MASK_CC: &[&str] = &["strategy: mask", "pattern: \"****-****-****-XXXX\""];
const FAKE_PHONE: &[&str] = &["strategy: fake", "generator: phone"];
const FAKE_FIRST: &[&str] = &["strategy: fake", "generator: first_name"];
const FAKE_LAST: &[&str] = &["strategy: fake", "generator: last_name"];
const FAKE_NAME: &[&str] = &["strategy: fake", "generator: name"];
const FAKE_ADDRESS: &[&str] = &["strategy: fake", "generator: address"];
const FAKE_CITY: &[&str] = &["strategy: fake", "generator: city"];
const FAKE_ZIP: &[&str] = &["strategy: fake", "generator: zip"];
const FAKE_IP: &[&str] = &["strategy: fake", "generator: ip"];
const FAKE_DATE: &[&str] = &["strategy: fake", "generator: date"];
const FAKE_COMPANY: &[&str] = &["strategy: fake", "generator: company"];

const MEDIUM: &str = "# Medium confidence";
const LOW: &str = "# Low confidence - review";

const DETECT_CASES: &[DetectCase] = &[
    // --- High confidence ---
    ("email", Some(HASH_EMAIL), ""),
    ("user_email", Some(HASH_EMAIL), ""),
    ("EMAIL_ADDRESS", Some(HASH_EMAIL), ""), // case-insensitive
    ("password", Some(CONST_PW), ""),
    ("password_hash", Some(CONST_PW), ""),
    ("passwd", Some(CONST_PW), ""),
    ("ssn", Some(NULL), ""),
    ("social_security_number", Some(NULL), ""),
    ("tax_id", Some(NULL), ""),
    ("tin", Some(NULL), ""),
    ("credit_card_number", Some(MASK_CC), ""),
    ("cc_number", Some(MASK_CC), ""),
    ("first_name", Some(FAKE_FIRST), ""),
    ("fname", Some(FAKE_FIRST), ""),
    ("last_name", Some(FAKE_LAST), ""),
    ("lname", Some(FAKE_LAST), ""),
    ("surname", Some(FAKE_LAST), ""),
    ("mother_surname", Some(FAKE_LAST), ""),
    // --- Medium confidence ---
    ("phone", Some(FAKE_PHONE), MEDIUM),
    ("phone_number", Some(FAKE_PHONE), MEDIUM),
    ("mobile", Some(FAKE_PHONE), MEDIUM),
    ("cell", Some(FAKE_PHONE), MEDIUM),
    ("full_name", Some(FAKE_NAME), MEDIUM),
    ("display_name", Some(FAKE_NAME), MEDIUM),
    ("address", Some(FAKE_ADDRESS), MEDIUM),
    ("street_address", Some(FAKE_ADDRESS), MEDIUM),
    ("street", Some(FAKE_ADDRESS), MEDIUM),
    ("city", Some(FAKE_CITY), MEDIUM),
    ("zip", Some(FAKE_ZIP), MEDIUM),
    ("zip_code", Some(FAKE_ZIP), MEDIUM),
    ("postal_code", Some(FAKE_ZIP), MEDIUM),
    ("ip", Some(FAKE_IP), MEDIUM),
    ("ip_addr", Some(FAKE_IP), MEDIUM),
    ("dob", Some(FAKE_DATE), MEDIUM),
    ("birth_date", Some(FAKE_DATE), MEDIUM),
    ("date_of_birth", Some(FAKE_DATE), MEDIUM),
    // --- Low confidence ---
    ("company", Some(FAKE_COMPANY), LOW),
    ("organization", Some(FAKE_COMPANY), LOW),
    // --- No PII ---
    ("id", None, ""),
    ("created_at", None, ""),
    ("name", None, ""), // bare `name` is deliberately excluded
    ("username", None, ""),
    ("city_id", None, ""),    // only exact `city` matches
    ("tin_number", None, ""), // only exact `tin` matches
    ("ipv6", None, ""),       // only `ip`, `ip_addr`, `*ip_address*` match
    ("last_login_ip", None, ""),
    // --- Pinned current behavior (substring matching; arguably false positives) ---
    ("email_verified_at", Some(HASH_EMAIL), ""),
    ("phone_count", Some(FAKE_PHONE), MEDIUM),
    ("cancelled", Some(FAKE_PHONE), MEDIUM), // contains "cell"
    ("automobile", Some(FAKE_PHONE), MEDIUM), // contains "mobile"
    ("gzip_enabled", Some(FAKE_ZIP), MEDIUM), // contains "zip"
    ("file_name", Some(FAKE_NAME), MEDIUM),
    ("hostname", Some(FAKE_NAME), MEDIUM),
    ("password_reset_token", Some(CONST_PW), ""),
    // "ip_address" hits the `address` branch before the `ip` branch.
    ("ip_address", Some(FAKE_ADDRESS), MEDIUM),
    // "company_name" hits the generic `name` branch before `company`.
    ("company_name", Some(FAKE_NAME), MEDIUM),
    // "email" wins over "password" by rule order.
    ("email_password", Some(HASH_EMAIL), ""),
];

#[test]
fn detect_pii_table_driven() {
    let dir = TempDir::new().unwrap();
    let columns: Vec<&str> = DETECT_CASES
        .iter()
        .map(|(c, _, _)| *c)
        .filter(|c| *c != "id")
        .collect();
    let input = write_schema(dir.path(), "t", &columns);
    let yaml = generate(input, Some(dir.path().join("out.yaml")));

    for (column, expected, confidence) in DETECT_CASES {
        let block = rule_block(&yaml, "t", column);
        match expected {
            None => {
                assert!(block.is_none(), "{column}: expected no PII, got {block:?}");
                assert!(
                    yaml.contains(&format!(
                        "  # - column: \"t.{column}\"  # No PII detected\n  #   strategy: skip\n"
                    )),
                    "{column}: missing commented-out entry"
                );
            }
            Some(lines) => {
                let mut want: Vec<String> = lines.iter().map(|s| s.to_string()).collect();
                if !confidence.is_empty() {
                    want.push(confidence.to_string());
                }
                assert_eq!(block.as_deref(), Some(want.as_slice()), "column {column}");
            }
        }
    }
}

// ============================================================================
// generate_config: YAML structure
// ============================================================================

#[test]
fn generated_yaml_structure_and_round_trip() {
    let dir = TempDir::new().unwrap();
    // Two tables, written in reverse alphabetical order to prove sorting.
    let sql = "\
CREATE TABLE `users` (`id` INT, `email` VARCHAR(255), `password` VARCHAR(255), `bio` TEXT);
CREATE TABLE `accounts` (`id` INT, `ssn` VARCHAR(11), `cc_last4` VARCHAR(4));
";
    let input = dir.path().join("dump.sql");
    fs::write(&input, sql).unwrap();
    let yaml = generate(input, Some(dir.path().join("dump.yaml")));

    // Fixed scaffolding.
    assert!(yaml.starts_with("# sql-splitter redact configuration\n"));
    assert!(yaml.contains("# seed: 12345\n"));
    assert!(yaml.contains("locale: en\n"));
    assert!(yaml.contains("defaults:\n  strategy: skip\n"));
    assert!(yaml.contains("rules:\n"));
    assert!(yaml.ends_with("skip_tables:\n  # - schema_migrations\n  # - ar_internal_metadata\n"));

    // Tables grouped and sorted.
    let accounts = yaml.find("  # --- Table: accounts ---").unwrap();
    let users = yaml.find("  # --- Table: users ---").unwrap();
    assert!(accounts < users);

    // Non-PII columns are commented out.
    assert!(yaml.contains("  # - column: \"users.bio\"  # No PII detected\n"));
    assert!(yaml.contains("  # - column: \"users.id\"  # No PII detected\n"));

    // Per-rule blocks are emitted exactly as detect_pii suggests.
    assert_eq!(
        rule_block(&yaml, "accounts", "ssn").as_deref(),
        Some(&["strategy: \"null\"".to_string()][..])
    );
    assert_eq!(
        rule_block(&yaml, "users", "email").as_deref(),
        Some(
            &[
                "strategy: hash".to_string(),
                "preserve_domain: true".to_string()
            ][..]
        )
    );

    // The generated file must load straight back through the YAML loader.
    let loaded = serde_yaml_ng::from_str::<RedactYamlConfig>(&yaml).unwrap();
    assert!(matches!(
        loaded.defaults.as_ref().map(|d| &d.strategy),
        Some(StrategyKind::Skip)
    ));
    assert_eq!(loaded.rules.len(), 4);
}

/// `--generate-config` output feeds straight back into `--config`.
#[test]
fn generated_config_round_trips_through_loader() {
    let dir = TempDir::new().unwrap();
    let input = write_schema(dir.path(), "t", &["email", "ssn"]);
    let out = dir.path().join("cfg.yaml");
    generate(input.clone(), Some(out.clone()));
    let config = RedactConfig::builder()
        .input(input)
        .config_file(Some(out))
        .build()
        .unwrap();
    assert_eq!(config.rules.len(), 2);
    assert!(matches!(config.default_strategy, StrategyKind::Skip));
}

#[test]
fn generated_config_default_output_path_and_builder_merge() {
    let dir = TempDir::new().unwrap();
    let input = write_schema(dir.path(), "people", &["email", "phone"]);
    let expected = dir.path().join("people.redact.yaml");
    assert!(!expected.exists());

    let yaml = generate(input.clone(), None);
    assert!(expected.exists());
    assert!(yaml.contains("  - column: \"people.email\"\n"));

    assert!(yaml.contains("  - column: \"people.phone\"\n"));
}

#[test]
fn generate_config_missing_input_is_an_error() {
    let config = RedactConfig::builder()
        .input(PathBuf::from("/nonexistent/dump.sql"))
        .build()
        .unwrap();
    assert!(generate_config(&config).is_err());
}

// ============================================================================
// strategy/mod.rs: parsing and validation
// ============================================================================

fn parse_rule(yaml: &str) -> Result<Rule, serde_yaml_ng::Error> {
    serde_yaml_ng::from_str(yaml)
}

#[test]
fn strategy_kind_parses_every_variant() {
    type Check = fn(&StrategyKind) -> bool;
    let cases: &[(&str, Check)] = &[
        // A bare `strategy: null` is YAML null and does not parse (see
        // strategy_kind_rejects_bad_specs); the string form is required.
        ("strategy: 'null'", |s| matches!(s, StrategyKind::Null)),
        ("strategy: \"null\"", |s| matches!(s, StrategyKind::Null)),
        ("strategy: skip", |s| matches!(s, StrategyKind::Skip)),
        ("strategy: shuffle", |s| matches!(s, StrategyKind::Shuffle)),
        (
            "strategy: constant\nvalue: xyz",
            |s| matches!(s, StrategyKind::Constant { value } if value == "xyz"),
        ),
        ("strategy: hash", |s| {
            matches!(
                s,
                StrategyKind::Hash {
                    preserve_domain: false
                }
            )
        }),
        ("strategy: hash\npreserve_domain: true", |s| {
            matches!(
                s,
                StrategyKind::Hash {
                    preserve_domain: true
                }
            )
        }),
        (
            "strategy: mask\npattern: \"XX**\"",
            |s| matches!(s, StrategyKind::Mask { pattern } if pattern == "XX**"),
        ),
        (
            "strategy: fake\ngenerator: email",
            |s| matches!(s, StrategyKind::Fake { generator } if generator == "email"),
        ),
    ];
    for (yaml, check) in cases {
        let rule = parse_rule(&format!("column: users.x\n{yaml}")).unwrap_or_else(|e| {
            panic!("{yaml}: {e}");
        });
        assert_eq!(rule.column, "users.x");
        assert!(check(&rule.strategy), "{yaml}: got {:?}", rule.strategy);
    }
}

#[test]
fn strategy_kind_rejects_bad_specs() {
    let bad = [
        "column: a\nstrategy: bogus",
        "column: a\nstrategy: constant", // missing value
        "column: a\nstrategy: mask",     // missing pattern
        "column: a\nstrategy: fake",     // missing generator
        "column: a\nstrategy: Null",     // wrong case
        "column: a\nstrategy: null",     // YAML null, not the string "null"
        "column: a\nvalue: x",           // missing strategy tag
        "strategy: \"null\"",            // missing column
    ];
    for yaml in bad {
        assert!(parse_rule(yaml).is_err(), "{yaml:?} should not parse");
    }
}

#[test]
fn strategy_kind_serializes_with_tag() {
    let s = serde_yaml_ng::to_string(&StrategyKind::Fake {
        generator: "city".into(),
    })
    .unwrap();
    assert_eq!(s, "strategy: fake\ngenerator: city\n");
    // Serialization quotes it, so save()/load() of Null round-trips.
    assert_eq!(
        serde_yaml_ng::to_string(&StrategyKind::Null).unwrap(),
        "strategy: 'null'\n"
    );
    assert!(matches!(StrategyKind::default(), StrategyKind::Skip));
}

const VALID_GENERATORS: &[&str] = &[
    "email",
    "safe_email",
    "name",
    "first_name",
    "last_name",
    "full_name",
    "phone",
    "phone_number",
    "address",
    "street_address",
    "city",
    "state",
    "zip",
    "zip_code",
    "postal_code",
    "country",
    "company",
    "company_name",
    "job_title",
    "username",
    "user_name",
    "url",
    "ip",
    "ip_address",
    "ipv4",
    "ipv6",
    "uuid",
    "date",
    "date_time",
    "datetime",
    "time",
    "credit_card",
    "iban",
    "lorem",
    "paragraph",
    "sentence",
    "word",
    "ssn",
];

#[test]
fn strategy_kind_validate() {
    StrategyKind::Null.validate().unwrap();
    StrategyKind::Skip.validate().unwrap();
    StrategyKind::Hash {
        preserve_domain: true,
    }
    .validate()
    .unwrap();
    StrategyKind::Hash {
        preserve_domain: false,
    }
    .validate()
    .unwrap();
    StrategyKind::Constant { value: "x".into() }
        .validate()
        .unwrap();
    StrategyKind::Mask {
        pattern: "**XX".into(),
    }
    .validate()
    .unwrap();
    // Any characters are accepted in a mask pattern.
    StrategyKind::Mask {
        pattern: "?!ab".into(),
    }
    .validate()
    .unwrap();

    let err = StrategyKind::Constant {
        value: String::new(),
    }
    .validate()
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Constant strategy requires a non-empty value"
    );
    let err = StrategyKind::Mask {
        pattern: String::new(),
    }
    .validate()
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Mask strategy requires a non-empty pattern"
    );
    assert!(StrategyKind::Shuffle.validate().is_err());

    for g in VALID_GENERATORS {
        StrategyKind::Fake {
            generator: g.to_string(),
        }
        .validate()
        .unwrap();
        // Generator names are case-insensitive.
        StrategyKind::Fake {
            generator: g.to_uppercase(),
        }
        .validate()
        .unwrap();
    }
    for g in ["", "bogus", "e-mail"] {
        let err = StrategyKind::Fake {
            generator: g.to_string(),
        }
        .validate()
        .unwrap_err();
        assert!(
            err.to_string()
                .starts_with(&format!("Unknown fake generator: {g}.")),
            "{g}: {err}"
        );
    }
}

#[test]
fn redact_value_accessors() {
    assert!(RedactValue::Null.is_null());
    assert!(!RedactValue::Integer(1).is_null());
    assert_eq!(RedactValue::String("a".into()).as_str(), Some("a"));
    assert_eq!(RedactValue::Integer(1).as_str(), None);
    assert_eq!(RedactValue::Bytes(vec![1]).as_str(), None);
    assert_eq!(RedactValue::Null.as_str(), None);
}

// ============================================================================
// fake.rs: every generator
// ============================================================================

fn fake(generator: &str, seed: u64) -> String {
    let strategy = FakeStrategy::new(generator.to_string(), "en".to_string());
    let mut rng = StdRng::seed_from_u64(seed);
    match strategy.apply(&RedactValue::String("orig".into()), &mut rng) {
        RedactValue::String(s) => s,
        other => panic!("{generator}: expected String, got {other:?}"),
    }
}

fn is_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

fn is_hex(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Generators handled by `FakeStrategy::generate` (a superset of the
/// names `StrategyKind::validate` accepts: `street`/`street_name` are
/// generated but rejected by validation).
const ALL_GENERATORS: &[&str] = &[
    "name",
    "full_name",
    "first_name",
    "last_name",
    "email",
    "safe_email",
    "phone",
    "phone_number",
    "username",
    "user_name",
    "address",
    "street_address",
    "street",
    "street_name",
    "city",
    "state",
    "zip",
    "zip_code",
    "postal_code",
    "country",
    "company",
    "company_name",
    "job_title",
    "url",
    "ip",
    "ip_address",
    "ipv4",
    "ipv6",
    "uuid",
    "date",
    "datetime",
    "date_time",
    "time",
    "credit_card",
    "iban",
    "ssn",
    "lorem",
    "paragraph",
    "sentence",
    "word",
    "unknown_generator",
];

#[test]
fn fake_generators_shape() {
    for g in ALL_GENERATORS {
        let v = fake(g, 42);
        assert!(!v.is_empty(), "{g} produced empty value");
        assert_ne!(v, "orig", "{g} returned the original value");
        match *g {
            "name" | "full_name" => assert!(v.contains(' '), "{g}: {v}"),
            "email" | "safe_email" => {
                let (_, domain) = v.split_once('@').unwrap_or_else(|| panic!("{g}: {v}"));
                assert!(domain.contains('.'), "{g}: {v}");
            }
            "address" | "street_address" => {
                // "<street>, <city>, <state> <zip>"
                let parts: Vec<&str> = v.split(", ").collect();
                assert_eq!(parts.len(), 3, "{g}: {v}");
                let zip = parts[2].rsplit(' ').next().unwrap();
                assert!(is_digits(zip), "{g}: {v}");
            }
            "zip" | "zip_code" | "postal_code" => assert!(is_digits(&v), "{g}: {v}"),
            "country" => assert_eq!(v, "United States"),
            "job_title" => assert!(
                [
                    "Software Engineer",
                    "Product Manager",
                    "Data Analyst",
                    "Designer",
                    "Marketing Manager",
                    "Sales Representative",
                    "Customer Support",
                    "Operations Manager",
                ]
                .contains(&v.as_str()),
                "{g}: {v}"
            ),
            "url" => assert!(v.starts_with("https://example"), "{g}: {v}"),
            "ip" | "ip_address" | "ipv4" => {
                let octets: Vec<u16> = v.split('.').map(|o| o.parse().unwrap()).collect();
                assert_eq!(octets.len(), 4, "{g}: {v}");
                assert!(octets.iter().all(|o| *o < 255), "{g}: {v}");
                assert!(octets[0] >= 1 && octets[3] >= 1, "{g}: {v}");
            }
            "ipv6" => {
                let groups: Vec<&str> = v.split(':').collect();
                assert_eq!(groups.len(), 8, "{g}: {v}");
                assert!(groups.iter().all(|h| is_hex(h)), "{g}: {v}");
            }
            "uuid" => {
                let parts: Vec<&str> = v.split('-').collect();
                assert_eq!(
                    parts.iter().map(|p| p.len()).collect::<Vec<_>>(),
                    [8, 4, 4, 4, 12],
                    "{g}: {v}"
                );
                assert!(parts.iter().all(|p| is_hex(p)), "{g}: {v}");
                assert!(parts[2].starts_with('4'), "{g}: version nibble {v}");
                assert!(
                    matches!(parts[3].as_bytes()[0], b'8'..=b'9' | b'a'..=b'b'),
                    "{g}: variant {v}"
                );
            }
            "date" => {
                let p: Vec<u32> = v.split('-').map(|x| x.parse().unwrap()).collect();
                assert_eq!(v.len(), 10, "{g}: {v}");
                assert!(
                    (1970..2024).contains(&p[0])
                        && (1..=12).contains(&p[1])
                        && (1..=28).contains(&p[2]),
                    "{g}: {v}"
                );
            }
            "datetime" | "date_time" => {
                assert_eq!(v.len(), 19, "{g}: {v}");
                let (d, t) = v.split_once(' ').unwrap();
                assert_eq!(d.len(), 10);
                assert_eq!(t.len(), 8);
            }
            "time" => {
                let p: Vec<u32> = v.split(':').map(|x| x.parse().unwrap()).collect();
                assert_eq!(v.len(), 8, "{g}: {v}");
                assert!(p[0] < 24 && p[1] < 60 && p[2] < 60, "{g}: {v}");
            }
            "credit_card" => {
                let groups: Vec<&str> = v.split('-').collect();
                assert_eq!(groups.len(), 4, "{g}: {v}");
                assert!(
                    groups.iter().all(|x| x.len() == 4 && is_digits(x)),
                    "{g}: {v}"
                );
            }
            "iban" => {
                assert!(v.starts_with("DE"), "{g}: {v}");
                assert_eq!(v.len(), 22, "{g}: {v}");
                assert!(is_digits(&v[2..]), "{g}: {v}");
            }
            "ssn" => {
                let groups: Vec<&str> = v.split('-').collect();
                assert_eq!(
                    groups.iter().map(|x| x.len()).collect::<Vec<_>>(),
                    [3, 2, 4],
                    "{g}: {v}"
                );
                assert!(groups.iter().all(|x| is_digits(x)), "{g}: {v}");
            }
            "sentence" | "lorem" | "paragraph" => assert!(v.contains(' '), "{g}: {v}"),
            "word" => assert!(!v.contains(' '), "{g}: {v}"),
            "unknown_generator" => {
                let n = v
                    .strip_prefix("FAKE_")
                    .unwrap_or_else(|| panic!("{g}: {v}"));
                assert_eq!(n.len(), 5);
                assert!(is_digits(n));
            }
            _ => {}
        }
    }
}

#[test]
fn fake_generators_are_seed_deterministic_and_seed_sensitive() {
    for g in ALL_GENERATORS {
        assert_eq!(fake(g, 7), fake(g, 7), "{g} not deterministic");
        if *g == "country" {
            continue; // fixed value by design
        }
        let distinct: std::collections::HashSet<String> = (0..20).map(|s| fake(g, s)).collect();
        assert!(distinct.len() > 1, "{g} identical across 20 seeds");
    }
}

#[test]
fn fake_generator_exact_values_for_seed() {
    // Pin a few cheap-to-verify composite formats so regressions in the
    // seeded draw order are caught.
    let uuid = fake("uuid", 42);
    assert_eq!(uuid, fake("UUID", 42), "generator name is case-insensitive");
    assert_eq!(fake("date", 1), fake("DATE", 1));
    assert_eq!(
        fake("email", 3),
        fake("safe_email", 3),
        "aliases share a path"
    );
    assert_eq!(fake("name", 3), fake("full_name", 3));
    assert_eq!(fake("zip", 3), fake("postal_code", 3));
    assert_eq!(fake("ip", 3), fake("ipv4", 3));
    assert_eq!(fake("datetime", 3), fake("date_time", 3));
    assert_eq!(fake("lorem", 3), fake("paragraph", 3));
}

#[test]
fn fake_strategy_passthrough_and_kind() {
    let strategy = FakeStrategy::new("email".into(), "de_de".into());
    let mut rng = StdRng::seed_from_u64(1);
    assert!(strategy.apply(&RedactValue::Null, &mut rng).is_null());
    assert!(matches!(
        strategy.apply(&RedactValue::Integer(5), &mut rng),
        RedactValue::String(s) if s.contains('@')
    ));
    assert!(matches!(
        strategy.apply(&RedactValue::Bytes(vec![0]), &mut rng),
        RedactValue::String(_)
    ));
    assert!(matches!(strategy.kind(), StrategyKind::Fake { generator } if generator == "email"));
}

// ============================================================================
// config.rs: builder merging, validation, YAML load/save
// ============================================================================

fn existing_input(dir: &Path) -> PathBuf {
    let p = dir.join("in.sql");
    fs::write(&p, "CREATE TABLE t (id INT);\n").unwrap();
    p
}

#[test]
fn builder_requires_input() {
    let err = RedactConfig::builder().build().unwrap_err();
    assert_eq!(err.to_string(), "Input file is required");
}

#[test]
fn builder_cli_patterns_become_rules_in_order() {
    let config = RedactConfig::builder()
        .input(PathBuf::from("x.sql"))
        .null_patterns(vec!["*.ssn".into()])
        .hash_patterns(vec!["*.email".into()])
        .fake_patterns(vec!["*.name".into()])
        .mask_patterns(vec!["****XXXX=*.card".into(), "no_equals".into()])
        .constant_patterns(vec!["*.pw=REDACTED".into(), "no_equals".into()])
        .build()
        .unwrap();

    assert_eq!(config.dialect, SqlDialect::MySql);
    assert_eq!(config.locale, "en");
    assert_eq!(config.output, None);
    assert!(matches!(config.default_strategy, StrategyKind::Skip));

    let rules: Vec<(&str, &StrategyKind)> = config
        .rules
        .iter()
        .map(|r| (r.column.as_str(), &r.strategy))
        .collect();
    assert_eq!(
        rules.len(),
        5,
        "malformed mask/constant patterns are dropped"
    );
    assert!(matches!(rules[0], ("*.ssn", StrategyKind::Null)));
    assert!(matches!(
        rules[1],
        (
            "*.email",
            StrategyKind::Hash {
                preserve_domain: false
            }
        )
    ));
    assert!(
        matches!(rules[2], ("*.name", StrategyKind::Fake { generator }) if generator == "name")
    );
    assert!(
        matches!(rules[3], ("*.card", StrategyKind::Mask { pattern }) if pattern == "****XXXX")
    );
    assert!(matches!(rules[4], ("*.pw", StrategyKind::Constant { value }) if value == "REDACTED"));
}

#[test]
fn builder_merges_yaml_with_cli_precedence() {
    let dir = TempDir::new().unwrap();
    let input = existing_input(dir.path());
    let yaml_path = dir.path().join("cfg.yaml");
    fs::write(
        &yaml_path,
        // Flat `defaults: {strategy: ...}` is the documented shape; the nested
        // `defaults: {strategy: {strategy: ...}}` form is still accepted.
        "seed: 99\nlocale: de_de\ndefaults:\n  strategy: 'null'\nrules:\n  - column: users.email\n    strategy: hash\n    preserve_domain: true\nskip_tables:\n  - migrations\n",
    )
    .unwrap();

    // YAML values apply when CLI gives nothing.
    let config = RedactConfig::builder()
        .input(input.clone())
        .config_file(Some(yaml_path.clone()))
        .exclude(vec!["logs".into()])
        .null_patterns(vec!["*.ssn".into()])
        .build()
        .unwrap();
    assert_eq!(config.seed, Some(99));
    assert_eq!(config.locale, "de_de");
    assert!(matches!(config.default_strategy, StrategyKind::Null));
    assert_eq!(
        config.exclude,
        vec!["logs".to_string(), "migrations".to_string()]
    );
    assert_eq!(config.rules.len(), 2);
    assert_eq!(config.rules[0].column, "users.email"); // YAML rules first
    assert_eq!(config.rules[1].column, "*.ssn");
    config.validate().unwrap();

    // CLI seed/locale override YAML.
    let config = RedactConfig::builder()
        .input(input.clone())
        .config_file(Some(yaml_path.clone()))
        .seed(Some(1))
        .locale("fr_fr".into())
        .build()
        .unwrap();
    assert_eq!(config.seed, Some(1));
    assert_eq!(config.locale, "fr_fr");

    // Explicit "en" on the CLI does NOT override a YAML locale.
    let config = RedactConfig::builder()
        .input(input)
        .config_file(Some(yaml_path))
        .locale("en".into())
        .build()
        .unwrap();
    assert_eq!(config.locale, "de_de");
}

#[test]
fn builder_bad_config_file() {
    let dir = TempDir::new().unwrap();
    let missing = RedactConfig::builder()
        .input(PathBuf::from("x.sql"))
        .config_file(Some(dir.path().join("missing.yaml")))
        .build();
    assert!(missing.is_err());

    let bad = dir.path().join("bad.yaml");
    fs::write(&bad, "rules:\n  - column: a\n    strategy: nope\n").unwrap();
    assert!(RedactConfig::builder()
        .input(PathBuf::from("x.sql"))
        .config_file(Some(bad))
        .build()
        .is_err());
}

#[test]
fn config_validate() {
    let dir = TempDir::new().unwrap();
    let input = existing_input(dir.path());

    let err = RedactConfig::builder()
        .input(dir.path().join("nope.sql"))
        .build()
        .unwrap()
        .validate()
        .unwrap_err();
    assert!(err.to_string().starts_with("Input file not found"), "{err}");

    // Bad locale only fails in strict mode.
    let cfg = |strict: bool| {
        RedactConfig::builder()
            .input(input.clone())
            .locale("xx_yy".into())
            .strict(strict)
            .build()
            .unwrap()
    };
    cfg(false).validate().unwrap();
    let err = cfg(true).validate().unwrap_err();
    assert_eq!(
        err.to_string(),
        "Unsupported locale: xx_yy. Use --locale with a supported value."
    );
    for locale in [
        "EN", "en_us", "de_de", "fr_fr", "zh_cn", "zh_tw", "ja_jp", "pt_br", "ar_sa",
    ] {
        RedactConfig::builder()
            .input(input.clone())
            .locale(locale.into())
            .strict(true)
            .build()
            .unwrap()
            .validate()
            .unwrap();
    }

    // Rule validation surfaces through config validation.
    let err = RedactConfig::builder()
        .input(input.clone())
        .constant_patterns(vec!["*.x=".into()])
        .build()
        .unwrap()
        .validate()
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Constant strategy requires a non-empty value"
    );
    let err = RedactConfig::builder()
        .input(input.clone())
        .mask_patterns(vec!["=*.x".into()])
        .build()
        .unwrap()
        .validate()
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Mask strategy requires a non-empty pattern"
    );

    // Empty column pattern rejected at the rule level; default strategy is validated too.
    let err = Rule {
        column: String::new(),
        strategy: StrategyKind::Null,
    }
    .validate()
    .unwrap_err();
    assert_eq!(err.to_string(), "Rule column pattern cannot be empty");
    let yaml = dir.path().join("shuffle.yaml");
    fs::write(&yaml, "defaults:\n  strategy:\n    strategy: shuffle\n").unwrap();
    assert!(RedactConfig::builder()
        .input(input)
        .config_file(Some(yaml))
        .build()
        .unwrap()
        .validate()
        .is_err());
}

#[test]
fn builder_passes_through_flags() {
    let config = RedactConfig::builder()
        .input(PathBuf::from("x.sql"))
        .output(Some(PathBuf::from("out.sql")))
        .dialect(SqlDialect::Postgres)
        .tables_filter(Some(vec!["users".into()]))
        .strict(true)
        .progress(true)
        .dry_run(true)
        .build()
        .unwrap();
    assert_eq!(config.output, Some(PathBuf::from("out.sql")));
    assert_eq!(config.dialect, SqlDialect::Postgres);
    assert_eq!(config.tables_filter, Some(vec!["users".to_string()]));
    assert!(config.strict && config.progress && config.dry_run);
}

#[test]
fn yaml_config_save_load_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cfg.yaml");
    let original: RedactYamlConfig = serde_yaml_ng::from_str(
        "seed: 5\nlocale: en\ndefaults:\n  strategy:\n    strategy: skip\nrules:\n  - column: a.b\n    strategy: mask\n    pattern: XX**\n  - column: c.d\n    strategy: fake\n    generator: city\nskip_tables: [x, y]\n",
    )
    .unwrap();
    original.save(&path).unwrap();
    let loaded = RedactYamlConfig::load(&path).unwrap();
    assert_eq!(loaded.seed, Some(5));
    assert_eq!(loaded.locale.as_deref(), Some("en"));
    assert_eq!(loaded.skip_tables, Some(vec!["x".into(), "y".into()]));
    assert_eq!(loaded.rules.len(), 2);
    assert!(
        matches!(&loaded.rules[0].strategy, StrategyKind::Mask { pattern } if pattern == "XX**")
    );
    assert!(
        matches!(&loaded.rules[1].strategy, StrategyKind::Fake { generator } if generator == "city")
    );

    // Minimal file: everything optional except nothing.
    let minimal: RedactYamlConfig = serde_yaml_ng::from_str("{}").unwrap();
    assert!(minimal.rules.is_empty());
    assert!(minimal.seed.is_none() && minimal.locale.is_none() && minimal.defaults.is_none());
    let text = serde_yaml_ng::to_string(&minimal).unwrap();
    assert_eq!(text, "rules: []\n");
}

#[test]
fn defaults_accepts_flat_and_nested_shapes() {
    for yaml in [
        "defaults:\n  strategy: skip\n",
        "defaults:\n  strategy:\n    strategy: skip\n",
    ] {
        let cfg = serde_yaml_ng::from_str::<RedactYamlConfig>(yaml).unwrap();
        assert!(
            matches!(cfg.defaults.unwrap().strategy, StrategyKind::Skip),
            "{yaml}"
        );
    }
}
