//! Tracks PostgreSQL enum type definitions and MySQL enum signatures for
//! bidirectional conversion during streaming SQL translation.
//!
//! When converting MySQL → PostgreSQL, MySQL `ENUM` columns are replaced with a
//! generated PostgreSQL enum type so that the constraint is preserved rather
//! than collapsed to a plain string. The registry records the signatures (sets
//! of labels) seen so far and produces deterministic type names, deduplicating
//! identical signatures when the strategy permits it.
//!
//! When converting PostgreSQL → MySQL, the registry records the labels of every
//! known PostgreSQL enum so that casts like `'active'::order_status` can be
//! stripped correctly — the cast is removed and the literal is left bare.

use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum EnumNamingStrategy {
    /// Generate one PostgreSQL type per table+column combination.
    /// Deterministic, no semantic coupling between unrelated columns.
    #[default]
    PerColumn,
    /// Reuse PostgreSQL types when two columns share identical enum labels,
    /// even across different tables.
    Dedupe,
}

#[derive(Default)]
pub struct EnumRegistry {
    /// PostgreSQL enum definitions: type_name → ordered labels
    pg_enums_by_name: HashMap<String, Vec<String>>,

    /// MySQL enum signatures: canonical(labels) → generated_pg_type_name
    /// Used for deduplication when converting MySQL → PostgreSQL
    enum_signatures: HashMap<String, String>,

    /// Track which CREATE TYPE statements have been emitted, with their labels.
    emitted_pg_types: HashMap<String, Vec<String>>,

    /// Naming strategy for generated PG types
    naming: EnumNamingStrategy,

    /// Bumped on every registration so callers can cache derived state.
    generation: u64,
}

impl EnumRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_naming(naming: EnumNamingStrategy) -> Self {
        Self {
            pg_enums_by_name: HashMap::new(),
            enum_signatures: HashMap::new(),
            emitted_pg_types: HashMap::new(),
            naming,
            generation: 0,
        }
    }

    pub fn register_pg_enum(&mut self, name: &str, labels: Vec<String>) {
        self.pg_enums_by_name.insert(name.to_string(), labels);
        self.generation += 1;
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn get_pg_enum(&self, name: &str) -> Option<&[String]> {
        self.pg_enums_by_name.get(name).map(|v| v.as_slice())
    }

    pub fn get_or_create_pg_type_for_signature(
        &mut self,
        table: &str,
        column: &str,
        labels: &[String],
    ) -> String {
        let table = sanitize_ident(table);
        let column = sanitize_ident(column);
        let signature: Vec<String> = labels.iter().map(|l| l.replace('\0', "")).collect();
        let signature = signature.join("\0");

        match self.naming {
            EnumNamingStrategy::Dedupe => {
                if let Some(existing) = self.enum_signatures.get(&signature) {
                    return existing.clone();
                }
                let base = format!("enum__{table}__{column}");
                if self.enum_signatures.values().any(|name| *name == base) {
                    let mut name = format!("{base}_2");
                    for n in 3u32.. {
                        if !self.enum_signatures.values().any(|v| v == &name) {
                            break;
                        }
                        name = format!("{base}_{n}");
                    }
                    self.enum_signatures.insert(signature, name.clone());
                    return name;
                }
                self.enum_signatures.insert(signature, base.clone());
                base
            }
            EnumNamingStrategy::PerColumn => format!("enum__{table}__{column}"),
        }
    }

    /// Record that a CREATE TYPE was emitted. Returns the labels it was
    /// previously emitted with, or `None` on first emission. Callers use the
    /// return value to detect label drift (e.g. ALTER TABLE MODIFY).
    pub fn mark_emitted(&mut self, name: &str, labels: &[String]) -> Option<Vec<String>> {
        self.emitted_pg_types
            .insert(name.to_string(), labels.to_vec())
    }

    pub fn emitted_labels(&self, name: &str) -> Option<&[String]> {
        self.emitted_pg_types.get(name).map(|v| v.as_slice())
    }

    pub fn is_known_pg_enum_type(&self, type_name: &str) -> bool {
        let normalized = Self::normalize_type_name(type_name);
        self.pg_enums_by_name.contains_key(&normalized)
    }

    pub fn pg_enum_labels_for_type(&self, type_name: &str) -> Option<&[String]> {
        let normalized = Self::normalize_type_name(type_name);
        self.pg_enums_by_name.get(&normalized).map(|v| v.as_slice())
    }

    /// Find the registered key that matches `name` (possibly
    /// differently-qualified). Exact match is tried first; otherwise
    /// returns the first key whose unqualified name matches.
    pub fn resolve_pg_enum_key(&self, name: &str) -> Option<String> {
        if self.pg_enums_by_name.contains_key(name) {
            return Some(name.to_string());
        }
        let unq = Self::normalize_type_name(name);
        let mut candidates: Vec<&String> = self
            .pg_enums_by_name
            .keys()
            .filter(|k| Self::normalize_type_name(k) == unq)
            .collect();
        candidates.sort();
        candidates.first().map(|k| (*k).clone())
    }

    /// Iterate over all registered PG enum type definitions.
    pub fn pg_enum_entries(&self) -> impl Iterator<Item = (&String, &Vec<String>)> {
        self.pg_enums_by_name.iter()
    }

    pub fn normalize_type_name(name: &str) -> String {
        let unqualified = name.rsplit('.').next().unwrap_or(name);
        unqualified
            .trim_matches('"')
            .trim_matches('`')
            .trim_matches('[')
            .trim_matches(']')
            .to_string()
    }
}

fn sanitize_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' {
            out.push(c);
        } else if c.is_ascii_uppercase() {
            out.push(c.to_ascii_lowercase());
        } else if c.is_alphanumeric() {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("col");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_lookup() {
        let mut registry = EnumRegistry::new();
        registry.register_pg_enum(
            "order_status",
            vec!["pending".into(), "shipped".into(), "cancelled".into()],
        );
        let labels = registry.get_pg_enum("order_status").unwrap();
        assert_eq!(labels, &["pending", "shipped", "cancelled"]);
    }

    #[test]
    fn test_per_column_naming() {
        let mut registry = EnumRegistry::new();
        let name = registry.get_or_create_pg_type_for_signature(
            "orders",
            "status",
            &["pending".into(), "shipped".into()],
        );
        assert_eq!(name, "enum__orders__status");
    }

    #[test]
    fn test_dedupe_naming() {
        let mut registry = EnumRegistry::with_naming(EnumNamingStrategy::Dedupe);
        let labels = &["pending".into(), "shipped".into()];
        let first = registry.get_or_create_pg_type_for_signature("orders", "status", labels);
        let second = registry.get_or_create_pg_type_for_signature("invoices", "state", labels);
        assert_eq!(first, second);
    }

    #[test]
    fn test_dedupe_different_labels() {
        let mut registry = EnumRegistry::with_naming(EnumNamingStrategy::Dedupe);
        let first = registry.get_or_create_pg_type_for_signature(
            "orders",
            "status",
            &["pending".into(), "shipped".into()],
        );
        let second =
            registry.get_or_create_pg_type_for_signature("orders", "status", &["cancelled".into()]);
        assert_ne!(first, second);
    }

    #[test]
    fn test_mark_emitted() {
        let mut registry = EnumRegistry::new();
        let labels = vec!["a".to_string(), "b".to_string()];
        assert_eq!(registry.mark_emitted("order_status", &labels), None);
        assert_eq!(registry.mark_emitted("order_status", &labels), Some(labels));
    }

    #[test]
    fn test_normalize_type_name() {
        assert_eq!(
            EnumRegistry::normalize_type_name("\"myschema\".\"order_status\""),
            "order_status"
        );
        assert_eq!(
            EnumRegistry::normalize_type_name("myschema.order_status"),
            "order_status"
        );
        assert_eq!(
            EnumRegistry::normalize_type_name("\"order_status\""),
            "order_status"
        );
        assert_eq!(
            EnumRegistry::normalize_type_name("order_status"),
            "order_status"
        );
        assert_eq!(
            EnumRegistry::normalize_type_name("`order_status`"),
            "order_status"
        );
        assert_eq!(
            EnumRegistry::normalize_type_name("[order_status]"),
            "order_status"
        );
    }

    #[test]
    fn test_normalize_schema_qualified() {
        assert_eq!(
            EnumRegistry::normalize_type_name("public.order_status"),
            "order_status"
        );
    }

    #[test]
    fn dedupe_collision_uses_deterministic_counter_not_hash() {
        let mut reg = EnumRegistry::with_naming(EnumNamingStrategy::Dedupe);
        let first = reg.get_or_create_pg_type_for_signature("t", "s", &["a".into()]);
        let second = reg.get_or_create_pg_type_for_signature("t", "s", &["b".into()]);
        let third = reg.get_or_create_pg_type_for_signature("t", "s", &["c".into()]);
        assert_eq!(first, "enum__t__s");
        assert_eq!(second, "enum__t__s_2");
        assert_eq!(third, "enum__t__s_3");
    }

    #[test]
    fn dedupe_signature_is_injective_for_labels_containing_commas() {
        let mut reg = EnumRegistry::with_naming(EnumNamingStrategy::Dedupe);
        let first = reg.get_or_create_pg_type_for_signature("t1", "a", &["a,b".into(), "c".into()]);
        let second =
            reg.get_or_create_pg_type_for_signature("t2", "b", &["a".into(), "b,c".into()]);
        assert_ne!(
            first, second,
            "labels [\"a,b\",\"c\"] and [\"a\",\"b,c\"] must not share a type"
        );
    }
}
