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

use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

    /// Track which CREATE TYPE statements have been emitted
    emitted_pg_types: HashSet<String>,

    /// Naming strategy for generated PG types
    naming: EnumNamingStrategy,
}

impl EnumRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_naming(naming: EnumNamingStrategy) -> Self {
        Self {
            pg_enums_by_name: HashMap::new(),
            enum_signatures: HashMap::new(),
            emitted_pg_types: HashSet::new(),
            naming,
        }
    }

    pub fn register_pg_enum(&mut self, name: &str, labels: Vec<String>) {
        self.pg_enums_by_name.insert(name.to_string(), labels);
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
        let signature = labels.join(",");

        match self.naming {
            EnumNamingStrategy::Dedupe => {
                if let Some(existing) = self.enum_signatures.get(&signature) {
                    return existing.clone();
                }
                let base = format!("enum__{table}__{column}");
                if self.enum_signatures.values().any(|name| *name == base) {
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut hasher = DefaultHasher::new();
                    signature.hash(&mut hasher);
                    let mut name = format!("{base}_{:x}", hasher.finish());
                    // Guard against hash collisions: if the generated name
                    // already exists with different labels, append a counter.
                    if let Some(existing_sig) =
                        self.enum_signatures
                            .iter()
                            .find(|(_, n)| **n == name)
                            .map(|(s, _)| s)
                    {
                        if existing_sig != &signature {
                            for n in 2u32.. {
                                name = format!("{base}_{:x}_{n}", hasher.finish());
                                if !self.enum_signatures.values().any(|v| v == &name) {
                                    break;
                                }
                            }
                        }
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

    pub fn mark_emitted(&mut self, name: &str) -> bool {
        self.emitted_pg_types.insert(name.to_string())
    }

    pub fn is_known_pg_enum_type(&self, type_name: &str) -> bool {
        let normalized = Self::normalize_type_name(type_name);
        self.pg_enums_by_name.contains_key(&normalized)
    }

    pub fn pg_enum_labels_for_type(&self, type_name: &str) -> Option<&[String]> {
        let normalized = Self::normalize_type_name(type_name);
        self.pg_enums_by_name.get(&normalized).map(|v| v.as_slice())
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
        assert!(registry.mark_emitted("order_status"));
        assert!(!registry.mark_emitted("order_status"));
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
}
