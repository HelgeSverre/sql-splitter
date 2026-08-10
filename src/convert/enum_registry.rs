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

    /// MySQL enum signatures: ordered labels → generated_pg_type_name
    /// Used for deduplication when converting MySQL → PostgreSQL
    enum_signatures: HashMap<Vec<String>, String>,

    /// Source table+column identity → generated PostgreSQL type name.
    pg_types_by_column: HashMap<String, String>,

    /// Names allocated after sanitization and PostgreSQL length limiting.
    used_pg_type_names: HashSet<String>,

    /// Next suffix to try for each length-limited base name.
    next_pg_type_suffix: HashMap<String, u32>,

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
            pg_types_by_column: HashMap::new(),
            used_pg_type_names: HashSet::new(),
            next_pg_type_suffix: HashMap::new(),
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

    pub fn rename_pg_enum(&mut self, old_name: &str, new_name: &str) -> bool {
        let Some(labels) = self.pg_enums_by_name.remove(old_name) else {
            return false;
        };
        self.pg_enums_by_name.insert(new_name.to_string(), labels);
        self.generation += 1;
        true
    }

    pub fn rename_pg_enum_value(&mut self, name: &str, old_value: &str, new_value: &str) -> bool {
        let Some(labels) = self.pg_enums_by_name.get_mut(name) else {
            return false;
        };
        let Some(value) = labels.iter_mut().find(|value| value.as_str() == old_value) else {
            return false;
        };
        *value = new_value.to_string();
        self.generation += 1;
        true
    }

    pub fn get_or_create_pg_type_for_signature(
        &mut self,
        table: &str,
        column: &str,
        labels: &[String],
    ) -> String {
        let column_key = format!("{table}\0{column}");
        if self.naming == EnumNamingStrategy::PerColumn {
            if let Some(existing) = self.pg_types_by_column.get(&column_key) {
                return existing.clone();
            }
        }
        let base = format!(
            "enum__{}__{}",
            sanitize_ident(table),
            sanitize_ident(column)
        );
        let name = match self.naming {
            EnumNamingStrategy::Dedupe => {
                if let Some(existing) = self.enum_signatures.get(labels) {
                    return existing.clone();
                }
                let name = self.allocate_type_name(&base);
                self.enum_signatures.insert(labels.to_vec(), name.clone());
                name
            }
            EnumNamingStrategy::PerColumn => self.allocate_type_name(&base),
        };
        self.pg_types_by_column.insert(column_key, name.clone());
        name
    }

    fn allocate_type_name(&mut self, base: &str) -> String {
        let allocation_key = truncate_utf8(base, 63).to_string();
        let mut number = self
            .next_pg_type_suffix
            .get(&allocation_key)
            .copied()
            .unwrap_or(1);
        loop {
            let suffix = if number == 1 {
                String::new()
            } else {
                format!("_{number}")
            };
            let prefix = truncate_utf8(base, 63 - suffix.len());
            let candidate = format!("{prefix}{suffix}");
            if self.used_pg_type_names.insert(candidate.clone()) {
                self.next_pg_type_suffix
                    .insert(allocation_key, number.saturating_add(1));
                return candidate;
            }
            number = number.saturating_add(1);
        }
    }

    /// Record that a CREATE TYPE was emitted. Returns the labels it was
    /// previously emitted with, or `None` on first emission. Callers use the
    /// return value to detect label drift (e.g. ALTER TABLE MODIFY).
    pub fn mark_emitted(&mut self, name: &str, labels: &[String]) -> Option<Vec<String>> {
        self.emitted_pg_types
            .insert(name.to_string(), labels.to_vec())
    }

    /// Iterate over all registered PG enum type definitions.
    pub fn pg_enum_entries(&self) -> impl Iterator<Item = (&String, &Vec<String>)> {
        self.pg_enums_by_name.iter()
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

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
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

    #[test]
    fn dedupe_signature_is_injective_for_embedded_nul() {
        let mut registry = EnumRegistry::with_naming(EnumNamingStrategy::Dedupe);
        let plain = registry.get_or_create_pg_type_for_signature("t1", "a", &["ab".into()]);
        let with_nul = registry.get_or_create_pg_type_for_signature("t2", "b", &["a\0b".into()]);
        assert_ne!(plain, with_nul);
    }

    #[test]
    fn per_column_names_are_unique_after_sanitization() {
        let mut reg = EnumRegistry::new();
        let first = reg.get_or_create_pg_type_for_signature("a-b", "x", &["one".into()]);
        let second = reg.get_or_create_pg_type_for_signature("a_b", "x", &["two".into()]);
        assert_eq!(first, "enum__a_b__x");
        assert_eq!(second, "enum__a_b__x_2");
    }

    #[test]
    fn generated_names_fit_postgres_limit_and_remain_unique() {
        let mut reg = EnumRegistry::new();
        let long_table = "a".repeat(80);
        let first = reg.get_or_create_pg_type_for_signature(&long_table, "x-y", &["one".into()]);
        let second = reg.get_or_create_pg_type_for_signature(&long_table, "x_y", &["two".into()]);
        assert!(first.len() <= 63, "{first}");
        assert!(second.len() <= 63, "{second}");
        assert_ne!(first, second);
        assert!(second.ends_with("_2"), "{second}");
    }
}
