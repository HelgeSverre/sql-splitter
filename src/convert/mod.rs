//! Convert command for translating SQL dumps between dialects.
//!
//! Supports conversion between MySQL, PostgreSQL, and SQLite dialects with:
//! - Identifier quoting conversion (backticks ↔ double quotes)
//! - String escape normalization (\' ↔ '')
//! - Data type mapping (AUTO_INCREMENT ↔ SERIAL ↔ INTEGER PRIMARY KEY)
//! - COPY FROM stdin → INSERT conversion
//! - Session header conversion
//! - Warning system for unsupported features

pub mod copy_to_insert;
#[cfg(feature = "fuzzing")]
pub mod ddl;
#[cfg(not(feature = "fuzzing"))]
pub(crate) mod ddl;
mod enum_parser;
mod enum_registry;
mod types;
mod warnings;

#[allow(unused_imports)]
pub use copy_to_insert::{
    copy_to_inserts, parse_copy_data, parse_copy_header, CopyHeader, CopyValue,
};

pub use enum_registry::EnumNamingStrategy;
use enum_registry::EnumRegistry;

use crate::parser::{Parser, SqlDialect, StatementType};
use crate::splitter::{open_input, open_input_with_progress};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;

#[allow(unused_imports)]
pub(crate) use types::map_column_type;
pub use types::TypeMapper;
pub use warnings::{print_warnings_summary, ConvertWarning, WarningCollector};

/// Configuration for the convert command
#[derive(Debug)]
pub struct ConvertConfig {
    /// Input SQL file
    pub input: PathBuf,
    /// Output SQL file (None for stdout)
    pub output: Option<PathBuf>,
    /// Source dialect (auto-detected if None)
    pub from_dialect: Option<SqlDialect>,
    /// Target dialect
    pub to_dialect: SqlDialect,
    /// Dry run mode
    pub dry_run: bool,
    /// Show progress
    pub progress: bool,
    /// Strict mode (fail on any unsupported feature)
    pub strict: bool,
    /// Enum naming strategy for MySQL→PostgreSQL conversion
    pub enum_naming: EnumNamingStrategy,
}

impl Default for ConvertConfig {
    fn default() -> Self {
        Self {
            input: PathBuf::new(),
            output: None,
            from_dialect: None,
            to_dialect: SqlDialect::Postgres,
            dry_run: false,
            progress: false,
            strict: false,
            enum_naming: EnumNamingStrategy::default(),
        }
    }
}

/// Statistics from convert operation
#[derive(Debug, Default, serde::Serialize)]
pub struct ConvertStats {
    /// Total statements processed
    pub statements_processed: u64,
    /// Statements converted
    pub statements_converted: u64,
    /// Statements passed through unchanged
    pub statements_unchanged: u64,
    /// Statements skipped (unsupported)
    pub statements_skipped: u64,
    /// Warnings generated
    pub warnings: Vec<ConvertWarning>,
}

/// Main converter that dispatches to specific dialect converters
pub struct Converter {
    from: SqlDialect,
    to: SqlDialect,
    warnings: WarningCollector,
    strict: bool,
    /// Pending COPY header for data block processing
    pending_copy_header: Option<CopyHeader>,
    /// Converted `CREATE TABLE` text, keyed by [`table_key`]. Only retained when
    /// the target needs to re-declare a column later (MySQL's `MODIFY COLUMN`
    /// takes a full definition), so this holds DDL for the schema and never row
    /// data.
    created_tables: HashMap<String, String>,
    /// Statements that only become valid once the rest of the dump has been
    /// applied; drained by [`Converter::take_deferred_statements`].
    deferred: Vec<String>,
    /// Enum type registry for PG↔MySQL enum conversion
    enum_registry: EnumRegistry,
    /// Pending CREATE TYPE statements to prepend before the next DDL statement
    pending_enum_types: Vec<String>,
    /// Cached PG→MySQL inline map; rebuilt when registry generation changes.
    inline_map_cache: Option<(u64, HashMap<String, String>)>,
    /// Cached known and ambiguous PostgreSQL enum names.
    enum_name_cache: Option<(u64, PgEnumNameIndex)>,
    /// Enum types already materialized into a target table definition.
    used_pg_enum_types: HashSet<String>,
    /// User-defined PostgreSQL type references seen before their definitions.
    unresolved_pg_type_refs: HashSet<String>,
}

/// Returns the cached inline map, rebuilding it if registry mutated.
fn get_enum_inline_map<'a>(
    cache: &'a mut Option<(u64, HashMap<String, String>)>,
    registry: &EnumRegistry,
) -> &'a HashMap<String, String> {
    let gen = registry.generation();
    let stale = cache.as_ref().is_none_or(|(c, _)| *c != gen);
    if stale {
        *cache = Some((gen, pg_enum_inline_map(registry)));
    }
    &cache
        .get_or_insert_with(|| (gen, pg_enum_inline_map(registry)))
        .1
}

/// Build a name-to-inline-ENUM-text map from the registry.
fn pg_enum_inline_map(registry: &EnumRegistry) -> HashMap<String, String> {
    let mut inline = HashMap::new();
    let mut unqualified = HashMap::<String, Option<String>>::new();
    for (name, labels) in registry.pg_enum_entries() {
        let text = if labels.is_empty() {
            "VARCHAR(255)".to_string()
        } else {
            let q: Vec<String> = labels
                .iter()
                .map(|l| format!("'{}'", l.replace('\'', "''")))
                .collect();
            format!("ENUM({})", q.join(","))
        };
        inline.insert(name.clone(), text.clone());
        if let Some(unq) = enum_parser::pg_type_unqualified_key(name) {
            unqualified
                .entry(unq)
                .and_modify(|entry| *entry = None)
                .or_insert(Some(text));
        }
    }
    for (name, text) in unqualified {
        if let Some(text) = text {
            inline.entry(name).or_insert(text);
        }
    }
    inline
}

struct PgEnumNameIndex {
    known: HashSet<String>,
    ambiguous: HashSet<String>,
}

fn pg_enum_name_index(registry: &EnumRegistry) -> PgEnumNameIndex {
    let mut known = HashSet::new();
    let mut unqualified = HashMap::<String, bool>::new();
    let mut ambiguous = HashSet::new();
    for (name, _) in registry.pg_enum_entries() {
        known.insert(name.clone());
        if let Some(unqualified_name) = enum_parser::pg_type_unqualified_key(name) {
            unqualified
                .entry(unqualified_name)
                .and_modify(|unique| *unique = false)
                .or_insert(true);
        }
    }
    for (name, unique) in unqualified {
        if unique {
            known.insert(name);
        } else {
            ambiguous.insert(name);
        }
    }
    PgEnumNameIndex { known, ambiguous }
}

fn get_enum_name_index<'a>(
    cache: &'a mut Option<(u64, PgEnumNameIndex)>,
    registry: &EnumRegistry,
) -> &'a PgEnumNameIndex {
    let generation = registry.generation();
    if cache
        .as_ref()
        .is_none_or(|(cached_generation, _)| *cached_generation != generation)
    {
        *cache = Some((generation, pg_enum_name_index(registry)));
    }
    &cache
        .get_or_insert_with(|| (generation, pg_enum_name_index(registry)))
        .1
}

/// Normalise a table name for lookup: unquoted, unqualified, lowercased, so
fn table_key(name: &str) -> String {
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']')
        .to_lowercase()
}

fn array_type_end(stmt: &str, type_end: usize) -> usize {
    let bytes = stmt.as_bytes();
    let mut end = type_end;
    let mut consumed_dimension = false;
    loop {
        let whitespace_start = end;
        while bytes.get(end).is_some_and(u8::is_ascii_whitespace) {
            end += 1;
        }
        if bytes.get(end) != Some(&b'[') {
            if !consumed_dimension {
                return type_end;
            }
            return whitespace_start;
        }
        let Some(close_offset) = bytes[end + 1..].iter().position(|byte| *byte == b']') else {
            return if consumed_dimension {
                whitespace_start
            } else {
                type_end
            };
        };
        end += close_offset + 2;
        consumed_dimension = true;
    }
}

fn rewrite_mysql_modify_to_postgres(stmt: &str) -> String {
    use once_cell::sync::Lazy;
    use regex::Regex;

    static RE_MODIFY: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?is)^(?P<head>\s*ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?)(?:ONLY\s+)?(?P<table>(?:`(?:``|[^`])*`|"(?:""|[^"])*"|\[[^]]+\]|[^\s.]+)(?:\s*\.\s*(?:`(?:``|[^`])*`|"(?:""|[^"])*"|\[[^]]+\]|[^\s.]+))*)\s+MODIFY(?:\s+COLUMN)?\s+(?P<column>`(?:``|[^`])*`|"(?:""|[^"])*"|\[[^]]+\]|\S+)\s+(?P<rest>.*)$"#)
        .unwrap()
    });
    RE_MODIFY
        .replace(stmt, "${head}${table} ALTER COLUMN ${column} TYPE ${rest}")
        .into_owned()
}

fn rewrite_postgres_alter_type_to_mysql(stmt: &str) -> String {
    use once_cell::sync::Lazy;
    use regex::Regex;

    static RE_ALTER_TYPE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(?is)^(?P<head>\s*ALTER\s+TABLE\s+)(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(?P<table>(?:`(?:``|[^`])*`|"(?:""|[^"])*"|\[[^]]+\]|[^\s.]+)(?:\s*\.\s*(?:`(?:``|[^`])*`|"(?:""|[^"])*"|\[[^]]+\]|[^\s.]+))*)\s+ALTER(?:\s+COLUMN)?\s+(?P<column>`(?:``|[^`])*`|"(?:""|[^"])*"|\[[^]]+\]|\S+)\s+(?:SET\s+DATA\s+)?TYPE\s+(?P<rest>.*)$"#)
        .unwrap()
    });
    RE_ALTER_TYPE
        .replace(stmt, "${head}${table} MODIFY COLUMN ${column} ${rest}")
        .into_owned()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AlterAction {
    MySqlModify,
    MySqlModifyIfExists,
    MySqlChange,
    PostgresAlterType,
    Other,
}

fn alter_action(stmt: &str, dialect: SqlDialect) -> AlterAction {
    let tokens = ddl::tokenize(stmt, ddl::LexRules::for_dialect(dialect));
    let tokens: Vec<_> = tokens
        .iter()
        .filter(|token| !matches!(token, ddl::Token::Whitespace(..) | ddl::Token::Comment(..)))
        .collect();
    if !tokens
        .first()
        .is_some_and(|token| token.ident_eq(stmt, "ALTER"))
        || !tokens
            .get(1)
            .is_some_and(|token| token.ident_eq(stmt, "TABLE"))
    {
        return AlterAction::Other;
    }
    let mut index = 2;
    if tokens
        .get(index)
        .is_some_and(|token| token.ident_eq(stmt, "IF"))
        && tokens
            .get(index + 1)
            .is_some_and(|token| token.ident_eq(stmt, "EXISTS"))
    {
        index += 2;
    }
    if tokens
        .get(index)
        .is_some_and(|token| token.ident_eq(stmt, "ONLY"))
    {
        index += 1;
    }
    if tokens
        .get(index)
        .and_then(|token| token.owned_ident_text(stmt))
        .is_none()
    {
        return AlterAction::Other;
    }
    index += 1;
    while tokens.get(index).is_some_and(|token| token.is_punct(b'.'))
        && tokens
            .get(index + 1)
            .and_then(|token| token.owned_ident_text(stmt))
            .is_some()
    {
        index += 2;
    }
    if tokens
        .get(index)
        .is_some_and(|token| token.ident_eq(stmt, "MODIFY"))
    {
        index += 1;
        if tokens
            .get(index)
            .is_some_and(|token| token.ident_eq(stmt, "COLUMN"))
        {
            index += 1;
        }
        if tokens
            .get(index)
            .is_some_and(|token| token.ident_eq(stmt, "IF"))
            && tokens
                .get(index + 1)
                .is_some_and(|token| token.ident_eq(stmt, "EXISTS"))
        {
            return AlterAction::MySqlModifyIfExists;
        }
        return AlterAction::MySqlModify;
    }
    if tokens
        .get(index)
        .is_some_and(|token| token.ident_eq(stmt, "CHANGE"))
    {
        return AlterAction::MySqlChange;
    }
    if !tokens
        .get(index)
        .is_some_and(|token| token.ident_eq(stmt, "ALTER"))
    {
        return AlterAction::Other;
    }
    index += 1;
    if tokens
        .get(index)
        .is_some_and(|token| token.ident_eq(stmt, "COLUMN"))
    {
        index += 1;
    }
    if tokens
        .get(index)
        .and_then(|token| token.owned_ident_text(stmt))
        .is_none()
    {
        return AlterAction::Other;
    }
    index += 1;
    if tokens
        .get(index)
        .is_some_and(|token| token.ident_eq(stmt, "SET"))
        && tokens
            .get(index + 1)
            .is_some_and(|token| token.ident_eq(stmt, "DATA"))
    {
        index += 2;
    }
    if tokens
        .get(index)
        .is_some_and(|token| token.ident_eq(stmt, "TYPE"))
    {
        AlterAction::PostgresAlterType
    } else {
        AlterAction::Other
    }
}

fn without_sql_comments(stmt: &str, dialect: SqlDialect) -> String {
    let tokens = ddl::tokenize(stmt, ddl::LexRules::for_dialect(dialect));
    let mut result = String::with_capacity(stmt.len());
    for token in tokens {
        if matches!(token, ddl::Token::Comment(..)) {
            result.push(' ');
        } else {
            result.push_str(&stmt[token.start()..token.end()]);
        }
    }
    result
}

fn has_top_level_comma(stmt: &str, dialect: SqlDialect) -> bool {
    let tokens = ddl::tokenize(stmt, ddl::LexRules::for_dialect(dialect));
    let mut depth = 0_u32;
    for token in tokens {
        if token.is_punct(b'(') {
            depth += 1;
        } else if token.is_punct(b')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && token.is_punct(b',') {
            return true;
        }
    }
    false
}

fn strip_alter_type_using(stmt: &str) -> String {
    let rules = ddl::LexRules {
        backtick_ident: true,
        ..ddl::LexRules::for_dialect(SqlDialect::Postgres)
    };
    let tokens = ddl::tokenize(stmt, rules);
    let Some(using) = tokens.iter().find(|token| token.ident_eq(stmt, "USING")) else {
        return stmt.to_string();
    };
    let mut result = stmt[..using.start()].trim_end().to_string();
    if stmt.trim_end().ends_with(';') {
        result.push(';');
    }
    result
}

fn strip_create_unlogged(stmt: &str) -> String {
    use once_cell::sync::Lazy;
    use regex::Regex;

    static RE_UNLOGGED: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)\bCREATE\s+UNLOGGED\s+TABLE\b").unwrap());
    RE_UNLOGGED.replace(stmt, "CREATE TABLE").into_owned()
}

/// The type `column` was declared with in a converted `CREATE TABLE`, e.g.
/// `"integer"` for `  id integer NOT NULL,`.
///
/// Anchored on the column-list delimiter (`(` or `,`) rather than start-of-line,
/// so it reads a definition whether the DDL is formatted one column per line or
/// collapsed onto one. Captures a single word plus an optional length: callers
/// re-declare integer columns, and a greedier pattern would swallow the
/// trailing `NOT NULL`. A key clause such as `PRIMARY KEY (id)` cannot match,
/// since a type has to follow the name.
fn declared_column_type(create_table: &str, column: &str) -> Option<String> {
    use regex::Regex;

    let pattern = format!(
        r#"(?i)[(,]\s*[`"\[]?{}[`"\]]?\s+(?P<type>[A-Za-z]+(?:\s*\(\s*\d+\s*\))?)"#,
        regex::escape(column)
    );
    let captures = Regex::new(&pattern).ok()?.captures(create_table)?;
    Some(captures.name("type")?.as_str().trim().to_string())
}

/// Whether converting between these two dialects preserves enum semantics.
pub(crate) fn enum_aware_pair(from: SqlDialect, to: SqlDialect) -> bool {
    matches!(
        (from, to),
        (SqlDialect::Postgres, SqlDialect::MySql) | (SqlDialect::MySql, SqlDialect::Postgres)
    )
}

impl Converter {
    pub fn new(from: SqlDialect, to: SqlDialect) -> Self {
        Self {
            from,
            to,
            warnings: WarningCollector::new(),
            strict: false,
            pending_copy_header: None,
            created_tables: HashMap::new(),
            deferred: Vec::new(),
            enum_registry: EnumRegistry::new(),
            pending_enum_types: Vec::new(),
            inline_map_cache: None,
            enum_name_cache: None,
            used_pg_enum_types: HashSet::new(),
            unresolved_pg_type_refs: HashSet::new(),
        }
    }

    /// Statements held back until the whole dump has been applied, in the order
    /// they were queued. Callers must append these to the output after the last
    /// converted statement; draining leaves the converter reusable.
    pub fn take_deferred_statements(&mut self) -> Vec<String> {
        std::mem::take(&mut self.deferred)
    }

    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    pub fn with_enum_naming(mut self, naming: EnumNamingStrategy) -> Self {
        self.enum_registry = EnumRegistry::with_naming(naming);
        self
    }

    /// Rewrite enum types in a CREATE TABLE / ALTER TABLE statement using the
    /// enum registry. For PG→MySQL: replaces PG type references with inline
    /// `ENUM(...)` definitions. For MySQL→PG: extracts inline ENUM definitions,
    /// registers them, replaces them with generated PG type names, and prepends
    fn prepend_pending_enum_types(&mut self, result: &mut String) {
        if !self.pending_enum_types.is_empty() {
            let types = std::mem::take(&mut self.pending_enum_types);
            let mut prefix = types.join("\n");
            prefix.push_str("\n\n");
            result.insert_str(0, &prefix);
        }
    }

    /// Narrow registered PG enum type refs to a string type for SQLite/MSSQL.
    fn narrow_pg_enum_columns(&mut self, stmt: &str, table_name: Option<&str>) -> String {
        let names = get_enum_name_index(&mut self.enum_name_cache, &self.enum_registry);
        if names.known.is_empty() {
            return stmt.to_string();
        }
        let refs = ddl::table_ddl_type_refs(stmt, SqlDialect::Postgres);
        if refs.is_empty() {
            return stmt.to_string();
        }
        let replacement = match self.to {
            SqlDialect::Mssql => "NVARCHAR(255)",
            _ => "TEXT",
        };
        let mut result = stmt.to_string();
        for r in refs.iter().rev() {
            if r.is_array {
                let raw_type = &stmt[r.type_span.0..r.type_span.1];
                let keys = enum_parser::pg_type_keys(raw_type);
                if keys.as_ref().is_some_and(|(key, unqualified)| {
                    names.known.contains(key) || names.known.contains(unqualified)
                }) {
                    let end = array_type_end(stmt, r.type_span.1);
                    result.replace_range(r.type_span.0..end, replacement);
                    self.warnings.add(ConvertWarning::LossyConversion {
                        from_type: format!("{}[]", r.type_name.1),
                        to_type: replacement.to_string(),
                        table: table_name.map(str::to_string),
                        column: Some(r.column.clone()),
                    });
                }
                continue;
            }
            let raw_type = &stmt[r.type_span.0..r.type_span.1];
            let keys = enum_parser::pg_type_keys(raw_type);
            if keys.as_ref().is_some_and(|(key, unqualified)| {
                names.known.contains(key) || names.known.contains(unqualified)
            }) {
                result.replace_range(r.type_span.0..r.type_span.1, replacement);
                self.warnings.add(ConvertWarning::LossyConversion {
                    from_type: format!("ENUM ({})", r.type_name.1),
                    to_type: replacement.to_string(),
                    table: table_name.map(|t| t.to_string()),
                    column: Some(r.column.clone()),
                });
            }
        }
        result
    }

    /// Replace registered PG enum type references with inline `ENUM(...)`.
    fn rewrite_pg_enum_types_to_mysql_inline(
        &mut self,
        stmt: &str,
    ) -> Result<String, ConvertWarning> {
        let rules = ddl::LexRules {
            backtick_ident: true,
            ..ddl::LexRules::for_dialect(SqlDialect::Postgres)
        };
        let refs = ddl::table_ddl_type_refs_with_rules(stmt, rules);
        if refs.is_empty() {
            return Ok(stmt.to_string());
        }
        let inline = get_enum_inline_map(&mut self.inline_map_cache, &self.enum_registry);
        let names = get_enum_name_index(&mut self.enum_name_cache, &self.enum_registry);
        let mut result = stmt.to_string();
        for r in refs.iter().rev() {
            let raw_type = &stmt[r.type_span.0..r.type_span.1];
            let keys = enum_parser::pg_type_keys(raw_type);
            if r.is_array {
                let matched = keys.as_ref().is_some_and(|(key, unqualified)| {
                    inline.contains_key(key) || inline.contains_key(unqualified)
                });
                if matched {
                    let end = array_type_end(stmt, r.type_span.1);
                    result.replace_range(r.type_span.0..end, "JSON");
                    self.warnings.add(ConvertWarning::LossyConversion {
                        from_type: format!("{}[]", r.type_name.1),
                        to_type: "JSON".to_string(),
                        table: None,
                        column: Some(r.column.clone()),
                    });
                } else if let Some((key, _)) = keys {
                    self.unresolved_pg_type_refs.insert(key);
                }
                continue;
            }
            if let Some(text) = keys
                .as_ref()
                .and_then(|(key, unqualified)| inline.get(key).or_else(|| inline.get(unqualified)))
            {
                result.replace_range(r.type_span.0..r.type_span.1, text);
                if let Some((key, _)) = keys {
                    self.used_pg_enum_types.insert(key);
                }
            } else if keys
                .as_ref()
                .is_some_and(|(_, unqualified)| names.ambiguous.contains(unqualified))
            {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: format!(
                        "ambiguous unqualified PostgreSQL enum type `{}`",
                        r.type_name.1
                    ),
                    suggestion: Some(
                        "Schema-qualify the type reference before conversion".to_string(),
                    ),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            } else if let Some((key, _)) = keys {
                self.unresolved_pg_type_refs.insert(key);
            }
        }
        Ok(result)
    }

    /// Extract inline ENUM definitions from a MySQL CREATE TABLE / ALTER TABLE,
    /// register them, replace with generated PG type names, and emit CREATE TYPE.
    ///
    /// `column_type_change`: true when this rewrite happens inside a MySQL
    /// `MODIFY COLUMN` (an *existing* column's type is changing), as opposed
    /// to `CREATE TABLE`/`ADD COLUMN` (no prior data to reconcile). PostgreSQL
    /// has no implicit cast between two distinct enum types -- and even the
    /// same-named type can gain labels between definitions -- so an
    /// `ALTER COLUMN ... TYPE` onto an enum must always cast through text or
    /// Postgres rejects the statement outright.
    fn rewrite_mysql_inline_enums_to_pg_types(
        &mut self,
        stmt: &str,
        table_name: Option<&str>,
        column_type_change: bool,
    ) -> Result<Option<String>, ConvertWarning> {
        let cols = ddl::mysql_inline_enum_columns(stmt);
        if cols.is_empty() {
            return Ok(Some(stmt.to_string()));
        }
        if cols
            .iter()
            .flat_map(|column| &column.labels)
            .any(|label| label.contains('\0'))
        {
            let warning = ConvertWarning::SkippedStatement {
                reason: "MySQL ENUM contains a NUL label that PostgreSQL text cannot represent"
                    .to_string(),
                statement_preview: stmt.trim().chars().take(60).collect(),
            };
            self.warnings.add(warning.clone());
            if self.strict {
                return Err(warning);
            }
            return Ok(None);
        }
        let table = table_name.unwrap_or("unknown_table");
        let mut result = stmt.to_string();
        let mut creates = Vec::new();
        let mut alters = Vec::new();
        let mut replacements: Vec<((usize, usize), String)> = Vec::new();
        for col in &cols {
            let pg_type_name = self.enum_registry.get_or_create_pg_type_for_signature(
                table,
                &col.column,
                &col.labels,
            );
            let q = pg_quote_ident(&pg_type_name);
            let replacement = if column_type_change {
                format!("{q} USING {}::text::{q}", pg_quote_ident(&col.column))
            } else {
                q.clone()
            };
            replacements.push((col.span, replacement));
            match self.enum_registry.mark_emitted(&pg_type_name, &col.labels) {
                None => {
                    let ql: Vec<String> = col
                        .labels
                        .iter()
                        .map(|l| format!("'{}'", l.replace('\'', "''")))
                        .collect();
                    if pg_type_name.len() > 63 {
                        self.warnings.add(ConvertWarning::UnsupportedFeature {
                            feature: format!(
                                "PG type name `{pg_type_name}` exceeds 63-byte NAMEDATALEN limit"
                            ),
                            suggestion: Some("Shorten table/column name".to_string()),
                        });
                    }
                    creates.push(format!("CREATE TYPE {q} AS ENUM ({});", ql.join(", ")));
                }
                Some(existing) if existing != col.labels => {
                    for l in col.labels.iter().filter(|l| !existing.contains(l)) {
                        alters.push(format!(
                            "ALTER TYPE {q} ADD VALUE IF NOT EXISTS '{}';",
                            l.replace('\'', "''")
                        ));
                    }
                    if !(col.labels.len() >= existing.len()
                        && col.labels.starts_with(existing.as_slice()))
                    {
                        self.warnings.add(ConvertWarning::UnsupportedFeature {
                            feature: format!(
                                "enum labels of `{pg_type_name}` changed between definitions"
                            ),
                            suggestion: Some("PG cannot drop/reorder labels".to_string()),
                        });
                    }
                }
                Some(_) => {}
            }
        }
        for ((s, e), q) in replacements.iter().rev() {
            result.replace_range(*s..*e, q);
        }
        self.pending_enum_types.extend(creates);
        self.pending_enum_types.extend(alters);
        Ok(Some(result))
    }

    pub(crate) fn is_enum_aware(&self) -> bool {
        enum_aware_pair(self.from, self.to)
    }

    /// Check if we have a pending COPY header (waiting for data block)
    pub fn has_pending_copy(&self) -> bool {
        self.pending_copy_header.is_some()
    }

    /// Process a COPY data block using the pending header
    pub fn process_copy_data(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, ConvertWarning> {
        if let Some(header) = self.pending_copy_header.take() {
            if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
                // Convert COPY data to INSERT statements
                let inserts = copy_to_inserts(&header, data, self.to);
                return Ok(inserts);
            }
        }
        // Pass through if same dialect or no pending header
        Ok(vec![data.to_vec()])
    }

    /// Convert a single statement
    pub fn convert_statement(&mut self, stmt: &[u8]) -> Result<Vec<u8>, ConvertWarning> {
        let (stmt_type, table_name) =
            Parser::<&[u8]>::parse_statement_with_dialect(stmt, self.from);

        let table = if table_name.is_empty() {
            None
        } else {
            Some(table_name.as_str())
        };

        match stmt_type {
            StatementType::CreateTable => self.convert_create_table(stmt, table),
            StatementType::Insert => self.convert_insert(stmt, table),
            StatementType::CreateIndex => self.convert_create_index(stmt),
            StatementType::AlterTable => self.convert_alter_table(stmt, table),
            StatementType::DropTable => self.convert_drop_table(stmt, table),
            StatementType::Copy => self.convert_copy(stmt, table),
            StatementType::Unknown => self.convert_other(stmt),
        }
    }

    /// Convert CREATE TABLE statement
    fn convert_create_table(
        &mut self,
        stmt: &[u8],
        table_name: Option<&str>,
    ) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let mut result = stmt_str.to_string();

        // Detect unsupported features BEFORE conversion (so we see original types)
        self.detect_unsupported_features(&result, table_name)?;

        // Strip PG casts and schema prefixes BEFORE enum rewriting so that
        // DEFAULT 'x'::order_status is cleaned to DEFAULT 'x' before the
        // type name is replaced with inline ENUM(...).
        if self.is_enum_aware() && self.from == SqlDialect::Postgres {
            result = self.strip_postgres_casts(&result);
            result = self.strip_schema_prefix(&result);
        }
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = strip_create_unlogged(&result);
        }

        // MySQL→PG: register/replace before type mapper. PG→SQLite/MSSQL: narrow.
        if self.from == SqlDialect::MySql && self.to == SqlDialect::Postgres {
            // false: CREATE TABLE has no existing column data to cast.
            let Some(rewritten) =
                self.rewrite_mysql_inline_enums_to_pg_types(&result, table_name, false)?
            else {
                return Ok(Vec::new());
            };
            result = rewritten;
        } else if self.from == SqlDialect::Postgres
            && matches!(self.to, SqlDialect::Sqlite | SqlDialect::Mssql)
        {
            result = self.narrow_pg_enum_columns(&result, table_name);
        }

        // Convert MSSQL-specific syntax BEFORE identifier conversion
        // (so we can strip [dbo]. schema prefix properly)
        if self.from == SqlDialect::Mssql && self.to != SqlDialect::Mssql {
            result = self.strip_mssql_schema_prefix(&result);
            result = self.convert_mssql_getdate(&result);
            result = self.strip_mssql_on_filegroup(&result);
            result = self.strip_mssql_clustered(&result);
            result = self.convert_mssql_unicode_strings(&result);
        }

        // PG→MySQL: map ordinary types first, then replace enums while quoted
        // type identity is still available. Injecting labels after type mapping
        // prevents label text such as `UUID` from being rewritten as a type.
        if self.from == SqlDialect::Postgres && self.to == SqlDialect::MySql {
            result = self.convert_data_types(&result);
            result = self.rewrite_pg_enum_types_to_mysql_inline(&result)?;
            result = self.convert_identifiers(&result);
        } else {
            result = self.convert_identifiers(&result);
            result = self.convert_data_types(&result);
        }

        // Convert AUTO_INCREMENT
        result = self.convert_auto_increment(&result, table_name);

        // Convert PostgreSQL-specific syntax
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = self.strip_postgres_casts(&result);
            result = self.convert_nextval(&result);
            result = self.convert_default_now(&result);
            result = self.strip_schema_prefix(&result);
        }

        // Convert string escapes
        result = self.convert_string_escapes(&result);

        // Strip MySQL conditional comments
        result = self.strip_conditional_comments(&result);

        // Convert ENGINE clause
        result = self.strip_engine_clause(&result);

        // Convert CHARSET/COLLATE
        result = self.strip_charset_clauses(&result);

        // Strip MySQL inline column/table COMMENT annotations
        result = self.strip_mysql_comment_clauses(&result);

        // Convert UNIQUE KEY ... USING BTREE table constraints
        result = self.convert_unique_key_constraint(&result);

        // MySQL is the only target that has to re-declare a column after the
        // fact (to restore AUTO_INCREMENT), and that needs the column's
        // converted type. Retained only for that target so no other conversion
        // pays for the bookkeeping.
        if self.to == SqlDialect::MySql {
            if let Some(name) = table_name {
                self.created_tables.insert(table_key(name), result.clone());
            }
        }

        // Prepend any pending CREATE TYPE statements (MySQL→PG enum conversion)
        self.prepend_pending_enum_types(&mut result);

        Ok(result.into_bytes())
    }

    /// Convert INSERT statement
    fn convert_insert(
        &mut self,
        stmt: &[u8],
        _table_name: Option<&str>,
    ) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let mut result = stmt_str.to_string();

        // Convert MSSQL-specific syntax BEFORE identifier conversion
        if self.from == SqlDialect::Mssql && self.to != SqlDialect::Mssql {
            result = self.strip_mssql_schema_prefix(&result);
            result = self.convert_mssql_unicode_strings(&result);
        }

        // Convert identifier quoting
        result = self.convert_identifiers(&result);

        // Convert PostgreSQL-specific syntax
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = self.strip_postgres_casts(&result);
            result = self.strip_schema_prefix(&result);
        }

        // Convert string escapes (careful with data!)
        result = self.convert_string_escapes(&result);

        Ok(result.into_bytes())
    }

    /// Convert CREATE INDEX statement
    fn convert_create_index(&mut self, stmt: &[u8]) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let mut result = stmt_str.to_string();

        // Convert MSSQL-specific syntax BEFORE identifier conversion
        if self.from == SqlDialect::Mssql && self.to != SqlDialect::Mssql {
            result = self.strip_mssql_schema_prefix(&result);
            result = self.strip_mssql_clustered(&result);
        }

        // Convert identifier quoting
        result = self.convert_identifiers(&result);

        // Convert PostgreSQL-specific syntax
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = self.strip_postgres_casts(&result);
            result = self.strip_schema_prefix(&result);
        }

        // Detect FULLTEXT/SPATIAL
        if result.contains("FULLTEXT") || result.contains("fulltext") {
            self.warnings.add(ConvertWarning::UnsupportedFeature {
                feature: "FULLTEXT INDEX".to_string(),
                suggestion: Some("Use PostgreSQL GIN index or skip".to_string()),
            });
            if self.strict {
                return Err(ConvertWarning::UnsupportedFeature {
                    feature: "FULLTEXT INDEX".to_string(),
                    suggestion: None,
                });
            }
        }

        Ok(result.into_bytes())
    }

    /// Convert ALTER TABLE statement
    fn convert_alter_table(
        &mut self,
        stmt: &[u8],
        table_name: Option<&str>,
    ) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let mut result = stmt_str.to_string();
        let comment_free = without_sql_comments(&result, self.from);
        let action = alter_action(&result, self.from);
        let mysql_modify = action == AlterAction::MySqlModify;
        let mysql_modify_if_exists = action == AlterAction::MySqlModifyIfExists;
        let mysql_change = action == AlterAction::MySqlChange;
        let postgres_alter_type = action == AlterAction::PostgresAlterType;
        if mysql_modify || mysql_modify_if_exists || mysql_change || postgres_alter_type {
            result = comment_free;
        }
        let mysql_enum_columns =
            if self.from == SqlDialect::MySql && self.to == SqlDialect::Postgres {
                ddl::mysql_inline_enum_columns(&result)
            } else {
                Vec::new()
            };

        if self.from == SqlDialect::MySql
            && self.to == SqlDialect::Postgres
            && mysql_modify_if_exists
            && !mysql_enum_columns.is_empty()
        {
            let warning = ConvertWarning::SkippedStatement {
                reason: "MariaDB MODIFY COLUMN IF EXISTS has no direct PostgreSQL equivalent"
                    .to_string(),
                statement_preview: result.trim().chars().take(60).collect(),
            };
            self.warnings.add(warning.clone());
            if self.strict {
                return Err(warning);
            }
            return Ok(Vec::new());
        }

        if self.from == SqlDialect::MySql
            && self.to == SqlDialect::Postgres
            && mysql_change
            && !mysql_enum_columns.is_empty()
        {
            let warning = ConvertWarning::SkippedStatement {
                reason:
                    "MySQL CHANGE COLUMN with ENUM requires separate rename and type operations"
                        .to_string(),
                statement_preview: result.trim().chars().take(60).collect(),
            };
            self.warnings.add(warning.clone());
            if self.strict {
                return Err(warning);
            }
            return Ok(Vec::new());
        }

        if self.from == SqlDialect::MySql
            && self.to == SqlDialect::Postgres
            && mysql_modify
            && mysql_enum_columns.iter().any(|column| {
                !result[column.span.1..]
                    .trim()
                    .trim_end_matches(';')
                    .trim()
                    .is_empty()
            })
        {
            let warning = ConvertWarning::UnsupportedFeature {
                feature: "MySQL MODIFY COLUMN with ENUM attributes or multiple ALTER actions"
                    .to_string(),
                suggestion: Some(
                    "Split the ALTER actions and apply column attributes separately".to_string(),
                ),
            };
            self.warnings.add(warning.clone());
            if self.strict {
                return Err(warning);
            }
            return Ok(Vec::new());
        }

        if self.from == SqlDialect::Postgres && self.to == SqlDialect::MySql && postgres_alter_type
        {
            let warning = ConvertWarning::UnsupportedFeature {
                feature: "PostgreSQL ALTER COLUMN TYPE converted to MySQL MODIFY COLUMN"
                    .to_string(),
                suggestion: Some(
                    "Verify NULL/default/comment attributes because MySQL MODIFY COLUMN requires the complete definition"
                        .to_string(),
                ),
            };
            self.warnings.add(warning.clone());
            if self.strict {
                return Err(warning);
            }
            if has_top_level_comma(&result, SqlDialect::Postgres) {
                let warning = ConvertWarning::SkippedStatement {
                    reason: "PostgreSQL multi-action ALTER TABLE must be split before conversion"
                        .to_string(),
                    statement_preview: result.trim().chars().take(60).collect(),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
                return Ok(Vec::new());
            }
        }

        // pg_dump expands `serial` into a plain integer column plus a separate
        // `ALTER COLUMN ... SET DEFAULT nextval(...)`. Removing just the
        // `DEFAULT nextval(...)` would leave a dangling `ALTER COLUMN id SET`,
        // so the statement never survives as-is. MySQL can restore the
        // auto-increment separately; every other target loses it.
        if self.from == SqlDialect::Postgres
            && self.to != SqlDialect::Postgres
            && self.is_sequence_default_alter(&result)
        {
            if let Some(restored) = self.defer_mysql_auto_increment(&result) {
                self.deferred.push(restored);
                return Ok(Vec::new());
            }
            let preview = self.strip_leading_sql_comments(&result);
            self.warnings.add(ConvertWarning::SkippedStatement {
                reason: "sequence-backed column default has no equivalent".to_string(),
                statement_preview: preview.trim().chars().take(60).collect(),
            });
            return Ok(Vec::new());
        }

        // Strip PG casts and schema prefixes BEFORE enum rewriting for PG→MySQL
        if self.is_enum_aware() && self.from == SqlDialect::Postgres {
            result = self.strip_postgres_casts(&result);
            result = self.strip_schema_prefix(&result);
        }

        // MySQL→PG: register/replace before type mapper. PG→SQLite/MSSQL: narrow.
        if self.from == SqlDialect::MySql && self.to == SqlDialect::Postgres {
            // mysql_modify: MODIFY COLUMN changes an *existing* column's
            // type, so it needs a USING cast; ADD COLUMN doesn't.
            let Some(rewritten) =
                self.rewrite_mysql_inline_enums_to_pg_types(&result, table_name, mysql_modify)?
            else {
                return Ok(Vec::new());
            };
            result = rewritten;
        } else if self.from == SqlDialect::Postgres
            && matches!(self.to, SqlDialect::Sqlite | SqlDialect::Mssql)
        {
            result = self.narrow_pg_enum_columns(&result, table_name);
        }

        if self.from == SqlDialect::Postgres && self.to == SqlDialect::MySql {
            result = self.convert_data_types(&result);
            result = self.rewrite_pg_enum_types_to_mysql_inline(&result)?;
            result = self.convert_identifiers(&result);
            result = rewrite_postgres_alter_type_to_mysql(&result);
            if postgres_alter_type {
                result = strip_alter_type_using(&result);
            }
        } else {
            result = self.convert_identifiers(&result);
            result = self.convert_data_types(&result);
        }

        if self.from == SqlDialect::MySql && self.to == SqlDialect::Postgres && mysql_modify {
            result = rewrite_mysql_modify_to_postgres(&result);
        }

        // Convert PostgreSQL-specific syntax
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = self.strip_postgres_casts(&result);
            result = self.convert_nextval(&result);
            result = self.convert_default_now(&result);
            result = self.strip_schema_prefix(&result);
            result = self.strip_postgres_only(&result);
        }

        // SQLite's ALTER TABLE understands only RENAME / ADD COLUMN /
        // DROP COLUMN, so a constraint attached after the table exists has to be
        // re-expressed as an index or dropped. Runs last: the rewrite reads the
        // table name out of the statement, which the steps above normalise.
        if self.to == SqlDialect::Sqlite {
            if let Some(rewritten) = self.rewrite_add_constraint_for_sqlite(&result) {
                return Ok(rewritten.into_bytes());
            }
        }

        self.prepend_pending_enum_types(&mut result);

        Ok(result.into_bytes())
    }
    /// such statement.
    ///
    /// `UNIQUE` and `PRIMARY KEY` become a unique index — SQLite enforces the
    /// same guarantee, though a re-expressed primary key is no longer a rowid
    /// alias and, unlike a real primary key, does not itself imply NOT NULL.
    /// A foreign key or check constraint cannot be attached to an existing
    /// SQLite table by any syntax, so it is dropped with a warning.
    ///
    /// Returns `None` for the ALTER forms SQLite already accepts (`ADD COLUMN`,
    /// `RENAME`, `DROP COLUMN`), which pass through untouched.
    fn rewrite_add_constraint_for_sqlite(&mut self, stmt: &str) -> Option<String> {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE_ADD_CONSTRAINT: Lazy<Regex> = Lazy::new(|| {
            Regex::new(
                r"(?is)^\s*ALTER\s+TABLE\s+(?P<table>\S+)\s+ADD\s+CONSTRAINT\s+(?P<name>[^\s(]+)\s+(?P<kind>UNIQUE|PRIMARY\s+KEY|FOREIGN\s+KEY|CHECK)\b(?P<rest>.*)$",
            )
            .unwrap()
        });
        static RE_LEADING_COLUMNS: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?s)^\s*\(([^)]*)\)").unwrap());

        // pg_dump prefixes each statement with a comment banner; the constraint
        // itself is what has to be matched.
        let body = self.strip_leading_sql_comments(stmt);
        let caps = RE_ADD_CONSTRAINT.captures(&body)?;
        let kind = caps["kind"]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let kind = kind.to_uppercase();

        if kind == "UNIQUE" || kind == "PRIMARY KEY" {
            if let Some(columns) = RE_LEADING_COLUMNS.captures(&caps["rest"]) {
                if kind == "PRIMARY KEY" {
                    self.warnings.add(ConvertWarning::UnsupportedFeature {
                        feature: format!("PRIMARY KEY added to existing table {}", &caps["table"]),
                        suggestion: Some(
                            "re-expressed as a UNIQUE INDEX; SQLite only accepts a \
                             primary key inside CREATE TABLE"
                                .to_string(),
                        ),
                    });
                }
                return Some(format!(
                    "CREATE UNIQUE INDEX {} ON {} ({});",
                    &caps["name"],
                    &caps["table"],
                    columns[1].trim()
                ));
            }
        }

        self.warnings.add(ConvertWarning::SkippedStatement {
            reason: format!("SQLite cannot add a {kind} constraint to an existing table"),
            statement_preview: body.trim().chars().take(60).collect(),
        });
        Some(String::new())
    }

    /// Convert DROP TABLE statement
    fn convert_drop_table(
        &mut self,
        stmt: &[u8],
        table_name: Option<&str>,
    ) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let mut result = stmt_str.to_string();

        // A dropped table's enum types must not be silently reused (and
        // widened) by a later, unrelated table created with the same name.
        if let Some(table) = table_name {
            self.enum_registry.forget_table(table);
        }

        result = self.convert_identifiers(&result);

        // Strip PostgreSQL schema prefix
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = self.strip_schema_prefix(&result);
        }

        Ok(result.into_bytes())
    }

    /// Convert COPY statement (PostgreSQL-specific)
    ///
    /// This handles the COPY header. The data block is processed separately
    /// via process_copy_data() when called from the run() function.
    fn convert_copy(
        &mut self,
        stmt: &[u8],
        _table_name: Option<&str>,
    ) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);

        // Check if this contains "FROM stdin" (COPY header) or is data
        let upper = stmt_str.to_uppercase();
        if upper.contains("FROM STDIN") {
            // This is a COPY header - parse it and store for later
            if let Some(header) = parse_copy_header(&stmt_str) {
                if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
                    // Store the header, will convert data block in process_copy_data
                    self.pending_copy_header = Some(header);
                    // Return empty - the actual INSERT will be generated from data
                    return Ok(Vec::new());
                }
            }
        }

        // If same dialect or couldn't parse, pass through
        Ok(stmt.to_vec())
    }

    /// Convert other statements (comments, session settings, etc.)
    fn convert_other(&mut self, stmt: &[u8]) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let result = stmt_str.to_string();
        let trimmed = result.trim();

        // Skip MySQL session commands when converting to other dialects
        if self.from == SqlDialect::MySql
            && self.to != SqlDialect::MySql
            && self.is_mysql_session_command(&result)
        {
            return Ok(Vec::new()); // Skip
        }

        // Skip PostgreSQL session commands and unsupported features when converting to other dialects
        if self.from == SqlDialect::Postgres
            && self.to != SqlDialect::Postgres
            && self.is_postgres_session_command(&result)
        {
            return Ok(Vec::new()); // Skip
        }
        // psql meta-commands are client directives, not SQL. pg_dump 16.6+ wraps
        // its output in `\restrict` / `\unrestrict`; fed to the mysql client,
        // `\u` is read as its "use database" shorthand and the import dies with
        // ERROR 1049.
        if self.from == SqlDialect::Postgres
            && self.to != SqlDialect::Postgres
            && self.is_psql_meta_command(trimmed)
        {
            return Ok(Vec::new()); // Skip
        }

        // Register enum types for every non-PostgreSQL target. MySQL inlines
        // them, while SQLite and MSSQL need the registry to narrow references.
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            if let Some(parsed) = enum_parser::pg_create_enum(trimmed) {
                let unqualified = enum_parser::pg_type_unqualified_key(&parsed.name);
                let used_before_definition = self.unresolved_pg_type_refs.remove(&parsed.name)
                    || unqualified
                        .as_ref()
                        .is_some_and(|name| self.unresolved_pg_type_refs.remove(name));
                if used_before_definition {
                    let warning = ConvertWarning::UnsupportedFeature {
                        feature: format!(
                            "PostgreSQL enum `{}` was defined after a table used it",
                            parsed.name
                        ),
                        suggestion: Some(
                            "Place CREATE TYPE before CREATE TABLE, as standard pg_dump does"
                                .to_string(),
                        ),
                    };
                    self.warnings.add(warning.clone());
                    if self.strict {
                        return Err(warning);
                    }
                }
                self.enum_registry
                    .register_pg_enum(&parsed.name, parsed.labels);
                return Ok(Vec::new()); // CREATE TYPE is not emitted for MySQL
            }
            if let Some(parsed) = enum_parser::pg_add_enum_value(trimmed) {
                // ALTER TYPE ADD VALUE: update registry so subsequent
                // CREATE TABLE columns get the full label list.
                let unqualified = enum_parser::pg_type_unqualified_key(&parsed.name);
                if self.used_pg_enum_types.contains(&parsed.name)
                    || unqualified
                        .as_ref()
                        .is_some_and(|name| self.used_pg_enum_types.contains(name))
                {
                    let warning = ConvertWarning::UnsupportedFeature {
                        feature: format!(
                            "ALTER TYPE ADD VALUE after `{}` was already used by a table",
                            parsed.name
                        ),
                        suggestion: Some(
                            "Move enum alterations before CREATE TABLE or update the MySQL columns manually"
                                .to_string(),
                        ),
                    };
                    self.warnings.add(warning.clone());
                    if self.strict {
                        return Err(warning);
                    }
                }
                let existing = self
                    .enum_registry
                    .get_pg_enum(&parsed.name)
                    .map(|labels| labels.to_vec());
                if let Some(mut labels) = existing {
                    if !labels.contains(&parsed.value) {
                        match parsed.position {
                            Some((before, true)) => {
                                if let Some(pos) = labels.iter().position(|l| *l == before) {
                                    labels.insert(pos + 1, parsed.value);
                                } else {
                                    labels.push(parsed.value);
                                }
                            }
                            Some((before, false)) => {
                                if let Some(pos) = labels.iter().position(|l| *l == before) {
                                    labels.insert(pos, parsed.value);
                                } else {
                                    labels.push(parsed.value);
                                }
                            }
                            None => labels.push(parsed.value),
                        }
                    }
                    self.enum_registry.register_pg_enum(&parsed.name, labels);
                } else {
                    let warning = ConvertWarning::UnsupportedFeature {
                        feature: format!("ALTER TYPE references unknown enum `{}`", parsed.name),
                        suggestion: Some(
                            "Place the enum CREATE TYPE before ALTER TYPE".to_string(),
                        ),
                    };
                    self.warnings.add(warning.clone());
                    if self.strict {
                        return Err(warning);
                    }
                }
                return Ok(Vec::new());
            }
            if let Some(rename) = enum_parser::pg_rename_enum(trimmed) {
                let updated = match rename {
                    enum_parser::ParsedRenameEnum::Type { old_name, new_name } => {
                        let updated = self.enum_registry.rename_pg_enum(&old_name, &new_name);
                        let old_unqualified = enum_parser::pg_type_unqualified_key(&old_name);
                        let was_used = self.used_pg_enum_types.remove(&old_name)
                            || old_unqualified
                                .as_ref()
                                .is_some_and(|name| self.used_pg_enum_types.remove(name));
                        if updated && was_used {
                            self.used_pg_enum_types.insert(new_name);
                        }
                        updated
                    }
                    enum_parser::ParsedRenameEnum::Value {
                        name,
                        old_value,
                        new_value,
                    } => {
                        let unqualified = enum_parser::pg_type_unqualified_key(&name);
                        if self.used_pg_enum_types.contains(&name)
                            || unqualified
                                .as_ref()
                                .is_some_and(|name| self.used_pg_enum_types.contains(name))
                        {
                            let warning = ConvertWarning::UnsupportedFeature {
                                feature: format!(
                                    "ALTER TYPE RENAME VALUE after `{name}` was used by a table"
                                ),
                                suggestion: Some(
                                    "Update the already-emitted MySQL column manually".to_string(),
                                ),
                            };
                            self.warnings.add(warning.clone());
                            if self.strict {
                                return Err(warning);
                            }
                        }
                        self.enum_registry
                            .rename_pg_enum_value(&name, &old_value, &new_value)
                    }
                };
                if !updated {
                    let warning = ConvertWarning::UnsupportedFeature {
                        feature: "ALTER TYPE RENAME references an unknown enum or value"
                            .to_string(),
                        suggestion: Some(
                            "Place the matching enum definition before ALTER TYPE".to_string(),
                        ),
                    };
                    self.warnings.add(warning.clone());
                    if self.strict {
                        return Err(warning);
                    }
                }
                return Ok(Vec::new());
            }
        }

        if self.from == SqlDialect::Postgres
            && self.to != SqlDialect::Postgres
            && self.is_postgres_only_feature(trimmed)
        {
            self.warnings.add(ConvertWarning::SkippedStatement {
                reason: "PostgreSQL-only feature".to_string(),
                statement_preview: trimmed.chars().take(60).collect(),
            });
            return Ok(Vec::new()); // Skip
        }

        // Skip SQLite pragmas when converting to other dialects
        if self.from == SqlDialect::Sqlite
            && self.to != SqlDialect::Sqlite
            && self.is_sqlite_pragma(&result)
        {
            return Ok(Vec::new()); // Skip
        }

        // Skip MSSQL session commands when converting to other dialects
        if self.from == SqlDialect::Mssql
            && self.to != SqlDialect::Mssql
            && self.is_mssql_session_command(&result)
        {
            return Ok(Vec::new()); // Skip
        }

        // Strip conditional comments
        if result.contains("/*!") {
            let stripped = self.strip_conditional_comments(&result);
            return Ok(stripped.into_bytes());
        }

        Ok(stmt.to_vec())
    }

    /// Check if statement is a MySQL session command
    fn is_mysql_session_command(&self, stmt: &str) -> bool {
        let upper = stmt.to_uppercase();
        upper.contains("SET NAMES")
            || upper.contains("SET CHARACTER")
            || upper.contains("SET SQL_MODE")
            || upper.contains("SET TIME_ZONE")
            || upper.contains("SET FOREIGN_KEY_CHECKS")
            || upper.contains("LOCK TABLES")
            || upper.contains("UNLOCK TABLES")
    }

    /// Check if statement is a PostgreSQL session command or unsupported statement
    fn is_postgres_session_command(&self, stmt: &str) -> bool {
        let upper = stmt.to_uppercase();
        // Session/transaction settings
        upper.contains("SET CLIENT_ENCODING")
            || upper.contains("SET STANDARD_CONFORMING_STRINGS")
            || upper.contains("SET CHECK_FUNCTION_BODIES")
            || upper.contains("SET SEARCH_PATH")
            || upper.contains("SET DEFAULT_TABLESPACE")
            || upper.contains("SET LOCK_TIMEOUT")
            || upper.contains("SET IDLE_IN_TRANSACTION_SESSION_TIMEOUT")
            || upper.contains("SET ROW_SECURITY")
            || upper.contains("SET STATEMENT_TIMEOUT")
            || upper.contains("SET XMLOPTION")
            || upper.contains("SET CLIENT_MIN_MESSAGES")
            || upper.contains("SET DEFAULT_TABLE_ACCESS_METHOD")
            || upper.contains("SELECT PG_CATALOG")
            // Ownership/permission statements
            || upper.contains("OWNER TO")
            || upper.contains("GRANT ")
            || upper.contains("REVOKE ")
    }

    /// Check if statement is a PostgreSQL-only feature that should be skipped
    fn is_postgres_only_feature(&self, stmt: &str) -> bool {
        // Strip leading comments to find the actual statement
        let stripped = self.strip_leading_sql_comments(stmt);
        let upper = stripped.to_uppercase();

        // These PostgreSQL features have no MySQL/SQLite equivalent
        upper.starts_with("CREATE DOMAIN")
            || upper.starts_with("CREATE TYPE")
            || upper.starts_with("CREATE FUNCTION")
            || upper.starts_with("CREATE PROCEDURE")
            || upper.starts_with("CREATE AGGREGATE")
            || upper.starts_with("CREATE OPERATOR")
            || upper.starts_with("CREATE SEQUENCE")
            || upper.starts_with("CREATE EXTENSION")
            || upper.starts_with("CREATE SCHEMA")
            || upper.starts_with("CREATE TRIGGER")
            || upper.starts_with("ALTER DOMAIN")
            || upper.starts_with("ALTER TYPE")
            || upper.starts_with("ALTER FUNCTION")
            || upper.starts_with("ALTER SEQUENCE")
            || upper.starts_with("ALTER SCHEMA")
            || upper.starts_with("COMMENT ON")
    }

    fn strip_leading_sql_comments(&self, stmt: &str) -> String {
        String::from_utf8_lossy(crate::parser::strip_leading_comments_and_whitespace(
            stmt.as_bytes(),
        ))
        .trim()
        .to_string()
    }

    /// Check if statement is a SQLite pragma
    fn is_sqlite_pragma(&self, stmt: &str) -> bool {
        let upper = stmt.to_uppercase();
        upper.contains("PRAGMA")
    }

    /// Check if statement is an MSSQL session command
    fn is_mssql_session_command(&self, stmt: &str) -> bool {
        let upper = stmt.to_uppercase();
        upper.contains("SET ANSI_NULLS")
            || upper.contains("SET QUOTED_IDENTIFIER")
            || upper.contains("SET NOCOUNT")
            || upper.contains("SET XACT_ABORT")
            || upper.contains("SET ARITHABORT")
            || upper.contains("SET ANSI_WARNINGS")
            || upper.contains("SET ANSI_PADDING")
            || upper.contains("SET CONCAT_NULL_YIELDS_NULL")
            || upper.contains("SET NUMERIC_ROUNDABORT")
            || upper.contains("SET IDENTITY_INSERT")
    }

    /// Convert identifier quoting based on dialects
    fn convert_identifiers(&self, stmt: &str) -> String {
        match (self.from, self.to) {
            (SqlDialect::MySql, SqlDialect::Postgres | SqlDialect::Sqlite) => {
                self.backticks_to_double_quotes(stmt)
            }
            (SqlDialect::MySql, SqlDialect::Mssql) => self.backticks_to_square_brackets(stmt),
            (SqlDialect::Postgres | SqlDialect::Sqlite, SqlDialect::MySql) => {
                self.double_quotes_to_backticks(stmt)
            }
            (SqlDialect::Postgres | SqlDialect::Sqlite, SqlDialect::Mssql) => {
                self.double_quotes_to_square_brackets(stmt)
            }
            (SqlDialect::Mssql, SqlDialect::MySql) => self.square_brackets_to_backticks(stmt),
            (SqlDialect::Mssql, SqlDialect::Postgres | SqlDialect::Sqlite) => {
                self.square_brackets_to_double_quotes(stmt)
            }
            _ => stmt.to_string(),
        }
    }

    /// Convert backticks to double quotes
    pub fn backticks_to_double_quotes(&self, stmt: &str) -> String {
        requote(stmt, '`', '"', '"')
    }

    /// Convert double quotes to backticks
    pub fn double_quotes_to_backticks(&self, stmt: &str) -> String {
        requote(stmt, '"', '`', '`')
    }

    /// Convert backticks to square brackets (for MSSQL)
    pub fn backticks_to_square_brackets(&self, stmt: &str) -> String {
        requote(stmt, '`', '[', ']')
    }

    /// Convert double quotes to square brackets (for MSSQL)
    pub fn double_quotes_to_square_brackets(&self, stmt: &str) -> String {
        requote(stmt, '"', '[', ']')
    }

    /// Convert square brackets to backticks (from MSSQL to MySQL)
    pub fn square_brackets_to_backticks(&self, stmt: &str) -> String {
        unbracket(stmt, '`')
    }

    /// Convert square brackets to double quotes (from MSSQL to PostgreSQL/SQLite)
    pub fn square_brackets_to_double_quotes(&self, stmt: &str) -> String {
        unbracket(stmt, '"')
    }

    /// Convert data types between dialects
    fn convert_data_types(&self, stmt: &str) -> String {
        TypeMapper::convert(stmt, self.from, self.to)
    }

    /// Convert AUTO_INCREMENT/SERIAL syntax
    fn convert_auto_increment(&self, stmt: &str, _table_name: Option<&str>) -> String {
        // Strip the MySQL table-level `AUTO_INCREMENT=N` option first (distinct from
        // the column-level `AUTO_INCREMENT` keyword handled below). Otherwise the
        // column-level replacements below strip the word "AUTO_INCREMENT" and leave
        // a dangling "=N" behind, e.g. `) ENGINE=InnoDB AUTO_INCREMENT=2 ...` becomes
        // `) =2 ...`. See https://github.com/HelgeSverre/sql-splitter/issues/64
        let stmt = if self.from == SqlDialect::MySql && self.to != SqlDialect::MySql {
            self.strip_mysql_auto_increment_table_option(stmt)
        } else {
            stmt.to_string()
        };
        let stmt = stmt.as_str();

        match (self.from, self.to) {
            (SqlDialect::MySql, SqlDialect::Postgres) => {
                // INT AUTO_INCREMENT → SERIAL, BIGINT AUTO_INCREMENT → BIGSERIAL,
                // case-insensitive (MySQL dumps commonly use lowercase
                // `auto_increment`) and string-literal-aware (a DEFAULT value or
                // CHECK expression containing the literal text "AUTO_INCREMENT"
                // must not be touched).
                self.convert_mysql_auto_increment_keyword(stmt)
            }
            (SqlDialect::MySql, SqlDialect::Sqlite) => {
                use once_cell::sync::Lazy;
                use regex::Regex;
                // `<int type> AUTO_INCREMENT` → INTEGER, then any bare keyword
                // dropped. Case-insensitive (MySQL dumps commonly lowercase
                // `auto_increment`) and only outside string literals so a
                // DEFAULT / value containing the text is left alone. Matching
                // the whole `…INT AUTO_INCREMENT` unit avoids the "BIGINTEGER"
                // substring hazard.
                static RE_SIZED_AI: Lazy<Regex> = Lazy::new(|| {
                    Regex::new(r"(?i)\b(?:BIG|MEDIUM|SMALL|TINY)?INT\s+AUTO_INCREMENT").unwrap()
                });
                // Trailing boundary only: the integer type may already have been
                // mapped and glued to the keyword (e.g. `INTEGERAUTO_INCREMENT`)
                // by convert_data_types running first.
                static RE_BARE_AI: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r"(?i)AUTO_INCREMENT\b").unwrap());
                map_outside_string_literals(stmt, |seg| {
                    let seg = RE_SIZED_AI.replace_all(seg, "INTEGER");
                    RE_BARE_AI.replace_all(&seg, "").into_owned()
                })
            }
            (SqlDialect::Postgres, SqlDialect::MySql) => {
                // SERIAL → INT AUTO_INCREMENT
                // BIGSERIAL → BIGINT AUTO_INCREMENT
                let result = stmt.replace("BIGSERIAL", "BIGINT AUTO_INCREMENT");
                let result = result.replace("bigserial", "BIGINT AUTO_INCREMENT");
                let result = result.replace("SMALLSERIAL", "SMALLINT AUTO_INCREMENT");
                let result = result.replace("smallserial", "SMALLINT AUTO_INCREMENT");
                let result = result.replace("SERIAL", "INT AUTO_INCREMENT");
                result.replace("serial", "INT AUTO_INCREMENT")
            }
            (SqlDialect::Postgres, SqlDialect::Sqlite) => {
                // SERIAL → INTEGER (SQLite auto-increments INTEGER PRIMARY KEY)
                let result = stmt.replace("BIGSERIAL", "INTEGER");
                let result = result.replace("bigserial", "INTEGER");
                let result = result.replace("SMALLSERIAL", "INTEGER");
                let result = result.replace("smallserial", "INTEGER");
                let result = result.replace("SERIAL", "INTEGER");
                result.replace("serial", "INTEGER")
            }
            (SqlDialect::Sqlite, SqlDialect::MySql) => {
                // SQLite uses INTEGER PRIMARY KEY for auto-increment
                // We can't easily detect this pattern, so just pass through
                stmt.to_string()
            }
            (SqlDialect::Sqlite, SqlDialect::Postgres) => {
                // SQLite uses INTEGER PRIMARY KEY for auto-increment
                // We can't easily detect this pattern, so just pass through
                stmt.to_string()
            }
            // MSSQL conversions
            (SqlDialect::MySql, SqlDialect::Mssql) => {
                use once_cell::sync::Lazy;
                use regex::Regex;
                // AUTO_INCREMENT → IDENTITY(1,1), keeping the integer type.
                // Case-insensitive and only outside string literals. BIGINT is
                // handled before the bare INT rule; `\bINT` won't match the
                // `INT` inside `BIGINT`.
                static RE_BIGINT_AI: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r"(?i)\bBIGINT\s+AUTO_INCREMENT").unwrap());
                static RE_INT_AI: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r"(?i)\bINT\s+AUTO_INCREMENT").unwrap());
                // Trailing boundary only (the type may already be glued to the
                // keyword by an earlier type mapping).
                static RE_BARE_AI: Lazy<Regex> =
                    Lazy::new(|| Regex::new(r"(?i)AUTO_INCREMENT\b").unwrap());
                map_outside_string_literals(stmt, |seg| {
                    let seg = RE_BIGINT_AI.replace_all(seg, "BIGINT IDENTITY(1,1)");
                    let seg = RE_INT_AI.replace_all(&seg, "INT IDENTITY(1,1)");
                    RE_BARE_AI.replace_all(&seg, "IDENTITY(1,1)").into_owned()
                })
            }
            (SqlDialect::Mssql, SqlDialect::MySql) => {
                // IDENTITY(1,1) → AUTO_INCREMENT
                self.convert_identity_to_auto_increment(stmt)
            }
            (SqlDialect::Postgres, SqlDialect::Mssql) => {
                // SERIAL → INT IDENTITY(1,1) (handled by type mapper)
                stmt.to_string()
            }
            (SqlDialect::Mssql, SqlDialect::Postgres) => {
                // IDENTITY(1,1) → SERIAL (need to add SERIAL instead)
                self.convert_identity_to_serial(stmt)
            }
            (SqlDialect::Sqlite, SqlDialect::Mssql) => {
                // SQLite → MSSQL: pass through
                stmt.to_string()
            }
            (SqlDialect::Mssql, SqlDialect::Sqlite) => {
                // IDENTITY → strip (SQLite uses INTEGER PRIMARY KEY)
                self.strip_identity(stmt)
            }
            _ => stmt.to_string(),
        }
    }

    /// Convert MSSQL IDENTITY to MySQL AUTO_INCREMENT
    fn convert_identity_to_auto_increment(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE_IDENTITY: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bIDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)").unwrap());

        RE_IDENTITY.replace_all(stmt, "AUTO_INCREMENT").to_string()
    }

    /// Convert MSSQL IDENTITY to PostgreSQL SERIAL
    fn convert_identity_to_serial(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        // Match INT IDENTITY(1,1) and replace with SERIAL
        static RE_BIGINT_IDENTITY: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bBIGINT\s+IDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)").unwrap());
        static RE_INT_IDENTITY: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bINT\s+IDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)").unwrap());
        static RE_SMALLINT_IDENTITY: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)\bSMALLINT\s+IDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)").unwrap()
        });

        let result = RE_BIGINT_IDENTITY
            .replace_all(stmt, "BIGSERIAL")
            .to_string();
        let result = RE_INT_IDENTITY.replace_all(&result, "SERIAL").to_string();
        RE_SMALLINT_IDENTITY
            .replace_all(&result, "SMALLSERIAL")
            .to_string()
    }

    /// Strip MSSQL IDENTITY clause for SQLite
    fn strip_identity(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE_IDENTITY: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*IDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)").unwrap());

        RE_IDENTITY.replace_all(stmt, "").to_string()
    }

    /// Convert string escape sequences
    fn convert_string_escapes(&self, stmt: &str) -> String {
        match (self.from, self.to) {
            // MySQL uses backslash escapes; PostgreSQL (standard strings),
            // SQLite and MSSQL all treat backslash as a literal byte and escape
            // a quote by doubling it, so the same decode applies to all three.
            (SqlDialect::MySql, SqlDialect::Postgres | SqlDialect::Sqlite | SqlDialect::Mssql) => {
                self.mysql_escapes_to_standard(stmt)
            }
            _ => stmt.to_string(),
        }
    }

    /// Convert MySQL backslash escapes to standard SQL double-quote escapes
    fn mysql_escapes_to_standard(&self, stmt: &str) -> String {
        let mut result = String::with_capacity(stmt.len());
        let mut chars = stmt.chars().peekable();
        let mut in_string = false;

        while let Some(c) = chars.next() {
            if c == '\'' {
                in_string = !in_string;
                result.push(c);
            } else if c == '\\' && in_string {
                // Decode the MySQL escape to the byte it represents. The target
                // dialects (Postgres standard strings, SQLite, MSSQL) treat
                // backslash as a literal, so a passed-through `\\` or `\n` would
                // be read as two literal characters and corrupt the data.
                match chars.next() {
                    Some(next) => match next {
                        // Quote becomes a doubled '' (standard SQL escaping).
                        '\'' => result.push_str("''"),
                        '"' => result.push('"'),
                        // One literal backslash.
                        '\\' => result.push('\\'),
                        'n' => result.push('\n'),
                        'r' => result.push('\r'),
                        't' => result.push('\t'),
                        '0' => result.push('\0'),
                        'b' => result.push('\u{08}'),
                        'Z' => result.push('\u{1a}'),
                        // MySQL keeps the backslash for the LIKE metacharacters.
                        '%' => result.push_str("\\%"),
                        '_' => result.push_str("\\_"),
                        // Any other escaped char is the char itself.
                        other => result.push(other),
                    },
                    // Trailing backslash at end of input.
                    None => result.push('\\'),
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Strip MySQL conditional comments /*!40101 ... */
    fn strip_conditional_comments(&self, stmt: &str) -> String {
        let mut result = String::with_capacity(stmt.len());
        let mut chars = stmt.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '/' && chars.peek() == Some(&'*') {
                chars.next(); // consume *
                if chars.peek() == Some(&'!') {
                    // Skip conditional comment
                    chars.next(); // consume !
                                  // Skip version number
                    while chars.peek().map(|c| c.is_ascii_digit()).unwrap_or(false) {
                        chars.next();
                    }
                    // Skip content until */
                    let mut depth = 1;
                    while depth > 0 {
                        match chars.next() {
                            Some('*') if chars.peek() == Some(&'/') => {
                                chars.next();
                                depth -= 1;
                            }
                            Some('/') if chars.peek() == Some(&'*') => {
                                chars.next();
                                depth += 1;
                            }
                            None => break,
                            _ => {}
                        }
                    }
                } else {
                    // Regular comment, keep it
                    result.push('/');
                    result.push('*');
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Strip ENGINE clause
    fn strip_engine_clause(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        if self.to == SqlDialect::MySql {
            return stmt.to_string();
        }

        // Remove ENGINE=InnoDB, ENGINE=MyISAM, etc.
        static RE_ENGINE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*ENGINE\s*=\s*\w+").unwrap());
        RE_ENGINE.replace_all(stmt, "").to_string()
    }

    /// Strip CHARSET/COLLATE clauses
    fn strip_charset_clauses(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        if self.to == SqlDialect::MySql {
            return stmt.to_string();
        }

        static RE_CHARSET: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*(DEFAULT\s+)?CHARSET\s*=\s*\w+").unwrap());
        static RE_COLLATE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*COLLATE\s*=?\s*\w+").unwrap());

        let result = RE_CHARSET.replace_all(stmt, "").to_string();
        RE_COLLATE.replace_all(&result, "").to_string()
    }

    /// Case-insensitively checks whether `keyword` starts at `chars[pos]`, with
    /// word boundaries on both sides (so e.g. "COMMENT" doesn't match inside
    /// "COMMENTARY" or a preceding identifier char like "XCOMMENT"). Returns the
    /// index just past the keyword on success.
    fn match_keyword_ci(chars: &[char], pos: usize, keyword: &str) -> Option<usize> {
        let kw_len = keyword.len();
        if pos > 0 && (chars[pos - 1].is_alphanumeric() || chars[pos - 1] == '_') {
            return None;
        }
        let end = pos + kw_len;
        if end > chars.len() {
            return None;
        }
        let candidate: String = chars[pos..end].iter().collect();
        if !candidate.eq_ignore_ascii_case(keyword) {
            return None;
        }
        if end < chars.len() && (chars[end].is_alphanumeric() || chars[end] == '_') {
            return None;
        }
        Some(end)
    }

    /// Removes trailing whitespace already pushed into `result`, so that stripping
    /// a clause also absorbs the whitespace that preceded it (matching the old
    /// `\s*<keyword>...` regex behavior).
    fn trim_trailing_whitespace(result: &mut String) {
        while result.ends_with(|c: char| c.is_whitespace()) {
            result.pop();
        }
    }

    /// Strip MySQL table-level `AUTO_INCREMENT=N` option. String-literal-aware so
    /// that a DEFAULT value or other string content containing the literal text
    /// "AUTO_INCREMENT=N" is left untouched.
    /// See https://github.com/HelgeSverre/sql-splitter/issues/64
    fn strip_mysql_auto_increment_table_option(&self, stmt: &str) -> String {
        let chars: Vec<char> = stmt.chars().collect();
        let n = chars.len();
        let mut result = String::with_capacity(stmt.len());
        let mut in_string = false;
        let mut i = 0;

        while i < n {
            let c = chars[i];
            if c == '\'' {
                in_string = !in_string;
                result.push(c);
                i += 1;
                continue;
            }
            if !in_string {
                if let Some(after_kw) = Self::match_keyword_ci(&chars, i, "AUTO_INCREMENT") {
                    let mut j = after_kw;
                    while j < n && chars[j].is_whitespace() {
                        j += 1;
                    }
                    if j < n && chars[j] == '=' {
                        j += 1;
                        while j < n && chars[j].is_whitespace() {
                            j += 1;
                        }
                        let digits_start = j;
                        while j < n && chars[j].is_ascii_digit() {
                            j += 1;
                        }
                        if j > digits_start {
                            Self::trim_trailing_whitespace(&mut result);
                            i = j;
                            continue;
                        }
                    }
                }
            }
            result.push(c);
            i += 1;
        }
        result
    }

    /// Strip MySQL column/table `COMMENT 'text'` (or `COMMENT='text'`) annotations.
    /// Neither is valid inside a PostgreSQL column definition or as a CREATE TABLE
    /// table option, so they are removed entirely rather than translated.
    /// String-literal-aware: only matches a real `COMMENT` clause, never the word
    /// "comment" sitting inside an unrelated DEFAULT value or CHECK expression.
    fn strip_mysql_comment_clauses(&self, stmt: &str) -> String {
        if self.to == SqlDialect::MySql {
            return stmt.to_string();
        }

        let chars: Vec<char> = stmt.chars().collect();
        let n = chars.len();
        let mut result = String::with_capacity(stmt.len());
        let mut in_string = false;
        let mut i = 0;

        while i < n {
            let c = chars[i];
            if c == '\'' {
                in_string = !in_string;
                result.push(c);
                i += 1;
                continue;
            }
            if !in_string {
                if let Some(after_kw) = Self::match_keyword_ci(&chars, i, "COMMENT") {
                    let mut j = after_kw;
                    while j < n && chars[j].is_whitespace() {
                        j += 1;
                    }
                    if j < n && chars[j] == '=' {
                        j += 1;
                        while j < n && chars[j].is_whitespace() {
                            j += 1;
                        }
                    }
                    if j < n && chars[j] == '\'' {
                        // Consume the quoted comment text itself, honoring ''
                        // as an escaped quote, then drop the whole clause.
                        j += 1;
                        loop {
                            if j >= n {
                                break;
                            }
                            if chars[j] == '\'' {
                                if j + 1 < n && chars[j + 1] == '\'' {
                                    j += 2;
                                    continue;
                                }
                                j += 1;
                                break;
                            }
                            j += 1;
                        }
                        Self::trim_trailing_whitespace(&mut result);
                        i = j;
                        continue;
                    }
                }
            }
            result.push(c);
            i += 1;
        }
        result
    }

    /// Convert MySQL's column-level `AUTO_INCREMENT` keyword to PostgreSQL's
    /// `SERIAL`/`BIGSERIAL`, case-insensitively and string-literal-aware (a
    /// DEFAULT value or CHECK expression containing the literal text
    /// "AUTO_INCREMENT" must not be touched — see finding in issue #64 follow-up
    /// review).
    fn convert_mysql_auto_increment_keyword(&self, stmt: &str) -> String {
        let chars: Vec<char> = stmt.chars().collect();
        let n = chars.len();
        let mut result = String::with_capacity(stmt.len());
        let mut in_string = false;
        let mut i = 0;

        while i < n {
            let c = chars[i];
            if c == '\'' {
                in_string = !in_string;
                result.push(c);
                i += 1;
                continue;
            }
            if !in_string {
                if let Some(after_type) = Self::match_keyword_ci(&chars, i, "BIGINT") {
                    let mut j = after_type;
                    while j < n && chars[j].is_whitespace() {
                        j += 1;
                    }
                    if let Some(end) = Self::match_keyword_ci(&chars, j, "AUTO_INCREMENT") {
                        result.push_str("BIGSERIAL");
                        i = end;
                        continue;
                    }
                }
                if let Some(after_type) = Self::match_keyword_ci(&chars, i, "INT") {
                    let mut j = after_type;
                    while j < n && chars[j].is_whitespace() {
                        j += 1;
                    }
                    if let Some(end) = Self::match_keyword_ci(&chars, j, "AUTO_INCREMENT") {
                        result.push_str("SERIAL");
                        i = end;
                        continue;
                    }
                }
                if let Some(end) = Self::match_keyword_ci(&chars, i, "AUTO_INCREMENT") {
                    // Clean up any remaining standalone occurrence (e.g. when
                    // other modifiers like NOT NULL sit between the type and
                    // the keyword).
                    i = end;
                    continue;
                }
            }
            result.push(c);
            i += 1;
        }
        result
    }

    /// Convert MySQL `UNIQUE KEY [name] (cols) [USING BTREE|HASH]` table constraint
    /// to standard `UNIQUE (cols)`, which is the closest PostgreSQL equivalent.
    /// Also strips MySQL prefix-length index annotations (e.g. `email(191)`),
    /// which have no PostgreSQL equivalent and aren't valid there.
    fn convert_unique_key_constraint(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::{Captures, Regex};

        if self.to == SqlDialect::MySql {
            return stmt.to_string();
        }

        // The column-list group allows one level of nested `(digits)` prefix-length
        // annotations (e.g. `("email"(191),"other")`), which are common in
        // real-world MySQL dumps using utf8mb4 prefix indexes.
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(
                r#"(?i)UNIQUE\s+KEY\s+(?:"[^"]*"\s+)?(\([^()]*(?:\(\d+\)[^()]*)*\))(?:\s*USING\s+(?:BTREE|HASH))?"#,
            )
            .unwrap()
        });
        static RE_PREFIX_LEN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\(\d+\)").unwrap());

        RE.replace_all(stmt, |caps: &Captures| {
            let cols = RE_PREFIX_LEN.replace_all(&caps[1], "");
            format!("UNIQUE {}", cols)
        })
        .to_string()
    }

    /// Strip PostgreSQL type casts (::type and ::regclass)
    fn strip_postgres_casts(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        // Match ::regclass, ::text, ::integer, etc. (including complex types
        // like character varying, schema-qualified names like ::public.status,
        // and quoted identifiers like ::"Order Status").
        static RE_CAST: Lazy<Regex> = Lazy::new(|| {
            Regex::new(
                r#"::(?:(?:[a-zA-Z_][a-zA-Z0-9_]*|"(?:[^"]|"")*"))(?:\s*\.\s*(?:[a-zA-Z_][a-zA-Z0-9_]*|"(?:[^"]|"")*"))*(?:\s+(?:[a-zA-Z_][a-zA-Z0-9_]*|"(?:[^"]|"")*"))*"#,
            )
            .unwrap()
        });

        // Only outside string literals: a value like '2001:db8::1' or
        // 'a::b text' must keep its ':: …' rather than have it stripped.
        map_outside_string_literals(stmt, |seg| RE_CAST.replace_all(seg, "").into_owned())
    }

    /// Convert nextval('sequence') to NULL or remove (AUTO_INCREMENT handles it)
    fn convert_nextval(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        // Match nextval('sequence_name'::regclass) or nextval('sequence_name')
        // Remove the DEFAULT nextval(...) entirely - AUTO_INCREMENT is already applied
        static RE_NEXTVAL: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*DEFAULT\s+nextval\s*\([^)]+\)").unwrap());

        RE_NEXTVAL.replace_all(stmt, "").to_string()
    }

    /// Rebuild a PostgreSQL sequence-backed default as a MySQL AUTO_INCREMENT
    /// column, returned as a statement to emit once the dump is fully applied.
    ///
    /// MySQL rejects an AUTO_INCREMENT column that is not a key, and pg_dump
    /// emits the primary key after the table data, so this cannot be applied in
    /// place — it has to run last. `MODIFY COLUMN` also takes a full column
    /// definition, so the type is read back from the `CREATE TABLE` this
    /// converter already emitted.
    ///
    /// Returns `None` when the target is not MySQL, when the table was never
    /// seen, or when the column's type does not read as an integer — in every
    /// one of those cases the caller drops the statement with a warning rather
    /// than emit a half-formed `MODIFY`.
    fn defer_mysql_auto_increment(&self, stmt: &str) -> Option<String> {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE_SEQUENCE_TARGET: Lazy<Regex> = Lazy::new(|| {
            Regex::new(
                r"(?is)\bALTER\s+TABLE\s+(?:ONLY\s+)?(?P<table>\S+)\s+ALTER\s+COLUMN\s+(?P<column>[^\s]+)\s+SET\s+DEFAULT\s+nextval",
            )
            .unwrap()
        });

        if self.to != SqlDialect::MySql {
            return None;
        }

        let caps = RE_SEQUENCE_TARGET.captures(stmt)?;
        let table = table_key(&caps["table"]);
        let column = caps["column"].trim_matches(|c| c == '"' || c == '`');
        let create_table = self.created_tables.get(&table)?;
        let column_type = declared_column_type(create_table, column)?;

        // A sequence always backs an integer column. Anything else means the
        // type was misread, and a wrong MODIFY would rewrite the column.
        if !matches!(
            column_type.to_uppercase().as_str(),
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "MEDIUMINT" | "TINYINT"
        ) {
            return None;
        }

        Some(format!(
            "ALTER TABLE `{table}` MODIFY COLUMN `{column}` {column_type} NOT NULL AUTO_INCREMENT;"
        ))
    }

    /// Strip PostgreSQL's `ONLY` inheritance qualifier from `ALTER TABLE ONLY t`.
    /// pg_dump attaches every constraint and column default that way, and no
    /// other engine accepts the keyword.
    fn strip_postgres_only(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE_ALTER_ONLY: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\b(ALTER\s+TABLE)\s+ONLY\s+").unwrap());

        RE_ALTER_ONLY.replace_all(stmt, "$1 ").to_string()
    }

    /// Whether an `ALTER TABLE` exists only to attach a sequence-backed default
    /// (`ALTER COLUMN x SET DEFAULT nextval(...)`) — pg_dump's expansion of a
    /// `serial` column.
    fn is_sequence_default_alter(&self, stmt: &str) -> bool {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE_SEQUENCE_DEFAULT: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?is)\bALTER\s+COLUMN\b.*?\bSET\s+DEFAULT\s+nextval\s*\(").unwrap()
        });

        RE_SEQUENCE_DEFAULT.is_match(stmt)
    }

    /// Whether the statement is a psql client meta-command rather than SQL.
    /// `\.` is excluded: it terminates a COPY data block and belongs to the
    /// COPY path.
    fn is_psql_meta_command(&self, stmt: &str) -> bool {
        let stripped = self.strip_leading_sql_comments(stmt);
        let trimmed = stripped.trim_start();
        trimmed.starts_with('\\') && !trimmed.starts_with("\\.")
    }

    /// Convert DEFAULT now() to DEFAULT CURRENT_TIMESTAMP
    fn convert_default_now(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE_NOW: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bDEFAULT\s+now\s*\(\s*\)").unwrap());

        RE_NOW
            .replace_all(stmt, "DEFAULT CURRENT_TIMESTAMP")
            .to_string()
    }

    /// Strip schema prefix from table names (e.g., public.users -> users)
    fn strip_schema_prefix(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        // Match schema.table patterns (with optional quotes)
        // Handle: public.table, "public"."table", public."table"
        static RE_SCHEMA: Lazy<Regex> =
            Lazy::new(|| Regex::new(r#"(?i)\b(public|pg_catalog|pg_temp)\s*\.\s*"#).unwrap());

        // Only outside string literals so a value like 'see public.docs' keeps
        // its text.
        map_outside_string_literals(stmt, |seg| RE_SCHEMA.replace_all(seg, "").into_owned())
    }

    /// Convert MSSQL GETDATE() to CURRENT_TIMESTAMP
    fn convert_mssql_getdate(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE_GETDATE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bGETDATE\s*\(\s*\)").unwrap());
        static RE_SYSDATETIME: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bSYSDATETIME\s*\(\s*\)").unwrap());
        static RE_GETUTCDATE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bGETUTCDATE\s*\(\s*\)").unwrap());

        let result = RE_GETDATE
            .replace_all(stmt, "CURRENT_TIMESTAMP")
            .to_string();
        let result = RE_SYSDATETIME
            .replace_all(&result, "CURRENT_TIMESTAMP")
            .to_string();
        RE_GETUTCDATE
            .replace_all(&result, "CURRENT_TIMESTAMP")
            .to_string()
    }

    /// Strip MSSQL ON [filegroup] clause
    fn strip_mssql_on_filegroup(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        // Match ON [PRIMARY] or ON [filegroup_name]
        static RE_ON_FILEGROUP: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s+ON\s*\[\s*\w+\s*\]").unwrap());

        RE_ON_FILEGROUP.replace_all(stmt, "").to_string()
    }

    /// Strip MSSQL CLUSTERED/NONCLUSTERED keywords
    fn strip_mssql_clustered(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE_CLUSTERED: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bCLUSTERED\s+").unwrap());
        static RE_NONCLUSTERED: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bNONCLUSTERED\s+").unwrap());

        let result = RE_CLUSTERED.replace_all(stmt, "").to_string();
        RE_NONCLUSTERED.replace_all(&result, "").to_string()
    }

    /// Convert MSSQL `N'unicode'` string literals to regular `'unicode'`
    /// strings by dropping the `N` prefix. Only a genuine prefix — an `N`
    /// immediately before a string-opening quote and not itself inside a string
    /// — is converted, so an `N'` sequence appearing inside string data (e.g.
    /// `'season N''4'`) is left untouched.
    fn convert_mssql_unicode_strings(&self, stmt: &str) -> String {
        let bytes = stmt.as_bytes();
        let mut out = String::with_capacity(stmt.len());
        let mut i = 0;
        // Everything in `stmt[..copied]` has already been emitted to `out`.
        let mut copied = 0;
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                // A string literal: skip it whole so its contents are preserved.
                i = skip_sql_string_literal(bytes, i);
            } else if matches!(bytes[i], b'N' | b'n')
                && bytes.get(i + 1) == Some(&b'\'')
                && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_'))
            {
                // A genuine `N'` string prefix: emit up to it, drop the `N`, and
                // resume at the opening quote (copied to the literal-skip path).
                out.push_str(&stmt[copied..i]);
                copied = i + 1;
                i += 1;
            } else {
                i += 1;
            }
        }
        out.push_str(&stmt[copied..]);
        out
    }

    /// Strip MSSQL schema prefix (dbo., etc.) from table names
    fn strip_mssql_schema_prefix(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        // Match [dbo].[table] or dbo.table and keep just [table] or table
        // We replace schema.table with just table, handling both bracketed and unbracketed forms
        static RE_MSSQL_SCHEMA: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\[?dbo\]?\s*\.\s*").unwrap());

        // Only outside string literals so a value like 'user dbo.smith' keeps
        // its text.
        map_outside_string_literals(stmt, |seg| {
            RE_MSSQL_SCHEMA.replace_all(seg, "").into_owned()
        })
    }

    /// Detect unsupported features and add warnings
    fn detect_unsupported_features(
        &mut self,
        stmt: &str,
        table_name: Option<&str>,
    ) -> Result<(), ConvertWarning> {
        let upper = stmt.to_uppercase();

        // MySQL-specific features
        if self.from == SqlDialect::MySql {
            // ENUM types
            if upper.contains("ENUM(") && !self.is_enum_aware() {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: format!(
                        "ENUM type{}",
                        table_name
                            .map(|t| format!(" in table {}", t))
                            .unwrap_or_default()
                    ),
                    suggestion: Some(
                        "Converted to VARCHAR - consider adding CHECK constraint".to_string(),
                    ),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            }

            // SET types (MySQL)
            if upper.contains("SET(") {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: format!(
                        "SET type{}",
                        table_name
                            .map(|t| format!(" in table {}", t))
                            .unwrap_or_default()
                    ),
                    suggestion: Some(
                        "Converted to VARCHAR - SET semantics not preserved".to_string(),
                    ),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            }

            // UNSIGNED
            if upper.contains("UNSIGNED") {
                self.warnings.add(ConvertWarning::UnsupportedFeature {
                    feature: "UNSIGNED modifier".to_string(),
                    suggestion: Some(
                        "Removed - consider adding CHECK constraint for non-negative values"
                            .to_string(),
                    ),
                });
            }
        }

        // PostgreSQL-specific features
        if self.from == SqlDialect::Postgres {
            // Array types
            if upper.contains("[]") || upper.contains("ARRAY[") {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: format!(
                        "Array type{}",
                        table_name
                            .map(|t| format!(" in table {}", t))
                            .unwrap_or_default()
                    ),
                    suggestion: Some(
                        "Array types not supported in target dialect - consider using JSON"
                            .to_string(),
                    ),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            }

            // INHERITS
            if upper.contains("INHERITS") {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: "Table inheritance (INHERITS)".to_string(),
                    suggestion: Some(
                        "PostgreSQL table inheritance not supported in target dialect".to_string(),
                    ),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            }

            // PARTITION BY
            if upper.contains("PARTITION BY") && self.to == SqlDialect::Sqlite {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: "Table partitioning".to_string(),
                    suggestion: Some("Partitioning not supported in SQLite".to_string()),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            }
        }

        Ok(())
    }

    /// Get collected warnings
    pub fn warnings(&self) -> &[ConvertWarning] {
        self.warnings.warnings()
    }
}

/// Run the convert command
pub fn run(config: ConvertConfig) -> anyhow::Result<ConvertStats> {
    let mut stats = ConvertStats::default();

    // Detect or use specified source dialect
    let from_dialect = if let Some(d) = config.from_dialect {
        d
    } else {
        let result = crate::parser::detect_dialect_from_file(&config.input)?;
        if config.progress {
            eprintln!(
                "Auto-detected source dialect: {} (confidence: {:?})",
                result.dialect, result.confidence
            );
        }
        result.dialect
    };

    // Check for same dialect
    if from_dialect == config.to_dialect {
        anyhow::bail!(
            "Source and target dialects are the same ({}). No conversion needed.",
            from_dialect
        );
    }

    // Get file size for progress tracking
    let file_size = std::fs::metadata(&config.input)?.len();

    let progress_bar = if config.progress {
        let pb = ProgressBar::new(file_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("█▓▒░  ")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        pb.set_message("Converting...");
        Some(pb)
    } else {
        None
    };

    // Create converter
    let mut converter = Converter::new(from_dialect, config.to_dialect)
        .with_strict(config.strict)
        .with_enum_naming(config.enum_naming);

    // Open input, transparently handling any supported compression format
    // (including zip archives), with optional progress tracking.
    let reader: Box<dyn Read> = if let Some(ref pb) = progress_bar {
        let pb_clone = pb.clone();
        open_input_with_progress(
            &config.input,
            Box::new(move |bytes| pb_clone.set_position(bytes)),
        )?
    } else {
        open_input(&config.input)?
    };
    let mut parser = Parser::with_dialect(reader, 64 * 1024, from_dialect);

    // Open output
    let mut writer: Box<dyn Write> = if config.dry_run {
        Box::new(std::io::sink())
    } else {
        match &config.output {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                Box::new(BufWriter::with_capacity(256 * 1024, File::create(path)?))
            }
            None => Box::new(BufWriter::new(std::io::stdout())),
        }
    };

    // Write header
    if !config.dry_run {
        write_header(&mut writer, &config, from_dialect)?;
    }

    // Process statements
    while let Some(stmt) = parser.read_statement()? {
        stats.statements_processed += 1;

        // Check if this is a COPY data block (follows a COPY header)
        if converter.has_pending_copy() {
            // This is a data block, convert it to INSERT statements
            match converter.process_copy_data(&stmt) {
                Ok(inserts) => {
                    for insert in inserts {
                        if !insert.is_empty() {
                            stats.statements_converted += 1;
                            if !config.dry_run {
                                writer.write_all(&insert)?;
                                writer.write_all(b"\n")?;
                            }
                        }
                    }
                }
                Err(warning) => {
                    stats.warnings.push(warning);
                    stats.statements_skipped += 1;
                }
            }
            continue;
        }

        match converter.convert_statement(&stmt) {
            Ok(converted) => {
                if converted.is_empty() {
                    stats.statements_skipped += 1;
                } else if converted == stmt {
                    stats.statements_unchanged += 1;
                    if !config.dry_run {
                        writer.write_all(&converted)?;
                        writer.write_all(b"\n")?;
                    }
                } else {
                    stats.statements_converted += 1;
                    if !config.dry_run {
                        writer.write_all(&converted)?;
                        writer.write_all(b"\n")?;
                    }
                }
            }
            Err(warning) => {
                stats.warnings.push(warning);
                stats.statements_skipped += 1;
            }
        }
    }

    // Statements that only become valid once everything else has been applied
    // (a MySQL AUTO_INCREMENT column has to be a key first, and pg_dump adds the
    // primary key after the data).
    for statement in converter.take_deferred_statements() {
        stats.statements_converted += 1;
        if !config.dry_run {
            writer.write_all(statement.as_bytes())?;
            writer.write_all(b"\n")?;
        }
    }

    // Collect warnings
    stats.warnings.extend(converter.warnings().iter().cloned());

    if let Some(pb) = progress_bar {
        pb.finish_with_message("done");
    }

    Ok(stats)
}

/// Write output header
fn write_header(
    writer: &mut dyn Write,
    config: &ConvertConfig,
    from: SqlDialect,
) -> std::io::Result<()> {
    writeln!(writer, "-- Converted by sql-splitter")?;
    writeln!(writer, "-- From: {} → To: {}", from, config.to_dialect)?;
    writeln!(writer, "-- Source: {}", config.input.display())?;
    writeln!(writer)?;

    // Write dialect-specific header
    match config.to_dialect {
        SqlDialect::Postgres => {
            writeln!(writer, "SET client_encoding = 'UTF8';")?;
            writeln!(writer, "SET standard_conforming_strings = on;")?;
        }
        SqlDialect::Sqlite => {
            writeln!(writer, "PRAGMA foreign_keys = OFF;")?;
        }
        SqlDialect::MySql => {
            writeln!(writer, "SET NAMES utf8mb4;")?;
            writeln!(writer, "SET FOREIGN_KEY_CHECKS = 0;")?;
        }
        SqlDialect::Mssql => {
            writeln!(writer, "SET ANSI_NULLS ON;")?;
            writeln!(writer, "SET QUOTED_IDENTIFIER ON;")?;
            writeln!(writer, "SET NOCOUNT ON;")?;
        }
    }
    writeln!(writer)?;

    Ok(())
}

/// Advance past a single-quoted SQL string literal starting at `start` (the
/// opening quote), returning the index just past the closing quote. A doubled
/// `''` is an escaped quote that keeps the string open. An unterminated literal
/// consumes to the end. `bytes[start]` must be `'`.
fn skip_sql_string_literal(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if bytes.get(i + 1) == Some(&b'\'') {
                i += 2; // escaped quote, stay inside the string
            } else {
                return i + 1; // closing quote
            }
        } else {
            i += 1;
        }
    }
    i
}

fn pg_quote_ident(name: &str) -> String {
    // If the name needs quoting (non-lowercase, special chars, or starts with digit),
    // wrap in double quotes. Otherwise return as-is for readability.
    let needs_quoting = name
        .chars()
        .any(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
        || name.is_empty()
        || name.chars().next().is_some_and(|c| c.is_ascii_digit());
    if needs_quoting {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

/// Apply `f` to every span of `stmt` that lies outside a single-quoted string
/// literal, copying the literals through verbatim. Used to keep statement-level
/// rewrites (cast/schema-prefix stripping) from mangling string *data*.
fn map_outside_string_literals(stmt: &str, f: impl Fn(&str) -> String) -> String {
    let bytes = stmt.as_bytes();
    let mut out = String::with_capacity(stmt.len());
    let mut i = 0;
    let mut seg_start = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            // Flush the preceding non-string span through `f`.
            out.push_str(&f(&stmt[seg_start..i]));
            let lit_end = skip_sql_string_literal(bytes, i);
            out.push_str(&stmt[i..lit_end]); // literal, verbatim
            i = lit_end;
            seg_start = i;
        } else {
            i += 1;
        }
    }
    out.push_str(&f(&stmt[seg_start..]));
    out
}

/// Rewrite one self-delimiting identifier quote (`` ` `` or `"`) into
/// `to_open`/`to_close`, skipping single-quoted string literals. A `'`
/// inside a quoted identifier does not open a string.
fn requote(stmt: &str, from: char, to_open: char, to_close: char) -> String {
    let mut result = String::with_capacity(stmt.len());
    let mut in_string = false;
    let mut in_ident = false;

    for c in stmt.chars() {
        if c == '\'' && !in_ident {
            in_string = !in_string;
            result.push(c);
        } else if c == from && !in_string {
            result.push(if in_ident { to_close } else { to_open });
            in_ident = !in_ident;
        } else {
            result.push(c);
        }
    }
    result
}

/// Rewrite MSSQL `[`/`]` into `to`, skipping single-quoted string literals.
/// Deliberately tracks no identifier state: mapping every bracket 1:1 turns
/// the `]]` escape into a doubled `to` quote, which is exactly the escape
/// MySQL and PostgreSQL expect.
fn unbracket(stmt: &str, to: char) -> String {
    let mut result = String::with_capacity(stmt.len());
    let mut in_string = false;

    for c in stmt.chars() {
        if c == '\'' {
            in_string = !in_string;
            result.push(c);
        } else if !in_string && (c == '[' || c == ']') {
            result.push(to);
        } else {
            result.push(c);
        }
    }
    result
}
