//! Positioned tokenization and column-definition walking for DDL rewriting.
//!
//! Enum conversion replaces types at a column's *type position*. Regex
//! approximations of that position are fragile (string literals, comments,
//! quoted identifiers, `E''`/dollar-quoted strings, backslash escapes, and
//! constraint names all produce false matches or misses), so this module
//! lexes the statement into tokens with byte spans and walks the column list
//! with the minimal grammar needed to find each column's type name exactly.

use crate::parser::SqlDialect;

/// Lexical rules that differ between dialects.
#[derive(Clone, Copy, Debug)]
pub struct LexRules {
    /// String literals interpret backslash escapes (MySQL).
    pub backslash_escapes: bool,
    /// `"..."` is an identifier (PostgreSQL/ANSI); otherwise a string literal
    /// (MySQL default).
    pub double_quote_ident: bool,
    /// `` `...` `` is an identifier (MySQL).
    pub backtick_ident: bool,
    /// `# ...` starts a line comment (MySQL).
    pub hash_comments: bool,
    /// `$tag$ ... $tag$` strings (PostgreSQL).
    pub dollar_quotes: bool,
}

impl LexRules {
    pub fn for_dialect(dialect: SqlDialect) -> Self {
        match dialect {
            SqlDialect::MySql => Self {
                backslash_escapes: true,
                double_quote_ident: false,
                backtick_ident: true,
                hash_comments: true,
                dollar_quotes: false,
            },
            _ => Self {
                backslash_escapes: false,
                double_quote_ident: true,
                backtick_ident: false,
                hash_comments: false,
                dollar_quotes: true,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// Bare identifier, e.g. `status`.
    Ident(usize, usize),
    /// Quoted identifier with its decoded value.
    QuotedIdent(usize, usize, String),
    /// String literal with its decoded value.
    Str(usize, usize, String),
    /// `-- ...`, `# ...`, or `/* ... */`.
    Comment(usize, usize),
    /// Single-byte punctuation.
    Punct(usize, u8),
    /// Whitespace.
    Whitespace(usize, usize),
    /// Anything else (numbers, operators, stray bytes, ...).
    Other(usize, usize),
}

impl Token {
    pub fn start(&self) -> usize {
        match self {
            Token::Ident(s, _)
            | Token::QuotedIdent(s, _, _)
            | Token::Str(s, _, _)
            | Token::Comment(s, _)
            | Token::Punct(s, _)
            | Token::Whitespace(s, _)
            | Token::Other(s, _) => *s,
        }
    }

    pub fn end(&self) -> usize {
        match self {
            Token::Ident(_, e)
            | Token::QuotedIdent(_, e, _)
            | Token::Str(_, e, _)
            | Token::Comment(_, e)
            | Token::Whitespace(_, e)
            | Token::Other(_, e) => *e,
            Token::Punct(s, _) => s + 1,
        }
    }

    pub fn owned_ident_text(&self, stmt: &str) -> Option<String> {
        match self {
            Token::Ident(s, e) => Some(stmt[*s..*e].to_string()),
            Token::QuotedIdent(_, _, v) => Some(v.clone()),
            _ => None,
        }
    }

    pub fn is_punct(&self, b: u8) -> bool {
        matches!(self, Token::Punct(_, p) if *p == b)
    }

    pub fn ident_eq(&self, stmt: &str, word: &str) -> bool {
        match self {
            Token::Ident(s, e) => stmt[*s..*e].eq_ignore_ascii_case(word),
            _ => false,
        }
    }
}

/// Lex `stmt` into tokens with byte spans. Never panics on malformed input:
/// unterminated constructs consume to the end.
pub fn tokenize(stmt: &str, rules: LexRules) -> Vec<Token> {
    let bytes = stmt.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b' ' | b'\t' | b'\r' | b'\n' => {
                let start = i;
                while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\r' | b'\n') {
                    i += 1;
                }
                tokens.push(Token::Whitespace(start, i));
            }
            b'\'' => {
                let start = i;
                // E'...' (PostgreSQL) enables backslash escapes for this literal.
                let e_prefix = i > 0
                    && matches!(bytes[i - 1], b'e' | b'E')
                    && (i < 2 || !is_ident_byte(bytes[i - 2]));
                let (end, value) = lex_quoted(bytes, i, b'\'', rules.backslash_escapes || e_prefix);
                tokens.push(Token::Str(start, end, value));
                i = end;
            }
            b'"' if rules.double_quote_ident => {
                let start = i;
                let (end, value) = lex_delimited_ident(bytes, i, b'"');
                tokens.push(Token::QuotedIdent(start, end, value));
                i = end;
            }
            b'"' => {
                let start = i;
                // MySQL string literal (double_quote_ident off): terminates at
                // the matching double quote, honoring backslash escapes.
                let (end, value) = lex_quoted(bytes, i, b'"', rules.backslash_escapes);
                tokens.push(Token::Str(start, end, value));
                i = end;
            }
            b'`' if rules.backtick_ident => {
                let start = i;
                let (end, value) = lex_delimited_ident(bytes, i, b'`');
                tokens.push(Token::QuotedIdent(start, end, value));
                i = end;
            }
            b'$' if rules.dollar_quotes => {
                if let Some((tag_end, end)) = lex_dollar_quoted(stmt, i) {
                    let value = stmt[tag_end..end - (tag_end - i)].to_string();
                    tokens.push(Token::Str(i, end, value));
                    i = end;
                } else {
                    tokens.push(Token::Punct(i, b'$'));
                    i += 1;
                }
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                let start = i;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                tokens.push(Token::Comment(start, i));
            }
            b'#' if rules.hash_comments => {
                let start = i;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                tokens.push(Token::Comment(start, i));
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let start = i;
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
                tokens.push(Token::Comment(start, i));
            }
            b'(' | b')' | b'[' | b']' | b',' | b';' | b'.' | b'=' => {
                tokens.push(Token::Punct(i, b));
                i += 1;
            }
            _ if is_ident_byte(b) => {
                let start = i;
                while i < bytes.len() && is_ident_byte(bytes[i]) {
                    i += 1;
                }
                tokens.push(Token::Ident(start, i));
            }
            _ => {
                tokens.push(Token::Other(i, i + 1));
                i += 1;
            }
        }
    }
    tokens
}

/// A column's type reference located by the column-list walk.
#[derive(Debug)]
pub struct TypeRef {
    /// Decoded column name.
    pub column: String,
    /// Byte span of the type name exactly as written (may be schema-qualified
    /// and/or quoted).
    pub type_span: (usize, usize),
    /// Decoded type name parts: `(schema, name)` with schema when qualified.
    pub type_name: (Option<String>, String),
    /// True when the type is followed by `[` (array reference).
    pub is_array: bool,
}

/// Keywords that start a *table-level* element inside a column list; a
/// definition beginning with one of these has no column name.
const TABLE_ELEMENT_KEYWORDS: &[&str] = &[
    "CONSTRAINT",
    "PRIMARY",
    "FOREIGN",
    "UNIQUE",
    "CHECK",
    "EXCLUDE",
    "KEY",
    "INDEX",
    "FULLTEXT",
    "SPATIAL",
    "LIKE",
];

/// Locate every column type reference in a CREATE TABLE or ALTER TABLE
/// statement. Handles the `CREATE TABLE (...)` column list plus the ALTER
/// TABLE actions that introduce or retype a column (`ADD [COLUMN]`, MySQL
/// `MODIFY`/`CHANGE`, PostgreSQL `ALTER [COLUMN] ... [SET DATA] TYPE`).
pub fn table_ddl_type_refs(stmt: &str, dialect: SqlDialect) -> Vec<TypeRef> {
    table_ddl_type_refs_with_rules(stmt, LexRules::for_dialect(dialect))
}

/// Like [`table_ddl_type_refs`] with explicit lex rules. Used mid-pipeline
/// when a statement is half-converted (e.g. PG→MySQL after identifier
/// quoting has switched to backticks but type mapping has not run yet).
pub fn table_ddl_type_refs_with_rules(stmt: &str, rules: LexRules) -> Vec<TypeRef> {
    let tokens = tokenize(stmt, rules);
    let sig: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t, Token::Whitespace(..) | Token::Comment(..)))
        .map(|(idx, _)| idx)
        .collect();

    let is_create = first_keyword(&tokens, &sig, stmt, "CREATE")
        .is_some_and(|c| keyword_at(&tokens, &sig, stmt, c + 1, "TABLE").is_some());
    if is_create {
        return create_table_refs(stmt, &tokens, &sig);
    }
    let is_alter = first_keyword(&tokens, &sig, stmt, "ALTER")
        .is_some_and(|a| keyword_at(&tokens, &sig, stmt, a + 1, "TABLE").is_some());
    if is_alter {
        return alter_table_refs(stmt, &tokens, &sig);
    }
    Vec::new()
}

fn create_table_refs(stmt: &str, tokens: &[Token], sig: &[usize]) -> Vec<TypeRef> {
    // The column list opens at the first `(` (after the table name).
    let Some(&open_idx) = sig.iter().find(|&&idx| tokens[idx].is_punct(b'(')) else {
        return Vec::new();
    };
    column_list_refs(stmt, tokens, sig, open_idx)
}

/// Parse the column list opened by `tokens[open_idx] == '('`: split on
/// top-level commas and extract a [`TypeRef`] from each column definition.
fn column_list_refs(stmt: &str, tokens: &[Token], sig: &[usize], open_idx: usize) -> Vec<TypeRef> {
    let mut refs = Vec::new();
    let mut depth = 0usize;
    let mut def_start: Option<usize> = None;

    for &idx in sig.iter().skip_while(|&&idx| idx != open_idx) {
        match &tokens[idx] {
            Token::Punct(_, b'(') => {
                if depth >= 1 && def_start.is_none() {
                    def_start = Some(idx);
                }
                depth += 1;
            }
            Token::Punct(_, b')') => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    if let Some(start) = def_start {
                        if let Some(r) = column_def_ref(stmt, tokens, sig, start, idx) {
                            refs.push(r);
                        }
                    }
                    break; // end of the column list
                }
            }
            Token::Punct(_, b',') if depth == 1 => {
                if let Some(start) = def_start.take() {
                    if let Some(r) = column_def_ref(stmt, tokens, sig, start, idx) {
                        refs.push(r);
                    }
                }
                def_start = None;
            }
            _ => {
                if depth >= 1 && def_start.is_none() {
                    def_start = Some(idx);
                }
            }
        }
    }
    refs
}

/// Extract the type reference from one column definition occupying the
/// significant-token range `[start_idx, end_idx)`.
fn column_def_ref(
    stmt: &str,
    tokens: &[Token],
    sig: &[usize],
    start_idx: usize,
    end_idx: usize,
) -> Option<TypeRef> {
    let range: Vec<usize> = sig
        .iter()
        .copied()
        .filter(|&idx| idx >= start_idx && idx < end_idx)
        .collect();
    if range.is_empty() {
        return None;
    }

    // Table-level elements have no column name.
    if let Token::Ident(s, e) = &tokens[range[0]] {
        let word = stmt[*s..*e].to_uppercase();
        if TABLE_ELEMENT_KEYWORDS.contains(&word.as_str()) {
            return None;
        }
    }
    let column = tokens[range[0]].owned_ident_text(stmt)?;

    // Type name: identifier, optionally schema-qualified (`schema.type`,
    // either part quoted).
    let &t0 = range.get(1)?;
    if !matches!(tokens[t0], Token::Ident(..) | Token::QuotedIdent(..)) {
        return None;
    }
    let first_part = tokens[t0].owned_ident_text(stmt)?;
    let (schema, name, name_end_idx) = if range.len() >= 4 && tokens[range[2]].is_punct(b'.') {
        if !matches!(tokens[range[3]], Token::Ident(..) | Token::QuotedIdent(..)) {
            return None;
        }
        (
            Some(first_part),
            tokens[range[3]].owned_ident_text(stmt)?,
            range[3],
        )
    } else {
        (None, first_part, t0)
    };

    let type_start = tokens[t0].start();
    let type_end = tokens[name_end_idx].end();
    let after = range.iter().copied().find(|&idx| idx > name_end_idx);
    let is_array = after.is_some_and(|idx| tokens[idx].is_punct(b'['));

    Some(TypeRef {
        column,
        type_span: (type_start, type_end),
        type_name: (schema, name),
        is_array,
    })
}

fn alter_table_refs(stmt: &str, tokens: &[Token], sig: &[usize]) -> Vec<TypeRef> {
    let mut refs = Vec::new();
    let mut k = 0;
    // Skip `ALTER TABLE [ONLY] name` (name may be schema-qualified/quoted).
    while k < sig.len() {
        let idx = sig[k];
        if tokens[idx].ident_eq(stmt, "TABLE") {
            k += 1;
            if k < sig.len() && tokens[sig[k]].ident_eq(stmt, "ONLY") {
                k += 1;
            }
            k = skip_qualified_name(tokens, sig, k);
            break;
        }
        k += 1;
    }

    while k < sig.len() {
        let idx = sig[k];
        if tokens[idx].ident_eq(stmt, "ADD") {
            k += 1;
            if k < sig.len() && tokens[sig[k]].ident_eq(stmt, "COLUMN") {
                k += 1;
            }
            if k < sig.len() && tokens[sig[k]].ident_eq(stmt, "IF") {
                k = skip_words(tokens, sig, stmt, k, &["IF", "NOT", "EXISTS"]);
            }
            // Constraint additions have no column type.
            if k < sig.len() {
                if let Token::Ident(s, e) = &tokens[sig[k]] {
                    let word = stmt[*s..*e].to_uppercase();
                    if TABLE_ELEMENT_KEYWORDS.contains(&word.as_str()) {
                        k += 1;
                        continue;
                    }
                }
            }
            if let Some((r, nk)) = parse_column_def_at(stmt, tokens, sig, k) {
                refs.push(r);
                k = nk;
            } else {
                k += 1;
            }
            continue;
        }
        if tokens[idx].ident_eq(stmt, "MODIFY") {
            k += 1;
            if k < sig.len() && tokens[sig[k]].ident_eq(stmt, "COLUMN") {
                k += 1;
            }
            // MariaDB: MODIFY [COLUMN] IF EXISTS col ...
            if k < sig.len() && tokens[sig[k]].ident_eq(stmt, "IF") {
                k = skip_words(tokens, sig, stmt, k, &["IF", "EXISTS"]);
            }
            if let Some((r, nk)) = parse_column_def_at(stmt, tokens, sig, k) {
                refs.push(r);
                k = nk;
            } else {
                k += 1;
            }
            continue;
        }
        if tokens[idx].ident_eq(stmt, "CHANGE") {
            k += 1;
            if k < sig.len() && tokens[sig[k]].ident_eq(stmt, "COLUMN") {
                k += 1;
            }
            // MariaDB: CHANGE [COLUMN] IF EXISTS old new ...
            if k < sig.len() && tokens[sig[k]].ident_eq(stmt, "IF") {
                k = skip_words(tokens, sig, stmt, k, &["IF", "EXISTS"]);
            }
            // CHANGE old_name new_name type ...
            if k < sig.len() {
                k += 1; // skip the old name
            }
            if let Some((r, nk)) = parse_column_def_at(stmt, tokens, sig, k) {
                refs.push(r);
                k = nk;
            } else {
                k += 1;
            }
            continue;
        }
        if tokens[idx].ident_eq(stmt, "ALTER") {
            k += 1;
            if k < sig.len() && tokens[sig[k]].ident_eq(stmt, "COLUMN") {
                k += 1;
            }
            if k >= sig.len() {
                break;
            }
            let Some(column) = tokens[sig[k]].owned_ident_text(stmt) else {
                k += 1;
                continue;
            };
            k += 1;
            // [SET DATA] TYPE new_type
            if k < sig.len() && tokens[sig[k]].ident_eq(stmt, "SET") {
                k = skip_words(tokens, sig, stmt, k, &["SET", "DATA"]);
            }
            if k < sig.len() && tokens[sig[k]].ident_eq(stmt, "TYPE") {
                k += 1;
                if k < sig.len() {
                    let t_idx = sig[k];
                    if let Some((schema, name, end_sig_idx)) =
                        read_qualified_name(tokens, sig, stmt, k)
                    {
                        let type_start = tokens[t_idx].start();
                        let type_end = tokens[end_sig_idx].end();
                        let after_tok = sig.iter().copied().find(|&i| i > end_sig_idx);
                        let is_array = after_tok.is_some_and(|i| tokens[i].is_punct(b'['));
                        refs.push(TypeRef {
                            column,
                            type_span: (type_start, type_end),
                            type_name: (schema, name),
                            is_array,
                        });
                        // Continue from a sig *position*, not a token index.
                        k = sig
                            .iter()
                            .position(|&i| i > end_sig_idx)
                            .unwrap_or(sig.len());
                        continue;
                    }
                }
            }
            continue;
        }
        k += 1;
    }
    refs
}

/// Parse `name type ...` starting at significant index `k`; returns the type
/// reference and the next *sig position* to continue from.
fn parse_column_def_at(
    stmt: &str,
    tokens: &[Token],
    sig: &[usize],
    k: usize,
) -> Option<(TypeRef, usize)> {
    if k >= sig.len() {
        return None;
    }
    let column = tokens[sig[k]].owned_ident_text(stmt)?;
    let t_idx = sig.get(k + 1).copied()?;
    if !matches!(tokens[t_idx], Token::Ident(..) | Token::QuotedIdent(..)) {
        return None;
    }
    let (schema, name, end_sig_idx) = read_qualified_name(tokens, sig, stmt, k + 1)?;
    let type_start = tokens[t_idx].start();
    let type_end = tokens[end_sig_idx].end();
    let after_tok = sig.iter().copied().find(|&i| i > end_sig_idx);
    let is_array = after_tok.is_some_and(|i| tokens[i].is_punct(b'['));
    // `next` must be a sig *position*, not a token index.
    let next = sig
        .iter()
        .position(|&i| i > end_sig_idx)
        .unwrap_or(sig.len());
    Some((
        TypeRef {
            column,
            type_span: (type_start, type_end),
            type_name: (schema, name),
            is_array,
        },
        next,
    ))
}

/// Read an optionally schema-qualified name starting at significant index k.
/// Returns `(schema, name, last_sig_index)`.
fn read_qualified_name(
    tokens: &[Token],
    sig: &[usize],
    stmt: &str,
    k: usize,
) -> Option<(Option<String>, String, usize)> {
    let first_idx = sig.get(k).copied()?;
    if !matches!(tokens[first_idx], Token::Ident(..) | Token::QuotedIdent(..)) {
        return None;
    }
    let first = tokens[first_idx].owned_ident_text(stmt)?;
    if let (Some(&dot_idx), Some(&second_idx)) = (sig.get(k + 1), sig.get(k + 2)) {
        if tokens[dot_idx].is_punct(b'.')
            && matches!(
                tokens[second_idx],
                Token::Ident(..) | Token::QuotedIdent(..)
            )
        {
            let second = tokens[second_idx].owned_ident_text(stmt)?;
            return Some((Some(first), second, second_idx));
        }
    }
    Some((None, first, first_idx))
}

fn skip_qualified_name(tokens: &[Token], sig: &[usize], k: usize) -> usize {
    let mut k = k;
    if k < sig.len() && matches!(tokens[sig[k]], Token::Ident(..) | Token::QuotedIdent(..)) {
        k += 1;
        while k + 1 < sig.len() && tokens[sig[k]].is_punct(b'.') {
            if matches!(
                tokens[sig[k + 1]],
                Token::Ident(..) | Token::QuotedIdent(..)
            ) {
                k += 2;
            } else {
                break;
            }
        }
    }
    k
}

/// Skip a fixed keyword sequence starting at `k` (case-insensitive); returns
/// the index after the last matched keyword.
fn skip_words(tokens: &[Token], sig: &[usize], stmt: &str, k: usize, words: &[&str]) -> usize {
    let mut k = k;
    for word in words {
        if k < sig.len() && tokens[sig[k]].ident_eq(stmt, word) {
            k += 1;
        } else {
            break;
        }
    }
    k
}

/// Sig position of the first token whose identifier text matches `word`.
fn first_keyword(tokens: &[Token], sig: &[usize], stmt: &str, word: &str) -> Option<usize> {
    sig.iter().position(|&idx| tokens[idx].ident_eq(stmt, word))
}

fn keyword_at(
    tokens: &[Token],
    sig: &[usize],
    stmt: &str,
    pos: usize,
    word: &str,
) -> Option<usize> {
    if pos < sig.len() && tokens[sig[pos]].ident_eq(stmt, word) {
        Some(pos)
    } else {
        None
    }
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

/// Lex a quoted string starting at `start`, terminated by `quote`. Returns
/// `(end, decoded)`; `quote quote` doubling always applies, backslash escapes
/// when enabled.
fn lex_quoted(bytes: &[u8], start: usize, quote: u8, backslash: bool) -> (usize, String) {
    fn append_raw(value: &mut String, bytes: &[u8]) {
        // `bytes` comes from a &str, so this cannot fail; fall back to
        // nothing rather than panic if it ever does.
        value.push_str(std::str::from_utf8(bytes).unwrap_or_default());
    }

    let mut value = String::new();
    let mut i = start + 1;
    let mut seg_start = i;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if backslash => {
                append_raw(&mut value, &bytes[seg_start..i]);
                if i + 1 < bytes.len() {
                    value.push(match bytes[i + 1] {
                        b'n' => '\n',
                        b't' => '\t',
                        b'0' => '\0',
                        c => c as char,
                    });
                    i += 2;
                } else {
                    i += 1;
                }
                seg_start = i;
            }
            q if q == quote => {
                if bytes.get(i + 1) == Some(&quote) {
                    append_raw(&mut value, &bytes[seg_start..i]);
                    value.push(quote as char);
                    i += 2;
                    seg_start = i;
                } else {
                    append_raw(&mut value, &bytes[seg_start..i]);
                    return (i + 1, value);
                }
            }
            _ => i += 1,
        }
    }
    append_raw(&mut value, &bytes[seg_start..]);
    (bytes.len(), value)
}

/// Lex a quoted identifier (`"..."` with `""` escape, or `` `...` `` with
/// ``` `` ``` escape). Returns `(end, decoded)`.
fn lex_delimited_ident(bytes: &[u8], start: usize, quote: u8) -> (usize, String) {
    fn append_raw(value: &mut String, bytes: &[u8]) {
        value.push_str(std::str::from_utf8(bytes).unwrap_or_default());
    }

    let mut value = String::new();
    let mut i = start + 1;
    let mut seg_start = i;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                append_raw(&mut value, &bytes[seg_start..i]);
                value.push(quote as char);
                i += 2;
                seg_start = i;
            } else {
                append_raw(&mut value, &bytes[seg_start..i]);
                return (i + 1, value);
            }
        } else {
            i += 1;
        }
    }
    append_raw(&mut value, &bytes[seg_start..]);
    (bytes.len(), value)
}

/// Lex a dollar-quoted string starting at `start` (`$tag$ ... $tag$`).
/// Returns `(inner_start, end)` where `end` is past the closing tag.
fn lex_dollar_quoted(stmt: &str, start: usize) -> Option<(usize, usize)> {
    let bytes = stmt.as_bytes();
    let mut j = start + 1;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    if j >= bytes.len() || bytes[j] != b'$' {
        return None;
    }
    let tag = &stmt[start..j + 1];
    let inner_start = j + 1;
    stmt[inner_start..]
        .find(tag)
        .map(|rel| (inner_start, inner_start + rel + tag.len()))
}

/// A column whose type is an inline MySQL `ENUM(...)` definition.
#[derive(Debug)]
pub struct InlineEnumCol {
    /// Decoded column name.
    pub column: String,
    /// Byte span of the full `ENUM('a','b')` text.
    pub span: (usize, usize),
    /// Decoded labels in order.
    pub labels: Vec<String>,
}

/// Locate inline `ENUM(...)` column types in a MySQL CREATE TABLE / ALTER
/// TABLE statement. `SET(...)` is deliberately excluded: it is multi-valued
/// and narrows to a string type via the type mapper instead.
pub fn mysql_inline_enum_columns(stmt: &str) -> Vec<InlineEnumCol> {
    let refs = table_ddl_type_refs(stmt, SqlDialect::MySql);
    if refs.is_empty() {
        return Vec::new();
    }
    let rules = LexRules::for_dialect(SqlDialect::MySql);
    let tokens = tokenize(stmt, rules);
    let mut out = Vec::new();
    for r in refs {
        if !r.type_name.1.eq_ignore_ascii_case("enum") {
            continue;
        }
        let Some(start_tok) = tokens.iter().position(|t| t.start() == r.type_span.0) else {
            continue;
        };
        // The label list opens at the next significant `(`.
        let mut i = start_tok + 1;
        while i < tokens.len() && matches!(tokens[i], Token::Whitespace(..) | Token::Comment(..)) {
            i += 1;
        }
        if i >= tokens.len() || !tokens[i].is_punct(b'(') {
            continue;
        }
        let mut depth = 0usize;
        let mut labels = Vec::new();
        let mut end = None;
        let mut j = i;
        while j < tokens.len() {
            match &tokens[j] {
                Token::Punct(_, b'(') => depth += 1,
                Token::Punct(_, b')') => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(tokens[j].end());
                        break;
                    }
                }
                Token::Str(_, _, v) if depth == 1 => labels.push(v.clone()),
                _ => {}
            }
            j += 1;
        }
        if let Some(end) = end {
            out.push(InlineEnumCol {
                column: r.column,
                span: (r.type_span.0, end),
                labels,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg_refs(stmt: &str) -> Vec<TypeRef> {
        table_ddl_type_refs(stmt, SqlDialect::Postgres)
    }

    fn mysql_refs(stmt: &str) -> Vec<TypeRef> {
        table_ddl_type_refs(stmt, SqlDialect::MySql)
    }

    #[test]
    fn create_table_columns_and_types() {
        let refs = pg_refs("CREATE TABLE t (id int, mood mood NOT NULL, s public.status);");
        assert_eq!(refs.len(), 3);
        assert_eq!(refs[0].column, "id");
        assert_eq!(refs[1].column, "mood");
        assert_eq!(refs[1].type_name, (None, "mood".into()));
        assert_eq!(refs[2].type_name, (Some("public".into()), "status".into()));
        assert!(!refs[1].is_array);
    }

    #[test]
    fn column_named_same_as_type() {
        let stmt = "CREATE TABLE person (id int, mood mood NOT NULL);";
        let refs = pg_refs(stmt);
        let mood = refs.iter().find(|r| r.column == "mood").unwrap();
        assert_eq!(&stmt[mood.type_span.0..mood.type_span.1], "mood");
    }

    #[test]
    fn table_level_constraints_are_skipped() {
        let refs = pg_refs(
            "CREATE TABLE t (a mood, CONSTRAINT mood CHECK (a IS NOT NULL), PRIMARY KEY (a), UNIQUE (a));",
        );
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].column, "a");
    }

    #[test]
    fn comments_and_literals_do_not_confuse_the_walk() {
        let stmt = "CREATE TABLE t (/* (fake mood */ a text DEFAULT 'mood (, fake', m mood);";
        let refs = pg_refs(stmt);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].column, "a");
        assert_eq!(refs[1].column, "m");
        assert_eq!(refs[1].type_name, (None, "mood".into()));
    }

    #[test]
    fn dollar_quoted_default_does_not_corrupt() {
        let stmt = "CREATE TABLE t (a text DEFAULT $$(, fake mood$$, m mood);";
        let refs = pg_refs(stmt);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[1].column, "m");
        assert_eq!(refs[1].type_name, (None, "mood".into()));
    }

    #[test]
    fn quoted_identifiers_and_non_ascii() {
        let refs =
            pg_refs("CREATE TABLE t (\"it's\" text, ståvus muud, \"Order Status\" \"My Type\");");
        assert_eq!(refs.len(), 3);
        assert_eq!(refs[0].column, "it's");
        assert_eq!(refs[1].column, "ståvus");
        assert_eq!(refs[1].type_name, (None, "muud".into()));
        assert_eq!(refs[2].column, "Order Status");
        assert_eq!(refs[2].type_name, (None, "My Type".into()));
    }

    #[test]
    fn array_suffix_detected() {
        let refs = pg_refs("CREATE TABLE t (m mood[]);");
        assert_eq!(refs.len(), 1);
        assert!(refs[0].is_array);
    }

    #[test]
    fn alter_table_add_column_pg() {
        let refs = pg_refs("ALTER TABLE t ADD COLUMN m mood;");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].column, "m");
        assert_eq!(refs[0].type_name, (None, "mood".into()));
    }

    #[test]
    fn alter_table_alter_column_type_pg() {
        let refs = pg_refs("ALTER TABLE t ALTER COLUMN m TYPE mood;");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].column, "m");
        assert_eq!(refs[0].type_name, (None, "mood".into()));
    }

    #[test]
    fn alter_table_add_constraint_is_not_a_column() {
        let refs = pg_refs("ALTER TABLE t ADD CONSTRAINT mood CHECK (a > 0);");
        assert!(refs.is_empty());
    }

    #[test]
    fn alter_table_modify_column_mysql() {
        let refs = mysql_refs("ALTER TABLE `t` MODIFY COLUMN `status` ENUM('a','b');");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].column, "status");
    }

    #[test]
    fn mysql_backtick_quoting() {
        let refs = mysql_refs("CREATE TABLE `users` (`status` ENUM('a','b') NOT NULL);");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].column, "status");
    }

    #[test]
    fn e_string_with_backslash_quote() {
        let stmt = "CREATE TABLE t (a text DEFAULT E'back\\'slash', m mood);";
        let refs = pg_refs(stmt);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[1].column, "m");
        assert_eq!(refs[1].type_name, (None, "mood".into()));
    }

    #[test]
    fn mysql_backslash_escapes_keep_columns_in_sync() {
        let refs = mysql_refs("CREATE TABLE t (v ENUM('a\\'b','c'), w SET('x','z'));");
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].column, "v");
        assert_eq!(refs[1].column, "w");
    }

    #[test]
    fn inline_enum_columns_labels_and_span() {
        let stmt = "CREATE TABLE t (id INT, v ENUM('a)','b'), w ENUM('it''s','x\\'y'));";
        let cols = mysql_inline_enum_columns(stmt);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].column, "v");
        assert_eq!(cols[0].labels, vec!["a)", "b"]);
        assert_eq!(&stmt[cols[0].span.0..cols[0].span.1], "ENUM('a)','b')");
        assert_eq!(cols[1].column, "w");
        assert_eq!(cols[1].labels, vec!["it's", "x'y"]);
    }

    #[test]
    fn inline_enum_columns_skips_set() {
        let cols = mysql_inline_enum_columns("CREATE TABLE t (s SET('a','b'));");
        assert!(cols.is_empty());
    }

    #[test]
    fn inline_enum_columns_alter_modify() {
        let cols = mysql_inline_enum_columns("ALTER TABLE t MODIFY COLUMN s ENUM('a','b');");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].column, "s");
        assert_eq!(cols[0].labels, vec!["a", "b"]);
    }

    #[test]
    fn inline_enum_columns_ignores_enum_in_string() {
        let cols =
            mysql_inline_enum_columns("CREATE TABLE t (a TEXT DEFAULT 'ENUM(''x'')', b TEXT);");
        assert!(cols.is_empty());
    }

    #[test]
    fn inline_enum_columns_multiline_with_leading_newline() {
        let stmt = "\nCREATE TABLE `users` (\n  `status` ENUM('active', 'inactive')\n);\n";
        let cols = mysql_inline_enum_columns(stmt);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].column, "status");
        assert_eq!(cols[0].labels, vec!["active", "inactive"]);
    }
}
