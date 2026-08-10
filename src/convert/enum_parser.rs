//! Parsing helpers for PostgreSQL enum type syntax.

use super::ddl::{self, LexRules, Token};
use crate::parser::SqlDialect;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedCreateEnum {
    pub(crate) name: String,
    pub(crate) labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedAlterEnum {
    pub(crate) name: String,
    pub(crate) value: String,
    pub(crate) position: Option<(String, bool)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedRenameEnum {
    Type {
        old_name: String,
        new_name: String,
    },
    Value {
        name: String,
        old_value: String,
        new_value: String,
    },
}

pub(crate) fn pg_create_enum(stmt: &str) -> Option<ParsedCreateEnum> {
    parse_create_enum(stmt)
}

pub(crate) fn pg_add_enum_value(stmt: &str) -> Option<ParsedAlterEnum> {
    parse_alter_enum(stmt)
}

pub(crate) fn pg_rename_enum(stmt: &str) -> Option<ParsedRenameEnum> {
    let tokens = ddl::tokenize(stmt, postgres_rules());
    let sig = significant(&tokens);
    let mut pos = 0;
    keyword(stmt, &tokens, &sig, &mut pos, "ALTER")?;
    keyword(stmt, &tokens, &sig, &mut pos, "TYPE")?;
    let (name, next) = qualified_name(stmt, &tokens, &sig, pos)?;
    pos = next;
    keyword(stmt, &tokens, &sig, &mut pos, "RENAME")?;
    if ident_at(stmt, &tokens, &sig, pos, "VALUE") {
        pos += 1;
        let old_value = string_at(stmt, &tokens, &sig, &mut pos)?;
        keyword(stmt, &tokens, &sig, &mut pos, "TO")?;
        let new_value = string_at(stmt, &tokens, &sig, &mut pos)?;
        trailing_semicolon_only(&tokens, &sig, pos)?;
        Some(ParsedRenameEnum::Value {
            name,
            old_value,
            new_value,
        })
    } else {
        keyword(stmt, &tokens, &sig, &mut pos, "TO")?;
        let (new_name, next) = qualified_name(stmt, &tokens, &sig, pos)?;
        trailing_semicolon_only(&tokens, &sig, next)?;
        Some(ParsedRenameEnum::Type {
            old_name: name,
            new_name,
        })
    }
}

/// Return the registry key for a PostgreSQL type reference.
///
/// Unquoted identifiers are case-folded. Quoted identifiers retain their case
/// and quoting so `mood`, `"Mood"`, and `"mood"` remain distinct.
pub(crate) fn pg_type_keys(type_name: &str) -> Option<(String, String)> {
    let tokens = ddl::tokenize(type_name, postgres_rules());
    let sig = significant(&tokens);
    let (name, next) = qualified_name(type_name, &tokens, &sig, 0)?;
    if next != sig.len() {
        return None;
    }
    let name_pos = if next == 3 { 2 } else { 0 };
    let unqualified = ident_key(type_name, tokens.get(*sig.get(name_pos)?)?)?;
    Some((name, unqualified))
}

pub(crate) fn pg_type_unqualified_key(type_name: &str) -> Option<String> {
    pg_type_keys(type_name).map(|(_, unqualified)| unqualified)
}

fn parse_create_enum(stmt: &str) -> Option<ParsedCreateEnum> {
    let tokens = ddl::tokenize(stmt, postgres_rules());
    let sig = significant(&tokens);
    let mut pos = 0;
    keyword(stmt, &tokens, &sig, &mut pos, "CREATE")?;
    keyword(stmt, &tokens, &sig, &mut pos, "TYPE")?;
    let (name, next) = qualified_name(stmt, &tokens, &sig, pos)?;
    pos = next;
    keyword(stmt, &tokens, &sig, &mut pos, "AS")?;
    keyword(stmt, &tokens, &sig, &mut pos, "ENUM")?;
    punct(&tokens, &sig, &mut pos, b'(')?;

    let mut labels = Vec::new();
    loop {
        let token = tokens.get(*sig.get(pos)?)?;
        match token {
            Token::Str(_, _, value) => {
                labels.push(value.clone());
                pos += 1;
            }
            Token::Ident(_, _) if token.ident_eq(stmt, "E") => pos += 1,
            Token::Ident(_, _) if token.ident_eq(stmt, "U") => {
                let amp = tokens.get(*sig.get(pos + 1)?)?;
                let Token::Other(start, end) = amp else {
                    return None;
                };
                if &stmt[*start..*end] != "&" {
                    return None;
                }
                let Token::Str(_, _, value) = tokens.get(*sig.get(pos + 2)?)? else {
                    return None;
                };
                let mut escape = '\\';
                let mut consumed = 3;
                if ident_at(stmt, &tokens, &sig, pos + 3, "UESCAPE") {
                    let Token::Str(_, _, escape_value) = tokens.get(*sig.get(pos + 4)?)? else {
                        return None;
                    };
                    let mut chars = escape_value.chars();
                    escape = chars.next()?;
                    if chars.next().is_some() {
                        return None;
                    }
                    consumed = 5;
                }
                labels.push(decode_unicode_escape_string(value, escape)?);
                pos += consumed;
            }
            Token::Punct(_, b',') => pos += 1,
            Token::Punct(_, b')') => {
                pos += 1;
                break;
            }
            _ => return None,
        }
    }
    trailing_semicolon_only(&tokens, &sig, pos)?;
    Some(ParsedCreateEnum { name, labels })
}

fn decode_unicode_escape_string(value: &str, escape: char) -> Option<String> {
    let chars: Vec<char> = value.chars().collect();
    let mut result = String::with_capacity(value.len());
    let mut index = 0;
    while index < chars.len() {
        if chars[index] != escape {
            result.push(chars[index]);
            index += 1;
            continue;
        }
        if chars.get(index + 1) == Some(&escape) {
            result.push(escape);
            index += 2;
            continue;
        }
        let extended = chars.get(index + 1) == Some(&'+');
        let digits = if extended { 6 } else { 4 };
        let start = index + if extended { 2 } else { 1 };
        let end = start + digits;
        let hex: String = chars.get(start..end)?.iter().collect();
        let mut codepoint = u32::from_str_radix(&hex, 16).ok()?;
        index = end;
        if (0xD800..=0xDBFF).contains(&codepoint) {
            if chars.get(index) != Some(&escape) {
                return None;
            }
            let low_start = index + 1;
            let low_end = low_start + 4;
            let low_hex: String = chars.get(low_start..low_end)?.iter().collect();
            let low = u32::from_str_radix(&low_hex, 16).ok()?;
            if !(0xDC00..=0xDFFF).contains(&low) {
                return None;
            }
            codepoint = 0x10000 + ((codepoint - 0xD800) << 10) + (low - 0xDC00);
            index = low_end;
        }
        result.push(char::from_u32(codepoint)?);
    }
    Some(result)
}

fn parse_alter_enum(stmt: &str) -> Option<ParsedAlterEnum> {
    let tokens = ddl::tokenize(stmt, postgres_rules());
    let sig = significant(&tokens);
    let mut pos = 0;
    keyword(stmt, &tokens, &sig, &mut pos, "ALTER")?;
    keyword(stmt, &tokens, &sig, &mut pos, "TYPE")?;
    let (name, next) = qualified_name(stmt, &tokens, &sig, pos)?;
    pos = next;
    keyword(stmt, &tokens, &sig, &mut pos, "ADD")?;
    keyword(stmt, &tokens, &sig, &mut pos, "VALUE")?;
    if ident_at(stmt, &tokens, &sig, pos, "IF") {
        pos += 1;
        keyword(stmt, &tokens, &sig, &mut pos, "NOT")?;
        keyword(stmt, &tokens, &sig, &mut pos, "EXISTS")?;
    }
    let value = string_at(stmt, &tokens, &sig, &mut pos)?;
    let position = if ident_at(stmt, &tokens, &sig, pos, "BEFORE")
        || ident_at(stmt, &tokens, &sig, pos, "AFTER")
    {
        let is_after = ident_at(stmt, &tokens, &sig, pos, "AFTER");
        pos += 1;
        Some((string_at(stmt, &tokens, &sig, &mut pos)?, is_after))
    } else {
        None
    };
    trailing_semicolon_only(&tokens, &sig, pos)?;
    Some(ParsedAlterEnum {
        name,
        value,
        position,
    })
}

fn postgres_rules() -> LexRules {
    LexRules::for_dialect(SqlDialect::Postgres)
}

fn significant(tokens: &[Token]) -> Vec<usize> {
    tokens
        .iter()
        .enumerate()
        .filter_map(|(index, token)| {
            (!matches!(token, Token::Whitespace(..) | Token::Comment(..))).then_some(index)
        })
        .collect()
}

fn keyword(
    stmt: &str,
    tokens: &[Token],
    sig: &[usize],
    pos: &mut usize,
    expected: &str,
) -> Option<()> {
    ident_at(stmt, tokens, sig, *pos, expected).then(|| *pos += 1)
}

fn ident_at(stmt: &str, tokens: &[Token], sig: &[usize], pos: usize, expected: &str) -> bool {
    sig.get(pos)
        .and_then(|index| tokens.get(*index))
        .is_some_and(|token| token.ident_eq(stmt, expected))
}

fn punct(tokens: &[Token], sig: &[usize], pos: &mut usize, expected: u8) -> Option<()> {
    sig.get(*pos)
        .and_then(|index| tokens.get(*index))
        .is_some_and(|token| token.is_punct(expected))
        .then(|| *pos += 1)
}

fn string_at(stmt: &str, tokens: &[Token], sig: &[usize], pos: &mut usize) -> Option<String> {
    if ident_at(stmt, tokens, sig, *pos, "E") {
        *pos += 1;
    }
    let Token::Str(_, _, value) = tokens.get(*sig.get(*pos)?)? else {
        return None;
    };
    *pos += 1;
    Some(value.clone())
}

fn qualified_name(
    stmt: &str,
    tokens: &[Token],
    sig: &[usize],
    pos: usize,
) -> Option<(String, usize)> {
    let first = ident_key(stmt, tokens.get(*sig.get(pos)?)?)?;
    if sig
        .get(pos + 1)
        .and_then(|index| tokens.get(*index))
        .is_some_and(|token| token.is_punct(b'.'))
    {
        let second = ident_key(stmt, tokens.get(*sig.get(pos + 2)?)?)?;
        Some((format!("{first}.{second}"), pos + 3))
    } else {
        Some((first, pos + 1))
    }
}

fn ident_key(stmt: &str, token: &Token) -> Option<String> {
    match token {
        Token::Ident(start, end) => Some(stmt[*start..*end].to_lowercase()),
        Token::QuotedIdent(_, _, value) => Some(format!("\"{}\"", value.replace('"', "\"\""))),
        _ => None,
    }
}

fn trailing_semicolon_only(tokens: &[Token], sig: &[usize], pos: usize) -> Option<()> {
    match sig.get(pos..) {
        Some([]) => Some(()),
        Some([index]) if tokens[*index].is_punct(b';') => Some(()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_enum_names_and_labels() {
        let stmt =
            "CREATE TYPE \"My Schema\".\"Order \"\"Status\" AS ENUM ('new', E'line\\n', $$cash$$);";
        assert_eq!(
            pg_create_enum(stmt),
            Some(ParsedCreateEnum {
                name: "\"My Schema\".\"Order \"\"Status\"".to_string(),
                labels: vec!["new".into(), "line\n".into(), "cash".into()]
            })
        );
    }

    #[test]
    fn does_not_match_enum_ddl_inside_other_sql() {
        assert_eq!(
            pg_create_enum("SELECT 'CREATE TYPE fake AS ENUM (''a'')';"),
            None
        );
        assert_eq!(
            pg_add_enum_value("SELECT 'ALTER TYPE mood ADD VALUE ''bad''';"),
            None
        );
    }

    #[test]
    fn parses_add_value() {
        let stmt = "ALTER TYPE \"My Schema\".mood ADD VALUE IF NOT EXISTS 'new' AFTER 'old';";
        assert_eq!(
            pg_add_enum_value(stmt),
            Some(ParsedAlterEnum {
                name: "\"My Schema\".mood".into(),
                value: "new".into(),
                position: Some(("old".into(), true))
            })
        );
    }

    #[test]
    fn type_keys_preserve_quoted_identifier_semantics() {
        assert_eq!(
            pg_type_keys("MOOD").as_ref().map(|keys| keys.0.as_str()),
            Some("mood")
        );
        assert_eq!(
            pg_type_keys("\"Mood\"")
                .as_ref()
                .map(|keys| keys.0.as_str()),
            Some("\"Mood\"")
        );
        assert_eq!(
            pg_type_keys("\"mood\"")
                .as_ref()
                .map(|keys| keys.0.as_str()),
            Some("\"mood\"")
        );
    }
}
