//! Parsing helpers for enum type syntax in PostgreSQL and MySQL.

use once_cell::sync::Lazy;
use regex::Regex;

static RE_PG_CREATE_ENUM: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)CREATE\s+TYPE\s+(?:(?P<schema>\w+)\.)?(?P<name>\w+)\s+AS\s+ENUM\s*\((?P<labels>[^)]*)\)",
    )
    .unwrap()
});

static RE_PG_ALTER_ENUM: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)ALTER\s+TYPE\s+(?:(?P<schema>\w+)\.)?(?P<name>\w+)\s+ADD\s+VALUE\s+'(?P<value>(?:[^']|'')*)'\s*(?:(?P<position>BEFORE|AFTER)\s+'(?P<existing>(?:[^']|'')*)')?",
    )
    .unwrap()
});

static RE_MYSQL_INLINE_ENUM: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bENUM\s*\((?P<labels>[^)]*(?:\([^)]*\)[^)]*)*)\)").unwrap());

pub fn pg_create_enum_name(stmt: &str) -> Option<String> {
    RE_PG_CREATE_ENUM.captures(stmt).map(|caps| {
        let name = caps.name("name").unwrap().as_str().trim_matches('"');
        // Preserve schema prefix to avoid collisions across schemas
        if let Some(schema) = caps.name("schema") {
            format!("{}.{}", schema.as_str().trim_matches('"'), name)
        } else {
            name.to_string()
        }
    })
}

pub fn pg_create_enum_labels(stmt: &str) -> Option<Vec<String>> {
    RE_PG_CREATE_ENUM
        .captures(stmt)
        .map(|caps| parse_enum_labels(caps.name("labels").unwrap().as_str()))
}

pub fn pg_add_enum_value(stmt: &str) -> Option<(String, Option<(String, bool)>)> {
    RE_PG_ALTER_ENUM.captures(stmt).map(|caps| {
        let value = caps.name("value").unwrap().as_str().replace("''", "'");
        let position = caps.name("position").map(|p| {
            let existing = caps.name("existing").unwrap().as_str().replace("''", "'");
            let is_after = p.as_str().eq_ignore_ascii_case("AFTER");
            (existing, is_after)
        });
        (value, position)
    })
}

pub fn pg_alter_enum_name(stmt: &str) -> Option<String> {
    RE_PG_ALTER_ENUM.captures(stmt).map(|caps| {
        caps.name("name").unwrap().as_str().to_string()
    })
}

pub fn mysql_inline_enum_labels(stmt: &str) -> Vec<(usize, Vec<String>)> {
    let mut results = Vec::new();
    for caps in RE_MYSQL_INLINE_ENUM.captures_iter(stmt) {
        let full = caps.get(0).unwrap();
        let labels = parse_enum_labels(caps.name("labels").unwrap().as_str());
        results.push((full.start(), labels));
    }
    results
}

/// Extract the column name immediately preceding `offset` in a CREATE TABLE
/// statement body. Looks backward from `offset` to find a backtick-quoted,
/// double-quoted, or bare identifier.
pub fn extract_column_name_before(stmt: &str, enum_offset: usize) -> Option<String> {
    let prefix = &stmt[..enum_offset];
    let prefix_bytes = prefix.as_bytes();
    let mut i = prefix_bytes.len();
    // Skip whitespace
    while i > 0 && prefix_bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    if i == 0 {
        return None;
    }
    // Check for backtick-quoted identifier
    if prefix_bytes[i - 1] == b'`' {
        let end = i - 1;
        i -= 1;
        while i > 0 && prefix_bytes[i - 1] != b'`' {
            i -= 1;
        }
        if i > 0 && prefix_bytes[i - 1] == b'`' {
            return Some(prefix[i..end].to_string());
        }
    }
    // Check for double-quoted identifier
    if prefix_bytes[i - 1] == b'"' {
        let end = i - 1;
        i -= 1;
        while i > 0 && prefix_bytes[i - 1] != b'"' {
            i -= 1;
        }
        if i > 0 && prefix_bytes[i - 1] == b'"' {
            return Some(prefix[i..end].to_string());
        }
    }
    // Bare identifier (alphanumeric + underscore, including non-ASCII).
    // MySQL/PG allow non-ASCII characters in unquoted identifiers.
    let end = i;
    while i > 0 {
        let b = prefix_bytes[i - 1];
        if b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80 {
            i -= 1;
        } else {
            break;
        }
    }
    if i < end {
        return Some(prefix[i..end].to_string());
    }
    None
}

pub fn parse_enum_labels(labels_str: &str) -> Vec<String> {
    let chars: Vec<char> = labels_str.chars().collect();
    let n = chars.len();
    let mut labels = Vec::new();
    let mut i = 0;

    while i < n {
        if chars[i] == '\'' {
            i += 1; // skip opening quote
            let mut label = String::new();
            while i < n {
                if chars[i] == '\'' {
                    if i + 1 < n && chars[i + 1] == '\'' {
                        label.push('\'');
                        i += 2;
                    } else {
                        // closing quote
                        i += 1;
                        break;
                    }
                } else {
                    label.push(chars[i]);
                    i += 1;
                }
            }
            labels.push(label);
        } else {
            i += 1;
        }
    }

    labels
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_labels() {
        let result = parse_enum_labels("'a','b','c'");
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_escaped_quotes() {
        let result = parse_enum_labels("'it''s'");
        assert_eq!(result, vec!["it's"]);
    }

    #[test]
    fn test_parse_empty_enum() {
        let result = parse_enum_labels("");
        assert_eq!(result, Vec::<String>::new());
    }

    #[test]
    fn test_parse_unicode_labels() {
        let result = parse_enum_labels("'\u{2705}', '\u{274c}'");
        assert_eq!(result, vec!["\u{2705}", "\u{274c}"]);
    }

    #[test]
    fn test_pg_create_enum_name() {
        let result = pg_create_enum_name("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
        assert_eq!(result, Some("mood".to_string()));

        let result = pg_create_enum_name("CREATE TYPE public.mood AS ENUM ('sad', 'ok', 'happy');");
        assert_eq!(result, Some("public.mood".to_string()));

        let result = pg_create_enum_name("CREATE TABLE foo (id int);");
        assert_eq!(result, None);
    }

    #[test]
    fn test_pg_create_enum_labels() {
        let result = pg_create_enum_labels("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
        assert_eq!(
            result,
            Some(vec![
                "sad".to_string(),
                "ok".to_string(),
                "happy".to_string()
            ])
        );
    }

    #[test]
    fn test_pg_add_enum_value() {
        let result = pg_add_enum_value("ALTER TYPE mood ADD VALUE 'excited' AFTER 'happy';");
        assert_eq!(
            result,
            Some(("excited".to_string(), Some(("happy".to_string(), true))))
        );

        let result = pg_add_enum_value("ALTER TYPE mood ADD VALUE 'sad' BEFORE 'ok';");
        assert_eq!(
            result,
            Some(("sad".to_string(), Some(("ok".to_string(), false))))
        );

        let result = pg_add_enum_value("ALTER TYPE mood ADD VALUE 'neutral';");
        assert_eq!(result, Some(("neutral".to_string(), None)));
    }

    #[test]
    fn test_mysql_inline_enum_labels() {
        let result =
            mysql_inline_enum_labels("CREATE TABLE t (status ENUM('active','inactive') NOT NULL);");
        assert_eq!(result.len(), 1);
        assert_eq!(&result[0].1, &["active", "inactive"]);

        let result = mysql_inline_enum_labels("CREATE TABLE t (a ENUM('x','y'), b ENUM('1','2'));");
        assert_eq!(result.len(), 2);
        assert_eq!(&result[0].1, &["x", "y"]);
        assert_eq!(&result[1].1, &["1", "2"]);
    }

    #[test]
    fn test_mysql_inline_enum_single_quote() {
        let result =
            mysql_inline_enum_labels("CREATE TABLE t (status ENUM('it''s','ok') NOT NULL);");
        assert_eq!(result.len(), 1);
        assert_eq!(&result[0].1, &["it's", "ok"]);
    }

    #[test]
    fn test_parse_enum_labels_with_spaces() {
        let result = parse_enum_labels("'a', 'b' , 'c'");
        assert_eq!(result, vec!["a", "b", "c"]);
    }
}
