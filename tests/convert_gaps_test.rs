//! Coverage-gap tests for `convert::mod` through the public API.

use sql_splitter::convert::{run, ConvertConfig, ConvertWarning, Converter, EnumNamingStrategy};
use sql_splitter::parser::SqlDialect;

fn convert(converter: &mut Converter, stmt: &str) -> String {
    String::from_utf8(converter.convert_statement(stmt.as_bytes()).unwrap()).unwrap()
}

fn feature_of(w: &ConvertWarning) -> &str {
    match w {
        ConvertWarning::UnsupportedFeature { feature, .. } => feature,
        other => panic!("expected UnsupportedFeature, got {other:?}"),
    }
}

#[test]
fn convert_config_default() {
    let config = ConvertConfig::default();
    assert_eq!(config.input, std::path::PathBuf::new());
    assert_eq!(config.output, None);
    assert_eq!(config.from_dialect, None);
    assert_eq!(config.to_dialect, SqlDialect::Postgres);
    assert!(!config.dry_run);
    assert!(!config.progress);
    assert!(!config.strict);
    assert_eq!(config.enum_naming, EnumNamingStrategy::PerColumn);
}

#[test]
fn pg_enum_column_is_narrowed_for_sqlite_and_mssql() {
    for (to, replacement) in [
        (SqlDialect::Sqlite, "TEXT"),
        (SqlDialect::Mssql, "NVARCHAR(255)"),
    ] {
        let mut c = Converter::new(SqlDialect::Postgres, to);
        assert_eq!(convert(&mut c, "CREATE TYPE mood AS ENUM ('a','b');"), "");
        let out = convert(&mut c, "CREATE TABLE t (m mood NOT NULL, q public.mood);");
        assert!(
            out.contains(&format!("m {replacement} NOT NULL")),
            "{to:?}: {out}"
        );
        assert!(out.contains(&format!("q {replacement}")), "{to:?}: {out}");
        let lossy: Vec<_> = c
            .warnings()
            .iter()
            .filter_map(|w| match w {
                ConvertWarning::LossyConversion {
                    from_type,
                    to_type,
                    table,
                    column,
                } => Some((
                    from_type.clone(),
                    to_type.clone(),
                    table.clone(),
                    column.clone(),
                )),
                _ => None,
            })
            .collect();
        // Columns are rewritten last-to-first and the collector dedupes on
        // (from_type, to_type), so only the first (`q`) warning survives.
        assert_eq!(
            lossy,
            vec![(
                "ENUM (mood)".to_string(),
                replacement.to_string(),
                Some("t".to_string()),
                Some("q".to_string())
            )],
            "{to:?}"
        );
    }
}

#[test]
fn ambiguous_unqualified_pg_enum_reference_warns_and_fails_strict() {
    for strict in [false, true] {
        let mut c = Converter::new(SqlDialect::Postgres, SqlDialect::MySql).with_strict(strict);
        convert(&mut c, "CREATE TYPE a.mood AS ENUM ('x');");
        convert(&mut c, "CREATE TYPE b.mood AS ENUM ('y');");
        let result = c.convert_statement(b"CREATE TABLE t (m mood);");
        let expected = "ambiguous unqualified PostgreSQL enum type `mood`";
        if strict {
            assert_eq!(feature_of(&result.unwrap_err()), expected);
        } else {
            // Left untouched: no inline ENUM is substituted.
            assert_eq!(result.unwrap(), b"CREATE TABLE t (m mood);");
        }
        assert_eq!(feature_of(&c.warnings()[0]), expected);
    }
}

#[test]
fn process_copy_data_passes_through_without_pending_header() {
    let mut c = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    assert!(!c.has_pending_copy());
    assert_eq!(c.process_copy_data(b"raw").unwrap(), vec![b"raw".to_vec()]);
}

#[test]
fn process_copy_data_converts_block_after_copy_header() {
    let mut c = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    assert_eq!(convert(&mut c, "COPY t (a, b) FROM stdin;"), "");
    assert!(c.has_pending_copy());
    let inserts = c.process_copy_data(b"1\tx\n2\t\\N\n\\.\n").unwrap();
    let inserts: Vec<String> = inserts
        .into_iter()
        .map(|i| String::from_utf8(i).unwrap())
        .collect();
    assert_eq!(inserts.len(), 1, "{inserts:?}");
    assert!(
        inserts[0].starts_with("INSERT INTO `t` (`a`, `b`) VALUES"),
        "{inserts:?}"
    );
    assert_eq!(
        inserts[0],
        "INSERT INTO `t` (`a`, `b`) VALUES\n('1', 'x'),\n('2', NULL);"
    );
    assert!(!c.has_pending_copy());
    // The header is consumed: a second block passes through untouched.
    assert_eq!(c.process_copy_data(b"raw").unwrap(), vec![b"raw".to_vec()]);
}

#[test]
fn process_copy_data_same_target_dialect_passes_through() {
    let mut c = Converter::new(SqlDialect::Postgres, SqlDialect::Postgres);
    convert(&mut c, "COPY t (a) FROM stdin;");
    assert_eq!(
        c.process_copy_data(b"1\n\\.\n").unwrap(),
        vec![b"1\n\\.\n".to_vec()]
    );
}

#[test]
fn create_index_mentioning_fulltext_warns_and_fails_strict() {
    let stmt = "CREATE INDEX idx_fulltext ON t (c);";
    let mut c = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    assert_eq!(convert(&mut c, stmt), stmt);
    assert_eq!(c.warnings().len(), 1);
    assert_eq!(feature_of(&c.warnings()[0]), "FULLTEXT INDEX");

    let mut c = Converter::new(SqlDialect::MySql, SqlDialect::Postgres).with_strict(true);
    let err = c.convert_statement(stmt.as_bytes()).unwrap_err();
    assert!(matches!(
        err,
        ConvertWarning::UnsupportedFeature { ref feature, suggestion: None } if feature == "FULLTEXT INDEX"
    ));
}

#[test]
fn mysql_change_column_with_enum_is_skipped() {
    let stmt = "ALTER TABLE t CHANGE COLUMN a b ENUM('x','y');";
    for strict in [false, true] {
        let mut c = Converter::new(SqlDialect::MySql, SqlDialect::Postgres).with_strict(strict);
        let result = c.convert_statement(stmt.as_bytes());
        let check = |w: &ConvertWarning| match w {
            ConvertWarning::SkippedStatement {
                reason,
                statement_preview,
            } => {
                assert_eq!(
                    reason,
                    "MySQL CHANGE COLUMN with ENUM requires separate rename and type operations"
                );
                assert_eq!(statement_preview, stmt);
            }
            other => panic!("{other:?}"),
        };
        if strict {
            check(&result.unwrap_err());
        } else {
            assert_eq!(result.unwrap(), b"");
        }
        check(&c.warnings()[0]);
    }
}

#[test]
fn alter_type_add_value_before_and_after_positions() {
    let cases = [
        (
            "ALTER TYPE mood ADD VALUE 'b' BEFORE 'c';",
            "ENUM('a','b','c')",
        ),
        (
            "ALTER TYPE mood ADD VALUE 'b' AFTER 'a';",
            "ENUM('a','b','c')",
        ),
        // Unknown anchors append.
        (
            "ALTER TYPE mood ADD VALUE 'b' BEFORE 'zzz';",
            "ENUM('a','c','b')",
        ),
        (
            "ALTER TYPE mood ADD VALUE 'b' AFTER 'zzz';",
            "ENUM('a','c','b')",
        ),
        // Already present: unchanged.
        ("ALTER TYPE mood ADD VALUE 'a' AFTER 'c';", "ENUM('a','c')"),
    ];
    for (alter, expected) in cases {
        let mut c = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
        convert(&mut c, "CREATE TYPE mood AS ENUM ('a','c');");
        assert_eq!(convert(&mut c, alter), "");
        assert_eq!(
            convert(&mut c, "CREATE TABLE t (m mood);"),
            format!("CREATE TABLE t (m {expected});"),
            "{alter}"
        );
        assert!(c.warnings().is_empty(), "{alter}: {:?}", c.warnings());
    }
}

#[test]
fn alter_type_add_value_on_unknown_enum_warns_and_fails_strict() {
    let expected = "ALTER TYPE references unknown enum `nope`";
    for strict in [false, true] {
        let mut c = Converter::new(SqlDialect::Postgres, SqlDialect::MySql).with_strict(strict);
        let result = c.convert_statement(b"ALTER TYPE nope ADD VALUE 'x';");
        if strict {
            assert_eq!(feature_of(&result.unwrap_err()), expected);
        } else {
            assert_eq!(result.unwrap(), b"");
        }
        assert_eq!(feature_of(&c.warnings()[0]), expected);
    }
}

#[test]
fn alter_type_rename_on_unknown_enum_warns_and_fails_strict() {
    let expected = "ALTER TYPE RENAME references an unknown enum or value";
    for stmt in [
        "ALTER TYPE nope RENAME TO other;",
        "ALTER TYPE nope RENAME VALUE 'a' TO 'b';",
    ] {
        for strict in [false, true] {
            let mut c = Converter::new(SqlDialect::Postgres, SqlDialect::MySql).with_strict(strict);
            let result = c.convert_statement(stmt.as_bytes());
            if strict {
                assert_eq!(feature_of(&result.unwrap_err()), expected, "{stmt}");
            } else {
                assert_eq!(result.unwrap(), b"", "{stmt}");
            }
            assert_eq!(feature_of(&c.warnings()[0]), expected, "{stmt}");
        }
    }
}

#[test]
fn mysql_escape_sequences_are_decoded_for_standard_targets() {
    let stmt = r#"INSERT INTO t VALUES ('a\nb\rc\td\0e\bf\Zg\%h\_i\qj\\k\'l\"m');"#;
    let expected = "'a\nb\rc\td\0e\u{8}f\u{1a}g\\%h\\_iqj\\k''l\"m'";
    for to in [SqlDialect::Postgres, SqlDialect::Sqlite, SqlDialect::Mssql] {
        let mut c = Converter::new(SqlDialect::MySql, to);
        let out = convert(&mut c, stmt);
        assert!(out.contains(expected), "{to:?}: {out:?}");
    }
    // Trailing backslash at end of input is kept.
    let mut c = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    let out = convert(&mut c, r"INSERT INTO t VALUES ('a\");
    assert!(out.ends_with(r"'a\"), "{out:?}");
}

#[test]
fn conditional_comments_are_stripped_and_regular_comments_kept() {
    let mut c = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    assert_eq!(
        convert(&mut c, "/* keep */ SELECT 1 /*!50001 x /* nested */ y */;"),
        "/* keep */ SELECT 1 ;"
    );
    // Unterminated conditional comment consumes to EOF.
    assert_eq!(convert(&mut c, "SELECT 2 /*!50001 abc"), "SELECT 2 ");
    // A `/*!` inside a CREATE TABLE goes through the same stripper.
    let out = convert(
        &mut c,
        "CREATE TABLE t (id INT) /*!50100 PARTITION BY HASH (id) */;",
    );
    assert!(out.ends_with("(id INT) ;"), "{out}");
}

#[test]
fn bigint_auto_increment_becomes_bigserial() {
    let mut c = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    let out = convert(
        &mut c,
        "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT AUTO_INCREMENT, note TEXT DEFAULT 'BIGINT AUTO_INCREMENT');",
    );
    assert_eq!(
        out,
        "CREATE TABLE t (id BIGSERIAL PRIMARY KEY, n SERIAL, note TEXT DEFAULT 'BIGINT AUTO_INCREMENT');"
    );
    let mut c = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    assert_eq!(
        convert(&mut c, "CREATE TABLE t (id bigint auto_increment);"),
        "CREATE TABLE t (id BIGSERIAL);"
    );
}

#[test]
fn mysql_enum_and_set_warn_for_non_enum_aware_targets() {
    let stmt = "CREATE TABLE t (s ENUM('a'), z SET('x','y'), u INT UNSIGNED);";
    for to in [SqlDialect::Sqlite, SqlDialect::Mssql] {
        let mut c = Converter::new(SqlDialect::MySql, to);
        convert(&mut c, stmt);
        let features: Vec<&str> = c.warnings().iter().map(feature_of).collect();
        assert_eq!(
            features,
            [
                "ENUM type in table t",
                "SET type in table t",
                "UNSIGNED modifier"
            ],
            "{to:?}"
        );

        let mut c = Converter::new(SqlDialect::MySql, to).with_strict(true);
        let err = c.convert_statement(stmt.as_bytes()).unwrap_err();
        assert_eq!(feature_of(&err), "ENUM type in table t");

        let mut c = Converter::new(SqlDialect::MySql, to).with_strict(true);
        let err = c
            .convert_statement(b"CREATE TABLE t (z SET('x','y'));")
            .unwrap_err();
        assert_eq!(feature_of(&err), "SET type in table t");
    }
    // MySQL→PostgreSQL is enum-aware: ENUM is converted, only SET warns.
    let mut c = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    convert(&mut c, stmt);
    let features: Vec<&str> = c.warnings().iter().map(feature_of).collect();
    assert_eq!(features, ["SET type in table t", "UNSIGNED modifier"]);
}

#[test]
fn pg_inherits_and_partition_by_warn_and_fail_strict() {
    let inherits = "CREATE TABLE c (x int) INHERITS (p);";
    for to in [SqlDialect::MySql, SqlDialect::Sqlite] {
        let mut c = Converter::new(SqlDialect::Postgres, to);
        convert(&mut c, inherits);
        assert_eq!(feature_of(&c.warnings()[0]), "Table inheritance (INHERITS)");
        let mut c = Converter::new(SqlDialect::Postgres, to).with_strict(true);
        let err = c.convert_statement(inherits.as_bytes()).unwrap_err();
        assert_eq!(feature_of(&err), "Table inheritance (INHERITS)");
    }

    let partition = "CREATE TABLE m (id int) PARTITION BY RANGE (id);";
    let mut c = Converter::new(SqlDialect::Postgres, SqlDialect::Sqlite);
    convert(&mut c, partition);
    assert_eq!(feature_of(&c.warnings()[0]), "Table partitioning");
    let mut c = Converter::new(SqlDialect::Postgres, SqlDialect::Sqlite).with_strict(true);
    let err = c.convert_statement(partition.as_bytes()).unwrap_err();
    assert_eq!(feature_of(&err), "Table partitioning");
    // Only SQLite lacks partitioning.
    let mut c = Converter::new(SqlDialect::Postgres, SqlDialect::MySql).with_strict(true);
    c.convert_statement(partition.as_bytes()).unwrap();
    assert!(c.warnings().is_empty());
}

#[test]
fn run_converts_copy_data_block_to_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.sql");
    let output = dir.path().join("out").join("out.sql");
    std::fs::write(
        &input,
        "CREATE TABLE t (a integer, b text);\nCOPY t (a, b) FROM stdin;\n1\tx\n2\ty\n\\.\nSELECT 1;",
    )
    .unwrap();

    let stats = run(ConvertConfig {
        input: input.clone(),
        output: Some(output.clone()),
        from_dialect: Some(SqlDialect::Postgres),
        to_dialect: SqlDialect::MySql,
        ..ConvertConfig::default()
    })
    .unwrap();

    // CREATE TABLE, COPY header, COPY data block, SELECT.
    assert_eq!(stats.statements_processed, 4);
    let out = std::fs::read_to_string(&output).unwrap();
    assert!(out.contains("INSERT INTO `t` (`a`, `b`) VALUES"), "{out}");
    assert!(out.contains("('1', 'x'),\n('2', 'y');"), "{out}");
    assert!(!out.contains("COPY "), "{out}");
    // The INSERT block is the only conversion; the COPY header is absorbed
    // (skipped); CREATE TABLE (identical types) and SELECT pass through.
    assert_eq!(stats.statements_converted, 1);
    assert_eq!(stats.statements_skipped, 1);
    assert_eq!(stats.statements_unchanged, 2);

    // Dry run: nothing written, same accounting.
    let dry = run(ConvertConfig {
        input,
        output: Some(dir.path().join("never.sql")),
        from_dialect: Some(SqlDialect::Postgres),
        to_dialect: SqlDialect::MySql,
        dry_run: true,
        ..ConvertConfig::default()
    })
    .unwrap();
    assert_eq!(dry.statements_converted, 1);
    assert!(!dir.path().join("never.sql").exists());
}
