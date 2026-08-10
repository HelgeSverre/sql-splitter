//! Unit tests for convert module, extracted from src/convert/mod.rs
//!
//! Tests that used private methods have been rewritten to test through the
//! public `convert_statement` interface.

use sql_splitter::convert::Converter;
use sql_splitter::parser::{mysql_insert, SqlDialect};
use sql_splitter::schema::SchemaBuilder;

#[test]
fn test_backticks_to_double_quotes() {
    let converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    assert_eq!(converter.backticks_to_double_quotes("`users`"), "\"users\"");
    assert_eq!(
        converter.backticks_to_double_quotes("`table_name`"),
        "\"table_name\""
    );
    // Preserve strings
    assert_eq!(
        converter.backticks_to_double_quotes("'hello `world`'"),
        "'hello `world`'"
    );
}

#[test]
fn test_double_quotes_to_backticks() {
    let converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    assert_eq!(converter.double_quotes_to_backticks("\"users\""), "`users`");
}

/// MySQL requires a length on VARCHAR, so any unbounded string type surviving
/// into the output is a statement MySQL rejects with ERROR 1064. Asserting this
/// over the *whole* converted statement — not just the column under test — is
/// what catches spellings and positions no individual case thought to cover.
fn assert_no_unbounded_string_type(output: &str, input: &str) {
    let upper = output.to_uppercase();
    for keyword in ["VARCHAR", "VARYING"] {
        let mut search_from = 0;
        while let Some(offset) = upper[search_from..].find(keyword) {
            let after = search_from + offset + keyword.len();
            assert!(
                upper[after..].trim_start().starts_with('('),
                "unbounded {keyword} left in output\n  input:  {input}\n  output: {output}"
            );
            search_from = after;
        }
    }
}

/// Every shape below was executed against a real MySQL 8 server. The unbounded
/// forms are rejected there (1064 for a bare `VARCHAR`, 1101 for a `TEXT` with a
/// DEFAULT, 1170 for a `TEXT` in a key), `VARCHAR(255)` is accepted in every
/// position, and MySQL already understands the sized `CHARACTER VARYING(n)`
/// spellings as VARCHAR synonyms — so those must survive untouched.
#[test]
fn test_postgres_unbounded_varchar_becomes_a_sized_mysql_varchar() {
    let cases: &[(&str, &str)] = &[
        // Spellings of an unbounded column. pg_dump emits `character varying`,
        // never `varchar`, so that spelling is the common real-world input.
        ("CREATE TABLE \"t\" (\"a\" VARCHAR);", "`a` VARCHAR(255)"),
        ("CREATE TABLE \"t\" (\"a\" varchar);", "`a` VARCHAR(255)"),
        (
            "CREATE TABLE \"t\" (\"a\" character varying);",
            "`a` VARCHAR(255)",
        ),
        (
            "CREATE TABLE \"t\" (\"a\" CHARACTER VARYING);",
            "`a` VARCHAR(255)",
        ),
        (
            "CREATE TABLE \"t\" (\"a\" char varying);",
            "`a` VARCHAR(255)",
        ),
        // Positions where MySQL rejects an unbounded TEXT outright.
        (
            "CREATE TABLE \"t\" (\"a\" varchar PRIMARY KEY);",
            "`a` VARCHAR(255) PRIMARY KEY",
        ),
        (
            "CREATE TABLE \"t\" (\"a\" varchar UNIQUE NOT NULL);",
            "`a` VARCHAR(255) UNIQUE NOT NULL",
        ),
        (
            "CREATE TABLE \"t\" (\"a\" varchar DEFAULT 'x');",
            "`a` VARCHAR(255) DEFAULT 'x'",
        ),
        (
            "CREATE TABLE \"t\" (\"a\" varchar REFERENCES \"o\"(\"id\"));",
            "`a` VARCHAR(255) REFERENCES",
        ),
        // Statement-final position: no trailing column-list token to anchor on.
        (
            "ALTER TABLE \"t\" ADD COLUMN \"a\" varchar;",
            "`a` VARCHAR(255)",
        ),
        (
            "ALTER TABLE \"t\" ADD COLUMN \"a\" character varying;",
            "`a` VARCHAR(255)",
        ),
        // A pg_dump default carries a redundant cast to the column's own type;
        // the cast is stripped, and rewriting inside it must not corrupt it.
        (
            "CREATE TABLE \"t\" (\"a\" character varying DEFAULT 'active'::character varying);",
            "`a` VARCHAR(255) DEFAULT 'active'",
        ),
        // Sized declarations are already valid MySQL and must not be rewritten.
        ("CREATE TABLE \"t\" (\"a\" varchar(64));", "`a` varchar(64)"),
        (
            "CREATE TABLE \"t\" (\"a\" character varying(120));",
            "`a` character varying(120)",
        ),
        (
            "CREATE TABLE \"t\" (\"a\" VARCHAR (64));",
            "`a` VARCHAR (64)",
        ),
    ];

    for (input, expected) in cases {
        let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
        let output = converter.convert_statement(input.as_bytes()).unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(
            output.contains(expected),
            "input:    {input}\n  expected: {expected}\n  got:      {output}"
        );
        assert_no_unbounded_string_type(&output, input);
    }
}

/// MySQL and SQL Server both require an explicit length on VARCHAR, and they
/// fail differently: MySQL rejects a bare declaration outright with ERROR 1064,
/// while SQL Server silently reads it as `VARCHAR(1)` — verified on SQL Server
/// 2022, where `sys.columns` reports `max_length: 1` and inserting `'u1'` dies
/// with Msg 2628 "Truncated value: 'u'". Every source dialect that permits an
/// unbounded declaration reaches both targets, so this is a property of the
/// target, not of any one dialect pair.
#[test]
fn test_unbounded_varchar_is_sized_for_every_target_that_requires_a_length() {
    let input = "CREATE TABLE \"t\" (\"a\" VARCHAR, \"b\" character varying, \"c\" VARCHAR(64));";

    for from in [SqlDialect::Postgres, SqlDialect::Sqlite] {
        for to in [SqlDialect::MySql, SqlDialect::Mssql] {
            let mut converter = Converter::new(from, to);
            let output = converter.convert_statement(input.as_bytes()).unwrap();
            let output = String::from_utf8(output).unwrap();

            assert_no_unbounded_string_type(&output, &format!("{from:?} -> {to:?}"));
            assert!(
                output.contains("VARCHAR(64)"),
                "{from:?} -> {to:?}: a sized declaration was rewritten: {output}"
            );
        }
    }
}

/// PostgreSQL and SQLite both accept an unbounded VARCHAR, so converting toward
/// them must not invent a width the source never declared.
#[test]
fn test_unbounded_varchar_is_left_alone_for_targets_that_accept_it() {
    let input = "CREATE TABLE \"t\" (\"a\" VARCHAR);";

    for (from, to) in [
        (SqlDialect::Sqlite, SqlDialect::Postgres),
        (SqlDialect::Mssql, SqlDialect::Postgres),
        (SqlDialect::Postgres, SqlDialect::Sqlite),
    ] {
        let mut converter = Converter::new(from, to);
        let output = converter.convert_statement(input.as_bytes()).unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(
            !output.contains("VARCHAR(255)"),
            "{from:?} -> {to:?}: invented a width: {output}"
        );
    }
}

fn convert_pg_to_mysql(input: &str) -> String {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    let output = converter.convert_statement(input.as_bytes()).unwrap();
    String::from_utf8(output).unwrap()
}

/// `pg_dump` attaches every constraint and column default with `ALTER TABLE
/// ONLY` — PostgreSQL inheritance syntax MySQL has no equivalent for. Leaving
/// `ONLY` in place fails with ERROR 1064 against a real MySQL 8 server, which
/// takes down every primary key, unique key and foreign key in the dump.
#[test]
fn test_postgres_alter_table_only_drops_the_only_keyword() {
    let cases = [
        "ALTER TABLE ONLY public.accounts\n    ADD CONSTRAINT accounts_pkey PRIMARY KEY (code);",
        "ALTER TABLE ONLY public.accounts\n    ADD CONSTRAINT accounts_email_key UNIQUE (email);",
        "ALTER TABLE ONLY public.posts\n    ADD CONSTRAINT posts_author_fkey FOREIGN KEY (author) REFERENCES public.accounts(code);",
        "alter table only accounts add constraint c check (code <> '');",
    ];

    for input in cases {
        let output = convert_pg_to_mysql(input);
        assert!(
            !output.to_uppercase().contains("ONLY"),
            "ONLY survived\n  input:  {input}\n  output: {output}"
        );
        assert!(
            output.to_uppercase().contains("ALTER TABLE"),
            "statement was lost entirely\n  input:  {input}\n  output: {output}"
        );
    }
}

/// `pg_dump` expands `serial` into a plain integer column plus a separate
/// `ALTER COLUMN ... SET DEFAULT nextval(...)`. Stripping just the `DEFAULT
/// nextval(...)` leaves a dangling `ALTER COLUMN id SET`, which is ERROR 1064;
/// the sequence has no MySQL equivalent, so the statement has to go entirely.
#[test]
fn test_postgres_sequence_default_statement_is_dropped_whole() {
    let dropped = [
        "ALTER TABLE ONLY public.posts ALTER COLUMN id SET DEFAULT nextval('public.posts_id_seq'::regclass);",
        "ALTER TABLE posts ALTER COLUMN id SET DEFAULT nextval('posts_id_seq');",
    ];
    for input in dropped {
        let output = convert_pg_to_mysql(input);
        assert!(
            output.trim().is_empty(),
            "expected the statement to be dropped\n  input:  {input}\n  output: {output}"
        );
    }

    // A literal default is valid MySQL and must survive: only the sequence
    // form is meaningless on the target.
    let output = convert_pg_to_mysql("ALTER TABLE posts ALTER COLUMN status SET DEFAULT 'draft';");
    assert!(
        output.contains("SET DEFAULT 'draft'"),
        "a literal default was dropped: {output}"
    );
}

/// `pg_dump` 16.6+/17.2+ wrap their output in `\restrict` / `\unrestrict` psql
/// meta-commands. These are not SQL; fed to the mysql client, `\u` is read as
/// its "use database" shorthand and the import dies with ERROR 1049.
#[test]
fn test_psql_meta_commands_are_dropped() {
    for input in [
        "\\restrict 4xICTE3Zgyh1Ancqkg5YGdhBzdL1FOdL2KI0k0V7QpCVzDtjlONbtyZx2fWZO1R",
        "\\unrestrict 4xICTE3Zgyh1Ancqkg5YGdhBzdL1FOdL2KI0k0V7QpCVzDtjlONbtyZx2fWZO1R",
        "\\connect app",
    ] {
        let output = convert_pg_to_mysql(input);
        assert!(
            output.trim().is_empty(),
            "psql meta-command survived\n  input:  {input}\n  output: {output}"
        );
    }
}

/// pg_dump expands `serial` into a plain integer column plus a separate
/// sequence-backed default, so a naive conversion silently drops the
/// auto-increment and every later `INSERT` without an explicit id fails. MySQL
/// can restore it with `MODIFY COLUMN`, but only once the column is a key —
/// MySQL rejects an AUTO_INCREMENT column that is not one, and pg_dump emits
/// the primary key after the data — so it has to be deferred to the end.
#[test]
fn test_postgres_serial_becomes_mysql_auto_increment() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TABLE public.posts (id integer NOT NULL, body text);")
        .unwrap();

    let inline = converter
        .convert_statement(
            b"ALTER TABLE ONLY public.posts ALTER COLUMN id SET DEFAULT nextval('public.posts_id_seq'::regclass);",
        )
        .unwrap();
    assert!(
        inline.is_empty(),
        "the sequence default must not be emitted inline"
    );

    let deferred = converter.take_deferred_statements();
    assert_eq!(deferred.len(), 1, "{deferred:?}");
    assert_eq!(
        deferred[0],
        "ALTER TABLE `posts` MODIFY COLUMN `id` integer NOT NULL AUTO_INCREMENT;"
    );
}

/// Without a matching CREATE TABLE there is no column type to re-declare, so the
/// converter must fall back to dropping the statement rather than emitting a
/// half-formed MODIFY.
#[test]
fn test_sequence_default_without_a_known_table_is_only_dropped() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    let output = converter
        .convert_statement(
            b"ALTER TABLE ONLY public.ghost ALTER COLUMN id SET DEFAULT nextval('public.ghost_id_seq'::regclass);",
        )
        .unwrap();

    assert!(output.is_empty());
    assert!(converter.take_deferred_statements().is_empty());
    assert!(!converter.warnings().is_empty());
}

/// SQLite and MSSQL have their own identity syntax and no MODIFY COLUMN, so the
/// MySQL reconstruction must not leak into them.
#[test]
fn test_sequence_default_is_not_reconstructed_for_other_targets() {
    for to in [SqlDialect::Sqlite, SqlDialect::Mssql] {
        let mut converter = Converter::new(SqlDialect::Postgres, to);
        converter
            .convert_statement(b"CREATE TABLE public.posts (id integer NOT NULL);")
            .unwrap();
        converter
            .convert_statement(
                b"ALTER TABLE ONLY public.posts ALTER COLUMN id SET DEFAULT nextval('public.posts_id_seq'::regclass);",
            )
            .unwrap();
        assert!(
            converter.take_deferred_statements().is_empty(),
            "{to:?} must not get a MySQL MODIFY COLUMN"
        );
    }
}

fn convert_pg_to_sqlite(input: &str) -> String {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::Sqlite);
    let output = converter.convert_statement(input.as_bytes()).unwrap();
    String::from_utf8(output).unwrap()
}

/// SQLite's ALTER TABLE understands only RENAME / ADD COLUMN / DROP COLUMN —
/// `ADD CONSTRAINT` is a syntax error there, verified against the sqlite3 CLI.
/// UNIQUE and PRIMARY KEY are re-expressed as a unique index, which SQLite
/// enforces identically; a foreign key cannot be attached to an existing table
/// by any syntax, so it has to be dropped.
#[test]
fn test_alter_table_add_constraint_is_reexpressed_for_sqlite() {
    let unique = convert_pg_to_sqlite(
        "ALTER TABLE ONLY public.accounts\n    ADD CONSTRAINT accounts_email_key UNIQUE (email);",
    );
    assert!(
        unique.contains("CREATE UNIQUE INDEX accounts_email_key ON accounts (email)"),
        "{unique}"
    );
    assert!(
        !unique.to_uppercase().contains("ADD CONSTRAINT"),
        "{unique}"
    );

    let pk = convert_pg_to_sqlite(
        "ALTER TABLE ONLY public.accounts\n    ADD CONSTRAINT accounts_pkey PRIMARY KEY (code);",
    );
    assert!(
        pk.contains("CREATE UNIQUE INDEX accounts_pkey ON accounts (code)"),
        "{pk}"
    );

    let multi = convert_pg_to_sqlite("ALTER TABLE t ADD CONSTRAINT t_ab_key UNIQUE (a, b);");
    assert!(multi.contains("ON t (a, b)"), "{multi}");

    // Statements SQLite already accepts must pass through untouched.
    let add_col = convert_pg_to_sqlite("ALTER TABLE accounts ADD COLUMN nickname varchar;");
    assert!(add_col.to_uppercase().contains("ADD COLUMN"), "{add_col}");
}

/// Dropping a foreign key removes a data-integrity guarantee, so it must never
/// happen silently.
#[test]
fn test_sqlite_foreign_key_constraint_is_dropped_with_a_warning() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::Sqlite);
    let output = converter
        .convert_statement(
            b"ALTER TABLE ONLY public.posts\n    ADD CONSTRAINT posts_author_fkey FOREIGN KEY (author) REFERENCES public.accounts(code);",
        )
        .unwrap();

    assert!(String::from_utf8(output).unwrap().trim().is_empty());
    assert!(
        !converter.warnings().is_empty(),
        "dropping a foreign key must warn"
    );
}

/// MySQL and MSSQL both support `ADD CONSTRAINT`, so the SQLite rewrite must not
/// leak into their output.
#[test]
fn test_add_constraint_survives_for_targets_that_support_it() {
    let mysql = convert_pg_to_mysql(
        "ALTER TABLE ONLY public.accounts\n    ADD CONSTRAINT accounts_pkey PRIMARY KEY (code);",
    );
    assert!(mysql.to_uppercase().contains("ADD CONSTRAINT"), "{mysql}");
    assert!(
        !mysql.to_uppercase().contains("CREATE UNIQUE INDEX"),
        "{mysql}"
    );
}

#[test]
fn test_mysql_escapes_to_standard() {
    // Test through convert_statement on INSERT
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"INSERT INTO t VALUES ('it\\'s');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("''"));
    assert!(!output_str.contains("\\'"));
}

#[test]
fn test_auto_increment_to_serial() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("SERIAL"));
    assert!(!output_str.contains("AUTO_INCREMENT"));
}

#[test]
fn test_strip_engine_clause() {
    // Test through convert_statement on CREATE TABLE
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT) ENGINE=InnoDB;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("ENGINE"));
    assert!(output_str.contains("CREATE TABLE"));
}

#[test]
fn test_mssql_bracketed_copy_inserts_keep_pk_and_fk_values_aligned() {
    let mut builder = SchemaBuilder::new();
    builder.parse_create_table(
        "CREATE TABLE users (\
            id integer NOT NULL, \
            metadata NVARCHAR(MAX), \
            PRIMARY KEY (id)\
        );",
    );
    builder.parse_create_table(
        "CREATE TABLE orders (id integer PRIMARY KEY, user_id integer, FOREIGN KEY (user_id) REFERENCES users (id));",
    );
    let schema = builder.build();
    let users = schema.get_table("users").expect("users schema");
    let orders = schema.get_table("orders").expect("orders schema");

    let parent_rows = mysql_insert::parse_insert_rows(
        b"INSERT INTO [users] ([id]) VALUES ('1');",
        users,
        SqlDialect::Mssql,
    )
    .expect("parse users row");
    let child_rows = mysql_insert::parse_insert_rows(
        b"INSERT INTO [orders] ([id], [user_id]) VALUES ('1', '1');",
        orders,
        SqlDialect::Mssql,
    )
    .expect("parse orders row");

    assert_eq!(parent_rows.len(), 1);
    assert_eq!(child_rows.len(), 1);
    assert_eq!(
        parent_rows[0].pk.as_ref(),
        Some(&child_rows[0].fk_values[0].1)
    );
}

#[test]
fn test_strip_mysql_table_auto_increment_option() {
    // Table-level AUTO_INCREMENT=N option (distinct from column-level AUTO_INCREMENT
    // keyword) must be fully removed, not just have "AUTO_INCREMENT" stripped and
    // "=2" left dangling. See https://github.com/HelgeSverre/sql-splitter/issues/64
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("AUTO_INCREMENT"));
    assert!(!output_str.contains("=2"));
    assert!(output_str.trim_end().ends_with(");"));
}

#[test]
fn test_strip_mysql_table_comment_option() {
    // Trailing table-level COMMENT='...' option is not valid PostgreSQL syntax
    // and must be stripped entirely.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT) COMMENT='some comment';";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"));
    assert!(!output_str.contains("some comment"));
    assert!(output_str.trim_end().ends_with(");"));
}

#[test]
fn test_strip_mysql_inline_column_comment() {
    // Inline column COMMENT 'text' is MySQL-only syntax; PostgreSQL rejects it
    // inside a column definition.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT COMMENT 'the id', name VARCHAR(20) COMMENT 'name field');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"));
    assert!(!output_str.contains("the id"));
    assert!(!output_str.contains("name field"));
}

#[test]
fn test_convert_unique_key_using_btree() {
    // MySQL's `UNIQUE KEY name (col) USING BTREE` table constraint has no direct
    // PostgreSQL equivalent inline; it must become a plain `UNIQUE (col)`.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, username VARCHAR(20), UNIQUE KEY `username` (`username`) USING BTREE);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"username\")"));
    assert!(!output_str.contains("USING BTREE"));
    assert!(!output_str.contains("UNIQUE KEY"));
}

#[test]
fn test_convert_issue_64_full_reproduction() {
    // Full reproduction of https://github.com/HelgeSverre/sql-splitter/issues/64:
    // converting a realistic MySQL CREATE TABLE (inline column comments, a unique
    // key with USING BTREE, and ENGINE/AUTO_INCREMENT/CHARSET/COMMENT table
    // options) must produce syntactically clean PostgreSQL output.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE `fa_admin` (\n  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID',\n  `username` varchar(20) DEFAULT '' COMMENT 'username',\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `username` (`username`) USING BTREE\n) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='admin table';";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(!output_str.contains("ENGINE"), "{output_str}");
    assert!(!output_str.contains("USING BTREE"), "{output_str}");
    assert!(!output_str.contains("UNIQUE KEY"), "{output_str}");
    assert!(output_str.contains("UNIQUE (\"username\")"), "{output_str}");
    assert!(output_str.trim_end().ends_with(");"), "{output_str}");
}

// --- Permutations of the AUTO_INCREMENT=N table option ---

#[test]
fn test_auto_increment_table_option_alone() {
    // No ENGINE/CHARSET around it at all.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=5;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("AUTO_INCREMENT"), "{output_str}");
    assert!(!output_str.contains("=5"), "{output_str}");
    assert!(output_str.trim_end().ends_with(");"), "{output_str}");
}

#[test]
fn test_auto_increment_table_option_before_engine() {
    // Unusual but valid MySQL ordering: AUTO_INCREMENT=N before ENGINE=.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=100 ENGINE=InnoDB;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("AUTO_INCREMENT"), "{output_str}");
    assert!(!output_str.contains("=100"), "{output_str}");
    assert!(!output_str.contains("ENGINE"), "{output_str}");
}

#[test]
fn test_auto_increment_table_option_lowercase() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT auto_increment PRIMARY KEY) engine=InnoDB auto_increment=2;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        !output_str.to_uppercase().contains("AUTO_INCREMENT"),
        "{output_str}"
    );
    assert!(!output_str.contains("=2"), "{output_str}");
}

#[test]
fn test_auto_increment_table_option_multi_digit() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB AUTO_INCREMENT=1234567;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("1234567"), "{output_str}");
    assert!(!output_str.contains("AUTO_INCREMENT"), "{output_str}");
}

#[test]
fn test_no_table_auto_increment_option_still_converts_column_keyword() {
    // Regression: tables with only the column-level keyword (no table option)
    // must still get SERIAL treatment.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("SERIAL"), "{output_str}");
    assert!(!output_str.contains("AUTO_INCREMENT"), "{output_str}");
}

// --- Permutations of COMMENT clauses ---

#[test]
fn test_column_comment_with_escaped_quote() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT COMMENT 'it\\'s the id');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(!output_str.contains("it's the id"), "{output_str}");
}

#[test]
fn test_last_column_comment_before_closing_paren() {
    // No trailing comma after the COMMENT — closing paren follows directly.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT, name VARCHAR(20) COMMENT 'the name');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(output_str.trim_end().ends_with(");"), "{output_str}");
}

#[test]
fn test_table_comment_without_equals_sign() {
    // MySQL allows the table-level COMMENT option without `=`.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT) COMMENT 'my table';";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(!output_str.contains("my table"), "{output_str}");
    assert!(output_str.trim_end().ends_with(");"), "{output_str}");
}

#[test]
fn test_table_comment_between_engine_and_charset() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT) ENGINE=InnoDB COMMENT='mid table' DEFAULT CHARSET=utf8mb4;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(!output_str.contains("ENGINE"), "{output_str}");
    assert!(!output_str.contains("CHARSET"), "{output_str}");
}

#[test]
fn test_comment_containing_parentheses() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT COMMENT 'contains (parens) here');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("COMMENT"), "{output_str}");
    assert!(!output_str.contains("contains"), "{output_str}");
    assert!(output_str.trim_end().ends_with(");"), "{output_str}");
}

#[test]
fn test_mixed_columns_with_and_without_comments() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT COMMENT 'the id', name VARCHAR(20), age INT COMMENT 'the age');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert_eq!(
        output_str.trim(),
        "CREATE TABLE t (id INT, name VARCHAR(20), age INT);"
    );
}

#[test]
fn test_standalone_comment_on_table_statement_untouched() {
    // Regression: a standalone `COMMENT ON TABLE ... IS '...'` statement is a
    // different statement type entirely and must not be touched by the
    // CREATE TABLE inline-comment stripping.
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::Postgres);

    let input = b"COMMENT ON TABLE foo IS 'bar';";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("COMMENT ON TABLE"), "{output_str}");
    assert!(output_str.contains("'bar'"), "{output_str}");
}

// --- Permutations of UNIQUE KEY / USING BTREE constraints ---

#[test]
fn test_unique_key_without_name() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT, email VARCHAR(50), UNIQUE KEY (`email`));";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"email\")"), "{output_str}");
    assert!(!output_str.contains("UNIQUE KEY"), "{output_str}");
}

#[test]
fn test_unique_key_without_using_clause() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INT, email VARCHAR(50), UNIQUE KEY `email` (`email`));";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"email\")"), "{output_str}");
    assert!(!output_str.contains("UNIQUE KEY"), "{output_str}");
}

#[test]
fn test_unique_key_using_hash() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, email VARCHAR(50), UNIQUE KEY `email` (`email`) USING HASH);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"email\")"), "{output_str}");
    assert!(!output_str.contains("USING HASH"), "{output_str}");
}

#[test]
fn test_unique_key_using_btree_lowercase() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, email VARCHAR(50), unique key `email` (`email`) using btree);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        output_str.to_uppercase().contains("UNIQUE (\"EMAIL\")"),
        "{output_str}"
    );
    assert!(
        !output_str.to_uppercase().contains("USING BTREE"),
        "{output_str}"
    );
}

#[test]
fn test_unique_key_multi_column() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, a INT, b INT, UNIQUE KEY `idx_ab` (`a`,`b`) USING BTREE);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"a\",\"b\")"), "{output_str}");
    assert!(!output_str.contains("UNIQUE KEY"), "{output_str}");
    assert!(!output_str.contains("USING BTREE"), "{output_str}");
}

#[test]
fn test_primary_key_untouched_by_unique_key_conversion() {
    // Regression: PRIMARY KEY must not be affected by the UNIQUE KEY regex.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, email VARCHAR(50), PRIMARY KEY (`id`), UNIQUE KEY `email` (`email`) USING BTREE);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("PRIMARY KEY (\"id\")"), "{output_str}");
    assert!(output_str.contains("UNIQUE (\"email\")"), "{output_str}");
}

#[test]
fn test_named_unique_constraint_untouched() {
    // Regression: a standard `CONSTRAINT name UNIQUE (...)` (not MySQL's
    // `UNIQUE KEY` form) must pass through unchanged.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input =
        b"CREATE TABLE t (id INT, email VARCHAR(50), CONSTRAINT uq_email UNIQUE (`email`));";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        output_str.contains("CONSTRAINT uq_email UNIQUE (\"email\")"),
        "{output_str}"
    );
}

// --- Adversarial findings: string-literal-aware stripping ---

#[test]
fn test_default_value_literally_comment_does_not_eat_next_column() {
    // The word "comment" appearing as a DEFAULT string value must not be
    // mistaken for a MySQL COMMENT clause and must not consume subsequent
    // column definitions.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (a VARCHAR(20) DEFAULT 'comment', b VARCHAR(20) DEFAULT 'x');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert_eq!(
        output_str.trim(),
        "CREATE TABLE t (a VARCHAR(20) DEFAULT 'comment', b VARCHAR(20) DEFAULT 'x');"
    );
}

#[test]
fn test_check_constraint_literal_comment_not_corrupted() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (col VARCHAR(10) CHECK (col <> 'COMMENT'), name VARCHAR(20) DEFAULT 'bob');";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        output_str.contains("CHECK (col <> 'COMMENT')"),
        "{output_str}"
    );
    assert!(
        output_str.contains("name VARCHAR(20) DEFAULT 'bob'"),
        "{output_str}"
    );
}

#[test]
fn test_escaped_quote_pair_in_default_value_not_corrupted() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (note VARCHAR(255) DEFAULT 'See comment ''123'' here', x INT);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        output_str.contains("DEFAULT 'See comment ''123'' here'"),
        "{output_str}"
    );
    assert!(output_str.contains(", x INT"), "{output_str}");
}

#[test]
fn test_default_value_literally_auto_increment_not_corrupted() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (label VARCHAR(20) DEFAULT 'AUTO_INCREMENT=5', id INT);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(
        output_str.contains("DEFAULT 'AUTO_INCREMENT=5'"),
        "{output_str}"
    );
}

// --- Adversarial findings: UNIQUE KEY with prefix-length index ---

#[test]
fn test_unique_key_with_prefix_length() {
    // MySQL utf8mb4 prefix-length indexes, e.g. `(email(191))`, are extremely
    // common in real dumps and must not be left as invalid Postgres syntax.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (email VARCHAR(255), UNIQUE KEY `email` (`email`(191)));";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("UNIQUE (\"email\")"), "{output_str}");
    assert!(!output_str.contains("UNIQUE KEY"), "{output_str}");
    assert!(!output_str.contains("(191)"), "{output_str}");
}

#[test]
fn test_strip_conditional_comments() {
    // Test through convert_statement
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"/*!40101 SET NAMES utf8 */;";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    // The conditional comment content should be stripped
    assert!(!output_str.contains("40101"));
}

#[test]
fn test_skip_mysql_session_commands() {
    // Test through convert_statement - these should return empty
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let input = b"SET NAMES utf8mb4;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"LOCK TABLES users WRITE;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    // Regular CREATE TABLE should not be empty
    let input = b"CREATE TABLE users (id INT);";
    let output = converter.convert_statement(input).unwrap();
    assert!(!output.is_empty());
}

#[test]
fn test_skip_postgres_session_commands() {
    // Test through convert_statement - these should return empty
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"SET client_encoding = 'UTF8';";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"SET search_path TO public;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    // Regular CREATE TABLE should not be empty
    let input = b"CREATE TABLE users (id INT);";
    let output = converter.convert_statement(input).unwrap();
    assert!(!output.is_empty());
}

#[test]
fn test_skip_sqlite_pragmas() {
    // Test through convert_statement - these should return empty
    let mut converter = Converter::new(SqlDialect::Sqlite, SqlDialect::MySql);

    let input = b"PRAGMA foreign_keys = ON;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"PRAGMA journal_mode = WAL;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    // Regular CREATE TABLE should not be empty
    let input = b"CREATE TABLE users (id INTEGER);";
    let output = converter.convert_statement(input).unwrap();
    assert!(!output.is_empty());
}

#[test]
fn test_serial_to_auto_increment() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"CREATE TABLE users (id SERIAL PRIMARY KEY);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("AUTO_INCREMENT"));
    assert!(!output_str.contains("SERIAL"));
}

#[test]
fn test_postgres_to_sqlite_types() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::Sqlite);

    let input = b"CREATE TABLE t (id SERIAL, data BYTEA, flag BOOLEAN);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("INTEGER"));
    assert!(output_str.contains("BLOB"));
    assert!(!output_str.contains("BYTEA"));
    assert!(!output_str.contains("SERIAL"));
}

#[test]
fn test_sqlite_to_postgres_types() {
    let mut converter = Converter::new(SqlDialect::Sqlite, SqlDialect::Postgres);

    let input = b"CREATE TABLE t (id INTEGER, val REAL, data BLOB);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("DOUBLE PRECISION"));
    assert!(output_str.contains("BYTEA"));
    assert!(!output_str.contains("REAL"));
    assert!(!output_str.contains("BLOB"));
}

#[test]
fn test_sqlite_to_mysql_types() {
    let mut converter = Converter::new(SqlDialect::Sqlite, SqlDialect::MySql);

    let input = b"CREATE TABLE t (id INTEGER, val REAL);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("INTEGER"));
    assert!(output_str.contains("DOUBLE"));
    assert!(!output_str.contains("REAL"));
}

#[test]
fn test_postgres_identifier_quoting_to_mysql() {
    let converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = "\"users\"";
    let output = converter.double_quotes_to_backticks(input);

    assert_eq!(output, "`users`");
}

#[test]
fn test_preserve_strings_in_identifier_conversion() {
    let converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = "SELECT 'hello \"world\"' FROM \"users\"";
    let output = converter.double_quotes_to_backticks(input);

    assert!(output.contains("'hello \"world\"'"));
    assert!(output.contains("`users`"));
}

#[test]
fn test_postgres_only_feature_detection() {
    // Test through convert_statement - these should return empty when converting from Postgres
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    // PostgreSQL-only features should be skipped
    let input = b"CREATE SEQUENCE my_seq;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"CREATE DOMAIN my_domain AS INTEGER;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"CREATE TYPE my_enum AS ENUM ('a', 'b');";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"CREATE TRIGGER my_trigger AFTER INSERT ON foo;";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    let input = b"COMMENT ON TABLE foo IS 'bar';";
    let output = converter.convert_statement(input).unwrap();
    assert!(output.is_empty());

    // Regular CREATE TABLE should NOT be empty
    let input = b"CREATE TABLE users (id INT);";
    let output = converter.convert_statement(input).unwrap();
    assert!(!output.is_empty());
}

#[test]
fn postgres_quoted_enum_name_converts_to_mysql_inline_enum() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let create_type = converter
        .convert_statement(b"CREATE TYPE \"Order Status\" AS ENUM ('new', 'done');")
        .unwrap();
    assert!(
        create_type.is_empty(),
        "PostgreSQL CREATE TYPE must not be emitted to MySQL: {}",
        String::from_utf8_lossy(&create_type)
    );

    let create_table = converter
        .convert_statement(b"CREATE TABLE orders (status \"Order Status\");")
        .unwrap();
    let output = String::from_utf8_lossy(&create_table);
    assert!(
        output.contains("status ENUM('new','done')"),
        "got: {output}"
    );
}

#[test]
fn postgres_enum_ddl_inside_string_literal_is_not_consumed() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    let input = b"SELECT 'CREATE TYPE fake AS ENUM (''a'')';";
    let output = converter.convert_statement(input).unwrap();
    assert!(!output.is_empty());
    assert!(String::from_utf8_lossy(&output).contains("CREATE TYPE fake"));
}

#[test]
fn quoted_and_unquoted_postgres_enum_names_remain_distinct() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TYPE \"Mood\" AS ENUM ('quoted');")
        .unwrap();
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM ('plain');")
        .unwrap();

    let output = converter
        .convert_statement(b"CREATE TABLE t (a \"Mood\", b mood);")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("a ENUM('quoted')"), "got: {output}");
    assert!(output.contains("b ENUM('plain')"), "got: {output}");
}

#[test]
fn postgres_enum_accepts_escape_and_dollar_quoted_labels() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM (E'line\\n', $$dollar$$, 'UUID');")
        .unwrap();

    let output = converter
        .convert_statement(b"CREATE TABLE t (value mood);")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("ENUM('line\n','dollar','UUID')"),
        "got: {output}"
    );
}

#[test]
fn postgres_add_enum_value_if_not_exists_does_not_duplicate_label() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM ('plain');")
        .unwrap();
    converter
        .convert_statement(b"ALTER TYPE mood ADD VALUE IF NOT EXISTS 'plain';")
        .unwrap();

    let output = converter
        .convert_statement(b"CREATE TABLE t (value mood);")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("ENUM('plain')"), "got: {output}");
    assert!(!output.contains("'plain','plain'"), "got: {output}");
}

#[test]
fn unsupported_postgres_alter_type_is_not_emitted_to_mysql() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM ('sad', 'ok');")
        .unwrap();

    let output = converter
        .convert_statement(b"ALTER TYPE mood RENAME VALUE 'sad' TO 'bad';")
        .unwrap();

    assert!(
        output.is_empty(),
        "PostgreSQL ALTER TYPE must not be emitted to MySQL: {}",
        String::from_utf8_lossy(&output)
    );
}

#[test]
fn generated_postgres_enum_names_do_not_collide_after_sanitization() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);

    let first = converter
        .convert_statement(b"CREATE TABLE `a-b` (`x` ENUM('one'));")
        .unwrap();
    let second = converter
        .convert_statement(b"CREATE TABLE `a_b` (`x` ENUM('two'));")
        .unwrap();
    let first = String::from_utf8_lossy(&first);
    let second = String::from_utf8_lossy(&second);

    assert!(first.contains("CREATE TYPE enum__a_b__x AS ENUM ('one');"));
    assert!(
        second.contains("CREATE TYPE enum__a_b__x_2 AS ENUM ('two');"),
        "the second enum needs a distinct type: {second}"
    );
    assert!(
        !second.contains("ALTER TYPE enum__a_b__x"),
        "unrelated enums must not be merged: {second}"
    );
}

#[test]
fn test_strip_postgres_casts() {
    // Test through convert_statement
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"INSERT INTO t VALUES ('val'::text);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("::text"));
}

#[test]
fn test_convert_nextval() {
    // Test through convert_statement on ALTER TABLE
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"ALTER TABLE t ALTER COLUMN id SET DEFAULT nextval('t_id_seq'::regclass);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("nextval"));
    assert!(!output_str.contains("t_id_seq"));
}

#[test]
fn test_convert_default_now() {
    // Test through convert_statement on CREATE TABLE
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"CREATE TABLE t (created_at TIMESTAMP DEFAULT now());";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(output_str.contains("CURRENT_TIMESTAMP"));
    assert!(!output_str.contains("now()"));
}

#[test]
fn test_strip_schema_prefix() {
    // Test through convert_statement
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);

    let input = b"INSERT INTO public.users VALUES (1);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("public."));
    assert!(output_str.contains("users"));
}

// =============================================================================
// Warning tests (extracted from src/convert/warnings.rs)
// =============================================================================

use sql_splitter::convert::{ConvertWarning, WarningCollector};

#[test]
fn test_warning_display() {
    let w = ConvertWarning::UnsupportedFeature {
        feature: "ENUM type".to_string(),
        suggestion: Some("Use VARCHAR with CHECK constraint".to_string()),
    };
    let s = w.to_string();
    assert!(s.contains("ENUM type"));
    assert!(s.contains("CHECK constraint"));
}

#[test]
fn test_warning_collector_dedup() {
    let mut collector = WarningCollector::new();

    collector.add(ConvertWarning::UnsupportedFeature {
        feature: "ENUM".to_string(),
        suggestion: None,
    });
    collector.add(ConvertWarning::UnsupportedFeature {
        feature: "ENUM".to_string(),
        suggestion: None,
    });

    assert_eq!(collector.count(), 1);
}

#[test]
fn test_warning_collector_limit() {
    let mut collector = WarningCollector::with_limit(5);

    for i in 0..10 {
        collector.add(ConvertWarning::UnsupportedFeature {
            feature: format!("Feature {}", i),
            suggestion: None,
        });
    }

    assert_eq!(collector.count(), 5);
}

// =============================================================================
// COPY → INSERT conversion tests (from src/convert/copy_to_insert.rs)
// =============================================================================

mod copy_to_insert_tests {
    use sql_splitter::convert::{
        copy_to_inserts, parse_copy_data, parse_copy_header, CopyHeader, CopyValue,
    };
    use sql_splitter::parser::SqlDialect;

    #[test]
    fn test_parse_copy_header_simple() {
        let header = "COPY users (id, name, email) FROM stdin;";
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.table, "users");
        assert_eq!(parsed.columns, vec!["id", "name", "email"]);
        assert!(parsed.schema.is_none());
    }

    #[test]
    fn test_parse_copy_header_with_schema() {
        let header = "COPY public.users (id, name) FROM stdin;";
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.schema, Some("public".to_string()));
        assert_eq!(parsed.table, "users");
    }

    #[test]
    fn test_parse_copy_header_quoted() {
        let header = r#"COPY "public"."my_table" ("id", "name") FROM stdin;"#;
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.schema, Some("public".to_string()));
        assert_eq!(parsed.table, "my_table");
    }

    #[test]
    fn test_parse_copy_header_with_comments() {
        let header = "--\n-- Data for table\n--\nCOPY users (id) FROM stdin;";
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.table, "users");
    }

    #[test]
    fn test_parse_copy_data() {
        let data = b"1\tAlice\talice@example.com\n2\tBob\tbob@example.com\n\\.";
        let rows = parse_copy_data(data);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 3);
    }

    #[test]
    fn test_null_handling() {
        let data = b"1\t\\N\ttest\n";
        let rows = parse_copy_data(data);
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][1], CopyValue::Null));
    }

    #[test]
    fn test_escape_sequences() {
        let data = b"hello\\tworld\\n\n";
        let rows = parse_copy_data(data);
        if let CopyValue::Text(s) = &rows[0][0] {
            assert_eq!(s, "hello\tworld\n");
        } else {
            panic!("Expected Text");
        }
    }

    #[test]
    fn test_copy_to_insert_mysql() {
        let header = CopyHeader {
            schema: None,
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
        };
        let data = b"1\tAlice\n2\tBob\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::MySql);
        assert_eq!(inserts.len(), 1);

        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("INSERT INTO `users`"));
        assert!(sql.contains("(`id`, `name`)"));
        assert!(sql.contains("('1', 'Alice')"));
        assert!(sql.contains("('2', 'Bob')"));
    }

    #[test]
    fn test_copy_to_insert_postgres() {
        let header = CopyHeader {
            schema: Some("public".to_string()),
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
        };
        let data = b"1\tAlice\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::Postgres);
        let sql = String::from_utf8_lossy(&inserts[0]);
        // Note: public schema is stripped for DuckDB compatibility
        assert!(sql.contains("\"users\""));
        assert!(sql.contains("\"id\""));
        assert!(sql.contains("\"name\""));
    }

    #[test]
    fn test_copy_to_insert_postgres_custom_schema() {
        // Non-standard schemas are preserved
        let header = CopyHeader {
            schema: Some("myschema".to_string()),
            table: "users".to_string(),
            columns: vec!["id".to_string()],
        };
        let data = b"1\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::Postgres);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("\"myschema\".\"users\""));
    }

    #[test]
    fn test_copy_to_insert_mssql_uses_bracketed_identifiers() {
        let header = CopyHeader {
            schema: Some("public".to_string()),
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
        };
        let data = b"1\tAlice\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::Mssql);
        let sql = String::from_utf8_lossy(&inserts[0]);

        assert!(sql.contains("INSERT INTO [users]"));
        assert!(sql.contains("([id], [name])"));
        assert!(!sql.contains("\"users\""));
    }

    #[test]
    fn test_copy_to_insert_with_null() {
        let header = CopyHeader {
            schema: None,
            table: "t".to_string(),
            columns: vec!["a".to_string(), "b".to_string()],
        };
        let data = b"1\t\\N\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::MySql);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("NULL"));
    }

    #[test]
    fn test_escape_quotes_mysql() {
        let header = CopyHeader {
            schema: None,
            table: "t".to_string(),
            columns: vec!["s".to_string()],
        };
        let data = b"it's a test\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::MySql);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("it\\'s a test"));
    }

    #[test]
    fn test_escape_quotes_sqlite() {
        let header = CopyHeader {
            schema: None,
            table: "t".to_string(),
            columns: vec!["s".to_string()],
        };
        let data = b"it's a test\n\\.";

        let inserts = copy_to_inserts(&header, data, SqlDialect::Sqlite);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("it''s a test"));
    }
}

#[test]
fn test_mysql_sized_int_auto_increment_to_sqlite() {
    // Regression: "BIGINT AUTO_INCREMENT" used to hit the "INT AUTO_INCREMENT"
    // substring replacement and produce invalid "BIGINTEGER".
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Sqlite);

    let input =
        b"CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, n SMALLINT AUTO_INCREMENT);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);

    assert!(!output_str.contains("BIGINTEGER"), "got: {output_str}");
    assert!(!output_str.contains("SMALLINTEGER"), "got: {output_str}");
    assert!(
        output_str.contains("INTEGER PRIMARY KEY"),
        "got: {output_str}"
    );
    assert!(!output_str.contains("AUTO_INCREMENT"), "got: {output_str}");
}

#[test]
fn test_enum_narrows_to_varchar_cross_dialect() {
    // MySQL→Postgres conversion preserves ENUM semantics with created types.
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    let input = b"CREATE TABLE t (kind ENUM('a','b') NOT NULL);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);
    assert!(
        output_str.contains("CREATE TYPE"),
        "MySQL→Postgres should emit CREATE TYPE for enum: {output_str}"
    );
    assert!(
        !output_str.contains("VARCHAR(255)"),
        "MySQL→Postgres enum should not become VARCHAR: {output_str}"
    );
    assert!(
        output_str.contains("enum__t__kind"),
        "should use actual column name; got: {output_str}"
    );
}

#[test]
fn test_mysql_to_pg_enum_uses_actual_column_names() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    let input = b"CREATE TABLE t (prio ENUM('low','med') NOT NULL, kind ENUM('a','b'));";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);
    assert!(
        output_str.contains("enum__t__prio"),
        "first column name wrong: {output_str}"
    );
    assert!(
        output_str.contains("enum__t__kind"),
        "second column name wrong: {output_str}"
    );
    assert!(
        !output_str.contains("enum__t__col"),
        "should not use 'col' as column name: {output_str}"
    );
}

#[test]
fn test_mysql_to_pg_enum_backtick_quoted_columns() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    let input = b"CREATE TABLE `users` (\n  `status` ENUM('active','inactive') NOT NULL\n);";
    let output = converter.convert_statement(input).unwrap();
    let output_str = String::from_utf8_lossy(&output);
    assert!(
        output_str.contains("enum__users__status"),
        "should extract backtick-quoted column name: {output_str}"
    );
    assert!(
        !output_str.contains("VARCHAR(255)"),
        "backtick enum should not become VARCHAR: {output_str}"
    );
}

#[test]
fn enum_conversion_handles_create_table_modifiers() {
    let mut mysql = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    let output = mysql
        .convert_statement(b"CREATE TEMPORARY TABLE t (x ENUM('a','b'));")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("CREATE TYPE enum__t__x"), "got: {output}");

    let mut postgres = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    postgres
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a','b');")
        .unwrap();
    let output = postgres
        .convert_statement(b"CREATE UNLOGGED TABLE t (x mood);")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("x ENUM('a','b')"));
    assert!(!output.contains("UNLOGGED"), "got: {output}");
}

#[test]
fn enum_alter_table_uses_target_dialect_grammar() {
    let mut mysql = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    mysql
        .convert_statement(b"CREATE TABLE t (x ENUM('a'));")
        .unwrap();
    let output = mysql
        .convert_statement(b"ALTER TABLE t MODIFY COLUMN x ENUM('a','b');")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("ALTER COLUMN x TYPE enum__t__x"),
        "got: {output}"
    );
    assert!(!output.contains("MODIFY COLUMN"), "got: {output}");

    let mut postgres = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    postgres
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a','b');")
        .unwrap();
    let output = postgres
        .convert_statement(b"ALTER TABLE t ALTER COLUMN x TYPE mood;")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("MODIFY COLUMN x ENUM('a','b')"),
        "got: {output}"
    );
}

#[test]
fn enum_alter_table_handles_quoted_names_and_drops_postgres_using() {
    let mut mysql = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    mysql
        .convert_statement(b"CREATE TABLE `My Table` (`Order Status` ENUM('a'));")
        .unwrap();
    let output = mysql
        .convert_statement(b"ALTER TABLE `My Table` MODIFY COLUMN `Order Status` ENUM('a','b');")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("ALTER COLUMN \"Order Status\" TYPE"),
        "got: {output}"
    );
    assert!(!output.contains("MODIFY COLUMN"), "got: {output}");

    let mut postgres = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    postgres
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a','b');")
        .unwrap();
    let output = postgres
        .convert_statement(
            b"ALTER TABLE ONLY \"My Table\" ALTER COLUMN \"Order Status\" TYPE mood USING \"Order Status\"::text::mood;",
        )
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("ALTER TABLE `My Table` MODIFY COLUMN `Order Status` ENUM('a','b');"),
        "got: {output}"
    );
    assert!(!output.contains("USING"), "got: {output}");
    assert!(!output.contains(" ONLY "), "got: {output}");
}

#[test]
fn enum_alter_table_handles_multiline_and_qualified_names() {
    let mut mysql = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    mysql
        .convert_statement(b"CREATE TABLE `My Db`.`My Table` (`Order Status` ENUM('a'));")
        .unwrap();
    let output = mysql
        .convert_statement(
            b"ALTER TABLE `My Db`.`My Table`\nMODIFY\nCOLUMN `Order Status` ENUM('a','b');",
        )
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("ALTER COLUMN \"Order Status\" TYPE"),
        "got: {output}"
    );
    assert!(!output.contains("MODIFY"), "got: {output}");

    let mut postgres = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    postgres
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a');")
        .unwrap();
    let output = postgres
        .convert_statement(
            b"ALTER TABLE \"My Schema\".\"My Table\"\nALTER\nCOLUMN \"Order Status\" TYPE mood;",
        )
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("MODIFY COLUMN `Order Status` ENUM('a')"),
        "got: {output}"
    );
}

#[test]
fn enum_alter_table_handles_comments_between_keywords() {
    let mut mysql = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    mysql
        .convert_statement(b"CREATE TABLE t (x ENUM('a'));")
        .unwrap();
    let output = mysql
        .convert_statement(b"ALTER TABLE t MODIFY /* keep */ COLUMN x ENUM('a','b');")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("ALTER COLUMN x TYPE"), "got: {output}");
    assert!(!output.contains("MODIFY"), "got: {output}");

    let mut postgres = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    postgres
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a');")
        .unwrap();
    let output = postgres
        .convert_statement(b"ALTER TABLE t ALTER /* keep */ COLUMN x TYPE mood;")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("MODIFY COLUMN x ENUM('a')"),
        "got: {output}"
    );
}

#[test]
fn alter_action_keywords_inside_literals_do_not_change_classification() {
    let mut mysql = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    let output = mysql
        .convert_statement(b"ALTER TABLE t ADD COLUMN action ENUM('CHANGE COLUMN','other');")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("ADD COLUMN action enum__t__action"),
        "got: {output}"
    );

    let mut postgres = Converter::new(SqlDialect::Postgres, SqlDialect::MySql).with_strict(true);
    let output = postgres
        .convert_statement(
            b"ALTER TABLE t ADD COLUMN note text DEFAULT 'ALTER COLUMN x TYPE mood';",
        )
        .unwrap();
    assert!(!output.is_empty());
}

#[test]
fn unrelated_using_clause_is_not_truncated() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    let input = b"ALTER TABLE t ADD CONSTRAINT ex EXCLUDE USING gist (x WITH =);";
    let output = converter.convert_statement(input).unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("USING gist"), "got: {output}");
}

#[test]
fn strict_rejects_mysql_enum_modify_with_attributes() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres).with_strict(true);
    converter
        .convert_statement(b"CREATE TABLE t (x ENUM('a'));")
        .unwrap();
    assert!(converter
        .convert_statement(b"ALTER TABLE t MODIFY COLUMN x ENUM('a','b') NOT NULL;")
        .is_err());
}

#[test]
fn postgres_enum_renames_update_registry() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM ('sad');")
        .unwrap();
    converter
        .convert_statement(b"ALTER TYPE mood RENAME VALUE 'sad' TO 'bad';")
        .unwrap();
    converter
        .convert_statement(b"ALTER TYPE mood RENAME TO feeling;")
        .unwrap();
    let output = converter
        .convert_statement(b"CREATE TABLE t (x feeling);")
        .unwrap();
    assert!(String::from_utf8_lossy(&output).contains("ENUM('bad')"));
}

#[test]
fn postgres_enum_array_becomes_json_with_warning() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a','b');")
        .unwrap();
    let output = converter
        .convert_statement(b"CREATE TABLE t (x mood[]);")
        .unwrap();
    assert!(String::from_utf8_lossy(&output).contains("x JSON"));
    assert!(!converter.warnings().is_empty());
}

#[test]
fn postgres_enum_array_narrows_for_sqlite_and_mssql() {
    for (target, expected) in [
        (SqlDialect::Sqlite, "x TEXT"),
        (SqlDialect::Mssql, "x NVARCHAR(255)"),
    ] {
        let mut converter = Converter::new(SqlDialect::Postgres, target);
        converter
            .convert_statement(b"CREATE TYPE mood AS ENUM ('a','b');")
            .unwrap();
        let output = converter
            .convert_statement(b"CREATE TABLE t (x mood[]);")
            .unwrap();
        let output = String::from_utf8_lossy(&output);
        assert!(output.contains(expected), "got: {output}");
        assert!(!output.contains("mood[]"), "got: {output}");
    }
}

#[test]
fn postgres_enum_array_consumes_spaced_bounded_and_repeated_dimensions() {
    for suffix in ["[ ]", "[3]", "[][]", " [ ] [4]"] {
        let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
        converter
            .convert_statement(b"CREATE TYPE mood AS ENUM ('a');")
            .unwrap();
        let statement = format!("CREATE TABLE t (x mood{suffix});");
        let output = converter.convert_statement(statement.as_bytes()).unwrap();
        let output = String::from_utf8_lossy(&output);
        assert!(output.contains("x JSON"), "suffix {suffix}, got: {output}");
        assert!(!output.contains('['), "suffix {suffix}, got: {output}");
    }
}

#[test]
fn postgres_enum_escape_forms_are_decoded() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(br"CREATE TYPE mood AS ENUM (E'\x41', E'\101', E'\r', U&'d\0061t');")
        .unwrap();
    let output = converter
        .convert_statement(b"CREATE TABLE t (x mood);")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("ENUM('A','A','\r','dat')"),
        "got: {output:?}"
    );
}

#[test]
fn postgres_enum_byte_and_custom_unicode_escapes_are_decoded() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(
            br"CREATE TYPE mood AS ENUM (E'\xC3\xA9', E'\303\251', U&'d!0061t' UESCAPE '!', U&'\D83D\DE00');",
        )
        .unwrap();
    let output = converter
        .convert_statement(b"CREATE TABLE t (x mood);")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("ENUM('é','é','dat','😀')"),
        "got: {output:?}"
    );
}

#[test]
fn mysql_enum_labels_use_mysql_escape_rules() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    let output = converter
        .convert_statement(br"CREATE TABLE t (x ENUM('\x41','\f','\Z'));")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("AS ENUM ('x41', 'f', '\u{001a}')"),
        "got: {output:?}"
    );
    assert!(!output.contains("AS ENUM ('A'"), "got: {output:?}");
}

#[test]
fn mysql_enum_nul_label_is_rejected_before_output_or_registry_mutation() {
    let statement = br"CREATE TABLE t (x ENUM('a\0b'));";

    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    let output = converter.convert_statement(statement).unwrap();
    assert!(output.is_empty());
    assert!(converter
        .warnings()
        .iter()
        .any(|warning| warning.to_string().contains("NUL")));

    let mut strict = Converter::new(SqlDialect::MySql, SqlDialect::Postgres).with_strict(true);
    assert!(strict.convert_statement(statement).is_err());
}

#[test]
fn mysql_multi_action_enum_alter_is_rejected_whole() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    converter
        .convert_statement(b"CREATE TABLE t (x ENUM('a'));")
        .unwrap();
    let output = converter
        .convert_statement(b"ALTER TABLE t MODIFY COLUMN x ENUM('a','b'), ADD COLUMN y INT;")
        .unwrap();
    assert!(output.is_empty());
    assert!(converter
        .warnings()
        .iter()
        .any(|warning| warning.to_string().contains("multiple ALTER actions")));
}

#[test]
fn mariadb_modify_if_exists_enum_alter_is_rejected_whole() {
    for clause in ["MODIFY IF EXISTS", "MODIFY COLUMN IF EXISTS"] {
        let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
        converter
            .convert_statement(b"CREATE TABLE t (x ENUM('a'));")
            .unwrap();
        let statement = format!("ALTER TABLE t {clause} x ENUM('a','b');");
        let output = converter.convert_statement(statement.as_bytes()).unwrap();
        assert!(
            output.is_empty(),
            "got: {}",
            String::from_utf8_lossy(&output)
        );
        assert!(converter
            .warnings()
            .iter()
            .any(|warning| warning.to_string().contains("IF EXISTS")));
    }
}

#[test]
fn mariadb_alter_table_if_exists_enum_modify_is_converted() {
    let mut converter = Converter::new(SqlDialect::MySql, SqlDialect::Postgres);
    converter
        .convert_statement(b"CREATE TABLE t (x ENUM('a'));")
        .unwrap();
    let output = converter
        .convert_statement(b"ALTER TABLE IF EXISTS t MODIFY COLUMN x ENUM('a','b');")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("ALTER TABLE IF EXISTS t ALTER COLUMN x TYPE"),
        "got: {output}"
    );
    assert!(!output.contains("MODIFY"), "got: {output}");
}

#[test]
fn postgres_alter_table_if_exists_enum_type_is_converted() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a');")
        .unwrap();
    let output = converter
        .convert_statement(b"ALTER TABLE IF EXISTS t ALTER COLUMN x TYPE mood;")
        .unwrap();
    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("ALTER TABLE t MODIFY COLUMN x ENUM('a')"),
        "got: {output}"
    );
    assert!(!output.contains("IF EXISTS"), "got: {output}");
}

#[test]
fn postgres_multi_action_enum_alter_is_rejected_whole() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a');")
        .unwrap();
    let output = converter
        .convert_statement(b"ALTER TABLE t ALTER COLUMN x TYPE mood, ALTER COLUMN y TYPE mood;")
        .unwrap();
    assert!(output.is_empty());
    assert!(converter
        .warnings()
        .iter()
        .any(|warning| warning.to_string().contains("multi-action ALTER TABLE")));
}

#[test]
fn postgres_enum_change_after_table_warns() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a');")
        .unwrap();
    converter
        .convert_statement(b"CREATE TABLE t (x mood);")
        .unwrap();
    converter
        .convert_statement(b"ALTER TYPE mood ADD VALUE 'b';")
        .unwrap();
    assert!(converter
        .warnings()
        .iter()
        .any(|warning| warning.to_string().contains("already used by a table")));
}

#[test]
fn qualified_enum_change_after_unqualified_use_warns() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TYPE public.mood AS ENUM ('a');")
        .unwrap();
    converter
        .convert_statement(b"CREATE TABLE t (x mood);")
        .unwrap();
    converter
        .convert_statement(b"ALTER TYPE public.mood ADD VALUE 'b';")
        .unwrap();
    assert!(converter
        .warnings()
        .iter()
        .any(|warning| warning.to_string().contains("already used by a table")));
}

#[test]
fn strict_rejects_enum_value_rename_after_table_use() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql).with_strict(true);
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a');")
        .unwrap();
    converter
        .convert_statement(b"CREATE TABLE t (x mood);")
        .unwrap();
    assert!(converter
        .convert_statement(b"ALTER TYPE mood RENAME VALUE 'a' TO 'b';")
        .is_err());
}

#[test]
fn postgres_enum_definition_after_use_warns_and_strict_fails() {
    let mut converter = Converter::new(SqlDialect::Postgres, SqlDialect::MySql);
    converter
        .convert_statement(b"CREATE TABLE t (x mood);")
        .unwrap();
    converter
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a');")
        .unwrap();
    assert!(converter
        .warnings()
        .iter()
        .any(|warning| warning.to_string().contains("defined after a table")));

    let mut strict = Converter::new(SqlDialect::Postgres, SqlDialect::MySql).with_strict(true);
    strict
        .convert_statement(b"CREATE TABLE t (x mood);")
        .unwrap();
    assert!(strict
        .convert_statement(b"CREATE TYPE mood AS ENUM ('a');")
        .is_err());
}
