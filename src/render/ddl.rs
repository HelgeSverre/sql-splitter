//! Normalized `CREATE TABLE` rendering for the synthetic-data renderer.
//!
//! [`render_create_table`] renders a [`PortableTable`] from scratch: columns,
//! primary key, unique constraints, check constraints, and (as a following
//! `ALTER TABLE`, so a child table can reference an already-created parent)
//! foreign keys. [`should_preserve_raw_ddl`] decides when the caller can skip
//! this and reuse the table's checked-in `create_statement` instead: only
//! when the render target is the same dialect the DDL was captured in *and*
//! the table itself carries a `create_statement` to preserve. Excluded-object
//! DDL rewriting (a table selection that changes which columns/FKs a
//! same-dialect `create_statement` may still reference) is Task 26/30's
//! responsibility, not this one.

use std::fmt::Write as _;

use crate::convert::map_column_type;
use crate::convert::WarningCollector;
use crate::parser::SqlDialect;
use crate::synthetic::schema::{PortableColumn, PortableTable};
use crate::transform_common::quote_ident;

/// Whether `table`'s raw `create_statement` may be emitted as-is: the render
/// target must match the dialect the DDL was captured in, and the table must
/// actually carry one (a hand-authored `kind: model` fixture usually will
/// not).
pub(crate) fn should_preserve_raw_ddl(
    table: &PortableTable,
    source_dialect: Option<SqlDialect>,
    target_dialect: SqlDialect,
) -> bool {
    table.create_statement.is_some() && source_dialect == Some(target_dialect)
}

/// The table identifier to render: `[dbo].[name]` under MSSQL production
/// style (see [`crate::render::RenderOptions::mssql_production_style`]),
/// otherwise the ordinary dialect-quoted identifier. A no-op outside
/// [`SqlDialect::Mssql`], so a `mssql_production_style` request against a
/// non-MSSQL render target changes nothing.
pub(crate) fn qualified_table(
    dialect: SqlDialect,
    name: &str,
    mssql_production_style: bool,
) -> String {
    if mssql_production_style && dialect == SqlDialect::Mssql {
        format!("[dbo].{}", quote_ident(dialect, name))
    } else {
        quote_ident(dialect, name)
    }
}

/// Render a normalized `CREATE TABLE` (plus a trailing `ALTER TABLE ADD
/// CONSTRAINT` per foreign key, and one `CREATE INDEX` per index) for
/// `table`, mapping every column's `source_type` from `from` to `to` via
/// [`map_column_type`]. Under `mssql_production_style` (MSSQL only): table
/// names are `[dbo].`-qualified, the primary key renders as a named
/// `CONSTRAINT [PK_<table>] PRIMARY KEY CLUSTERED` instead of an inline
/// `PRIMARY KEY`, and the `CREATE TABLE` closes with an `ON [PRIMARY]`
/// filegroup clause.
pub(crate) fn render_create_table(
    table: &PortableTable,
    from: SqlDialect,
    to: SqlDialect,
    warnings: &mut WarningCollector,
    mssql_production_style: bool,
) -> String {
    let mut sql = String::new();
    let quoted_table = qualified_table(to, &table.name, mssql_production_style);
    let mssql_production_style = mssql_production_style && to == SqlDialect::Mssql;

    let mut clauses: Vec<String> = Vec::with_capacity(
        table.columns.len()
            + usize::from(!table.primary_key.is_empty())
            + table.unique_constraints.len()
            + table.check_constraints.len(),
    );
    for column in &table.columns {
        clauses.push(render_column_def(column, from, to, warnings));
    }
    if !table.primary_key.is_empty() {
        let cols = join_idents(to, &table.primary_key);
        if mssql_production_style {
            let pk_name = quote_ident(to, &format!("PK_{}", table.name));
            clauses.push(format!(
                "  CONSTRAINT {pk_name} PRIMARY KEY CLUSTERED ({cols})"
            ));
        } else {
            clauses.push(format!("  PRIMARY KEY ({cols})"));
        }
    }
    for unique in &table.unique_constraints {
        let cols = join_idents(to, &unique.columns);
        match &unique.name {
            Some(name) => clauses.push(format!(
                "  CONSTRAINT {} UNIQUE ({cols})",
                quote_ident(to, name)
            )),
            None => clauses.push(format!("  UNIQUE ({cols})")),
        }
    }
    for check in &table.check_constraints {
        match &check.name {
            Some(name) => clauses.push(format!(
                "  CONSTRAINT {} CHECK ({})",
                quote_ident(to, name),
                check.expression
            )),
            None => clauses.push(format!("  CHECK ({})", check.expression)),
        }
    }

    let _ = writeln!(sql, "CREATE TABLE {quoted_table} (");
    let _ = writeln!(sql, "{}", clauses.join(",\n"));
    if mssql_production_style {
        let _ = writeln!(sql, ") ON [PRIMARY];");
    } else {
        let _ = writeln!(sql, ");");
    }

    // Foreign keys are added after the CREATE TABLE (rather than inline) so a
    // child table can be created before every referenced parent exists in
    // the output stream and still reference it correctly once all tables in
    // dependency order have been emitted.
    for relationship in &table.relationships {
        let _ = writeln!(
            sql,
            "ALTER TABLE {quoted_table} ADD {}FOREIGN KEY ({}) REFERENCES {} ({});",
            match &relationship.name {
                Some(name) => format!("CONSTRAINT {} ", quote_ident(to, name)),
                None => String::new(),
            },
            join_idents(to, &relationship.columns),
            qualified_table(to, &relationship.referenced_table, mssql_production_style),
            join_idents(to, &relationship.referenced_columns),
        );
    }

    for index in &table.indexes {
        let kind = if index.unique {
            "UNIQUE INDEX"
        } else {
            "INDEX"
        };
        let _ = writeln!(
            sql,
            "CREATE {kind} {} ON {quoted_table} ({});",
            quote_ident(to, &index.name),
            join_idents(to, &index.columns),
        );
    }

    sql
}

/// Render one column definition: quoted name, mapped type, nullability,
/// raw `DEFAULT` expression (carried over verbatim; converting a default
/// expression's own dialect-specific syntax is out of scope here), and a
/// dialect-appropriate identity/auto-increment clause.
fn render_column_def(
    column: &PortableColumn,
    from: SqlDialect,
    to: SqlDialect,
    warnings: &mut WarningCollector,
) -> String {
    let mapped_type = map_column_type(&column.source_type, from, to, warnings);
    let mut def = format!("  {} {mapped_type}", quote_ident(to, &column.name),);
    if !column.nullable {
        def.push_str(" NOT NULL");
    }
    if let Some(default_sql) = &column.default_sql {
        let _ = write!(def, " DEFAULT {default_sql}");
    }
    if column.identity {
        def.push_str(identity_clause(to));
    }
    def
}

/// The dialect-specific clause marking a column as database-generated
/// (`IDENTITY`/`AUTO_INCREMENT`/`GENERATED ALWAYS AS IDENTITY`). SQLite has
/// no equivalent keyword (an `INTEGER PRIMARY KEY` column auto-increments
/// implicitly), so it renders nothing.
fn identity_clause(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::MySql => " AUTO_INCREMENT",
        SqlDialect::Postgres => " GENERATED ALWAYS AS IDENTITY",
        SqlDialect::Sqlite => "",
        SqlDialect::Mssql => " IDENTITY(1,1)",
    }
}

/// Quote and comma-join a list of column names for the given dialect.
fn join_idents(dialect: SqlDialect, names: &[String]) -> String {
    names
        .iter()
        .map(|name| quote_ident(dialect, name))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synthetic::schema::{PortableRelationship, PortableUniqueConstraint};
    use crate::synthetic::SqlTypeFamily;

    fn column(name: &str, source_type: &str, nullable: bool) -> PortableColumn {
        PortableColumn {
            name: name.to_string(),
            source_type: source_type.to_string(),
            family: SqlTypeFamily::Other,
            nullable,
            primary_key: false,
            unique: false,
            default_sql: None,
            generated: false,
            identity: false,
            collation: None,
        }
    }

    #[test]
    fn preserves_raw_ddl_only_when_dialect_matches_and_statement_exists() {
        let mut table = PortableTable {
            name: "t".into(),
            columns: vec![],
            primary_key: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
            indexes: vec![],
            create_statement: Some("CREATE TABLE t (id INT);".into()),
            relationships: vec![],
        };
        assert!(should_preserve_raw_ddl(
            &table,
            Some(SqlDialect::MySql),
            SqlDialect::MySql
        ));
        assert!(!should_preserve_raw_ddl(
            &table,
            Some(SqlDialect::MySql),
            SqlDialect::Postgres
        ));
        assert!(!should_preserve_raw_ddl(&table, None, SqlDialect::MySql));

        table.create_statement = None;
        assert!(!should_preserve_raw_ddl(
            &table,
            Some(SqlDialect::MySql),
            SqlDialect::MySql
        ));
    }

    #[test]
    fn renders_columns_primary_key_and_maps_types_cross_dialect() {
        let table = PortableTable {
            name: "orders".into(),
            columns: vec![
                column("id", "BIGINT", false),
                column("total", "DECIMAL(10,2)", false),
            ],
            primary_key: vec!["id".to_string()],
            unique_constraints: vec![],
            check_constraints: vec![],
            indexes: vec![],
            create_statement: None,
            relationships: vec![],
        };
        let mut warnings = WarningCollector::new();
        let sql = render_create_table(
            &table,
            SqlDialect::MySql,
            SqlDialect::Mssql,
            &mut warnings,
            false,
        );
        assert!(sql.starts_with("CREATE TABLE [orders] (\n"));
        assert!(sql.contains("[id] BIGINT NOT NULL"));
        assert!(sql.contains("PRIMARY KEY ([id])"));
    }

    #[test]
    fn mssql_production_style_qualifies_names_and_uses_a_named_clustered_pk() {
        let table = PortableTable {
            name: "orders".into(),
            columns: vec![column("id", "BIGINT", false)],
            primary_key: vec!["id".to_string()],
            unique_constraints: vec![],
            check_constraints: vec![],
            indexes: vec![],
            create_statement: None,
            relationships: vec![PortableRelationship {
                name: Some("fk_customer".into()),
                columns: vec!["id".to_string()],
                referenced_table: "customers".to_string(),
                referenced_columns: vec!["id".to_string()],
            }],
        };
        let mut warnings = WarningCollector::new();
        let sql = render_create_table(
            &table,
            SqlDialect::Mssql,
            SqlDialect::Mssql,
            &mut warnings,
            true,
        );
        assert!(sql.starts_with("CREATE TABLE [dbo].[orders] (\n"));
        assert!(sql.contains("CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED ([id])"));
        assert!(!sql.contains("  PRIMARY KEY ([id])"));
        assert!(sql.contains(") ON [PRIMARY];"));
        assert!(sql.contains("REFERENCES [dbo].[customers] ([id]);"));

        // Not MSSQL: production style has no effect on other dialects.
        let mut warnings = WarningCollector::new();
        let sql = render_create_table(
            &table,
            SqlDialect::Postgres,
            SqlDialect::Postgres,
            &mut warnings,
            true,
        );
        assert!(sql.starts_with("CREATE TABLE \"orders\" (\n"));
        assert!(sql.contains("  PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn renders_unique_check_fk_and_index_clauses() {
        let table = PortableTable {
            name: "orders".into(),
            columns: vec![column("customer_id", "BIGINT", false)],
            primary_key: vec![],
            unique_constraints: vec![PortableUniqueConstraint {
                name: Some("uq_customer".into()),
                columns: vec!["customer_id".to_string()],
            }],
            check_constraints: vec![],
            indexes: vec![crate::synthetic::schema::PortableIndex {
                name: "idx_customer".into(),
                columns: vec!["customer_id".to_string()],
                unique: false,
                index_type: None,
            }],
            create_statement: None,
            relationships: vec![PortableRelationship {
                name: Some("fk_customer".into()),
                columns: vec!["customer_id".to_string()],
                referenced_table: "customers".to_string(),
                referenced_columns: vec!["id".to_string()],
            }],
        };
        let mut warnings = WarningCollector::new();
        let sql = render_create_table(
            &table,
            SqlDialect::Postgres,
            SqlDialect::Postgres,
            &mut warnings,
            false,
        );
        assert!(sql.contains("CONSTRAINT \"uq_customer\" UNIQUE (\"customer_id\")"));
        assert!(sql.contains(
            "ALTER TABLE \"orders\" ADD CONSTRAINT \"fk_customer\" FOREIGN KEY (\"customer_id\") REFERENCES \"customers\" (\"id\");"
        ));
        assert!(sql.contains("CREATE INDEX \"idx_customer\" ON \"orders\" (\"customer_id\");"));
    }
}
