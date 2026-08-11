//! Strict, durable PostgreSQL AST contracts for reviewed migration objects.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    visit_expressions, visit_relations, BinaryOperator, CreateFunction, CreateFunctionBody,
    CreateTableOptions, CreateView, DataType, Expr, FunctionReturnType, Ident, ObjectName,
    Statement,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

pub const POSTGRES_AST_FORMAT_VERSION: u32 = 1;
pub const POSTGRES_AST_MAX_RECURSION: usize = 64;

#[derive(Debug, Error)]
pub enum PostgresAstError {
    #[error("PostgreSQL SQL parse failed: {0}")]
    Parse(#[from] sqlparser::parser::ParserError),
    #[error("durable PostgreSQL AST JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported PostgreSQL AST: {0}")]
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresAstIdentity {
    pub parts: Vec<Ident>,
}

impl From<&ObjectName> for PostgresAstIdentity {
    fn from(name: &ObjectName) -> Self {
        Self {
            parts: name
                .0
                .iter()
                .filter_map(|part| part.as_ident().cloned())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresAstDependencyHints {
    pub relations: BTreeSet<PostgresAstIdentity>,
    pub functions: BTreeSet<PostgresAstIdentity>,
    pub types: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresViewAst {
    pub format_version: u32,
    pub statement: CreateView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSqlFunctionAst {
    pub format_version: u32,
    pub statement: CreateFunction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PostgresDurableAst {
    View(Box<PostgresViewAst>),
    SqlFunction(Box<PostgresSqlFunctionAst>),
}

impl PostgresDurableAst {
    pub fn validate(&self) -> Result<(), PostgresAstError> {
        match self {
            Self::View(view) => view.validate(),
            Self::SqlFunction(function) => function.validate(),
        }
    }

    /// Return non-authoritative names that must be resolved against `pg_depend`.
    pub fn syntactic_dependency_hints(&self) -> PostgresAstDependencyHints {
        match self {
            Self::View(view) => view.syntactic_dependency_hints(),
            Self::SqlFunction(function) => function.syntactic_dependency_hints(),
        }
    }

    pub fn render_canonical(&self) -> Result<String, PostgresAstError> {
        self.validate()?;
        Ok(match self {
            Self::View(view) => Statement::CreateView(view.statement.clone()).to_string(),
            Self::SqlFunction(function) => {
                Statement::CreateFunction(function.statement.clone()).to_string()
            }
        })
    }

    pub fn canonical_json(&self) -> Result<String, PostgresAstError> {
        self.validate()?;
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_canonical_json(json: &str) -> Result<Self, PostgresAstError> {
        let ast: Self = serde_json::from_str(json)?;
        ast.validate()?;
        let rendered = ast.render_canonical()?;
        let reparsed = parse_postgres_durable_statement(&rendered)?;
        if ast != reparsed {
            return Err(PostgresAstError::Unsupported(
                "serialized AST does not have a stable canonical round trip".into(),
            ));
        }
        Ok(ast)
    }
}

impl PostgresViewAst {
    pub fn validate(&self) -> Result<(), PostgresAstError> {
        require_version(self.format_version)?;
        let view = &self.statement;
        if view.or_alter
            || view.or_replace
            || view.materialized
            || view.secure
            || view.name_before_not_exists
            || view.if_not_exists
            || view.temporary
            || view.copy_grants
            || view.with_no_schema_binding
            || view.comment.is_some()
            || view.to.is_some()
            || view.params.is_some()
            || !view.cluster_by.is_empty()
            || view.options != CreateTableOptions::None
        {
            return Err(PostgresAstError::Unsupported(
                "view is not an ordinary create-only PostgreSQL view".into(),
            ));
        }
        require_qualified_name(&view.name, "view")
            .and_then(|()| require_safe_query(&view.query))?;
        validate_dependency_names(&Statement::CreateView(view.clone()))
    }

    pub fn syntactic_dependency_hints(&self) -> PostgresAstDependencyHints {
        dependency_hints_for(&Statement::CreateView(self.statement.clone()), &[])
    }
}

impl PostgresSqlFunctionAst {
    pub fn validate(&self) -> Result<(), PostgresAstError> {
        require_version(self.format_version)?;
        let function = &self.statement;
        if function.or_alter
            || function.or_replace
            || function.temporary
            || function.if_not_exists
            || function.using.is_some()
            || function.determinism_specifier.is_some()
            || function.options.is_some()
            || function.remote_connection.is_some()
            || !function.set_params.is_empty()
        {
            return Err(PostgresAstError::Unsupported(
                "function uses a non-create-only or unsafe clause".into(),
            ));
        }
        require_qualified_name(&function.name, "function")?;
        if function
            .language
            .as_ref()
            .is_none_or(|language| !language.value.eq_ignore_ascii_case("sql"))
        {
            return Err(PostgresAstError::Unsupported(
                "function must explicitly use LANGUAGE SQL".into(),
            ));
        }
        match &function.function_body {
            Some(CreateFunctionBody::Return(_)) => Ok(()),
            Some(CreateFunctionBody::AsBeforeOptions { .. })
            | Some(CreateFunctionBody::AsAfterOptions(_)) => Err(PostgresAstError::Unsupported(
                "raw string or dollar-quoted routine bodies are forbidden".into(),
            )),
            Some(_) => Err(PostgresAstError::Unsupported(
                "only parsed SQL-standard RETURN bodies are supported by sqlparser 0.62".into(),
            )),
            None => Err(PostgresAstError::Unsupported(
                "SQL function has no parsed body".into(),
            )),
        }?;
        for data_type in function_types_ast(function) {
            reject_custom_type(data_type)?;
        }
        validate_dependency_names(&Statement::CreateFunction(function.clone()))
    }

    pub fn syntactic_dependency_hints(&self) -> PostgresAstDependencyHints {
        let types = function_types(&self.statement);
        dependency_hints_for(&Statement::CreateFunction(self.statement.clone()), &types)
    }
}

pub fn parse_postgres_create_view(sql: &str) -> Result<PostgresViewAst, PostgresAstError> {
    match parse_one(sql)? {
        Statement::CreateView(statement) => {
            let ast = PostgresViewAst {
                format_version: POSTGRES_AST_FORMAT_VERSION,
                statement,
            };
            ast.validate()?;
            Ok(ast)
        }
        _ => Err(PostgresAstError::Unsupported(
            "expected exactly one CREATE VIEW statement".into(),
        )),
    }
}

pub fn parse_postgres_sql_function(sql: &str) -> Result<PostgresSqlFunctionAst, PostgresAstError> {
    match parse_one(sql)? {
        Statement::CreateFunction(statement) => {
            let ast = PostgresSqlFunctionAst {
                format_version: POSTGRES_AST_FORMAT_VERSION,
                statement,
            };
            ast.validate()?;
            Ok(ast)
        }
        _ => Err(PostgresAstError::Unsupported(
            "expected exactly one CREATE FUNCTION statement".into(),
        )),
    }
}

pub fn parse_postgres_durable_statement(sql: &str) -> Result<PostgresDurableAst, PostgresAstError> {
    let statement = parse_one(sql)?;
    let ast = match statement {
        Statement::CreateView(statement) => PostgresDurableAst::View(Box::new(PostgresViewAst {
            format_version: POSTGRES_AST_FORMAT_VERSION,
            statement,
        })),
        Statement::CreateFunction(statement) => {
            PostgresDurableAst::SqlFunction(Box::new(PostgresSqlFunctionAst {
                format_version: POSTGRES_AST_FORMAT_VERSION,
                statement,
            }))
        }
        _ => {
            return Err(PostgresAstError::Unsupported(
                "only CREATE VIEW and CREATE FUNCTION are durable".into(),
            ));
        }
    };
    ast.validate()?;
    Ok(ast)
}

fn parse_one(sql: &str) -> Result<Statement, PostgresAstError> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::new(&dialect)
        .with_recursion_limit(POSTGRES_AST_MAX_RECURSION)
        .try_with_sql(sql)?
        .parse_statements()?;
    if statements.len() != 1 {
        return Err(PostgresAstError::Unsupported(
            "exactly one SQL statement is required".into(),
        ));
    }
    Ok(statements.remove(0))
}

fn require_version(version: u32) -> Result<(), PostgresAstError> {
    if version == POSTGRES_AST_FORMAT_VERSION {
        Ok(())
    } else {
        Err(PostgresAstError::Unsupported(format!(
            "unsupported PostgreSQL AST format version {version}"
        )))
    }
}

fn require_qualified_name(name: &ObjectName, kind: &str) -> Result<(), PostgresAstError> {
    if name.0.len() == 2 && name.0.iter().all(|part| part.as_ident().is_some()) {
        Ok(())
    } else {
        Err(PostgresAstError::Unsupported(format!(
            "{kind} name must be exactly schema-qualified"
        )))
    }
}

fn require_safe_query(query: &sqlparser::ast::Query) -> Result<(), PostgresAstError> {
    if !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(PostgresAstError::Unsupported(
            "view query uses a locking or non-PostgreSQL execution clause".into(),
        ));
    }
    Ok(())
}

fn validate_dependency_names(statement: &Statement) -> Result<(), PostgresAstError> {
    let mut error = None;
    let _ = visit_relations(statement, |name| {
        if name.0.len() != 2 || name.0.iter().any(|part| part.as_ident().is_none()) {
            error = Some("every relation reference must be exactly schema-qualified");
        }
        ControlFlow::<()>::Continue(())
    });
    let _ = visit_expressions(statement, |expression| {
        if let Expr::Function(function) = expression {
            if function.name.0.len() != 2
                || function.name.0.iter().any(|part| part.as_ident().is_none())
            {
                error = Some("every function reference must be exactly schema-qualified");
            }
        }
        match expression {
            Expr::BinaryOp {
                op: BinaryOperator::Custom(_),
                ..
            } => error = Some("custom operators require catalog-resolved dependencies"),
            Expr::Cast {
                data_type: DataType::Custom(_, _),
                ..
            } => {
                error = Some("casts to domains or custom types require catalog resolution");
            }
            Expr::Collate { .. } => {
                error = Some("explicit collations require catalog-resolved dependencies");
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    if let Some(error) = error {
        Err(PostgresAstError::Unsupported(error.into()))
    } else {
        Ok(())
    }
}

fn dependency_hints_for(statement: &Statement, types: &[String]) -> PostgresAstDependencyHints {
    let mut dependencies = PostgresAstDependencyHints {
        types: types.iter().cloned().collect(),
        ..PostgresAstDependencyHints::default()
    };
    let _ = visit_relations(statement, |relation| {
        dependencies
            .relations
            .insert(PostgresAstIdentity::from(relation));
        ControlFlow::<()>::Continue(())
    });
    let _ = visit_expressions(statement, |expression| {
        if let Expr::Function(function) = expression {
            dependencies
                .functions
                .insert(PostgresAstIdentity::from(&function.name));
        }
        ControlFlow::<()>::Continue(())
    });
    dependencies
}

fn function_types_ast(function: &CreateFunction) -> Vec<&DataType> {
    let mut types = function
        .args
        .iter()
        .flatten()
        .map(|argument| &argument.data_type)
        .collect::<Vec<_>>();
    if let Some(FunctionReturnType::DataType(data_type) | FunctionReturnType::SetOf(data_type)) =
        &function.return_type
    {
        types.push(data_type);
    }
    types
}

fn reject_custom_type(data_type: &DataType) -> Result<(), PostgresAstError> {
    if matches!(data_type, DataType::Custom(_, _)) {
        Err(PostgresAstError::Unsupported(
            "domains and custom function types require catalog resolution".into(),
        ))
    } else {
        Ok(())
    }
}

fn function_types(function: &CreateFunction) -> Vec<String> {
    let mut types = function
        .args
        .iter()
        .flatten()
        .map(|argument| argument.data_type.to_string())
        .collect::<Vec<_>>();
    match &function.return_type {
        Some(FunctionReturnType::DataType(data_type))
        | Some(FunctionReturnType::SetOf(data_type)) => types.push(data_type.to_string()),
        None => {}
    }
    types
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostile_quoted_identifiers_round_trip() {
        let ast = parse_postgres_create_view(
            r#"CREATE VIEW "odd.schema"."v""name" ("out""col") AS SELECT "in""col" FROM "src.schema"."t""name""#,
        )
        .unwrap();
        let durable = PostgresDurableAst::View(Box::new(ast));
        let reparsed =
            parse_postgres_durable_statement(&durable.render_canonical().unwrap()).unwrap();
        assert_eq!(reparsed, durable);
    }

    #[test]
    fn postgres_deparsed_view_with_qualified_dependencies_is_accepted() {
        parse_postgres_create_view(
            "CREATE VIEW public.account_values AS SELECT id, public.double_id(id) AS doubled FROM public.accounts;",
        )
        .unwrap();
    }

    #[test]
    fn parsed_sql_return_function_round_trips_json() {
        let ast = parse_postgres_durable_statement(
            r#"CREATE FUNCTION "app"."plus_one"("x" integer) RETURNS integer LANGUAGE SQL IMMUTABLE RETURN "x" + 1"#,
        )
        .unwrap();
        let json = ast.canonical_json().unwrap();
        assert_eq!(PostgresDurableAst::from_canonical_json(&json).unwrap(), ast);
    }

    #[test]
    fn raw_and_dollar_quoted_bodies_are_rejected() {
        for sql in [
            "CREATE FUNCTION app.f() RETURNS integer LANGUAGE SQL AS 'SELECT 1'",
            "CREATE FUNCTION app.f() RETURNS integer LANGUAGE SQL AS $$ SELECT 1 $$",
        ] {
            assert!(matches!(
                parse_postgres_sql_function(sql),
                Err(PostgresAstError::Unsupported(_))
            ));
        }
    }

    #[test]
    fn multiple_statements_are_rejected() {
        assert!(matches!(
            parse_postgres_durable_statement(
                "CREATE VIEW app.v AS SELECT 1; DROP TABLE app.accounts"
            ),
            Err(PostgresAstError::Unsupported(_))
        ));
    }

    #[test]
    fn parser_recursion_is_bounded() {
        let nested = format!(
            "CREATE VIEW app.v AS SELECT {}1{}",
            "(".repeat(POSTGRES_AST_MAX_RECURSION + 10),
            ")".repeat(POSTGRES_AST_MAX_RECURSION + 10)
        );
        assert!(parse_postgres_create_view(&nested).is_err());
    }

    #[test]
    fn unqualified_relation_and_function_names_are_rejected() {
        for sql in [
            "CREATE VIEW app.v AS SELECT id FROM accounts",
            "CREATE VIEW app.v AS SELECT lower(name) FROM app.accounts",
        ] {
            assert!(matches!(
                parse_postgres_create_view(sql),
                Err(PostgresAstError::Unsupported(_))
            ));
        }
    }

    #[test]
    fn unresolved_operator_cast_domain_and_collation_dependencies_are_rejected() {
        for sql in [
            "CREATE VIEW app.v AS SELECT 1 OPERATOR(app.===) 1",
            "CREATE VIEW app.v AS SELECT id::app.account_id FROM app.accounts",
            "CREATE VIEW app.v AS SELECT name COLLATE app.custom FROM app.accounts",
            "CREATE FUNCTION app.f(x app.account_id) RETURNS integer LANGUAGE SQL RETURN 1",
        ] {
            assert!(
                parse_postgres_durable_statement(sql).is_err(),
                "unsafe dependency form was accepted: {sql}"
            );
        }
    }

    #[test]
    fn dependency_hints_are_explicitly_syntactic() {
        let ast = parse_postgres_durable_statement(
            "CREATE VIEW app.v AS SELECT pg_catalog.abs(id) FROM app.accounts",
        )
        .unwrap();
        let hints = ast.syntactic_dependency_hints();
        assert_eq!(hints.relations.len(), 1);
        assert_eq!(hints.functions.len(), 1);
    }
}
