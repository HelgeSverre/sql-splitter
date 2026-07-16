//! Tests for the synthetic-data-generation diagnostics module.

use sql_splitter::diagnostic::{Diagnostic, DiagnosticBag, Severity, SourceLocation};

#[test]
fn diagnostic_bag_keeps_independent_errors() {
    let mut bag = DiagnosticBag::default();
    bag.error("GEN-MISSING-TABLE", "tables.orders", "table does not exist");
    bag.error(
        "GEN-MISSING-COLUMN",
        "tables.users.columns.email",
        "column does not exist",
    );
    assert_eq!(bag.errors().count(), 2);
    assert!(serde_json::to_value(&bag).unwrap()["diagnostics"].is_array());
}

#[test]
fn diagnostic_bag_into_result_errs_when_errors_present() {
    let mut bag = DiagnosticBag::default();
    bag.error("GEN-MISSING-TABLE", "tables.orders", "table does not exist");
    assert!(bag.into_result(()).is_err());
}

#[test]
fn diagnostic_bag_into_result_oks_when_only_warnings_present() {
    let mut bag = DiagnosticBag::default();
    bag.warning(
        "GEN-UNUSED-COLUMN",
        "tables.orders.columns.legacy_flag",
        "column is never referenced by a generator or planner",
    );
    assert_eq!(bag.into_result(42).unwrap(), 42);
}

#[test]
fn diagnostic_bag_into_result_oks_when_empty() {
    let bag = DiagnosticBag::default();
    assert_eq!(bag.into_result("value").unwrap(), "value");
}

#[test]
fn diagnostic_errors_excludes_warnings() {
    let mut bag = DiagnosticBag::default();
    bag.warning(
        "GEN-UNUSED-COLUMN",
        "tables.orders.columns.legacy_flag",
        "unused column",
    );
    bag.error("GEN-MISSING-TABLE", "tables.orders", "table does not exist");
    assert_eq!(bag.errors().count(), 1);
    assert_eq!(bag.diagnostics.len(), 2);
}

#[test]
fn diagnostic_display_renders_code_path_related_and_help() {
    let diagnostic = Diagnostic {
        code: "GEN-COLUMN-OWNER-CONFLICT".to_string(),
        severity: Severity::Error,
        path: "tables.orders.columns.total".to_string(),
        message: "tables.orders.columns.total is produced by both:".to_string(),
        help: Some(
            "remove the column generator or remove `total` from the planner mapping".to_string(),
        ),
        related: vec![
            SourceLocation {
                path: "columns.total.generator".to_string(),
                description: None,
            },
            SourceLocation {
                path: "planners[0]".to_string(),
                description: Some("commerce.order_family".to_string()),
            },
        ],
    };

    let rendered = diagnostic.to_string();
    let expected = "error[GEN-COLUMN-OWNER-CONFLICT] tables.orders.columns.total\n  \
tables.orders.columns.total is produced by both:\n    \
- columns.total.generator\n    \
- planners[0] (commerce.order_family)\n  \
help: remove the column generator or remove `total` from the planner mapping";

    assert_eq!(rendered, expected);
}

#[test]
fn diagnostic_display_omits_help_and_related_when_absent() {
    let diagnostic = Diagnostic {
        code: "GEN-MISSING-TABLE".to_string(),
        severity: Severity::Error,
        path: "tables.orders".to_string(),
        message: "table does not exist".to_string(),
        help: None,
        related: Vec::new(),
    };

    assert_eq!(
        diagnostic.to_string(),
        "error[GEN-MISSING-TABLE] tables.orders\n  table does not exist"
    );
}

#[test]
fn diagnostic_json_shape_omits_absent_optional_fields() {
    let mut bag = DiagnosticBag::default();
    bag.error("GEN-MISSING-TABLE", "tables.orders", "table does not exist");

    let value = serde_json::to_value(&bag).unwrap();
    let diagnostics = value["diagnostics"].as_array().unwrap();
    assert_eq!(diagnostics.len(), 1);

    let first = &diagnostics[0];
    assert_eq!(first["code"], "GEN-MISSING-TABLE");
    assert_eq!(first["severity"], "error");
    assert_eq!(first["path"], "tables.orders");
    assert_eq!(first["message"], "table does not exist");
    assert!(first.get("help").is_none());
    assert!(first.get("related").is_none());
}

#[test]
fn diagnostic_json_roundtrips_through_serde() {
    let mut bag = DiagnosticBag::default();
    bag.warning(
        "GEN-UNUSED-COLUMN",
        "tables.orders.columns.legacy_flag",
        "unused column",
    );

    let json = serde_json::to_string(&bag).unwrap();
    let restored: DiagnosticBag = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.diagnostics, bag.diagnostics);
}
