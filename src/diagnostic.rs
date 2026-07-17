//! Structured diagnostics for the synthetic data generator.
//!
//! Config loading, compilation, heuristics, and verification all need to
//! report problems without stopping at the first one: a config with three
//! unknown tables should say so three times, not once. [`DiagnosticBag`] is
//! the single home for collecting those problems, and [`Diagnostic`] is the
//! stable shape shared by human (`Display`) and machine (JSON) reports.
//!
//! Codes are plain strings rather than an enum so that statically linked
//! extensions can mint their own namespaced codes (e.g. `EXT-FOO-BAR`)
//! without changing this crate.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable diagnostic code, e.g. `GEN-MISSING-TABLE`.
///
/// Kept as a plain string (not an enum) so extensions can define their own
/// namespaced codes without a dependency on this crate's enum.
pub type DiagnosticCode = String;

/// How serious a [`Diagnostic`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Warning,
    Error,
}

/// A named location related to a diagnostic, e.g. the other place a column
/// is produced from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceLocation {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl fmt::Display for SourceLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.description {
            Some(description) => write!(f, "{} ({description})", self.path),
            None => write!(f, "{}", self.path),
        }
    }
}

/// A single diagnostic: a stable code, severity, YAML path, message, and
/// optional help/related locations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: DiagnosticCode,
    pub severity: Severity,
    pub path: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related: Vec<SourceLocation>,
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let severity = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };
        let mut lines = vec![
            format!("{severity}[{}] {}", self.code, self.path),
            format!("  {}", self.message),
        ];
        for location in &self.related {
            lines.push(format!("    - {location}"));
        }
        if let Some(help) = &self.help {
            lines.push(format!("  help: {help}"));
        }
        write!(f, "{}", lines.join("\n"))
    }
}

/// Collects independent diagnostics from a single compilation pass.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticBag {
    pub diagnostics: Vec<Diagnostic>,
}

impl DiagnosticBag {
    /// Records an error-severity diagnostic and returns it for further
    /// customization (e.g. setting `help` or `related`).
    pub fn error(
        &mut self,
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> &mut Diagnostic {
        self.push(Severity::Error, code, path, message)
    }

    /// Records a warning-severity diagnostic and returns it for further
    /// customization (e.g. setting `help` or `related`).
    pub fn warning(
        &mut self,
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> &mut Diagnostic {
        self.push(Severity::Warning, code, path, message)
    }

    fn push(
        &mut self,
        severity: Severity,
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> &mut Diagnostic {
        self.diagnostics.push(Diagnostic {
            code: code.into(),
            severity,
            path: path.into(),
            message: message.into(),
            help: None,
            related: Vec::new(),
        });
        self.diagnostics
            .last_mut()
            .expect("just pushed a diagnostic")
    }

    /// Iterates over error-severity diagnostics only.
    pub fn errors(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity == Severity::Error)
    }

    /// True if any error-severity diagnostic has been recorded. Warnings
    /// alone do not count.
    pub fn has_errors(&self) -> bool {
        self.errors().next().is_some()
    }

    /// True if any diagnostic (of any severity) carries `code`.
    pub fn has_code(&self, code: &str) -> bool {
        self.diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == code)
    }

    /// Converts the bag into a `Result`: `Ok(value)` if no error-severity
    /// diagnostic was recorded, otherwise `Err(self)` with all diagnostics
    /// (including warnings) intact for reporting.
    pub fn into_result<T>(self, value: T) -> Result<T, DiagnosticBag> {
        if self.has_errors() {
            Err(self)
        } else {
            Ok(value)
        }
    }
}

impl fmt::Display for DiagnosticBag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rendered: Vec<String> = self.diagnostics.iter().map(Diagnostic::to_string).collect();
        write!(f, "{}", rendered.join("\n"))
    }
}

impl std::error::Error for DiagnosticBag {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_defaults_to_no_help_or_related() {
        let mut bag = DiagnosticBag::default();
        bag.error("GEN-MISSING-TABLE", "tables.orders", "table does not exist");
        let diagnostic = &bag.diagnostics[0];
        assert_eq!(diagnostic.severity, Severity::Error);
        assert!(diagnostic.help.is_none());
        assert!(diagnostic.related.is_empty());
    }

    #[test]
    fn has_errors_is_false_for_warnings_only() {
        let mut bag = DiagnosticBag::default();
        bag.warning(
            "GEN-UNUSED-COLUMN",
            "tables.orders.columns.legacy_flag",
            "unused column",
        );
        assert!(!bag.has_errors());
    }
}
