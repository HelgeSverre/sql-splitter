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

pub mod codes;

/// Canonical website page for built-in synthetic-generation diagnostics.
pub const DIAGNOSTICS_DOCUMENTATION_URL: &str =
    "https://sql-splitter.dev/commands/generate/diagnostics/";

/// Stable diagnostic code, e.g. `GEN-MISSING-TABLE`.
///
/// Kept as a plain string (not an enum) so extensions can define their own
/// namespaced codes without a dependency on this crate's enum.
pub type DiagnosticCode = String;

/// Area of the generate pipeline that owns a built-in diagnostic definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticCategory {
    Config,
    Selection,
    Generator,
    Modifier,
    Planner,
    Inference,
    Profiling,
    Rendering,
    Verification,
    Runtime,
    Registry,
    Privacy,
}

/// Usual severity of a built-in diagnostic code.
///
/// A definition may be [`Variable`](TypicalSeverity::Variable) when policy
/// selects the actual severity for each occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypicalSeverity {
    Info,
    Advisory,
    Warning,
    Error,
    Variable,
}

/// Stable metadata shared by every occurrence of a built-in diagnostic code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiagnosticDefinition {
    pub code: &'static str,
    pub title: &'static str,
    pub category: DiagnosticCategory,
    pub typical_severity: TypicalSeverity,
    pub summary: &'static str,
}

impl DiagnosticDefinition {
    /// Canonical case-preserving anchor for this built-in diagnostic.
    pub fn documentation_url(&self) -> String {
        format!("{DIAGNOSTICS_DOCUMENTATION_URL}#{}", self.code)
    }
}

impl From<&'static DiagnosticDefinition> for DiagnosticCode {
    fn from(definition: &'static DiagnosticDefinition) -> Self {
        definition.code.to_string()
    }
}

/// How serious a [`Diagnostic`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Advisory,
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

impl Diagnostic {
    /// Creates a diagnostic occurrence with no help or related locations.
    pub fn new(
        severity: Severity,
        code: impl Into<DiagnosticCode>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            severity,
            path: path.into(),
            message: message.into(),
            help: None,
            related: Vec::new(),
        }
    }

    pub fn info(
        code: impl Into<DiagnosticCode>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::new(Severity::Info, code, path, message)
    }

    pub fn advisory(
        code: impl Into<DiagnosticCode>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::new(Severity::Advisory, code, path, message)
    }

    pub fn warning(
        code: impl Into<DiagnosticCode>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::new(Severity::Warning, code, path, message)
    }

    pub fn error(
        code: impl Into<DiagnosticCode>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::new(Severity::Error, code, path, message)
    }

    /// Canonical documentation URL for built-ins, or `None` for extension codes.
    pub fn documentation_url(&self) -> Option<String> {
        codes::find(&self.code).map(DiagnosticDefinition::documentation_url)
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let severity = match self.severity {
            Severity::Info => "info",
            Severity::Advisory => "advisory",
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
        if let Some(definition) = codes::find(&self.code) {
            lines.push(format!("  docs: {}", definition.documentation_url()));
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
    /// Records an informational diagnostic. Informational diagnostics never
    /// block generation and are not promoted by strict mode.
    pub fn info(
        &mut self,
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> &mut Diagnostic {
        self.push(Severity::Info, code, path, message)
    }

    /// Records an advisory diagnostic. Advisories are non-fatal safety notices
    /// that remain visible even when ordinary reporting is quiet.
    pub fn advisory(
        &mut self,
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> &mut Diagnostic {
        self.push(Severity::Advisory, code, path, message)
    }

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
        self.diagnostics
            .push(Diagnostic::new(severity, code, path, message));
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
