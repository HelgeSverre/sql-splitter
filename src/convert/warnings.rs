//! Warning system for convert command.
//!
//! Tracks and reports unsupported features, lossy conversions,
//! and other issues that arise during dialect conversion.

/// Warning types that can occur during conversion
#[derive(Debug, Clone, PartialEq)]
pub enum ConvertWarning {
    /// Feature not supported in target dialect
    UnsupportedFeature {
        feature: String,
        suggestion: Option<String>,
    },
    /// Data type conversion may lose precision
    LossyConversion {
        from_type: String,
        to_type: String,
        table: Option<String>,
        column: Option<String>,
    },
    /// Statement was skipped
    SkippedStatement {
        reason: String,
        statement_preview: String,
    },
    /// COPY statement needs conversion (PostgreSQL)
    CopyNotConverted { table: String },
}

impl std::fmt::Display for ConvertWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvertWarning::UnsupportedFeature {
                feature,
                suggestion,
            } => {
                write!(f, "Unsupported feature: {}", feature)?;
                if let Some(s) = suggestion {
                    write!(f, " ({})", s)?;
                }
                Ok(())
            }
            ConvertWarning::LossyConversion {
                from_type,
                to_type,
                table,
                column,
            } => {
                write!(f, "Lossy conversion: {} → {}", from_type, to_type)?;
                if let Some(t) = table {
                    write!(f, " in table {}", t)?;
                    if let Some(c) = column {
                        write!(f, ".{}", c)?;
                    }
                }
                Ok(())
            }
            ConvertWarning::SkippedStatement {
                reason,
                statement_preview,
            } => {
                write!(f, "Skipped: {} ({})", reason, statement_preview)
            }
            ConvertWarning::CopyNotConverted { table } => {
                write!(
                    f,
                    "COPY statement for table '{}' not converted - requires INSERT conversion",
                    table
                )
            }
        }
    }
}

/// Collects warnings during conversion
#[derive(Debug, Default)]
pub struct WarningCollector {
    warnings: Vec<ConvertWarning>,
    max_warnings: usize,
}

impl WarningCollector {
    pub fn new() -> Self {
        Self {
            warnings: Vec::new(),
            max_warnings: 100, // Limit to avoid memory issues
        }
    }

    pub fn with_limit(limit: usize) -> Self {
        Self {
            warnings: Vec::new(),
            max_warnings: limit,
        }
    }

    /// Add a warning
    pub fn add(&mut self, warning: ConvertWarning) {
        if self.warnings.len() < self.max_warnings {
            // Deduplicate similar warnings
            if !self.warnings.iter().any(|w| Self::is_similar(w, &warning)) {
                self.warnings.push(warning);
            }
        }
    }

    /// Check if two warnings are similar enough to deduplicate
    fn is_similar(a: &ConvertWarning, b: &ConvertWarning) -> bool {
        match (a, b) {
            (
                ConvertWarning::UnsupportedFeature { feature: f1, .. },
                ConvertWarning::UnsupportedFeature { feature: f2, .. },
            ) => f1 == f2,
            (
                ConvertWarning::LossyConversion {
                    from_type: f1,
                    to_type: t1,
                    ..
                },
                ConvertWarning::LossyConversion {
                    from_type: f2,
                    to_type: t2,
                    ..
                },
            ) => f1 == f2 && t1 == t2,
            _ => false,
        }
    }

    /// Get all collected warnings
    pub fn warnings(&self) -> &[ConvertWarning] {
        &self.warnings
    }

    /// Check if any warnings were collected
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Get warning count
    pub fn count(&self) -> usize {
        self.warnings.len()
    }

    /// Print summary of warnings
    pub fn print_summary(&self) {
        if self.warnings.is_empty() {
            return;
        }

        eprintln!("\nConversion warnings ({}):", self.warnings.len());
        for warning in &self.warnings {
            eprintln!("  ⚠ {}", warning);
        }

        if self.warnings.len() >= self.max_warnings {
            eprintln!("  ... (additional warnings truncated)");
        }
    }
}
