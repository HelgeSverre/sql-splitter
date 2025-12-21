//! JSON output formatter for diff results.

use crate::differ::DiffResult;

/// Format diff result as JSON
pub fn format_json(result: &DiffResult) -> String {
    serde_json::to_string_pretty(result).unwrap_or_else(|e| format!("{{\"error\": \"{}\"}}", e))
}
