//! Output formatters for diff results.

mod json;
mod sql;
mod text;

pub use json::format_json;
pub use sql::format_sql;
pub use text::format_text;

use super::{DiffOutputFormat, DiffResult};
use crate::parser::SqlDialect;

/// Format diff result according to the specified format
pub fn format_diff(result: &DiffResult, format: DiffOutputFormat, dialect: SqlDialect) -> String {
    match format {
        DiffOutputFormat::Text => format_text(result),
        DiffOutputFormat::Json => format_json(result),
        DiffOutputFormat::Sql => format_sql(result, dialect),
    }
}
