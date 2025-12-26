//! Output formatting for query results.

use super::QueryResult;
use std::io::Write;

/// Output format for query results
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum OutputFormat {
    /// ASCII table format (default)
    #[default]
    Table,
    /// JSON array format
    Json,
    /// JSON lines format (one object per line)
    JsonLines,
    /// CSV format
    Csv,
    /// Tab-separated values
    Tsv,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "table" => Ok(OutputFormat::Table),
            "json" => Ok(OutputFormat::Json),
            "jsonl" | "jsonlines" | "ndjson" => Ok(OutputFormat::JsonLines),
            "csv" => Ok(OutputFormat::Csv),
            "tsv" => Ok(OutputFormat::Tsv),
            _ => Err(format!(
                "Unknown format: {}. Valid: table, json, jsonl, csv, tsv",
                s
            )),
        }
    }
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Table => write!(f, "table"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::JsonLines => write!(f, "jsonl"),
            OutputFormat::Csv => write!(f, "csv"),
            OutputFormat::Tsv => write!(f, "tsv"),
        }
    }
}

/// Formatter for query results
pub struct QueryResultFormatter;

impl QueryResultFormatter {
    /// Format a query result to a string
    pub fn format(result: &QueryResult, format: OutputFormat) -> String {
        match format {
            OutputFormat::Table => Self::format_table(result),
            OutputFormat::Json => Self::format_json(result),
            OutputFormat::JsonLines => Self::format_jsonl(result),
            OutputFormat::Csv => Self::format_csv(result),
            OutputFormat::Tsv => Self::format_tsv(result),
        }
    }

    /// Write formatted result to a writer
    pub fn write<W: Write>(
        result: &QueryResult,
        format: OutputFormat,
        writer: &mut W,
    ) -> std::io::Result<()> {
        let output = Self::format(result, format);
        writer.write_all(output.as_bytes())
    }

    /// Format as ASCII table
    fn format_table(result: &QueryResult) -> String {
        if result.columns.is_empty() {
            return String::new();
        }

        // Calculate column widths
        let mut widths: Vec<usize> = result.columns.iter().map(|c| c.len()).collect();

        for row in &result.rows {
            for (i, val) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(val.len());
                }
            }
        }

        // Cap widths at 50 chars for readability
        let max_width = 50;
        widths.iter_mut().for_each(|w| *w = (*w).min(max_width));

        let mut output = String::new();

        // Top border
        output.push('┌');
        for (i, width) in widths.iter().enumerate() {
            output.push_str(&"─".repeat(*width + 2));
            if i < widths.len() - 1 {
                output.push('┬');
            }
        }
        output.push_str("┐\n");

        // Header row
        output.push('│');
        for (i, col) in result.columns.iter().enumerate() {
            let truncated = Self::truncate(col, widths[i]);
            output.push_str(&format!(" {:width$} │", truncated, width = widths[i]));
        }
        output.push('\n');

        // Header separator
        output.push('├');
        for (i, width) in widths.iter().enumerate() {
            output.push_str(&"─".repeat(*width + 2));
            if i < widths.len() - 1 {
                output.push('┼');
            }
        }
        output.push_str("┤\n");

        // Data rows
        for row in &result.rows {
            output.push('│');
            for (i, val) in row.iter().enumerate() {
                if i < widths.len() {
                    let truncated = Self::truncate(val, widths[i]);
                    output.push_str(&format!(" {:width$} │", truncated, width = widths[i]));
                }
            }
            output.push('\n');
        }

        // Bottom border
        output.push('└');
        for (i, width) in widths.iter().enumerate() {
            output.push_str(&"─".repeat(*width + 2));
            if i < widths.len() - 1 {
                output.push('┴');
            }
        }
        output.push_str("┘\n");

        // Row count
        output.push_str(&format!(
            "{} row{}\n",
            result.rows.len(),
            if result.rows.len() == 1 { "" } else { "s" }
        ));

        output
    }

    /// Truncate a string to a maximum length
    fn truncate(s: &str, max_len: usize) -> String {
        if s.len() <= max_len {
            s.to_string()
        } else {
            format!("{}…", &s[..max_len - 1])
        }
    }

    /// Format as JSON array
    fn format_json(result: &QueryResult) -> String {
        let rows: Vec<serde_json::Value> = result
            .rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = result
                    .columns
                    .iter()
                    .zip(row.iter())
                    .map(|(col, val)| (col.clone(), Self::json_value(val)))
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();

        serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".to_string())
    }

    /// Format as JSON lines (NDJSON)
    fn format_jsonl(result: &QueryResult) -> String {
        result
            .rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = result
                    .columns
                    .iter()
                    .zip(row.iter())
                    .map(|(col, val)| (col.clone(), Self::json_value(val)))
                    .collect();
                serde_json::to_string(&serde_json::Value::Object(obj))
                    .unwrap_or_else(|_| "{}".to_string())
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Convert a string value to appropriate JSON type
    fn json_value(val: &str) -> serde_json::Value {
        if val == "NULL" {
            return serde_json::Value::Null;
        }

        // Try to parse as number
        if let Ok(n) = val.parse::<i64>() {
            return serde_json::Value::Number(n.into());
        }
        if let Ok(n) = val.parse::<f64>() {
            if let Some(num) = serde_json::Number::from_f64(n) {
                return serde_json::Value::Number(num);
            }
        }

        // Try to parse as boolean
        if val.eq_ignore_ascii_case("true") {
            return serde_json::Value::Bool(true);
        }
        if val.eq_ignore_ascii_case("false") {
            return serde_json::Value::Bool(false);
        }

        // Return as string
        serde_json::Value::String(val.to_string())
    }

    /// Format as CSV
    fn format_csv(result: &QueryResult) -> String {
        let mut output = String::new();

        // Header
        output.push_str(&Self::csv_row(&result.columns));
        output.push('\n');

        // Data rows
        for row in &result.rows {
            output.push_str(&Self::csv_row(row));
            output.push('\n');
        }

        output
    }

    /// Format a single CSV row
    fn csv_row(values: &[String]) -> String {
        values
            .iter()
            .map(|v| Self::csv_escape(v))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Escape a value for CSV
    fn csv_escape(val: &str) -> String {
        if val.contains(',') || val.contains('"') || val.contains('\n') || val.contains('\r') {
            format!("\"{}\"", val.replace('"', "\"\""))
        } else {
            val.to_string()
        }
    }

    /// Format as TSV
    fn format_tsv(result: &QueryResult) -> String {
        let mut output = String::new();

        // Header
        output.push_str(&result.columns.join("\t"));
        output.push('\n');

        // Data rows
        for row in &result.rows {
            let escaped: Vec<String> = row
                .iter()
                .map(|v| v.replace('\t', "\\t").replace('\n', "\\n"))
                .collect();
            output.push_str(&escaped.join("\t"));
            output.push('\n');
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_result() -> QueryResult {
        QueryResult {
            columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            column_types: vec![
                "INTEGER".to_string(),
                "VARCHAR".to_string(),
                "INTEGER".to_string(),
            ],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
                vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
            ],
            execution_time_secs: 0.001,
        }
    }

    #[test]
    fn test_format_table() {
        let result = sample_result();
        let output = QueryResultFormatter::format(&result, OutputFormat::Table);
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
        assert!(output.contains("2 rows"));
    }

    #[test]
    fn test_format_json() {
        let result = sample_result();
        let output = QueryResultFormatter::format(&result, OutputFormat::Json);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["name"], "Alice");
        assert_eq!(parsed[0]["age"], 30);
    }

    #[test]
    fn test_format_csv() {
        let result = sample_result();
        let output = QueryResultFormatter::format(&result, OutputFormat::Csv);
        assert!(output.starts_with("id,name,age\n"));
        assert!(output.contains("1,Alice,30"));
    }

    #[test]
    fn test_csv_escape() {
        assert_eq!(QueryResultFormatter::csv_escape("hello"), "hello");
        assert_eq!(
            QueryResultFormatter::csv_escape("hello,world"),
            "\"hello,world\""
        );
        assert_eq!(
            QueryResultFormatter::csv_escape("say \"hi\""),
            "\"say \"\"hi\"\"\""
        );
    }

    #[test]
    fn test_format_tsv() {
        let result = sample_result();
        let output = QueryResultFormatter::format(&result, OutputFormat::Tsv);
        assert!(output.starts_with("id\tname\tage\n"));
        assert!(output.contains("1\tAlice\t30"));
    }

    #[test]
    fn test_json_value_conversion() {
        assert_eq!(
            QueryResultFormatter::json_value("NULL"),
            serde_json::Value::Null
        );
        assert_eq!(
            QueryResultFormatter::json_value("42"),
            serde_json::json!(42)
        );
        assert_eq!(
            QueryResultFormatter::json_value("3.14"),
            serde_json::json!(3.14)
        );
        assert_eq!(
            QueryResultFormatter::json_value("true"),
            serde_json::json!(true)
        );
        assert_eq!(
            QueryResultFormatter::json_value("hello"),
            serde_json::json!("hello")
        );
    }
}
