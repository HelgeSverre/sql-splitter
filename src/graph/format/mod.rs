//! Output format implementations for ERD visualization.

mod dot;
mod html;
pub(crate) mod json;
mod mermaid;

pub use dot::to_dot;
pub use html::to_html;
pub use json::to_json;
pub use mermaid::to_mermaid;
#[allow(unused_imports)]
pub use json::{ColumnJson, ErdJson, ErdStats, RelationshipJson, TableJson};

use std::fmt;
use std::str::FromStr;

/// Output format for ERD export
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Graphviz DOT format (ERD style)
    #[default]
    Dot,
    /// Mermaid erDiagram format
    Mermaid,
    /// JSON format for programmatic use
    Json,
    /// Interactive HTML with toggleable dark mode
    Html,
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "dot" | "graphviz" => Ok(OutputFormat::Dot),
            "mermaid" | "mmd" => Ok(OutputFormat::Mermaid),
            "json" => Ok(OutputFormat::Json),
            "html" => Ok(OutputFormat::Html),
            _ => Err(format!(
                "Unknown format: {}. Valid options: dot, mermaid, json, html",
                s
            )),
        }
    }
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputFormat::Dot => write!(f, "dot"),
            OutputFormat::Mermaid => write!(f, "mermaid"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::Html => write!(f, "html"),
        }
    }
}

impl OutputFormat {
    /// Get file extension for this format
    pub fn extension(&self) -> &'static str {
        match self {
            OutputFormat::Dot => "dot",
            OutputFormat::Mermaid => "mmd",
            OutputFormat::Json => "json",
            OutputFormat::Html => "html",
        }
    }

    /// Detect format from file extension
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "dot" | "gv" => Some(OutputFormat::Dot),
            "mmd" | "mermaid" => Some(OutputFormat::Mermaid),
            "json" => Some(OutputFormat::Json),
            "html" | "htm" => Some(OutputFormat::Html),
            "png" | "svg" | "pdf" => Some(OutputFormat::Dot), // Will be rendered
            _ => None,
        }
    }
}

/// Layout direction for diagram
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Layout {
    /// Left to right
    #[default]
    LR,
    /// Top to bottom
    TB,
}

impl FromStr for Layout {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "lr" | "left-right" | "horizontal" => Ok(Layout::LR),
            "tb" | "td" | "top-bottom" | "top-down" | "vertical" => Ok(Layout::TB),
            _ => Err(format!("Unknown layout: {}. Valid options: lr, tb", s)),
        }
    }
}

impl fmt::Display for Layout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Layout::LR => write!(f, "lr"),
            Layout::TB => write!(f, "tb"),
        }
    }
}
