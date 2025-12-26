//! Graph command tests for real-world SQL dumps.
//!
//! Tests ERD generation in all formats: HTML, DOT, Mermaid, JSON.

use super::{Fixture, TEST_CASES};
use sql_splitter::graph::{find_cycles, to_dot, to_html, to_json, to_mermaid, GraphView, Layout};
use sql_splitter::parser::{Parser, StatementType};
use sql_splitter::schema::{SchemaBuilder, SchemaGraph};
use std::fs::File;

/// Build a schema graph from a fixture
fn build_graph(fixture: &Fixture) -> Option<SchemaGraph> {
    let file = File::open(&fixture.sql_path).ok()?;
    let mut parser = Parser::with_dialect(file, 64 * 1024, fixture.dialect());
    let mut builder = SchemaBuilder::new();

    while let Some(stmt) = parser.read_statement().ok()? {
        let stmt_str = String::from_utf8_lossy(&stmt);
        let (stmt_type, _) =
            Parser::<&[u8]>::parse_statement_with_dialect(&stmt, fixture.dialect());

        match stmt_type {
            StatementType::CreateTable => {
                builder.parse_create_table(&stmt_str);
            }
            StatementType::AlterTable => {
                builder.parse_alter_table(&stmt_str);
            }
            StatementType::CreateIndex => {
                builder.parse_create_index(&stmt_str);
            }
            _ => {}
        }
    }

    Some(SchemaGraph::from_schema(builder.build()))
}

/// Run graph tests for a single test case
fn run_graph_tests(case: &'static super::cases::TestCase) {
    let fixture = match Fixture::get(case) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Skipping {}: {}", case.name, e);
            return;
        }
    };

    eprintln!(
        "Testing graph: {} ({}, {})",
        case.name,
        case.dialect,
        fixture.file_size_display()
    );

    let graph = match build_graph(&fixture) {
        Some(g) => g,
        None => {
            eprintln!("  ⚠ Failed to build graph");
            return;
        }
    };

    if graph.is_empty() {
        eprintln!("  ⚠ No tables found");
        return;
    }

    let view = GraphView::from_schema_graph(&graph);

    // Test HTML format
    let html = to_html(&view, &format!("ERD - {}", case.name));
    assert!(html.contains("<!DOCTYPE html>"), "HTML should have doctype");
    assert!(html.contains("erDiagram"), "HTML should contain erDiagram");
    eprintln!("  ✓ HTML ({} bytes)", html.len());

    // Test DOT format
    let dot = to_dot(&view, Layout::LR);
    assert!(
        dot.contains("digraph ERD"),
        "DOT should have digraph header"
    );
    eprintln!("  ✓ DOT ({} bytes)", dot.len());

    // Test Mermaid format
    let mermaid = to_mermaid(&view);
    assert!(
        mermaid.contains("erDiagram"),
        "Mermaid should have erDiagram"
    );
    eprintln!("  ✓ Mermaid ({} bytes)", mermaid.len());

    // Test JSON format
    let json = to_json(&view);
    assert!(json.contains("\"tables\""), "JSON should have tables key");
    assert!(
        json.contains("\"relationships\""),
        "JSON should have relationships key"
    );
    // Verify it's valid JSON
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should be valid JSON");
    assert!(parsed.get("tables").is_some(), "JSON should have tables");
    eprintln!("  ✓ JSON ({} bytes)", json.len());

    // Test cycle detection
    let cycles = find_cycles(&view);
    if cycles.is_empty() {
        eprintln!("  ✓ No cycles detected");
    } else {
        eprintln!("  ✓ {} cycles detected", cycles.len());
    }

    eprintln!(
        "  Summary: {} tables, {} relationships",
        view.table_count(),
        view.edge_count()
    );
}

// Generate individual test functions for each case

#[test]
#[ignore]
fn graph_mysql_classicmodels() {
    run_graph_tests(super::cases::get_case("mysql-classicmodels").unwrap());
}

#[test]
#[ignore]
fn graph_mysql_sakila_schema() {
    run_graph_tests(super::cases::get_case("mysql-sakila-schema").unwrap());
}

#[test]
#[ignore]
fn graph_mysql_world() {
    run_graph_tests(super::cases::get_case("mysql-world").unwrap());
}

#[test]
#[ignore]
fn graph_postgres_pagila_schema() {
    run_graph_tests(super::cases::get_case("postgres-pagila-schema").unwrap());
}

#[test]
#[ignore]
fn graph_postgres_northwind() {
    run_graph_tests(super::cases::get_case("postgres-northwind").unwrap());
}

#[test]
#[ignore]
fn graph_postgres_airlines_small() {
    run_graph_tests(super::cases::get_case("postgres-airlines-small").unwrap());
}

#[test]
#[ignore]
fn graph_chinook_postgres() {
    run_graph_tests(super::cases::get_case("chinook-postgres").unwrap());
}

#[test]
#[ignore]
fn graph_chinook_mysql() {
    run_graph_tests(super::cases::get_case("chinook-mysql").unwrap());
}

#[test]
#[ignore]
fn graph_chinook_sqlite() {
    run_graph_tests(super::cases::get_case("chinook-sqlite").unwrap());
}

/// Run all graph tests
#[test]
#[ignore]
fn all_graph_tests() {
    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;

    for case in TEST_CASES {
        let fixture = match Fixture::get(case) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Skipping {}: {}", case.name, e);
                skipped += 1;
                continue;
            }
        };

        let graph = match build_graph(&fixture) {
            Some(g) => g,
            None => {
                eprintln!("✗ {} (failed to build graph)", case.name);
                failed += 1;
                continue;
            }
        };

        if graph.is_empty() {
            eprintln!("⚠ {} (no tables)", case.name);
            skipped += 1;
            continue;
        }

        let view = GraphView::from_schema_graph(&graph);

        // Test all formats
        let html = to_html(&view, &format!("ERD - {}", case.name));
        let dot = to_dot(&view, Layout::LR);
        let mermaid = to_mermaid(&view);
        let json = to_json(&view);

        let html_ok = html.contains("<!DOCTYPE html>") && html.contains("erDiagram");
        let dot_ok = dot.contains("digraph ERD");
        let mermaid_ok = mermaid.contains("erDiagram");
        let json_ok = serde_json::from_str::<serde_json::Value>(&json).is_ok();

        if html_ok && dot_ok && mermaid_ok && json_ok {
            eprintln!(
                "✓ {} ({} tables, {} rels)",
                case.name,
                view.table_count(),
                view.edge_count()
            );
            passed += 1;
        } else {
            eprintln!(
                "✗ {} (html:{} dot:{} mmd:{} json:{})",
                case.name, html_ok, dot_ok, mermaid_ok, json_ok
            );
            failed += 1;
        }
    }

    eprintln!(
        "\nGraph tests: {} passed, {} failed, {} skipped",
        passed, failed, skipped
    );
}
