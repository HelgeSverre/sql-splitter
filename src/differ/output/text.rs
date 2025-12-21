//! Text output formatter for diff results.

use crate::differ::DiffResult;

/// Format diff result as human-readable text
pub fn format_text(result: &DiffResult) -> String {
    let mut output = String::new();

    // Schema changes
    if let Some(ref schema) = result.schema {
        output.push_str("Schema Changes:\n");

        if schema.tables_added.is_empty()
            && schema.tables_removed.is_empty()
            && schema.tables_modified.is_empty()
        {
            output.push_str("  (no schema changes)\n");
        } else {
            // Added tables
            for table in &schema.tables_added {
                output.push_str(&format!("  + Table '{}' (new)\n", table.name));
                for col in &table.columns {
                    let nullable = if col.is_nullable { "NULL" } else { "NOT NULL" };
                    let pk = if col.is_primary_key { " [PK]" } else { "" };
                    output.push_str(&format!(
                        "      + {} {} {}{}\n",
                        col.name, col.col_type, nullable, pk
                    ));
                }
            }

            // Removed tables
            for table_name in &schema.tables_removed {
                output.push_str(&format!("  - Table '{}' (removed)\n", table_name));
            }

            // Modified tables
            for modification in &schema.tables_modified {
                output.push_str(&format!("  ~ Table '{}':\n", modification.table_name));

                for col in &modification.columns_added {
                    let nullable = if col.is_nullable { "NULL" } else { "NOT NULL" };
                    output.push_str(&format!(
                        "      + Column '{}' {} {}\n",
                        col.name, col.col_type, nullable
                    ));
                }

                for col in &modification.columns_removed {
                    output.push_str(&format!("      - Column '{}' {}\n", col.name, col.col_type));
                }

                for change in &modification.columns_modified {
                    let mut changes = Vec::new();
                    if let (Some(old_type), Some(new_type)) = (&change.old_type, &change.new_type) {
                        changes.push(format!("{} → {}", old_type, new_type));
                    }
                    if let (Some(old_null), Some(new_null)) =
                        (change.old_nullable, change.new_nullable)
                    {
                        let old_str = if old_null { "NULL" } else { "NOT NULL" };
                        let new_str = if new_null { "NULL" } else { "NOT NULL" };
                        changes.push(format!("{} → {}", old_str, new_str));
                    }
                    output.push_str(&format!(
                        "      ~ Column '{}': {}\n",
                        change.name,
                        changes.join(", ")
                    ));
                }

                if modification.pk_changed {
                    let old_pk = modification
                        .old_pk
                        .as_ref()
                        .map(|pk| pk.join(", "))
                        .unwrap_or_else(|| "(none)".to_string());
                    let new_pk = modification
                        .new_pk
                        .as_ref()
                        .map(|pk| pk.join(", "))
                        .unwrap_or_else(|| "(none)".to_string());
                    output.push_str(&format!(
                        "      ~ PRIMARY KEY: ({}) → ({})\n",
                        old_pk, new_pk
                    ));
                }

                for fk in &modification.fks_added {
                    output.push_str(&format!(
                        "      + FK ({}) → {}.({}))\n",
                        fk.columns.join(", "),
                        fk.referenced_table,
                        fk.referenced_columns.join(", ")
                    ));
                }

                for fk in &modification.fks_removed {
                    output.push_str(&format!(
                        "      - FK ({}) → {}.({}))\n",
                        fk.columns.join(", "),
                        fk.referenced_table,
                        fk.referenced_columns.join(", ")
                    ));
                }

                for idx in &modification.indexes_added {
                    let unique_marker = if idx.is_unique { " [unique]" } else { "" };
                    let type_marker = idx
                        .index_type
                        .as_ref()
                        .map(|t| format!(" [{}]", t))
                        .unwrap_or_default();
                    output.push_str(&format!(
                        "      + Index '{}' on ({}){}{}\n",
                        idx.name,
                        idx.columns.join(", "),
                        unique_marker,
                        type_marker
                    ));
                }

                for idx in &modification.indexes_removed {
                    let unique_marker = if idx.is_unique { " [unique]" } else { "" };
                    let type_marker = idx
                        .index_type
                        .as_ref()
                        .map(|t| format!(" [{}]", t))
                        .unwrap_or_default();
                    output.push_str(&format!(
                        "      - Index '{}' on ({}){}{}\n",
                        idx.name,
                        idx.columns.join(", "),
                        unique_marker,
                        type_marker
                    ));
                }
            }
        }

        output.push('\n');
    }

    // Data changes
    if let Some(ref data) = result.data {
        output.push_str("Data Changes:\n");

        if data.tables.is_empty() {
            output.push_str("  (no data changes)\n");
        } else {
            // Sort tables for consistent output
            let mut table_names: Vec<_> = data.tables.keys().collect();
            table_names.sort();

            for table_name in table_names {
                let diff = &data.tables[table_name];

                // Skip tables with no changes
                if diff.added_count == 0 && diff.removed_count == 0 && diff.modified_count == 0 {
                    continue;
                }

                let mut parts = Vec::new();
                if diff.added_count > 0 {
                    parts.push(format!("+{} rows", diff.added_count));
                }
                if diff.removed_count > 0 {
                    parts.push(format!("-{} rows", diff.removed_count));
                }
                if diff.modified_count > 0 {
                    parts.push(format!("~{} modified", diff.modified_count));
                }

                let truncated_note = if diff.truncated { " [truncated]" } else { "" };

                output.push_str(&format!(
                    "  Table '{}': {}{}\n",
                    table_name,
                    parts.join(", "),
                    truncated_note
                ));

                // Show sample PKs if available (verbose mode)
                if !diff.sample_added_pks.is_empty() {
                    let samples = &diff.sample_added_pks;
                    let remaining = diff.added_count as usize - samples.len();
                    let suffix = if remaining > 0 {
                        format!("... (+{} more)", remaining)
                    } else {
                        String::new()
                    };
                    output.push_str(&format!(
                        "    Added PKs: {}{}\n",
                        samples.join(", "),
                        suffix
                    ));
                }

                if !diff.sample_removed_pks.is_empty() {
                    let samples = &diff.sample_removed_pks;
                    let remaining = diff.removed_count as usize - samples.len();
                    let suffix = if remaining > 0 {
                        format!("... (+{} more)", remaining)
                    } else {
                        String::new()
                    };
                    output.push_str(&format!(
                        "    Removed PKs: {}{}\n",
                        samples.join(", "),
                        suffix
                    ));
                }

                if !diff.sample_modified_pks.is_empty() {
                    let samples = &diff.sample_modified_pks;
                    let remaining = diff.modified_count as usize - samples.len();
                    let suffix = if remaining > 0 {
                        format!("... (+{} more)", remaining)
                    } else {
                        String::new()
                    };
                    output.push_str(&format!(
                        "    Modified PKs: {}{}\n",
                        samples.join(", "),
                        suffix
                    ));
                }
            }
        }

        output.push('\n');
    }

    // Warnings
    if !result.warnings.is_empty() {
        output.push_str("Warnings:\n");
        for warning in &result.warnings {
            if let Some(ref table) = warning.table {
                output.push_str(&format!("  ⚠ Table '{}': {}\n", table, warning.message));
            } else {
                output.push_str(&format!("  ⚠ {}\n", warning.message));
            }
        }
        output.push('\n');
    }

    // Summary
    output.push_str("Summary:\n");
    output.push_str(&format!(
        "  {} tables added, {} removed, {} modified\n",
        result.summary.tables_added, result.summary.tables_removed, result.summary.tables_modified
    ));
    output.push_str(&format!(
        "  {} rows added, {} removed, {} modified\n",
        result.summary.rows_added, result.summary.rows_removed, result.summary.rows_modified
    ));

    if result.summary.truncated {
        output.push_str("  (some tables truncated due to memory limits)\n");
    }

    output
}
