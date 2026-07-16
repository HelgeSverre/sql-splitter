//! Loads a `model`/`overrides` document together with its local imports.
//!
//! The document formats themselves (see [`super::model`] and
//! [`super::overrides`]) know nothing about imports beyond carrying an
//! `imports: Vec<PathBuf>` field — resolving them, rejecting anything
//! unsafe (remote, recursive, or role-mismatched), and merging them with
//! deterministic root-wins semantics is this module's job.
//!
//! See the design doc's "### Imports" section for the merge rules this
//! implements: imported files are always `kind: overrides`, resolve
//! relative to the importing file, and cannot themselves import; two
//! imports may never define the same configuration path; the root
//! document may freely override anything an import defines; maps merge by
//! key while lists replace wholesale.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_yaml_ng::Value;

use crate::diagnostic::{DiagnosticBag, SourceLocation};

use super::model::SyntheticFile;

/// Envelope fields read generically off any document (root or imported)
/// before it is known to satisfy its full role-specific schema. Unknown
/// fields are intentionally allowed; the eventual typed parse enforces
/// them.
#[derive(Debug, Default, Deserialize)]
struct ImportEnvelope {
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    imports: Vec<String>,
}

/// Loads a root synthetic-data document plus its resolved local imports,
/// producing a single [`SyntheticFile`] ready for compilation.
pub struct ConfigLoader;

impl ConfigLoader {
    /// Reads `path`, resolves and merges any `imports:` it declares, and
    /// typed-deserializes the merged document into the role (`model` or
    /// `overrides`) the root document declares.
    ///
    /// Import resolution happens before the typed parse so a root document
    /// that only becomes complete once its imports are merged in (the
    /// common case — splitting a large `tables:` map across files) still
    /// validates successfully.
    pub fn load(path: &Path) -> Result<SyntheticFile, DiagnosticBag> {
        let root_value = read_yaml(path)?;
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));

        let envelope: ImportEnvelope = serde_yaml_ng::from_value(root_value.clone())
            .map_err(|err| single_error("GEN-CONFIG-PARSE", &path.display().to_string(), &err))?;

        let mut bag = DiagnosticBag::default();
        let mut imports = Vec::with_capacity(envelope.imports.len());
        for raw in &envelope.imports {
            match resolve_import(base_dir, raw) {
                Ok(resolved) => imports.push(resolved),
                Err(mut import_bag) => bag.diagnostics.append(&mut import_bag.diagnostics),
            }
        }
        if bag.has_errors() {
            return Err(bag);
        }

        let merged = merge_yaml(root_value, imports)?;
        SyntheticFile::parse_value(merged)
            .map_err(|err| single_error("GEN-CONFIG-ROLE", &path.display().to_string(), &err))
    }
}

/// Rejects remote/absolute import paths, then loads and validates the
/// imported file itself: it must be `kind: overrides` and must not declare
/// any imports of its own.
fn resolve_import(base_dir: &Path, raw: &str) -> Result<(PathBuf, Value), DiagnosticBag> {
    if raw.contains("://") || Path::new(raw).is_absolute() {
        let mut bag = DiagnosticBag::default();
        bag.error(
            "GEN-IMPORT-REMOTE",
            "imports",
            format!("import path `{raw}` must be local and relative; remote or absolute paths are rejected"),
        );
        return Err(bag);
    }

    let resolved = base_dir.join(raw);
    let value = read_yaml(&resolved)?;
    let envelope: ImportEnvelope = serde_yaml_ng::from_value(value.clone())
        .map_err(|err| single_error("GEN-CONFIG-PARSE", &resolved.display().to_string(), &err))?;

    if envelope.kind.as_deref() != Some("overrides") {
        let mut bag = DiagnosticBag::default();
        bag.error(
            "GEN-IMPORT-KIND",
            "imports",
            format!(
                "imported file `{}` must be `kind: overrides`, found `kind: {}`",
                resolved.display(),
                envelope.kind.as_deref().unwrap_or("<missing>")
            ),
        );
        return Err(bag);
    }
    if !envelope.imports.is_empty() {
        let mut bag = DiagnosticBag::default();
        bag.error(
            "GEN-IMPORT-NESTED",
            "imports",
            format!(
                "imported file `{}` declares its own `imports:`; imported files cannot themselves import",
                resolved.display()
            ),
        );
        return Err(bag);
    }

    Ok((resolved, value))
}

/// Reads and parses `path` as a YAML [`Value`], surfacing I/O and parse
/// failures (including `serde_yaml_ng`'s duplicate-key rejection) as
/// diagnostics.
fn read_yaml(path: &Path) -> Result<Value, DiagnosticBag> {
    let text = fs::read_to_string(path)
        .map_err(|err| single_error("GEN-CONFIG-IO", &path.display().to_string(), &err))?;
    serde_yaml_ng::from_str(&text)
        .map_err(|err| single_error("GEN-CONFIG-PARSE", &path.display().to_string(), &err))
}

fn single_error(code: &str, path: &str, err: &impl std::fmt::Display) -> DiagnosticBag {
    let mut bag = DiagnosticBag::default();
    bag.error(code, path, err.to_string());
    bag
}

/// Merges a root document's imports (in declaration order, with
/// path-level collision detection) and then merges the root document
/// itself on top with no collision checking — the root always wins.
///
/// Returns the merged [`Value`], still untyped: [`SyntheticFile::parse_value`]
/// performs the final role-specific validation.
pub fn merge_yaml(root: Value, imports: Vec<(PathBuf, Value)>) -> Result<Value, DiagnosticBag> {
    let mut bag = DiagnosticBag::default();
    let mut merged = Value::Mapping(Default::default());
    let mut occupied = BTreeMap::<String, PathBuf>::new();

    for (source, value) in imports {
        let content = strip_envelope_keys(value);
        merge_import(&mut merged, content, "", &mut occupied, &source, &mut bag);
    }
    bag.into_result(())?;

    merge_root(&mut merged, root);
    Ok(merged)
}

/// Removes the envelope-only keys (`version`, `kind`, `imports`) from an
/// imported document before it is merged. These are validated separately
/// in [`resolve_import`] and must never leak into the merged body — the
/// root's own `version`/`kind` must win unconditionally, and an import's
/// `imports` list (always empty by the time we get here) carries no
/// content.
fn strip_envelope_keys(value: Value) -> Value {
    let Value::Mapping(mut mapping) = value else {
        return value;
    };
    for key in ["version", "kind", "imports"] {
        mapping.remove(key);
    }
    Value::Mapping(mapping)
}

/// Merges one import's content into the shared accumulator, recording
/// every leaf path it touches in `occupied` so a later import that
/// redefines the same path is reported as [`DiagnosticBag`] error
/// `GEN-IMPORT-COLLISION` naming both source files and the exact path.
fn merge_import(
    target: &mut Value,
    incoming: Value,
    path: &str,
    occupied: &mut BTreeMap<String, PathBuf>,
    source: &Path,
    bag: &mut DiagnosticBag,
) {
    let Value::Mapping(incoming_map) = incoming else {
        record_leaf_collision(path, source, occupied, bag);
        *target = incoming;
        occupied.insert(path.to_string(), source.to_path_buf());
        return;
    };

    if !target.is_mapping() {
        *target = Value::Mapping(Default::default());
    }
    let target_map = target.as_mapping_mut().expect("just ensured mapping");

    for (key, incoming_value) in incoming_map {
        let child_path = join_path(path, key.as_str().unwrap_or_default());
        let slot = target_map.entry(key).or_insert(Value::Null);
        merge_import(slot, incoming_value, &child_path, occupied, source, bag);
    }
}

fn record_leaf_collision(
    path: &str,
    source: &Path,
    occupied: &BTreeMap<String, PathBuf>,
    bag: &mut DiagnosticBag,
) {
    if let Some(prior_source) = occupied.get(path) {
        if prior_source != source {
            bag.error(
                "GEN-IMPORT-COLLISION",
                path,
                format!("`{path}` is defined by more than one import"),
            )
            .related = vec![
                SourceLocation {
                    path: prior_source.display().to_string(),
                    description: None,
                },
                SourceLocation {
                    path: source.display().to_string(),
                    description: None,
                },
            ];
        }
    }
}

/// Merges the root document on top of the already-merged imports. Unlike
/// [`merge_import`], this never checks for collisions: the root document
/// is always allowed to override whatever its imports defined.
fn merge_root(target: &mut Value, incoming: Value) {
    let Value::Mapping(incoming_map) = incoming else {
        *target = incoming;
        return;
    };

    if !target.is_mapping() {
        *target = Value::Mapping(Default::default());
    }
    let target_map = target.as_mapping_mut().expect("just ensured mapping");

    for (key, incoming_value) in incoming_map {
        if incoming_value.is_mapping() {
            let slot = target_map
                .entry(key)
                .or_insert(Value::Mapping(Default::default()));
            merge_root(slot, incoming_value);
        } else {
            target_map.insert(key, incoming_value);
        }
    }
}

fn join_path(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}.{key}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_root_replaces_lists_wholesale() {
        let root: Value =
            serde_yaml_ng::from_str("tables: { orders: { relationships: [1, 2] } }").unwrap();
        let mut merged: Value =
            serde_yaml_ng::from_str("tables: { orders: { relationships: [9] } }").unwrap();
        merge_root(&mut merged, root);
        let rendered = serde_yaml_ng::to_string(&merged).unwrap();
        assert!(rendered.contains("- 1"));
        assert!(rendered.contains("- 2"));
        assert!(!rendered.contains("- 9"));
    }

    #[test]
    fn strip_envelope_keys_drops_version_kind_and_imports() {
        let value: Value =
            serde_yaml_ng::from_str("version: 1\nkind: overrides\nimports: []\ntables: {}\n")
                .unwrap();
        let stripped = strip_envelope_keys(value);
        let mapping = stripped.as_mapping().unwrap();
        assert!(!mapping.contains_key("version"));
        assert!(!mapping.contains_key("kind"));
        assert!(!mapping.contains_key("imports"));
        assert!(mapping.contains_key("tables"));
    }
}
