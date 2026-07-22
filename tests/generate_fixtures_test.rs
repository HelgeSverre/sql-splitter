//! Guard against schema drift in the committed stress-test model fixtures.
//!
//! The `tests/fixtures/generate/stress/*.yaml` models are large, hand-authored
//! examples that exercise the full generator/planner/modifier surface. They are
//! easy to leave behind when the config schema evolves, so this test compiles
//! every one of them (the equivalent of `generate --check`) and fails loudly if
//! any stops compiling. Compilation is cheap even for the huge fixtures because
//! it only resolves the plan — no rows are generated.

use std::path::Path;

use sql_splitter::generate::{CompileOptions, ModelCompiler};
use sql_splitter::synthetic::ConfigLoader;

/// Every committed stress fixture (root models; imported override files are
/// pulled in automatically by `ConfigLoader`).
const STRESS_FIXTURES: &[&str] = &[
    "banking_ledger",
    "car_dealership",
    "cms_kitchensink",
    "everything",
    "multitenant_workflow",
    "odoo_erp",
];

#[test]
fn every_stress_fixture_compiles() {
    for name in STRESS_FIXTURES {
        let path = format!("tests/fixtures/generate/stress/{name}.yaml");
        let file = ConfigLoader::load(Path::new(&path))
            .unwrap_or_else(|bag| panic!("`{name}` failed to load/merge:\n{bag}"));
        let model = file
            .into_model()
            .unwrap_or_else(|bag| panic!("`{name}` is not a `kind: model`:\n{bag}"));
        ModelCompiler::standard()
            .compile(model, CompileOptions::default())
            .unwrap_or_else(|bag| panic!("`{name}` failed to compile:\n{bag}"));
    }
}
