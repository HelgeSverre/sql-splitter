//! Public-API fixture helpers for generating a small, FK-consistent, multi-table
//! SQL dump from `tests/fixtures/generate/legacy_fixture.yaml` with
//! [`sql_splitter::generate::Generate`].

use sql_splitter::generate::{CompileOptions, Generate, TableCountOverride};
use sql_splitter::parser::SqlDialect;
use tempfile::{NamedTempFile, TempPath};

/// Path to the shared model every fixture consumer generates from.
fn model_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/generate/legacy_fixture.yaml")
}

/// Generate one dump from the shared `legacy_fixture.yaml` model, rendered
/// for `dialect`, and return the path it was written to.
///
/// `rows` overrides the model's root `tenants` row count (`None` keeps the
/// model's authored default, roughly the old crate's `Scale::Small`); larger
/// values approximate the old crate's bigger scale presets, since every
/// per-tenant table is a `relation.children` of `tenants` and scales with it.
/// `tables` restricts generation to a subset of tables (`None` generates
/// every table). `seed` is the run's root seed, for reproducibility.
///
/// Callers that need the same dump for several assertions should call this
/// once per test case and reuse the returned path — regenerating per
/// assertion defeats the point of a fixed seed and slows the suite down for
/// no benefit.
pub fn generated_fixture(
    dialect: SqlDialect,
    rows: Option<u64>,
    tables: Option<&[&str]>,
    seed: u64,
) -> TempPath {
    let path = NamedTempFile::new()
        .expect("create temp file for generated fixture")
        .into_temp_path();

    let mut compile = CompileOptions {
        seed: Some(seed),
        ..Default::default()
    };
    if let Some(rows) = rows {
        compile
            .table_rows
            .push(TableCountOverride::rows("tenants", rows));
    }
    if let Some(tables) = tables {
        compile.tables = tables.iter().map(|t| t.to_string()).collect();
    }

    Generate::builder()
        .config(model_path())
        .output_dialect(dialect)
        .compile(compile)
        .output(path.to_path_buf())
        .run()
        .expect("generate legacy fixture from the public generate API");

    path
}
