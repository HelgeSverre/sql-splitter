#![no_main]

use std::fmt;
use std::hint::black_box;
use std::io;

use libfuzzer_sys::fuzz_target;
use sql_splitter::generate::{
    CompileOptions, GenerationEngine, GenerationPlan, ModelCompiler, RenderOptions,
};
use sql_splitter::parser::SqlDialect;
use sql_splitter::render::SqlRenderer;
use sql_splitter::synthetic::{InsertMode, OutputMode, SyntheticFile};

const MAX_INPUT_BYTES: usize = 64 * 1024;
const MAX_ROWS_PER_TABLE: u64 = 8;
const FAMILY_BUDGET_BYTES: u64 = 16 * 1024;

fn observe_error(error: impl fmt::Display) {
    drop(black_box(error.to_string()));
}

fn render_options(plan: &GenerationPlan) -> RenderOptions {
    RenderOptions {
        dialect: plan
            .output
            .dialect
            .or(plan.input_dialect)
            .unwrap_or(SqlDialect::MySql),
        source_dialect: plan.input_dialect,
        mode: plan.output.mode.unwrap_or(OutputMode::SchemaAndData),
        no_copy: matches!(plan.output.inserts, Some(InsertMode::Insert)),
        batch_size: plan.output.batch_size.unwrap_or(1_000) as usize,
        mssql_production_style: false,
        mssql_go: None,
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }
    let input = match std::str::from_utf8(data) {
        Ok(input) => input,
        Err(error) => {
            observe_error(error);
            return;
        }
    };
    let file = match SyntheticFile::parse_str(input) {
        Ok(file) => file,
        Err(error) => {
            observe_error(error);
            return;
        }
    };
    let model = match file.into_model() {
        Ok(model) => model,
        Err(error) => {
            observe_error(error);
            return;
        }
    };
    let options = CompileOptions {
        seed: Some(0),
        max_rows: Some(MAX_ROWS_PER_TABLE),
        family_budget_bytes: Some(FAMILY_BUDGET_BYTES),
        ..CompileOptions::default()
    };
    let plan = match ModelCompiler::standard().compile(model, options) {
        Ok(plan) => plan,
        Err(error) => {
            observe_error(error);
            return;
        }
    };

    let mut renderer = SqlRenderer::new(io::sink(), render_options(&plan));
    if let Err(error) = GenerationEngine::new(plan).run(&mut renderer) {
        observe_error(error);
        return;
    }
    if let Err(error) = renderer.finish() {
        observe_error(error);
    }
});
