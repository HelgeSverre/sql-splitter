//! Keeps the user-facing generate documentation synchronized with the built-in
//! registry and diagnostic catalog.

use std::path::{Path, PathBuf};

use sql_splitter::diagnostic::codes;
use sql_splitter::generate::{CompileOptions, ExtensionRegistry, ModelCompiler};
use sql_splitter::synthetic::SyntheticFile;

const WEBSITE_ROOT: &str = "website/src/content/docs/commands/generate";

fn visit_markdown(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            visit_markdown(&path, files);
        } else if matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("md" | "mdx")
        ) {
            files.push(path);
        }
    }
}

fn read_tree(dir: &str) -> String {
    let mut files = Vec::new();
    visit_markdown(Path::new(dir), &mut files);
    files.sort();
    files
        .into_iter()
        .map(|path| std::fs::read_to_string(path).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn website_documents_every_standard_operator_and_alias() {
    let docs = read_tree(WEBSITE_ROOT);
    let registry = ExtensionRegistry::standard();

    for descriptor in registry.generators().map(|factory| factory.descriptor()) {
        assert!(
            docs.contains(&format!("`{}`", descriptor.kind)),
            "generator `{}` is absent from the website docs",
            descriptor.kind
        );
        for alias in descriptor.aliases {
            assert!(
                docs.contains(&format!("`{alias}`")),
                "generator alias `{alias}` for `{}` is absent from the website docs",
                descriptor.kind
            );
        }
    }
    for descriptor in registry.modifiers().map(|factory| factory.descriptor()) {
        assert!(
            docs.contains(&format!("`{}`", descriptor.kind)),
            "modifier `{}` is absent from the website docs",
            descriptor.kind
        );
        for alias in descriptor.aliases {
            assert!(
                docs.contains(&format!("`{alias}`")),
                "modifier alias `{alias}` for `{}` is absent from the website docs",
                descriptor.kind
            );
        }
    }
    for descriptor in registry.planners().map(|factory| factory.descriptor()) {
        assert!(
            docs.contains(&format!("`{}`", descriptor.kind)),
            "planner `{}` is absent from the website docs",
            descriptor.kind
        );
        for alias in descriptor.aliases {
            assert!(
                docs.contains(&format!("`{alias}`")),
                "planner alias `{alias}` for `{}` is absent from the website docs",
                descriptor.kind
            );
        }
    }
}

#[test]
fn every_builtin_diagnostic_has_an_exact_case_preserving_heading() {
    let page = std::fs::read_to_string(format!("{WEBSITE_ROOT}/diagnostics.mdx")).unwrap();
    for definition in codes::ALL {
        assert!(
            page.lines().any(|line| {
                line.starts_with("## ") && line.ends_with(&format!("\\{{#{}\\}}", definition.code))
            }),
            "{} lacks an exact explicit heading anchor",
            definition.code
        );
    }
}

#[test]
fn website_generate_docs_are_user_facing() {
    let docs = read_tree(WEBSITE_ROOT);
    for forbidden in [
        "just ",
        "tests/fixtures",
        "src/generate",
        "docs/generate",
        "../../src/",
        "in the repository",
    ] {
        assert!(
            !docs.contains(forbidden),
            "website docs contain source-repository language `{forbidden}`"
        );
    }
}

#[test]
fn marked_complete_model_examples_compile() {
    let docs = read_tree(WEBSITE_ROOT);
    let marker = "{/* validate-generate-model */}";
    let mut count = 0;
    let mut rest = docs.as_str();

    while let Some(start) = rest.find(marker) {
        let after_marker = &rest[start + marker.len()..];
        let fence = "```yaml\n";
        let fence_start = after_marker
            .find(fence)
            .expect("marked YAML fence follows marker");
        let yaml_start = fence_start + fence.len();
        let after = &after_marker[yaml_start..];
        let end = after.find("\n```").expect("marked YAML fence closes");
        let yaml = &after[..end];
        let model = SyntheticFile::parse_str(yaml)
            .unwrap_or_else(|error| panic!("documented model does not parse: {error}\n{yaml}"))
            .into_model()
            .expect("marked example is a complete model");
        ModelCompiler::standard()
            .compile(model, CompileOptions::default())
            .unwrap_or_else(|bag| panic!("documented model does not compile:\n{bag}\n{yaml}"));
        count += 1;
        rest = &after[end + 4..];
    }

    assert!(
        count > 0,
        "no complete model examples are marked for validation"
    );
}
