//! Semantic-name heuristics: map a column's *name* (corroborated by its value
//! shape where possible) to a semantic generator — email, person, address,
//! commerce, money, file, network, and similar.
//!
//! A match on name *and* shape is `High` confidence and ranks as a strong
//! semantic rule; a name-only match on an ambiguous term (`name`, `id`,
//! `email`-without-`@`) stays `Low` so it can be beaten by observed evidence and
//! is surfaced as a conservative, explainable decision.

use super::{generator, Candidate, ColumnContext, Confidence, Precedence};
use crate::profile::evidence::ColumnEvidence;
use crate::synthetic::schema::SqlTypeFamily;

/// Propose semantic-name candidates for a column.
pub(super) fn candidates(ctx: &ColumnContext<'_>) -> Vec<Candidate> {
    let column = ctx.column();
    let name = column.name.to_ascii_lowercase();
    let text_like = matches!(
        column.family,
        SqlTypeFamily::Text | SqlTypeFamily::Other | SqlTypeFamily::Uuid
    );

    let mut out = Vec::new();

    // --- money (decimal columns named like an amount) ---
    if column.family == SqlTypeFamily::Decimal
        && contains_any(
            &name,
            &[
                "price", "amount", "cost", "total", "balance", "salary", "fee",
            ],
        )
    {
        out.push(strong("commerce.money", "semantic_money", Confidence::High));
        return out;
    }

    if !text_like {
        return out;
    }

    // --- email ---
    if contains_any(&name, &["email", "e_mail"]) {
        let confidence = if shape_has_at_sign(ctx.evidence()) {
            Confidence::High
        } else {
            Confidence::Low // ambiguous: named email but no `@` observed
        };
        out.push(strong("internet.email", "semantic_email", confidence));
    } else if name.contains("url") || name.contains("uri") || name.contains("website") {
        out.push(strong("internet.url", "semantic_url", Confidence::High));
    } else if name.contains("ip_address") || name == "ip" || name.ends_with("_ip") {
        out.push(strong("internet.ipv4", "semantic_ip", Confidence::Medium));
    } else if name.contains("username") || name.contains("user_name") || name == "login" {
        out.push(strong(
            "person.username",
            "semantic_username",
            Confidence::High,
        ));
    } else if name.contains("first_name") || name.contains("firstname") || name == "fname" {
        out.push(strong(
            "person.first_name",
            "semantic_person",
            Confidence::High,
        ));
    } else if name.contains("last_name") || name.contains("lastname") || name == "lname" {
        out.push(strong(
            "person.last_name",
            "semantic_person",
            Confidence::High,
        ));
    } else if name.contains("full_name") || name == "name" || name.ends_with("_name") {
        // `name` alone is ambiguous: propose, but conservatively.
        let confidence = if name.contains("full_name") {
            Confidence::Medium
        } else {
            Confidence::Low
        };
        out.push(strong("person.full_name", "semantic_person", confidence));
    } else if contains_any(&name, &["phone", "mobile", "tel"]) {
        out.push(strong("phone.number", "semantic_phone", Confidence::High));
    } else if name.contains("city") {
        out.push(strong("address.city", "semantic_address", Confidence::High));
    } else if contains_any(&name, &["postcode", "postal", "zip"]) {
        out.push(strong(
            "address.postcode",
            "semantic_address",
            Confidence::High,
        ));
    } else if name.contains("country") {
        out.push(strong(
            "address.country",
            "semantic_address",
            Confidence::High,
        ));
    } else if name.contains("street") || name.contains("address") {
        out.push(strong(
            "address.line1",
            "semantic_address",
            Confidence::Medium,
        ));
    } else if name.contains("currency") {
        out.push(strong(
            "commerce.currency",
            "semantic_commerce",
            Confidence::High,
        ));
    } else if name.contains("company") || name.contains("organization") {
        out.push(strong("company.name", "semantic_company", Confidence::High));
    } else if name.contains("slug") {
        out.push(strong("text.slug", "semantic_text", Confidence::High));
    } else if name.contains("mime") {
        out.push(strong("file.mime_type", "semantic_file", Confidence::High));
    } else if name.contains("filename") || name.contains("file_name") {
        out.push(strong("file.name", "semantic_file", Confidence::High));
    } else if contains_any(&name, &["description", "bio", "comment", "notes", "body"]) {
        out.push(strong(
            "text.paragraph",
            "semantic_text",
            Confidence::Medium,
        ));
    }

    out
}

/// A strong-semantic candidate tagged with its semantic annotation.
fn strong(kind: &str, reason: &'static str, confidence: Confidence) -> Candidate {
    let semantic = kind.to_string();
    Candidate::new(
        Precedence::StrongSemantic,
        confidence,
        reason,
        generator(kind),
    )
    .with_semantic(semantic)
}

/// Whether the observed string shape includes an `@`-style punctuation marker
/// consistent with an email address.
fn shape_has_at_sign(evidence: Option<&ColumnEvidence>) -> bool {
    match evidence.and_then(|e| e.string_shape.as_ref()) {
        Some(shape) => {
            shape.classes.punctuation && shape.common_suffix.contains('@')
                || shape.common_prefix.contains('@')
                || evidence
                    .map(|e| e.sample_values.iter().any(|v| v.contains('@')))
                    .unwrap_or(false)
        }
        None => false,
    }
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}
