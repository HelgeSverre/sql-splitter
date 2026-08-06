#![no_main]

use libfuzzer_sys::fuzz_target;
use sql_splitter::convert::ddl::{tokenize, LexRules};

fuzz_target!(|data: &[u8]| {
    // Verify the tokenizer never panics on arbitrary input and produces
    // consistent spans (concatenated token slices equal input).
    let Ok(stmt) = std::str::from_utf8(data) else { return; };

    for rules in &[mysql_rules(), pg_rules()] {
        let tokens = tokenize(stmt, *rules);
        // Round-trip: reconstructing from token spans must match input.
        let reconstructed: String = tokens
            .iter()
            .map(|t| &stmt[t.start()..t.end()])
            .collect();
        assert_eq!(reconstructed, stmt, "round-trip mismatch with {rules:?}");

        // All spans must be ordered non-decreasing and within bounds.
        let mut prev_end = 0;
        for t in &tokens {
            let s = t.start();
            let e = t.end();
            assert!(s <= e, "start {s} > end {e}");
            assert!(s >= prev_end, "spans not ordered: {prev_end} then {s}");
            prev_end = e;
            assert!(e <= stmt.len(), "end {e} > stmt len {}", stmt.len());
            // Each span must slice to valid UTF-8.
            assert!(std::str::from_utf8(&data[s..e]).is_ok(), "invalid utf8 at {s}..{e}");
        }
    }
});

fn mysql_rules() -> LexRules {
    LexRules {
        backslash_escapes: true,
        double_quote_ident: false,
        backtick_ident: true,
        hash_comments: true,
        dollar_quotes: false,
    }
}

fn pg_rules() -> LexRules {
    LexRules {
        backslash_escapes: false,
        double_quote_ident: true,
        backtick_ident: false,
        hash_comments: false,
        dollar_quotes: true,
    }
}
