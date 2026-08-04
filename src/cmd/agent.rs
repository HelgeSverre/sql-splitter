//! Detection of AI coding agents driving the CLI, so `--help` can point them at
//! the machine-readable docs.
//!
//! There is no `CI=true` equivalent for agents yet — the standard is still an
//! open proposal — so this is a best-effort scan of the vendor variables agents
//! set in the environment of the commands they run. `SQL_SPLITTER_AGENT`
//! overrides the sniffing in both directions (`0` off, anything else on).

/// Variables agents export into the environment of commands they run.
/// A non-empty value is the signal; the values themselves differ per vendor
/// (`1`, `true`, `seatbelt`, a session id), so they are not compared.
const AGENT_ENV_VARS: &[&str] = &[
    "AI_AGENT",              // generic convention (Vercel's detect-agent)
    "AGENT",                 // generic convention (Bun's isAIAgent)
    "CLAUDECODE",            // Claude Code
    "CLAUDE_CODE",           // Claude Code (editor extensions)
    "CURSOR_AGENT",          // Cursor
    "GEMINI_CLI",            // Gemini CLI
    "CODEX_SANDBOX",         // Codex CLI (sandboxed runs only)
    "CODEX_THREAD_ID",       // Codex CLI
    "OPENCODE",              // OpenCode
    "OPENCODE_CLIENT",       // OpenCode
    "AMP_CURRENT_THREAD_ID", // Amp
    "AUGMENT_AGENT",         // Augment
    "CLINE_ACTIVE",          // Cline
    "ANTIGRAVITY_AGENT",     // Antigravity
    "TRAE_AI_SHELL_ID",      // TRAE AI
    "REPL_ID",               // Replit agent
];

/// Whether the CLI looks like it is being driven by an AI coding agent.
pub(crate) fn is_agent() -> bool {
    match std::env::var("SQL_SPLITTER_AGENT").as_deref() {
        Ok("0" | "false" | "") => false,
        Ok(_) => true,
        Err(_) => AGENT_ENV_VARS
            .iter()
            .any(|name| std::env::var_os(name).is_some_and(|value| !value.is_empty())),
    }
}
