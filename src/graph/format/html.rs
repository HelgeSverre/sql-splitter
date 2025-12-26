//! HTML format output with embedded Mermaid ERD and sql-splitter branding.

use crate::graph::format::mermaid;
use crate::graph::view::GraphView;

/// Generate interactive HTML with embedded Mermaid ERD and dark/light mode toggle
pub fn to_html(view: &GraphView, title: &str) -> String {
    let mermaid_code = mermaid::to_mermaid(view);

    let total_columns: usize = view.sorted_tables().iter().map(|t| t.columns.len()).sum();
    let stats = format!(
        "{} tables · {} columns · {} relationships",
        view.table_count(),
        total_columns,
        view.edge_count()
    );

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/panzoom@9/dist/panzoom.min.js"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Monda:wght@400;700&display=swap" rel="stylesheet">
  <style>
    :root {{
      --color-bg: #0a0a0a;
      --color-surface: #111111;
      --color-text: #e6edf3;
      --color-text-muted: #8b949e;
      --color-border: #27272a;
      --color-accent: #58a6ff;
    }}

    [data-theme="light"] {{
      --color-bg: #ffffff;
      --color-surface: #f6f8fa;
      --color-text: #1f2328;
      --color-text-muted: #656d76;
      --color-border: #d0d7de;
      --color-accent: #0969da;
    }}

    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    html, body {{ height: 100%; overflow: hidden; }}

    body {{
      font-family: 'Monda', -apple-system, BlinkMacSystemFont, sans-serif;
      background: var(--color-bg);
      color: var(--color-text);
      transition: background-color 0.2s, color 0.2s;
    }}

    .diagram-container {{
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      bottom: 44px;
      overflow: hidden;
      cursor: grab;
    }}

    .diagram-container:active {{
      cursor: grabbing;
    }}

    .mermaid {{
      display: inline-block;
      transform-origin: 0 0;
    }}

    .mermaid svg {{
      max-width: none !important;
    }}

    .bottom-bar {{
      position: fixed;
      bottom: 0;
      left: 0;
      right: 0;
      height: 44px;
      background: var(--color-surface);
      border-top: 1px solid var(--color-border);
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 0 16px;
      font-size: 13px;
    }}

    .bar-left {{
      display: flex;
      align-items: center;
      gap: 12px;
    }}

    .logo {{
      display: flex;
      align-items: center;
      gap: 6px;
      text-decoration: none;
      color: var(--color-text);
      font-weight: 700;
    }}

    .logo-icon {{
      font-size: 1.3em;
      color: var(--color-accent);
    }}

    .sep {{
      color: var(--color-border);
    }}

    .title {{
      color: var(--color-text-muted);
    }}

    .bar-right {{
      display: flex;
      align-items: center;
      gap: 12px;
    }}

    .stats {{
      color: var(--color-text-muted);
    }}

    .btn {{
      background: none;
      border: 1px solid var(--color-border);
      border-radius: 4px;
      padding: 5px 10px;
      cursor: pointer;
      color: var(--color-text-muted);
      font-family: inherit;
      font-size: 12px;
      display: flex;
      align-items: center;
      gap: 5px;
      transition: border-color 0.15s, color 0.15s;
    }}

    .btn:hover {{
      border-color: var(--color-accent);
      color: var(--color-accent);
    }}

    .btn.copied {{
      border-color: #3fb950;
      color: #3fb950;
    }}

    .btn svg {{
      width: 14px;
      height: 14px;
    }}

    .icon-btn {{
      background: none;
      border: none;
      padding: 6px;
      cursor: pointer;
      color: var(--color-text-muted);
      display: flex;
      transition: color 0.15s;
    }}

    .icon-btn:hover {{
      color: var(--color-accent);
    }}

    .icon-btn svg {{
      width: 16px;
      height: 16px;
    }}

    .icon-sun {{ display: none; }}
    .icon-moon {{ display: block; }}
    [data-theme="light"] .icon-sun {{ display: block; }}
    [data-theme="light"] .icon-moon {{ display: none; }}

    @media (max-width: 600px) {{
      .title, .stats {{ display: none; }}
    }}
  </style>
</head>
<body data-theme="dark">
  <div class="diagram-container">
    <div class="mermaid" id="diagram">
{mermaid_code}
    </div>
  </div>

  <div class="bottom-bar">
    <div class="bar-left">
      <a href="https://github.com/helgesverre/sql-splitter" class="logo" target="_blank" title="sql-splitter">
        <span class="logo-icon">;</span>
        <span>sql-splitter</span>
      </a>
      <span class="sep">·</span>
      <span class="title">{title}</span>
    </div>

    <div class="bar-right">
      <span class="stats">{stats}</span>
      <button class="btn" id="copyBtn" onclick="copyMermaid()" title="Copy Mermaid code">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
          <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
        </svg>
        <span id="copyText">Copy</span>
      </button>
      <button class="icon-btn" onclick="toggleTheme()" title="Toggle theme">
        <svg class="icon-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/>
        </svg>
        <svg class="icon-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
        </svg>
      </button>
      <a href="https://github.com/helgesverre/sql-splitter" class="icon-btn" target="_blank" title="GitHub">
        <svg viewBox="0 0 24 24" fill="currentColor">
          <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z"/>
        </svg>
      </a>
    </div>
  </div>

  <script>
    const mermaidCode = `{mermaid_code_escaped}`;
    let panzoomInstance = null;

    function getPreferredTheme() {{
      const saved = localStorage.getItem('erd-theme');
      if (saved) return saved;
      return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }}

    function setTheme(theme) {{
      document.body.setAttribute('data-theme', theme);
      localStorage.setItem('erd-theme', theme);
      reinitMermaid(theme);
    }}

    function toggleTheme() {{
      const current = document.body.getAttribute('data-theme');
      setTheme(current === 'dark' ? 'light' : 'dark');
    }}

    function copyMermaid() {{
      navigator.clipboard.writeText(mermaidCode).then(() => {{
        const btn = document.getElementById('copyBtn');
        const txt = document.getElementById('copyText');
        btn.classList.add('copied');
        txt.textContent = 'Copied!';
        setTimeout(() => {{
          btn.classList.remove('copied');
          txt.textContent = 'Copy';
        }}, 2000);
      }});
    }}

    function initPanzoom() {{
      const diagram = document.getElementById('diagram');
      if (panzoomInstance) panzoomInstance.dispose();
      panzoomInstance = panzoom(diagram, {{
        maxZoom: 5,
        minZoom: 0.1,
        bounds: false,
        boundsPadding: 0.1
      }});
    }}

    function reinitMermaid(theme) {{
      mermaid.initialize({{
        startOnLoad: false,
        theme: theme === 'dark' ? 'dark' : 'default',
        maxTextSize: 500000,
        er: {{ useMaxWidth: false }},
        securityLevel: 'loose'
      }});
      const container = document.getElementById('diagram');
      container.innerHTML = mermaidCode;
      container.removeAttribute('data-processed');
      mermaid.run({{ nodes: [container] }}).then(() => initPanzoom());
    }}

    document.addEventListener('DOMContentLoaded', () => {{
      const theme = getPreferredTheme();
      document.body.setAttribute('data-theme', theme);
      mermaid.initialize({{
        startOnLoad: true,
        theme: theme === 'dark' ? 'dark' : 'default',
        maxTextSize: 500000,
        er: {{ useMaxWidth: false }},
        securityLevel: 'loose'
      }});
      mermaid.run().then(() => initPanzoom());
    }});
  </script>
</body>
</html>"##,
        title = escape_html(title),
        mermaid_code = indent_mermaid(&mermaid_code),
        stats = stats,
        mermaid_code_escaped = escape_js(&mermaid_code),
    )
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn escape_js(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('`', "\\`")
        .replace("${", "\\${")
}

fn indent_mermaid(code: &str) -> String {
    code.lines()
        .map(|line| format!("      {}", line))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::view::{ColumnInfo, TableInfo};
    use ahash::AHashMap;

    fn create_test_view() -> GraphView {
        let mut tables = AHashMap::new();
        tables.insert(
            "users".to_string(),
            TableInfo {
                name: "users".to_string(),
                columns: vec![ColumnInfo {
                    name: "id".to_string(),
                    col_type: "INT".to_string(),
                    is_primary_key: true,
                    is_foreign_key: false,
                    is_nullable: false,
                    references_table: None,
                    references_column: None,
                }],
            },
        );
        GraphView {
            tables,
            edges: vec![],
        }
    }

    #[test]
    fn test_html_branding() {
        let view = create_test_view();
        let output = to_html(&view, "Test Schema");
        assert!(output.contains("sql-splitter"));
        assert!(output.contains("--color-accent: #58a6ff"));
    }

    #[test]
    fn test_html_copy_button() {
        let view = create_test_view();
        let output = to_html(&view, "Test Schema");
        assert!(output.contains("copyMermaid()"));
        assert!(output.contains("Copy Mermaid code"));
    }

    #[test]
    fn test_html_contains_mermaid() {
        let view = create_test_view();
        let output = to_html(&view, "Test Schema");
        assert!(output.contains("erDiagram"));
        assert!(output.contains("maxTextSize: 500000"));
    }

    #[test]
    fn test_html_stats() {
        let view = create_test_view();
        let output = to_html(&view, "Test Schema");
        assert!(output.contains("1 tables"));
        assert!(output.contains("1 columns"));
    }

    #[test]
    fn test_html_has_panzoom() {
        let view = create_test_view();
        let output = to_html(&view, "Test Schema");
        assert!(output.contains("panzoom"));
        assert!(output.contains("initPanzoom"));
    }
}
