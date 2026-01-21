import { useState, useMemo } from 'react';

type Category = 'transform' | 'inspect' | 'verify' | 'subset' | 'privacy';

interface Command {
  id: string;
  label: string;
  desc: string;
  category: Category;
}

const categories: Record<Category, { color: string; label: string }> = {
  transform: { color: '#0ea5e9', label: 'Transform' },
  inspect: { color: '#8b5cf6', label: 'Inspect' },
  verify: { color: '#f59e0b', label: 'Verify' },
  subset: { color: '#10b981', label: 'Subset' },
  privacy: { color: '#ef4444', label: 'Privacy' },
};

const commands: Command[] = [
  { id: 'split', label: 'split', desc: 'Split dump into per-table files', category: 'transform' },
  { id: 'merge', label: 'merge', desc: 'Combine files into one dump', category: 'transform' },
  { id: 'convert', label: 'convert', desc: 'Transform between dialects', category: 'transform' },
  { id: 'analyze', label: 'analyze', desc: 'View table statistics', category: 'inspect' },
  { id: 'query', label: 'query', desc: 'SQL analytics with DuckDB', category: 'inspect' },
  { id: 'graph', label: 'graph', desc: 'Generate ERD diagrams', category: 'inspect' },
  { id: 'order', label: 'order', desc: 'Topological FK ordering', category: 'inspect' },
  { id: 'validate', label: 'validate', desc: 'Check integrity', category: 'verify' },
  { id: 'diff', label: 'diff', desc: 'Compare two dumps', category: 'verify' },
  { id: 'sample', label: 'sample', desc: 'Create reduced datasets', category: 'subset' },
  { id: 'shard', label: 'shard', desc: 'Extract tenant data', category: 'subset' },
  { id: 'redact', label: 'redact', desc: 'Anonymize PII', category: 'privacy' },
];

const groupedCommands = Object.entries(categories).map(([cat, info]) => ({
  category: cat as Category,
  ...info,
  commands: commands.filter(c => c.category === cat),
}));

export default function CommandBuilder() {
  const [currentCommand, setCurrentCommand] = useState('split');
  const [inputFile, setInputFile] = useState('dump.sql');
  const [inputFile2, setInputFile2] = useState('new.sql');
  const [output, setOutput] = useState('output/');
  const [dialect, setDialect] = useState('');
  const [toDialect, setToDialect] = useState('postgres');
  const [percent, setPercent] = useState('10');
  const [tenantColumn, setTenantColumn] = useState('tenant_id');
  const [tenantValue, setTenantValue] = useState('');
  const [queryText, setQueryText] = useState('SELECT COUNT(*) FROM users');
  const [format, setFormat] = useState('');
  const [hashPatterns, setHashPatterns] = useState('');
  const [fakePatterns, setFakePatterns] = useState('');
  const [progress, setProgress] = useState(false);
  const [dryRun, setDryRun] = useState(false);
  const [json, setJson] = useState(false);
  const [strict, setStrict] = useState(false);
  const [failFast, setFailFast] = useState(false);
  const [preserveRelations, setPreserveRelations] = useState(false);
  const [interactive, setInteractive] = useState(false);
  const [reverse, setReverse] = useState(false);
  const [copied, setCopied] = useState(false);

  const cmd = currentCommand;

  const generatedCommand = useMemo(() => {
    const parts: string[] = ['sql-splitter', cmd];

    // Input files
    if (cmd === 'diff') {
      parts.push(inputFile, inputFile2);
    } else if (cmd !== 'query' || !interactive) {
      parts.push(inputFile);
    }

    // Query text
    if (cmd === 'query' && queryText && !interactive) {
      parts.push(`"${queryText}"`);
    }

    // Output
    if (output && !['analyze', 'validate', 'query', 'order'].includes(cmd)) {
      parts.push('-o', output);
    }

    // Dialect
    if (dialect && cmd !== 'convert') {
      parts.push('-d', dialect);
    }

    // Convert
    if (cmd === 'convert' && toDialect) {
      parts.push('--to', toDialect);
      if (output) parts.push('-o', output);
    }

    // Sample
    if (cmd === 'sample' && percent) {
      parts.push('--percent', percent);
    }

    // Shard
    if (cmd === 'shard') {
      if (tenantColumn) parts.push('--tenant-column', tenantColumn);
      if (tenantValue) parts.push('--tenant-value', tenantValue);
      if (output) parts.push('-o', output);
    }

    // Redact
    if (cmd === 'redact') {
      if (hashPatterns) {
        hashPatterns.split(',').map(p => p.trim()).filter(Boolean).forEach(p => {
          parts.push('--hash', `"${p}"`);
        });
      }
      if (fakePatterns) {
        fakePatterns.split(',').map(p => p.trim()).filter(Boolean).forEach(p => {
          parts.push('--fake', `"${p}"`);
        });
      }
      if (output) parts.push('-o', output);
    }

    // Format
    if (format) {
      if (cmd === 'query') parts.push('-f', format);
      else if (['graph', 'diff'].includes(cmd)) parts.push('--format', format);
    }

    // Flags
    if (progress && !['order'].includes(cmd)) parts.push('--progress');
    if (dryRun && !['analyze', 'validate', 'query', 'graph', 'order'].includes(cmd)) parts.push('--dry-run');
    if (json && cmd !== 'query') parts.push('--json');
    if (strict && ['validate', 'convert', 'redact'].includes(cmd)) parts.push('--strict');
    if (failFast && ['split', 'analyze', 'convert', 'validate'].includes(cmd)) parts.push('--fail-fast');
    if (preserveRelations && cmd === 'sample') parts.push('--preserve-relations');
    if (interactive && cmd === 'query') parts.push('--interactive');
    if (reverse && cmd === 'order') parts.push('--reverse');

    return parts.join(' ');
  }, [cmd, inputFile, inputFile2, output, dialect, toDialect, percent, tenantColumn, tenantValue, 
      queryText, format, hashPatterns, fakePatterns, progress, dryRun, json, strict, failFast, 
      preserveRelations, interactive, reverse]);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(generatedCommand);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch (e) {
      console.error('Copy failed:', e);
    }
  };

  const showOutput = !['analyze', 'validate', 'order', 'query'].includes(cmd);
  const showInput2 = cmd === 'diff';
  const showToDialect = cmd === 'convert';
  const showCmdOptions = ['sample', 'shard', 'query', 'graph', 'diff', 'redact'].includes(cmd);

  return (
    <div className="cb-root">
      {/* Output Panel */}
      <div className="cb-output-panel">
        <div className="cb-output-header">
          <span className="cb-output-label">Generated Command</span>
          <button className={`cb-copy-btn ${copied ? 'copied' : ''}`} onClick={handleCopy} type="button">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
              <rect x="9" y="9" width="13" height="13" rx="2" />
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
            </svg>
            <span>{copied ? 'Copied!' : 'Copy'}</span>
          </button>
        </div>
        <div className="cb-output-code">
          <pre id="generated-command"><code>{generatedCommand}</code></pre>
        </div>
      </div>

      <div className="cb-main">
        {/* Command Picker */}
        <div className="cb-commands">
          <div className="cb-section-title">Select Command</div>
          {groupedCommands.map(group => (
            <div key={group.category} className="cb-cmd-group">
              <div className="cb-cmd-group-label" style={{ color: group.color }}>{group.label}</div>
              <div className="cb-cmd-list">
                {group.commands.map(c => (
                  <button
                    key={c.id}
                    type="button"
                    className={`cb-cmd-btn ${currentCommand === c.id ? 'active' : ''}`}
                    data-command={c.id}
                    onClick={() => setCurrentCommand(c.id)}
                    style={{ '--cat-color': group.color } as React.CSSProperties}
                  >
                    <span className="cb-cmd-name">{c.label}</span>
                    <span className="cb-cmd-desc">{c.desc}</span>
                  </button>
                ))}
              </div>
            </div>
          ))}
        </div>

        {/* Options Panel */}
        <div className="cb-options">
          {/* Files */}
          <div className="cb-option-group">
            <div className="cb-option-title">Files</div>
            <div className="cb-field">
              <label htmlFor="input-file">Input</label>
              <input type="text" id="input-file" value={inputFile} onChange={e => setInputFile(e.target.value)} />
            </div>
            {showInput2 && (
              <div className="cb-field" id="input-file2-group">
                <label htmlFor="input-file2">Compare With</label>
                <input type="text" id="input-file2" value={inputFile2} onChange={e => setInputFile2(e.target.value)} />
              </div>
            )}
            {showOutput && (
              <div className="cb-field" id="output-group">
                <label htmlFor="output">Output</label>
                <input type="text" id="output" value={output} onChange={e => setOutput(e.target.value)} />
              </div>
            )}
          </div>

          {/* Dialect */}
          <div className="cb-option-group" id="dialect-section">
            <div className="cb-option-title">Dialect</div>
            {!showToDialect && (
              <div className="cb-field" id="dialect-group">
                <label htmlFor="dialect">Source</label>
                <select id="dialect" value={dialect} onChange={e => setDialect(e.target.value)}>
                  <option value="">Auto-detect</option>
                  <option value="mysql">MySQL</option>
                  <option value="postgres">PostgreSQL</option>
                  <option value="sqlite">SQLite</option>
                  <option value="mssql">MSSQL</option>
                </select>
              </div>
            )}
            {showToDialect && (
              <div className="cb-field" id="to-dialect-group">
                <label htmlFor="to-dialect">Convert To</label>
                <select id="to-dialect" value={toDialect} onChange={e => setToDialect(e.target.value)}>
                  <option value="postgres">PostgreSQL</option>
                  <option value="mysql">MySQL</option>
                  <option value="sqlite">SQLite</option>
                  <option value="mssql">MSSQL</option>
                </select>
              </div>
            )}
          </div>

          {/* Command-specific Options */}
          {showCmdOptions && (
            <div className="cb-option-group" id="cmd-options-section">
              <div className="cb-option-title">Options</div>
              {cmd === 'sample' && (
                <div className="cb-field" id="percent-group">
                  <label htmlFor="percent">Sample %</label>
                  <input type="number" id="percent" min={1} max={100} value={percent} onChange={e => setPercent(e.target.value)} />
                </div>
              )}
              {cmd === 'shard' && (
                <>
                  <div className="cb-field" id="tenant-column-group">
                    <label htmlFor="tenant-column">Tenant Column</label>
                    <input type="text" id="tenant-column" value={tenantColumn} onChange={e => setTenantColumn(e.target.value)} />
                  </div>
                  <div className="cb-field" id="tenant-value-group">
                    <label htmlFor="tenant-value">Tenant Value</label>
                    <input type="text" id="tenant-value" value={tenantValue} onChange={e => setTenantValue(e.target.value)} />
                  </div>
                </>
              )}
              {cmd === 'query' && (
                <div className="cb-field" id="query-group">
                  <label htmlFor="query-text">SQL Query</label>
                  <input type="text" id="query-text" value={queryText} onChange={e => setQueryText(e.target.value)} />
                </div>
              )}
              {['graph', 'diff', 'query'].includes(cmd) && (
                <div className="cb-field" id="format-group">
                  <label htmlFor="format">Format</label>
                  <select id="format" value={format} onChange={e => setFormat(e.target.value)}>
                    <option value="">Default</option>
                    <option value="json">JSON</option>
                    <option value="mermaid">Mermaid</option>
                    <option value="dot">DOT</option>
                    <option value="csv">CSV</option>
                  </select>
                </div>
              )}
              {cmd === 'redact' && (
                <>
                  <div className="cb-field" id="hash-group">
                    <label htmlFor="hash-patterns">Hash Columns</label>
                    <input type="text" id="hash-patterns" value={hashPatterns} onChange={e => setHashPatterns(e.target.value)} placeholder="*.email, *.ssn" />
                  </div>
                  <div className="cb-field" id="fake-group">
                    <label htmlFor="fake-patterns">Fake Data</label>
                    <input type="text" id="fake-patterns" value={fakePatterns} onChange={e => setFakePatterns(e.target.value)} placeholder="*.name, *.phone" />
                  </div>
                </>
              )}
            </div>
          )}

          {/* Flags */}
          <div className="cb-option-group">
            <div className="cb-option-title">Flags</div>
            <div className="cb-flags">
              {!['order'].includes(cmd) && (
                <label className="cb-flag" id="progress-opt">
                  <input type="checkbox" id="progress" checked={progress} onChange={e => setProgress(e.target.checked)} />
                  <span>--progress</span>
                </label>
              )}
              {!['analyze', 'validate', 'query', 'graph', 'order'].includes(cmd) && (
                <label className="cb-flag" id="dry-run-opt">
                  <input type="checkbox" id="dry-run" checked={dryRun} onChange={e => setDryRun(e.target.checked)} />
                  <span>--dry-run</span>
                </label>
              )}
              {cmd !== 'query' && (
                <label className="cb-flag" id="json-opt">
                  <input type="checkbox" id="json" checked={json} onChange={e => setJson(e.target.checked)} />
                  <span>--json</span>
                </label>
              )}
              {['validate', 'convert', 'redact'].includes(cmd) && (
                <label className="cb-flag" id="strict-opt">
                  <input type="checkbox" id="strict" checked={strict} onChange={e => setStrict(e.target.checked)} />
                  <span>--strict</span>
                </label>
              )}
              {['split', 'analyze', 'convert', 'validate'].includes(cmd) && (
                <label className="cb-flag" id="fail-fast-opt">
                  <input type="checkbox" id="fail-fast" checked={failFast} onChange={e => setFailFast(e.target.checked)} />
                  <span>--fail-fast</span>
                </label>
              )}
              {cmd === 'sample' && (
                <label className="cb-flag" id="preserve-relations-opt">
                  <input type="checkbox" id="preserve-relations" checked={preserveRelations} onChange={e => setPreserveRelations(e.target.checked)} />
                  <span>--preserve-relations</span>
                </label>
              )}
              {cmd === 'query' && (
                <label className="cb-flag" id="interactive-opt">
                  <input type="checkbox" id="interactive" checked={interactive} onChange={e => setInteractive(e.target.checked)} />
                  <span>--interactive</span>
                </label>
              )}
              {cmd === 'order' && (
                <label className="cb-flag" id="reverse-opt">
                  <input type="checkbox" id="reverse" checked={reverse} onChange={e => setReverse(e.target.checked)} />
                  <span>--reverse</span>
                </label>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
