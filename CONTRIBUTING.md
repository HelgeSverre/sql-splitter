# Contributing to sql-splitter

Thanks for your interest in contributing! This document covers everything you need to get started.

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/helgesverre/sql-splitter.git
   cd sql-splitter
   ```

2. **Install Rust 1.70+**
   - All platforms: [rustup.rs](https://rustup.rs/)
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

3. **Build and test**
   ```bash
   cargo build --release
   cargo test
   ```

## Code Style

- Run `cargo fmt` before committing
- Run `cargo clippy` to check for issues
- Keep functions focused and small
- Add comments for non-obvious logic

## Testing

- Run all tests: `cargo test`
- Run with output: `cargo test -- --nocapture`
- Run benchmarks: `cargo bench`

All PRs must pass tests. Add tests for new functionality.

## Benchmarking

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench parser

# With profiling (requires flamegraph)
cargo flamegraph --bench parser
```

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and linting:
   ```bash
   cargo fmt
   cargo clippy
   cargo test
   ```
5. Commit with a clear message
6. Push and open a PR

### PR Expectations

- Describe what the PR does and why
- Keep changes focused (one feature/fix per PR)
- Update documentation if needed
- Add tests for new functionality

## Reporting Issues

- Search existing issues first
- Include reproduction steps for bugs
- Include Rust version (`rustc --version`) and OS
- Be specific about expected vs actual behavior

## Questions?

Open an issue or start a discussion.
