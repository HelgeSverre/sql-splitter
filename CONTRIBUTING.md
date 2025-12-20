# Contributing to sql-splitter

Thanks for your interest in contributing! This document covers everything you need to get started.

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/helgesverre/sql-splitter.git
   cd sql-splitter
   ```

2. **Install Go 1.21+**
   - macOS: `brew install go`
   - Linux: See [go.dev/doc/install](https://go.dev/doc/install)

3. **Build and test**
   ```bash
   make build
   make test
   ```

## Code Style

- Run `go fmt ./...` before committing
- Run `make lint` to check for issues (requires [golangci-lint](https://golangci-lint.run/))
- Keep functions focused and small
- Add comments for non-obvious logic

## Testing

- Run all tests: `make test`
- Run with coverage: `make test-cover`
- Run benchmarks: `make bench`

All PRs must pass tests. Add tests for new functionality.

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and linting
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
- Include Go version and OS
- Be specific about expected vs actual behavior

## Questions?

Open an issue or start a discussion.
