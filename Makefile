.PHONY: build test bench clean install run-split run-analyze


# Show help
help:
	@echo "Available targets:"
	@echo "  build          - Build the application"
	@echo "  build-release  - Build optimized release binary"
	@echo "  test           - Run all tests"
	@echo "  test-cover     - Run tests with coverage report"
	@echo "  bench          - Run all benchmarks"
	@echo "  bench-cpu      - Run parser benchmark with CPU profiling"
	@echo "  bench-mem      - Run parser benchmark with memory profiling"
	@echo "  clean          - Remove build artifacts"
	@echo "  install        - Install to GOPATH/bin"
	@echo "  fmt            - Format code"
	@echo "  lint           - Run linter"
	@echo "  run-split      - Run split command on test.sql"
	@echo "  run-analyze    - Run analyze command on test.sql"


# Build the application
build:
	go build -o sql-splitter -ldflags="-s -w" .

# Build with optimizations for release
build-release:
	CGO_ENABLED=0 go build -o sql-splitter -ldflags="-s -w -X main.version=$(shell git describe --tags --always)" .

# Run all tests
test:
	go test -v ./...

# Run tests with coverage
test-cover:
	go test -cover ./...
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run benchmarks
bench:
	go test -bench=. -benchmem ./...

# Run specific benchmark with profiling
bench-cpu:
	go test -bench=BenchmarkParser_ReadStatement -cpuprofile=cpu.prof ./internal/parser
	go tool pprof -http=:8080 cpu.prof

bench-mem:
	go test -bench=BenchmarkParser_ReadStatement -memprofile=mem.prof ./internal/parser
	go tool pprof -http=:8080 mem.prof

# Clean build artifacts
clean:
	rm -f sql-splitter
	rm -f coverage.out coverage.html
	rm -f *.prof
	rm -rf output/ tables/ out/

# Install to GOPATH/bin
install:
	go install .

# Format code
fmt:
	go fmt ./...

# Run linter
lint:
	golangci-lint run

# Run split command on test file
run-split:
	@if [ ! -f test.sql ]; then \
		echo "Error: test.sql not found"; \
		exit 1; \
	fi
	./sql-splitter split test.sql -o output -v

# Run analyze command on test file
run-analyze:
	@if [ ! -f test.sql ]; then \
		echo "Error: test.sql not found"; \
		exit 1; \
	fi
	./sql-splitter analyze test.sql --progress
