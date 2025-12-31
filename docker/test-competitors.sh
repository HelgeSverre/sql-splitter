#!/bin/bash
# Test all competitor tools one-by-one to verify they work
# This script builds a Docker image with all competitors and tests them

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

show_help() {
    cat << EOF
Test competitor tools in Docker container

Usage: test-competitors.sh [OPTIONS]

Options:
  --help       Show this help message
  --build      Build the Docker image (skip if already built)
  --skip-build Don't rebuild the image
  --size MB    Test file size in MB (default: 50)
  --output DIR Output directory for test results (default: /tmp/benchmark-results)

Examples:
  test-competitors.sh --build --size 100
  test-competitors.sh --skip-build --size 50
EOF
}

BUILD_IMAGE=false
FILE_SIZE_MB=50
OUTPUT_DIR="/tmp/benchmark-results"

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --build)
            BUILD_IMAGE=true
            shift
            ;;
        --skip-build)
            BUILD_IMAGE=false
            shift
            ;;
        --size)
            FILE_SIZE_MB=$2
            shift 2
            ;;
        --output)
            OUTPUT_DIR=$2
            shift 2
            ;;
        *)
            show_help
            exit 1
            ;;
    esac
done

mkdir -p "$OUTPUT_DIR"

echo -e "${BOLD}${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${BLUE}  SQL Splitter Competitor Benchmarking Suite${NC}"
echo -e "${BOLD}${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# Build Docker image if requested
if [ "$BUILD_IMAGE" = true ]; then
    echo -e "${YELLOW}Building Docker image...${NC}"
    docker build -f "$SCRIPT_DIR/Dockerfile.benchmark" -t sql-splitter-bench:latest "$REPO_ROOT"
    echo -e "${GREEN}✓ Docker image built${NC}"
    echo ""
fi

# Test basic tool detection
echo -e "${BOLD}Testing tool availability...${NC}"
docker run --rm sql-splitter-bench:latest benchmark-runner --list
echo ""

# Generate test data inside container
echo -e "${BOLD}Generating test data (${FILE_SIZE_MB}MB)...${NC}"
CONTAINER_OUTPUT="/tmp/benchmark_${FILE_SIZE_MB}mb.sql"
docker run --rm \
    -v "$OUTPUT_DIR:/output" \
    sql-splitter-bench:latest \
    benchmark-runner --generate "$FILE_SIZE_MB" --export "/output/benchmark_${FILE_SIZE_MB}mb.md"

echo -e "${GREEN}✓ Benchmark complete!${NC}"
echo -e "Results saved to: ${BOLD}$OUTPUT_DIR${NC}"
echo ""

# Check if markdown file was created
if [ -f "$OUTPUT_DIR/benchmark_${FILE_SIZE_MB}mb.md" ]; then
    echo -e "${BOLD}Results:${NC}"
    echo ""
    head -30 "$OUTPUT_DIR/benchmark_${FILE_SIZE_MB}mb.md"
    if [ -f "$OUTPUT_DIR/benchmark_${FILE_SIZE_MB}mb_enhanced.md" ]; then
        echo ""
        echo -e "${BOLD}Enhanced results (with memory & throughput):${NC}"
        echo ""
        head -20 "$OUTPUT_DIR/benchmark_${FILE_SIZE_MB}mb_enhanced.md"
    fi
else
    echo -e "${YELLOW}Note: Check Docker output for detailed results${NC}"
fi
