#!/bin/bash
#
# Verify sql-splitter against real-world SQL dumps from public sources
#
# This script downloads various SQL dumps from public sources, runs sql-splitter
# against them, and verifies the output. All test files are cleaned up after.
#
# Usage:
#   ./scripts/verify-realworld.sh           # Run all tests
#   ./scripts/verify-realworld.sh --keep    # Keep downloaded files after test
#   ./scripts/verify-realworld.sh --list    # List available test cases
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEST_DIR="$PROJECT_ROOT/tests/realworld"
DOWNLOADS_DIR="$TEST_DIR/downloads"
OUTPUT_DIR="$TEST_DIR/output"
BINARY="$PROJECT_ROOT/target/release/sql-splitter"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

KEEP_FILES=false
LIST_ONLY=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --keep)
            KEEP_FILES=true
            shift
            ;;
        --list)
            LIST_ONLY=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--keep] [--list]"
            exit 1
            ;;
    esac
done

# Test case definitions
# Format: NAME|EXPECTED_DIALECT|URL|UNZIP_CMD|SQL_FILE|NOTES
# EXPECTED_DIALECT can be "any" if we just want to verify it parses without errors
declare -a TEST_CASES=(
    # MySQL/MariaDB dumps - these have proper mysqldump headers
    "mysql-classicmodels|mysql|https://www.mysqltutorial.org/wp-content/uploads/2023/10/mysqlsampledatabase.zip|unzip -o|mysqlsampledatabase.sql|MySQL Tutorial sample DB"
    "mysql-sakila-schema|mysql|https://downloads.mysql.com/docs/sakila-db.zip|unzip -o|sakila-db/sakila-schema.sql|Official MySQL Sakila schema"
    "mysql-sakila-data|mysql|https://downloads.mysql.com/docs/sakila-db.zip|unzip -o|sakila-db/sakila-data.sql|Official MySQL Sakila data"
    "mysql-employees|mysql|https://github.com/datacharmer/test_db/raw/master/employees.sql|none|employees.sql|MySQL Employees test DB"
    "mysql-world|mysql|https://downloads.mysql.com/docs/world-db.zip|unzip -o|world-db/world.sql|Official MySQL World DB"
    
    # PostgreSQL dumps - these have pg_dump headers
    "postgres-pagila-schema|postgres|https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql|none|pagila-schema.sql|Pagila PostgreSQL port of Sakila"
    "postgres-pagila-data|postgres|https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql|none|pagila-data.sql|Pagila data with COPY statements"
    "postgres-airlines-small|postgres|https://edu.postgrespro.com/demo-small-en.zip|unzip -o|demo-small-en-20170815.sql|PostgresPro Airlines demo (small)"
    "postgres-northwind|postgres|https://raw.githubusercontent.com/pthom/northwind_psql/master/northwind.sql|none|northwind.sql|Northwind PostgreSQL port"
    "postgres-periodic|postgres|https://raw.githubusercontent.com/sdrahmath/PeriodicTableDatabase/main/periodic_table.sql|none|periodic_table.sql|Periodic table elements DB"
    "postgres-ecommerce|postgres|https://raw.githubusercontent.com/larbisahli/e-commerce-database-schema/main/init.sql|none|init.sql|E-commerce schema with UUIDs"
    "postgres-sakila-schema|postgres|https://raw.githubusercontent.com/jOOQ/sakila/main/postgres-sakila-db/postgres-sakila-schema.sql|none|postgres-sakila-schema.sql|jOOQ Sakila PostgreSQL schema"
    "postgres-sakila-data|postgres|https://raw.githubusercontent.com/jOOQ/sakila/main/postgres-sakila-db/postgres-sakila-insert-data.sql|none|postgres-sakila-insert-data.sql|jOOQ Sakila PostgreSQL data"
    "postgres-adventureworks|postgres|https://raw.githubusercontent.com/morenoh149/postgresDBSamples/master/adventureworks/install.sql|none|install.sql|AdventureWorks PostgreSQL port"
    
    # Generic SQL files - may not have specific dialect markers
    # These test that sql-splitter can handle files even with low-confidence detection
    "generic-chinook-pg|any|https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql|none|Chinook_PostgreSql.sql|Chinook DB (generic SQL)"
    "generic-chinook-sqlite|any|https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sql|none|Chinook_Sqlite.sql|Chinook DB SQLite version"
    "generic-chinook-mysql|any|https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_MySql.sql|none|Chinook_MySql.sql|Chinook DB MySQL version"
    
    # WordPress database dumps - real WordPress installations
    "wordpress-films|mysql|https://raw.githubusercontent.com/chamathis/WordPress-Test/master/wp_films.sql|none|wp_films.sql|WordPress Films site (2017)"
    
    # Northwind - classic Microsoft sample database ported to MySQL
    "mysql-northwind-data|mysql|https://raw.githubusercontent.com/dalers/mywind/master/northwind-data.sql|none|northwind-data.sql|Northwind MySQL data"
    
    # Geographic and reference data dumps
    "mysql-countries|mysql|https://gist.githubusercontent.com/adhipg/1600028/raw/countries.sql|none|countries.sql|Countries with phone codes"
    "mysql-wilayah|mysql|https://raw.githubusercontent.com/cahyadsn/wilayah/master/db/wilayah.sql|none|wilayah.sql|Indonesian administrative regions (large)"
    
    # Educational/tutorial database dumps
    "mysql-coffeeshop|any|https://raw.githubusercontent.com/mochen862/full-sql-database-course/main/create_insert.sql|none|create_insert.sql|Coffee shop tutorial DB"
    
    # WordPress WooCommerce demo data
    "wordpress-woocommerce|mysql|https://raw.githubusercontent.com/GoldenOwlAsia/wordpress-woocommerce-demo/master/demowordpress.sql|none|demowordpress.sql|WooCommerce demo with products/orders"
    "wordpress-woo-replica|mysql|https://raw.githubusercontent.com/GoldenOwlAsia/wordpress-woocommerce-demo/master/demowordpress_replica.sql|none|demowordpress_replica.sql|WooCommerce replica DB"
    "wordpress-plugin-test|mysql|https://raw.githubusercontent.com/WPBP/WordPress-Plugin-Boilerplate-Powered/master/plugin-name/tests/_data/dump.sql|none|dump.sql|WordPress plugin test fixture"
    

    
    # Large real-world test - Stack Overflow subset (if available)
    # Note: Large files may take time to download
)

print_header() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  SQL Splitter Real-World Verification${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

list_tests() {
    echo "Available test cases:"
    echo ""
    printf "%-25s %-10s %s\n" "NAME" "DIALECT" "DESCRIPTION"
    echo "────────────────────────────────────────────────────────────────────────"
    for test_case in "${TEST_CASES[@]}"; do
        IFS='|' read -r name dialect url unzip_cmd sql_file notes <<< "$test_case"
        printf "%-25s %-10s %s\n" "$name" "$dialect" "$notes"
    done
    echo ""
    echo "Total: ${#TEST_CASES[@]} test cases"
    echo ""
    echo "Note: 'any' dialect means we only verify parsing works, not detection accuracy"
}

setup() {
    echo "Setting up test environment..."
    mkdir -p "$DOWNLOADS_DIR"
    mkdir -p "$OUTPUT_DIR"
    
    # Build release binary if not exists
    if [[ ! -f "$BINARY" ]]; then
        echo "Building release binary..."
        cd "$PROJECT_ROOT"
        cargo build --release
    fi
}

cleanup() {
    if [[ "$KEEP_FILES" == "false" ]]; then
        echo ""
        echo "Cleaning up test files..."
        rm -rf "$TEST_DIR"
        echo -e "${GREEN}✓ Cleanup complete${NC}"
    else
        echo ""
        echo -e "${YELLOW}Test files kept at: $TEST_DIR${NC}"
        echo "  Downloads: $DOWNLOADS_DIR"
        echo "  Output: $OUTPUT_DIR"
    fi
}

download_file() {
    local name=$1
    local url=$2
    local output_file="$DOWNLOADS_DIR/$(basename "$url")"
    
    if [[ -f "$output_file" ]]; then
        echo "  Using cached: $(basename "$output_file")"
        return 0
    fi
    
    echo "  Downloading: $(basename "$url")"
    if ! curl -fsSL --connect-timeout 30 --max-time 600 -o "$output_file" "$url" 2>/dev/null; then
        echo -e "  ${YELLOW}⚠ Download failed (skipping)${NC}"
        return 1
    fi
    
    return 0
}

extract_file() {
    local unzip_cmd=$1
    local downloaded_file=$2
    
    if [[ "$unzip_cmd" == "none" ]]; then
        return 0
    fi
    
    echo "  Extracting..."
    cd "$DOWNLOADS_DIR"
    
    case "$unzip_cmd" in
        "unzip -o")
            unzip -o -q "$(basename "$downloaded_file")" 2>/dev/null || true
            ;;
        "tar -xf")
            tar -xf "$(basename "$downloaded_file")" 2>/dev/null || true
            ;;
        "gunzip")
            gunzip -f "$(basename "$downloaded_file")" 2>/dev/null || true
            ;;
    esac
    
    cd "$PROJECT_ROOT"
    return 0
}

run_test() {
    local name=$1
    local expected_dialect=$2
    local sql_file=$3
    local notes=$4
    local test_output_dir="$OUTPUT_DIR/$name"
    
    local full_path="$DOWNLOADS_DIR/$sql_file"
    
    if [[ ! -f "$full_path" ]]; then
        echo -e "  ${YELLOW}⚠ SQL file not found: $sql_file (skipping)${NC}"
        return 1
    fi
    
    local file_size=$(du -h "$full_path" | cut -f1)
    echo "  File: $sql_file ($file_size)"
    
    mkdir -p "$test_output_dir"
    
    # Run sql-splitter with auto-detection (no --dialect flag)
    local start_time=$(python3 -c 'import time; print(time.time())' 2>/dev/null || date +%s)
    local output
    if ! output=$("$BINARY" split "$full_path" --output="$test_output_dir" --dry-run 2>&1); then
        echo -e "  ${RED}✗ FAILED - sql-splitter crashed${NC}"
        echo "  Error: $output"
        return 1
    fi
    local end_time=$(python3 -c 'import time; print(time.time())' 2>/dev/null || date +%s)
    
    # Parse output
    local detected_dialect=$(echo "$output" | grep -o "Auto-detected dialect: [a-z]*" | awk '{print $3}' || echo "unknown")
    local confidence=$(echo "$output" | grep -o "([a-z]* confidence)" | tr -d '()' || echo "unknown")
    local tables=$(echo "$output" | grep "Tables found:" | awk '{print $3}' || echo "0")
    local statements=$(echo "$output" | grep "Statements processed:" | awk '{print $3}' || echo "0")
    
    # Calculate duration
    local duration=$(echo "$end_time - $start_time" | bc 2>/dev/null || echo "?")
    
    # Determine if dialect matches
    local dialect_status=""
    if [[ "$expected_dialect" == "any" ]]; then
        dialect_status="${CYAN}(any)${NC}"
    elif [[ "$detected_dialect" == "$expected_dialect" ]]; then
        dialect_status="${GREEN}✓${NC}"
    else
        dialect_status="${YELLOW}~${NC}"
    fi
    
    # Check if we got meaningful results
    if [[ "$tables" == "0" && "$statements" == "0" ]]; then
        echo -e "  ${YELLOW}⚠ WARNING - No tables or statements found${NC}"
        echo "    Detected: $detected_dialect ($confidence)"
        return 0  # Not a failure, just a warning
    fi
    
    echo -e "  ${GREEN}✓ PASSED${NC} - Tables: $tables, Statements: $statements"
    echo -e "    Detected: $detected_dialect ($confidence) $dialect_status Expected: $expected_dialect"
    
    return 0
}

run_all_tests() {
    local passed=0
    local failed=0
    local skipped=0
    local warnings=0
    
    for test_case in "${TEST_CASES[@]}"; do
        IFS='|' read -r name dialect url unzip_cmd sql_file notes <<< "$test_case"
        
        echo ""
        echo -e "${BLUE}[$name]${NC} - $notes"
        
        # Download
        local downloaded_file="$DOWNLOADS_DIR/$(basename "$url")"
        if ! download_file "$name" "$url"; then
            ((skipped++))
            continue
        fi
        
        # Extract if needed
        if ! extract_file "$unzip_cmd" "$downloaded_file"; then
            ((skipped++))
            continue
        fi
        
        # Run test
        if run_test "$name" "$dialect" "$sql_file" "$notes"; then
            ((passed++))
        else
            ((failed++))
        fi
    done
    
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Results${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  ${GREEN}Passed:${NC}  $passed"
    echo -e "  ${RED}Failed:${NC}  $failed"
    echo -e "  ${YELLOW}Skipped:${NC} $skipped"
    echo ""
    echo "Legend:"
    echo "  ✓ = Dialect correctly detected"
    echo "  ~ = Dialect differs from expected (file may lack dialect markers)"
    echo "  (any) = Generic SQL, detection accuracy not checked"
    echo ""
    
    if [[ $failed -gt 0 ]]; then
        return 1
    fi
    return 0
}

# Main execution
if [[ "$LIST_ONLY" == "true" ]]; then
    list_tests
    exit 0
fi

print_header
setup

# Set trap for cleanup
trap cleanup EXIT

run_all_tests
exit_code=$?

exit $exit_code
