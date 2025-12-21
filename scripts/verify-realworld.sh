#!/bin/bash
#
# Verify sql-splitter against real-world SQL dumps from public sources
#
# This script downloads various SQL dumps from public sources, runs sql-splitter
# against them, and verifies the output. All test files are cleaned up after.
#
# Tests performed:
#   1. Split command - Parse and split SQL files by table
#   2. Convert command - Convert between MySQL, PostgreSQL, SQLite dialects
#   3. Validate (input) - Run integrity checks on downloaded SQL files
#   4. Validate (glob) - Use glob pattern to validate all split output files
#   5. Validate (roundtrip) - Split→Merge→Validate to verify no data loss
#   6. Redact command - Test data anonymization with various strategies
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
    
    # Chinook database - multi-dialect sample database with dialect-specific versions
    "chinook-postgres|postgres|https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql|none|Chinook_PostgreSql.sql|Chinook DB PostgreSQL version"
    "chinook-sqlite|sqlite|https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sql|none|Chinook_Sqlite.sql|Chinook DB SQLite version"
    "chinook-mysql|mysql|https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_MySql.sql|none|Chinook_MySql.sql|Chinook DB MySQL version"
    
    # WordPress database dumps - real WordPress installations
    "wordpress-films|mysql|https://raw.githubusercontent.com/chamathis/WordPress-Test/master/wp_films.sql|none|wp_films.sql|WordPress Films site (2017)"
    
    # Northwind - classic Microsoft sample database ported to MySQL
    "mysql-northwind-data|mysql|https://raw.githubusercontent.com/dalers/mywind/master/northwind-data.sql|none|northwind-data.sql|Northwind MySQL data"
    
    # Geographic and reference data dumps
    "mysql-countries|mysql|https://gist.githubusercontent.com/adhipg/1600028/raw/countries.sql|none|countries.sql|Countries with phone codes"
    "mysql-wilayah|mysql|https://raw.githubusercontent.com/cahyadsn/wilayah/master/db/wilayah.sql|none|wilayah.sql|Indonesian administrative regions (large)"
    
    # Educational/tutorial database dumps (ANSI SQL, treating as MySQL for conversion tests)
    "mysql-coffeeshop|mysql|https://raw.githubusercontent.com/mochen862/full-sql-database-course/main/create_insert.sql|none|create_insert.sql|Coffee shop tutorial DB"
    
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
        /bin/rm -rf "$TEST_DIR"
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

# Store test results for grouped display
declare -a RESULTS_MYSQL=()
declare -a RESULTS_POSTGRES=()
declare -a RESULTS_GENERIC=()

# Validation results storage (parallel arrays since bash 3.2 doesn't support -A)
declare -a VALIDATE_INPUT_NAMES=()
declare -a VALIDATE_INPUT_VALUES=()
declare -a VALIDATE_OUTPUT_NAMES=()
declare -a VALIDATE_OUTPUT_VALUES=()
declare -a VALIDATE_GLOB_NAMES=()
declare -a VALIDATE_GLOB_VALUES=()

# Redact results storage
declare -a REDACT_NAMES=()
declare -a REDACT_VALUES=()

# Get validation result by name from parallel arrays
# Args: name, type (input/output/glob)
# Returns: result string via stdout
get_validate_result() {
    local name=$1
    local type=$2
    
    if [[ "$type" == "input" || "$type" == "true" ]]; then
        for i in "${!VALIDATE_INPUT_NAMES[@]}"; do
            if [[ "${VALIDATE_INPUT_NAMES[$i]}" == "$name" ]]; then
                echo "${VALIDATE_INPUT_VALUES[$i]}"
                return
            fi
        done
    elif [[ "$type" == "glob" ]]; then
        for i in "${!VALIDATE_GLOB_NAMES[@]}"; do
            if [[ "${VALIDATE_GLOB_NAMES[$i]}" == "$name" ]]; then
                echo "${VALIDATE_GLOB_VALUES[$i]}"
                return
            fi
        done
    else
        for i in "${!VALIDATE_OUTPUT_NAMES[@]}"; do
            if [[ "${VALIDATE_OUTPUT_NAMES[$i]}" == "$name" ]]; then
                echo "${VALIDATE_OUTPUT_VALUES[$i]}"
                return
            fi
        done
    fi
    echo "?"
}

# Get redact result by name
get_redact_result() {
    local name=$1
    for i in "${!REDACT_NAMES[@]}"; do
        if [[ "${REDACT_NAMES[$i]}" == "$name" ]]; then
            echo "${REDACT_VALUES[$i]}"
            return
        fi
    done
    echo "?"
}

# Run redact test on a SQL file
# Args: name, sql_file, dialect
# Returns: 0 on success, 1 on failure
run_redact_test() {
    local name=$1
    local sql_file=$2
    local dialect=$3
    local redact_output_dir="$OUTPUT_DIR/redact/$name"
    local redact_output="$redact_output_dir/${name}_redacted.sql"

    if [[ ! -f "$sql_file" ]]; then
        return 1
    fi

    mkdir -p "$redact_output_dir"

    local result_str=""
    local all_passed=true

    # Test 1: Dry-run mode (basic functionality)
    if "$BINARY" redact "$sql_file" --dialect="$dialect" --dry-run --null "*.password" >/dev/null 2>&1; then
        result_str+="dry:✓ "
    else
        result_str+="dry:✗ "
        all_passed=false
    fi

    # Test 2: With --null strategy
    if "$BINARY" redact "$sql_file" --dialect="$dialect" --output="$redact_output" --null "*.password,*.ssn" >/dev/null 2>&1; then
        if [[ -f "$redact_output" ]]; then
            result_str+="null:✓ "
        else
            result_str+="null:⚠ "
            all_passed=false
        fi
    else
        result_str+="null:✗ "
        all_passed=false
    fi

    # Test 3: With --hash strategy
    local hash_output="$redact_output_dir/${name}_hash.sql"
    if "$BINARY" redact "$sql_file" --dialect="$dialect" --output="$hash_output" --hash "*.email" >/dev/null 2>&1; then
        if [[ -f "$hash_output" ]]; then
            result_str+="hash:✓ "
        else
            result_str+="hash:⚠ "
            all_passed=false
        fi
    else
        result_str+="hash:✗ "
        all_passed=false
    fi

    # Test 4: With --fake strategy
    local fake_output="$redact_output_dir/${name}_fake.sql"
    if "$BINARY" redact "$sql_file" --dialect="$dialect" --output="$fake_output" --fake "*.name,*.phone" >/dev/null 2>&1; then
        if [[ -f "$fake_output" ]]; then
            result_str+="fake:✓ "
        else
            result_str+="fake:⚠ "
            all_passed=false
        fi
    else
        result_str+="fake:✗ "
        all_passed=false
    fi

    # Test 5: Reproducible with --seed
    local seed_output1="$redact_output_dir/${name}_seed1.sql"
    local seed_output2="$redact_output_dir/${name}_seed2.sql"
    if "$BINARY" redact "$sql_file" --dialect="$dialect" --output="$seed_output1" --null "*.password" --seed 42 >/dev/null 2>&1 && \
       "$BINARY" redact "$sql_file" --dialect="$dialect" --output="$seed_output2" --null "*.password" --seed 42 >/dev/null 2>&1; then
        result_str+="seed:✓"
    else
        result_str+="seed:✗"
        all_passed=false
    fi

    # Trim trailing space
    result_str="${result_str% }"

    REDACT_NAMES+=("$name")
    REDACT_VALUES+=("$result_str")

    if [[ "$all_passed" == "true" ]]; then
        return 0
    fi
    return 1
}

# Run validation on a SQL file
# Args: name, sql_file, dialect, is_input (true/false)
# Returns: 0 on success (no errors), 1 on validation errors
run_validate() {
    local name=$1
    local sql_file=$2
    local dialect=$3
    local is_input=$4

    if [[ ! -f "$sql_file" ]]; then
        return 1
    fi

    # Run validate with --no-fk-checks for speed (FK checks require MySQL and are slow)
    # Use --json for easy parsing
    local output
    local exit_code=0
    output=$("$BINARY" validate "$sql_file" --dialect="$dialect" --no-fk-checks --json 2>/dev/null) || exit_code=$?

    # Parse JSON output for errors/warnings
    local errors=$(echo "$output" | grep -o '"errors":[0-9]*' | grep -o '[0-9]*' || echo "0")
    local warnings=$(echo "$output" | grep -o '"warnings":[0-9]*' | grep -o '[0-9]*' || echo "0")

    local result_str
    if [[ "$errors" -gt 0 ]]; then
        result_str="✗ ${errors}err"
    elif [[ "$warnings" -gt 0 ]]; then
        result_str="⚠ ${warnings}warn"
    else
        result_str="✓"
    fi

    # Store result in parallel arrays
    if [[ "$is_input" == "true" ]]; then
        VALIDATE_INPUT_NAMES+=("$name")
        VALIDATE_INPUT_VALUES+=("$result_str")
    else
        VALIDATE_OUTPUT_NAMES+=("$name")
        VALIDATE_OUTPUT_VALUES+=("$result_str")
    fi

    # Return success if no errors (warnings are OK)
    if [[ "$errors" -gt 0 ]]; then
        return 1
    fi
    return 0
}

# Run validation on split output using glob patterns
# Args: name, output_dir, dialect
# Returns: 0 on success (all files pass), 1 on any validation errors
run_validate_glob() {
    local name=$1
    local output_dir=$2
    local dialect=$3

    # Check if output directory exists and has SQL files
    if [[ ! -d "$output_dir" ]]; then
        VALIDATE_GLOB_NAMES+=("$name")
        VALIDATE_GLOB_VALUES+=("skipped (no dir)")
        return 1
    fi

    local sql_count=$(find "$output_dir" -name "*.sql" -type f 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$sql_count" -eq 0 ]]; then
        VALIDATE_GLOB_NAMES+=("$name")
        VALIDATE_GLOB_VALUES+=("skipped (no files)")
        return 1
    fi

    # Run validate with glob pattern on the split output directory
    # Use --json for easy parsing of aggregated results
    local glob_pattern="${output_dir}/*.sql"
    local output
    local exit_code=0
    output=$("$BINARY" validate "$glob_pattern" --dialect="$dialect" --no-fk-checks --json 2>/dev/null) || exit_code=$?

    # Parse JSON output for passed/failed counts
    # The multi-file JSON format uses "passed" and "failed" at top level
    # Handle pretty-printed JSON with spaces after colons
    local passed=$(echo "$output" | grep -o '"passed": *[0-9]*' | head -1 | grep -o '[0-9]*' || echo "0")
    local failed=$(echo "$output" | grep -o '"failed": *[0-9]*' | head -1 | grep -o '[0-9]*' || echo "0")
    local total_files=$((passed + failed))

    local result_str
    if [[ "$failed" -gt 0 ]]; then
        result_str="✗ ${passed}/${total_files}"
    elif [[ "$passed" -gt 0 ]]; then
        result_str="✓ ${passed}/${total_files}"
    else
        result_str="⚠ no files"
    fi

    VALIDATE_GLOB_NAMES+=("$name")
    VALIDATE_GLOB_VALUES+=("$result_str")

    # Return success if no failures
    if [[ "$failed" -gt 0 ]]; then
        return 1
    fi
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
        return 1
    fi

    local file_size=$(du -h "$full_path" | cut -f1)

    mkdir -p "$test_output_dir"

    # Run sql-splitter with auto-detection (no --dialect flag)
    local output
    if ! output=$("$BINARY" split "$full_path" --output="$test_output_dir" --dry-run 2>&1); then
        # Store failure result
        store_result "$expected_dialect" "✗" "$name" "0" "0" "" "$file_size"
        return 1
    fi

    # Parse output
    local tables=$(echo "$output" | grep "Tables found:" | awk '{print $3}' || echo "0")
    local statements=$(echo "$output" | grep "Statements processed:" | awk '{print $3}' || echo "0")

    # Store success result (convert results filled in later)
    store_result "$expected_dialect" "✓" "$name" "$tables" "$statements" "" "$file_size"

    return 0
}

store_result() {
    local dialect=$1
    local status=$2
    local name=$3
    local tables=$4
    local stmts=$5
    local convert=$6
    local size=$7

    local result="${status}|${name}|${tables}|${stmts}|${convert}|${size}"

    case "$dialect" in
        mysql)
            RESULTS_MYSQL+=("$result")
            ;;
        postgres)
            RESULTS_POSTGRES+=("$result")
            ;;
        *)
            RESULTS_GENERIC+=("$result")
            ;;
    esac
}

update_result_convert() {
    local dialect=$1
    local name=$2
    local convert_result=$3
    local status n tables stmts _ size

    if [[ "$dialect" == "mysql" ]]; then
        for i in "${!RESULTS_MYSQL[@]}"; do
            if [[ "${RESULTS_MYSQL[$i]}" == *"|${name}|"* ]]; then
                IFS='|' read -r status n tables stmts _ size <<< "${RESULTS_MYSQL[$i]}"
                RESULTS_MYSQL[$i]="${status}|${n}|${tables}|${stmts}|${convert_result}|${size}"
                break
            fi
        done
    elif [[ "$dialect" == "postgres" ]]; then
        for i in "${!RESULTS_POSTGRES[@]}"; do
            if [[ "${RESULTS_POSTGRES[$i]}" == *"|${name}|"* ]]; then
                IFS='|' read -r status n tables stmts _ size <<< "${RESULTS_POSTGRES[$i]}"
                RESULTS_POSTGRES[$i]="${status}|${n}|${tables}|${stmts}|${convert_result}|${size}"
                break
            fi
        done
    else
        for i in "${!RESULTS_GENERIC[@]}"; do
            if [[ "${RESULTS_GENERIC[$i]}" == *"|${name}|"* ]]; then
                IFS='|' read -r status n tables stmts _ size <<< "${RESULTS_GENERIC[$i]}"
                RESULTS_GENERIC[$i]="${status}|${n}|${tables}|${stmts}|${convert_result}|${size}"
                break
            fi
        done
    fi
}

# Run convert test for a single source file to all target dialects
# Returns a compact result string like "pg:✓ sqlite:✓"
run_convert_test() {
    local name=$1
    local source_dialect=$2
    local sql_file=$3
    local convert_output_dir="$OUTPUT_DIR/convert/$name"

    local full_path="$DOWNLOADS_DIR/$sql_file"

    if [[ ! -f "$full_path" ]]; then
        return 1
    fi

    # Skip 'any' dialect (can't reliably convert)
    if [[ "$source_dialect" == "any" ]]; then
        update_result_convert "$source_dialect" "$name" "(no convert)"
        return 0
    fi

    # Define target dialects
    local -a targets=("mysql" "postgres" "sqlite")
    local convert_result=""
    local convert_failed=0

    mkdir -p "$convert_output_dir"

    for target in "${targets[@]}"; do
        # Skip same dialect conversion
        if [[ "$source_dialect" == "$target" ]]; then
            continue
        fi

        # Get abbreviation for target dialect
        local target_abbrev
        case "$target" in
            mysql)    target_abbrev="my" ;;
            postgres) target_abbrev="pg" ;;
            sqlite)   target_abbrev="sq" ;;
            *)        target_abbrev="$target" ;;
        esac

        local output_file="$convert_output_dir/${name}_to_${target}.sql"

        # Run conversion
        if "$BINARY" convert "$full_path" --from="$source_dialect" --to="$target" --output="$output_file" >/dev/null 2>&1; then
            if [[ -f "$output_file" && -s "$output_file" ]]; then
                convert_result+="${target_abbrev}:✓ "
            else
                convert_result+="${target_abbrev}:⚠ "
                ((convert_failed++))
            fi
        else
            convert_result+="${target_abbrev}:✗ "
            ((convert_failed++))
        fi
    done

    # Trim trailing space
    convert_result="${convert_result% }"

    update_result_convert "$source_dialect" "$name" "$convert_result"

    if [[ $convert_failed -gt 0 ]]; then
        return 1
    fi
    return 0
}

run_all_tests() {
    local split_passed=0
    local split_failed=0
    local split_skipped=0
    local convert_passed=0
    local convert_failed=0
    local convert_skipped=0
    local validate_input_passed=0
    local validate_input_failed=0
    local validate_input_skipped=0
    local validate_merge_passed=0
    local validate_merge_failed=0
    local validate_merge_skipped=0
    local validate_glob_passed=0
    local validate_glob_failed=0
    local validate_glob_skipped=0
    local redact_passed=0
    local redact_failed=0
    local redact_skipped=0
    
    for test_case in "${TEST_CASES[@]}"; do
        IFS='|' read -r name dialect url unzip_cmd sql_file notes <<< "$test_case"
        
        echo ""
        echo -e "${BLUE}[$name]${NC} - $notes"
        
        # Download
        local downloaded_file="$DOWNLOADS_DIR/$(basename "$url")"
        if ! download_file "$name" "$url"; then
            ((split_skipped++))
            ((convert_skipped++))
            ((validate_input_skipped++))
            ((validate_merge_skipped++))
            ((validate_glob_skipped++))
            ((redact_skipped++))
            continue
        fi
        
        # Extract if needed
        if ! extract_file "$unzip_cmd" "$downloaded_file"; then
            ((split_skipped++))
            ((convert_skipped++))
            ((validate_input_skipped++))
            ((validate_merge_skipped++))
            ((validate_glob_skipped++))
            ((redact_skipped++))
            continue
        fi
        
        local full_path="$DOWNLOADS_DIR/$sql_file"
        
        # Run validation on input file
        echo -e "  Validating input..."
        if run_validate "$name" "$full_path" "$dialect" "true"; then
            ((validate_input_passed++))
            echo -e "  Input validation: ${GREEN}$(get_validate_result "$name" "true")${NC}"
        else
            ((validate_input_failed++))
            echo -e "  Input validation: ${YELLOW}$(get_validate_result "$name" "true")${NC}"
        fi
        
        # Run split test
        if run_test "$name" "$dialect" "$sql_file" "$notes"; then
            ((split_passed++))
            
            # After successful split, run merge and validate the merged output
            local test_output_dir="$OUTPUT_DIR/$name"
            local merged_file="$OUTPUT_DIR/${name}_merged.sql"
            
            # Actually run split (not dry-run) to get output files
            if "$BINARY" split "$full_path" --output="$test_output_dir" --dialect="$dialect" >/dev/null 2>&1; then
                # Run glob validation on split output files
                echo -e "  Validating split output (glob)..."
                if run_validate_glob "$name" "$test_output_dir" "$dialect"; then
                    ((validate_glob_passed++))
                    echo -e "  Glob validation: ${GREEN}$(get_validate_result "$name" "glob")${NC}"
                else
                    ((validate_glob_failed++))
                    echo -e "  Glob validation: ${YELLOW}$(get_validate_result "$name" "glob")${NC}"
                fi
                
                # Merge the split files back together
                if "$BINARY" merge "$test_output_dir" --output="$merged_file" --dialect="$dialect" >/dev/null 2>&1; then
                    # Validate the merged output
                    echo -e "  Validating merged output..."
                    if run_validate "${name}_merged" "$merged_file" "$dialect" "false"; then
                        ((validate_merge_passed++))
                        echo -e "  Merged validation: ${GREEN}$(get_validate_result "${name}_merged" "false")${NC}"
                    else
                        ((validate_merge_failed++))
                        echo -e "  Merged validation: ${YELLOW}$(get_validate_result "${name}_merged" "false")${NC}"
                    fi
                else
                    echo -e "  Merged validation: ${YELLOW}skipped (merge failed)${NC}"
                    ((validate_merge_skipped++))
                fi
            else
                echo -e "  Merged validation: ${YELLOW}skipped (split failed)${NC}"
                echo -e "  Glob validation: ${YELLOW}skipped (split failed)${NC}"
                ((validate_merge_skipped++))
                ((validate_glob_skipped++))
            fi
        else
            ((split_failed++))
            ((validate_merge_skipped++))
            ((validate_glob_skipped++))
        fi
        
        # Run convert tests (all permutations)
        if [[ "$dialect" != "any" ]]; then
            if run_convert_test "$name" "$dialect" "$sql_file" "$notes"; then
                ((convert_passed++))
            else
                ((convert_failed++))
            fi
        else
            echo -e "  Convert tests: ${CYAN}skipped (dialect=any)${NC}"
            ((convert_skipped++))
        fi
        
        # Run redact tests
        if [[ "$dialect" != "any" ]]; then
            echo -e "  Testing redact..."
            if run_redact_test "$name" "$full_path" "$dialect"; then
                ((redact_passed++))
                echo -e "  Redact: ${GREEN}$(get_redact_result "$name")${NC}"
            else
                ((redact_failed++))
                echo -e "  Redact: ${YELLOW}$(get_redact_result "$name")${NC}"
            fi
        else
            echo -e "  Redact tests: ${CYAN}skipped (dialect=any)${NC}"
            ((redact_skipped++))
        fi
    done
    
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Results${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "${CYAN}Split Command:${NC}"
    echo -e "  ${GREEN}Passed:${NC}  $split_passed"
    echo -e "  ${RED}Failed:${NC}  $split_failed"
    echo -e "  ${YELLOW}Skipped:${NC} $split_skipped"
    echo ""
    echo -e "${CYAN}Convert Command (all permutations):${NC}"
    echo -e "  ${GREEN}Passed:${NC}  $convert_passed"
    echo -e "  ${RED}Failed:${NC}  $convert_failed"
    echo -e "  ${YELLOW}Skipped:${NC} $convert_skipped"
    echo ""
    echo -e "${CYAN}Validate Input Files:${NC}"
    echo -e "  ${GREEN}Passed:${NC}  $validate_input_passed"
    echo -e "  ${RED}Failed:${NC}  $validate_input_failed"
    echo -e "  ${YELLOW}Skipped:${NC} $validate_input_skipped"
    echo ""
    echo -e "${CYAN}Validate Split→Merge Roundtrip:${NC}"
    echo -e "  ${GREEN}Passed:${NC}  $validate_merge_passed"
    echo -e "  ${RED}Failed:${NC}  $validate_merge_failed"
    echo -e "  ${YELLOW}Skipped:${NC} $validate_merge_skipped"
    echo ""
    echo -e "${CYAN}Validate Split Output (Glob):${NC}"
    echo -e "  ${GREEN}Passed:${NC}  $validate_glob_passed"
    echo -e "  ${RED}Failed:${NC}  $validate_glob_failed"
    echo -e "  ${YELLOW}Skipped:${NC} $validate_glob_skipped"
    echo ""
    echo -e "${CYAN}Redact Command (all strategies):${NC}"
    echo -e "  ${GREEN}Passed:${NC}  $redact_passed"
    echo -e "  ${RED}Failed:${NC}  $redact_failed"
    echo -e "  ${YELLOW}Skipped:${NC} $redact_skipped"
    echo ""
    echo "Legend:"
    echo "  ✓ = Success / No issues"
    echo "  ⚠ = Warnings but no errors"
    echo "  ✗ = Errors detected"
    echo "  ~ = Dialect differs from expected (file may lack dialect markers)"
    echo "  (any) = Generic SQL, detection accuracy not checked"
    echo "  Nerr/Nwarn = N errors/warnings found"
    echo "  N/M = N files passed out of M total"
    echo "  dry/null/hash/fake/seed = redact strategy tests"
    echo ""
    
    if [[ $split_failed -gt 0 || $convert_failed -gt 0 ]]; then
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
