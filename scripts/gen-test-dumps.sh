#!/bin/bash
#
# Generate authentic SQL dumps from multiple database engines
#
# This script:
# 1. Connects to Docker containers running MySQL, MariaDB, and PostgreSQL
# 2. Creates 50+ tables with 10-100k rows each
# 3. Uses native dump tools (mysqldump, pg_dump, sqlite3) to generate dumps
# 4. Stores dumps in tests/data/generated/
#
# Prerequisites:
#   docker compose -f docker/docker-compose.databases.yml up -d
#
# Usage:
#   ./scripts/gen-test-dumps.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$PROJECT_DIR/tests/data/generated"
COMPOSE_FILE="$PROJECT_DIR/docker/docker-compose.databases.yml"

# Database connection settings
MYSQL_HOST="127.0.0.1"
MYSQL_PORT="13306"
MYSQL_USER="root"
MYSQL_PASS="testpass"
MYSQL_DB="testdb"

MARIADB_HOST="127.0.0.1"
MARIADB_PORT="13307"
MARIADB_USER="root"
MARIADB_PASS="testpass"
MARIADB_DB="testdb"

POSTGRES_HOST="127.0.0.1"
POSTGRES_PORT="15432"
POSTGRES_USER="testuser"
POSTGRES_PASS="testpass"
POSTGRES_DB="testdb"

# Generation settings (use env vars for override, e.g., NUM_TABLES=5 ./scripts/gen-test-dumps.sh)
NUM_TABLES=${NUM_TABLES:-50}
MIN_ROWS=${MIN_ROWS:-10000}
MAX_ROWS=${MAX_ROWS:-100000}

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${BLUE}  $1${NC}"
    echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${BOLD}${CYAN}─── $1 ───${NC}"
    echo ""
}

check_docker() {
    print_section "Checking Docker containers"
    
    if ! docker compose -f "$COMPOSE_FILE" ps --status running 2>/dev/null | grep -q "sql-splitter"; then
        echo -e "${YELLOW}Docker containers not running. Starting them...${NC}"
        docker compose -f "$COMPOSE_FILE" up -d
        
        echo "Waiting for databases to be ready..."
        sleep 10
        
        # Wait for healthchecks
        for i in {1..30}; do
            if docker compose -f "$COMPOSE_FILE" ps --status running | grep -q "healthy"; then
                break
            fi
            echo "  Waiting... ($i/30)"
            sleep 2
        done
    fi
    
    echo -e "${GREEN}✓${NC} Docker containers ready"
}

# Generate SQL to create tables and insert data
generate_mysql_schema() {
    local table_num=$1
    local num_rows=$2
    local table_name="table_$(printf '%03d' $table_num)"
    
    cat << EOF
-- Table: $table_name ($num_rows rows)
DROP TABLE IF EXISTS \`$table_name\`;
CREATE TABLE \`$table_name\` (
  \`id\` bigint unsigned NOT NULL AUTO_INCREMENT,
  \`col_int\` int DEFAULT NULL,
  \`col_varchar\` varchar(100) DEFAULT NULL,
  \`col_text\` text,
  \`col_decimal\` decimal(10,2) DEFAULT NULL,
  \`created_at\` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

EOF
}

generate_postgres_schema() {
    local table_num=$1
    local num_rows=$2
    local table_name="table_$(printf '%03d' $table_num)"
    
    cat << EOF
-- Table: $table_name ($num_rows rows)
DROP TABLE IF EXISTS "$table_name";
CREATE TABLE "$table_name" (
  "id" BIGSERIAL PRIMARY KEY,
  "col_int" INTEGER,
  "col_varchar" VARCHAR(100),
  "col_text" TEXT,
  "col_decimal" DECIMAL(10,2),
  "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

EOF
}

generate_sqlite_schema() {
    local table_num=$1
    local num_rows=$2
    local table_name="table_$(printf '%03d' $table_num)"
    
    cat << EOF
-- Table: $table_name ($num_rows rows)
DROP TABLE IF EXISTS "$table_name";
CREATE TABLE "$table_name" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "col_int" INTEGER,
  "col_varchar" TEXT,
  "col_text" TEXT,
  "col_decimal" REAL,
  "created_at" TEXT DEFAULT CURRENT_TIMESTAMP
);

EOF
}

# Batch insert generator for better performance
generate_mysql_inserts() {
    local table_num=$1
    local num_rows=$2
    local table_name="table_$(printf '%03d' $table_num)"
    local batch_size=1000
    
    echo "LOCK TABLES \`$table_name\` WRITE;"
    
    for ((i=1; i<=num_rows; i+=batch_size)); do
        local end=$((i + batch_size - 1))
        [ $end -gt $num_rows ] && end=$num_rows
        
        echo -n "INSERT INTO \`$table_name\` (\`col_int\`, \`col_varchar\`, \`col_text\`, \`col_decimal\`) VALUES "
        
        local first=1
        for ((j=i; j<=end; j++)); do
            [ $first -eq 0 ] && echo -n ","
            first=0
            local val=$((RANDOM % 10000))
            echo -n "($val,'value_$j','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore.',$val.$((RANDOM % 100)))"
        done
        echo ";"
    done
    
    echo "UNLOCK TABLES;"
}

generate_postgres_inserts() {
    local table_num=$1
    local num_rows=$2
    local table_name="table_$(printf '%03d' $table_num)"
    
    # Use COPY for PostgreSQL (much faster)
    echo "COPY \"$table_name\" (\"col_int\", \"col_varchar\", \"col_text\", \"col_decimal\") FROM stdin;"
    
    for ((i=1; i<=num_rows; i++)); do
        local val=$((RANDOM % 10000))
        echo -e "$val\tvalue_$i\tLorem ipsum dolor sit amet, consectetur adipiscing elit.\t$val.$((RANDOM % 100))"
    done
    
    echo "\\."
}

generate_sqlite_inserts() {
    local table_num=$1
    local num_rows=$2
    local table_name="table_$(printf '%03d' $table_num)"
    
    echo "BEGIN TRANSACTION;"
    
    for ((i=1; i<=num_rows; i++)); do
        local val=$((RANDOM % 10000))
        echo "INSERT INTO \"$table_name\" (\"col_int\", \"col_varchar\", \"col_text\", \"col_decimal\") VALUES ($val,'value_$i','Lorem ipsum dolor sit amet.',$val.$((RANDOM % 100)));"
    done
    
    echo "COMMIT;"
}

populate_mysql() {
    print_section "Populating MySQL"
    
    local temp_sql=$(mktemp)
    
    echo "Generating schema and data for $NUM_TABLES tables..."
    
    for ((t=1; t<=NUM_TABLES; t++)); do
        local num_rows=$((MIN_ROWS + RANDOM % (MAX_ROWS - MIN_ROWS)))
        echo -ne "  Table $t/$NUM_TABLES ($num_rows rows)...\r"
        
        generate_mysql_schema $t $num_rows >> "$temp_sql"
        generate_mysql_inserts $t $num_rows >> "$temp_sql"
    done
    
    echo ""
    echo "Loading data into MySQL..."
    
    mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" < "$temp_sql"
    
    rm -f "$temp_sql"
    echo -e "${GREEN}✓${NC} MySQL populated"
}

populate_mariadb() {
    print_section "Populating MariaDB"
    
    local temp_sql=$(mktemp)
    
    echo "Generating schema and data for $NUM_TABLES tables..."
    
    for ((t=1; t<=NUM_TABLES; t++)); do
        local num_rows=$((MIN_ROWS + RANDOM % (MAX_ROWS - MIN_ROWS)))
        echo -ne "  Table $t/$NUM_TABLES ($num_rows rows)...\r"
        
        generate_mysql_schema $t $num_rows >> "$temp_sql"
        generate_mysql_inserts $t $num_rows >> "$temp_sql"
    done
    
    echo ""
    echo "Loading data into MariaDB..."
    
    mysql -h "$MARIADB_HOST" -P "$MARIADB_PORT" -u "$MARIADB_USER" -p"$MARIADB_PASS" "$MARIADB_DB" < "$temp_sql"
    
    rm -f "$temp_sql"
    echo -e "${GREEN}✓${NC} MariaDB populated"
}

populate_postgres() {
    print_section "Populating PostgreSQL"
    
    local temp_sql=$(mktemp)
    
    echo "Generating schema and data for $NUM_TABLES tables..."
    
    for ((t=1; t<=NUM_TABLES; t++)); do
        local num_rows=$((MIN_ROWS + RANDOM % (MAX_ROWS - MIN_ROWS)))
        echo -ne "  Table $t/$NUM_TABLES ($num_rows rows)...\r"
        
        generate_postgres_schema $t $num_rows >> "$temp_sql"
        generate_postgres_inserts $t $num_rows >> "$temp_sql"
    done
    
    echo ""
    echo "Loading data into PostgreSQL..."
    
    PGPASSWORD="$POSTGRES_PASS" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$temp_sql" > /dev/null 2>&1
    
    rm -f "$temp_sql"
    echo -e "${GREEN}✓${NC} PostgreSQL populated"
}

generate_sqlite_db() {
    print_section "Generating SQLite database"
    
    local temp_sql=$(mktemp)
    local sqlite_db="$OUTPUT_DIR/sqlite_test.db"
    
    echo "Generating schema and data for $NUM_TABLES tables..."
    
    # Use smaller row counts for SQLite (single-threaded)
    local sqlite_max_rows=$((MAX_ROWS / 10))
    local sqlite_min_rows=$((MIN_ROWS / 10))
    
    for ((t=1; t<=NUM_TABLES; t++)); do
        local num_rows=$((sqlite_min_rows + RANDOM % (sqlite_max_rows - sqlite_min_rows)))
        echo -ne "  Table $t/$NUM_TABLES ($num_rows rows)...\r"
        
        generate_sqlite_schema $t $num_rows >> "$temp_sql"
        generate_sqlite_inserts $t $num_rows >> "$temp_sql"
    done
    
    echo ""
    echo "Loading data into SQLite..."
    
    rm -f "$sqlite_db"
    sqlite3 "$sqlite_db" < "$temp_sql"
    
    rm -f "$temp_sql"
    echo -e "${GREEN}✓${NC} SQLite database created"
}

dump_mysql() {
    print_section "Dumping MySQL (mysqldump)"
    
    local output="$OUTPUT_DIR/mysql_mysqldump.sql"
    
    echo "Running mysqldump (inside container)..."
    docker exec sql-splitter-mysql mysqldump \
        -uroot -p"$MYSQL_PASS" \
        --databases "$MYSQL_DB" \
        --single-transaction \
        --routines \
        --triggers \
        --events \
        > "$output" 2>/dev/null
    
    local size=$(du -h "$output" | cut -f1)
    echo -e "${GREEN}✓${NC} MySQL dump: $output ($size)"
}

dump_mariadb() {
    print_section "Dumping MariaDB (mariadb-dump)"
    
    local output="$OUTPUT_DIR/mariadb_dump.sql"
    
    echo "Running mariadb-dump (inside container)..."
    docker exec sql-splitter-mariadb mariadb-dump \
        -uroot -p"$MARIADB_PASS" \
        --databases "$MARIADB_DB" \
        --single-transaction \
        --routines \
        --triggers \
        --events \
        > "$output" 2>/dev/null
    
    local size=$(du -h "$output" | cut -f1)
    echo -e "${GREEN}✓${NC} MariaDB dump: $output ($size)"
}

dump_postgres() {
    print_section "Dumping PostgreSQL (pg_dump)"
    
    # COPY format (default, faster)
    local output_copy="$OUTPUT_DIR/postgres_pg_dump_copy.sql"
    echo "Running pg_dump (COPY format, inside container)..."
    docker exec -e PGPASSWORD="$POSTGRES_PASS" sql-splitter-postgres \
        pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > "$output_copy"
    local size=$(du -h "$output_copy" | cut -f1)
    echo -e "${GREEN}✓${NC} PostgreSQL dump (COPY): $output_copy ($size)"
    
    # INSERT format (for comparison)
    local output_inserts="$OUTPUT_DIR/postgres_pg_dump_inserts.sql"
    echo "Running pg_dump (INSERT format, inside container)..."
    docker exec -e PGPASSWORD="$POSTGRES_PASS" sql-splitter-postgres \
        pg_dump -U "$POSTGRES_USER" --inserts "$POSTGRES_DB" > "$output_inserts"
    size=$(du -h "$output_inserts" | cut -f1)
    echo -e "${GREEN}✓${NC} PostgreSQL dump (INSERT): $output_inserts ($size)"
}

dump_sqlite() {
    print_section "Dumping SQLite (.dump)"
    
    local sqlite_db="$OUTPUT_DIR/sqlite_test.db"
    local output="$OUTPUT_DIR/sqlite_dump.sql"
    
    if [ ! -f "$sqlite_db" ]; then
        echo -e "${YELLOW}SQLite database not found, skipping${NC}"
        return
    fi
    
    echo "Running sqlite3 .dump..."
    sqlite3 "$sqlite_db" ".dump" > "$output"
    
    local size=$(du -h "$output" | cut -f1)
    echo -e "${GREEN}✓${NC} SQLite dump: $output ($size)"
}

generate_summary() {
    print_section "Generated Files Summary"
    
    echo "| File | Size | Tables | Format |"
    echo "|------|------|--------|--------|"
    
    for f in "$OUTPUT_DIR"/*.sql; do
        if [ -f "$f" ]; then
            local name=$(basename "$f")
            local size=$(du -h "$f" | cut -f1)
            local tables=$NUM_TABLES
            local format=""
            
            case "$name" in
                mysql_*) format="mysqldump" ;;
                mariadb_*) format="mariadb-dump" ;;
                postgres_*copy*) format="pg_dump (COPY)" ;;
                postgres_*insert*) format="pg_dump (INSERT)" ;;
                sqlite_*) format="sqlite3 .dump" ;;
            esac
            
            echo "| $name | $size | $tables | $format |"
        fi
    done
}

main() {
    print_header "Multi-Database Test Dump Generator"
    
    # Check dependencies
    if ! command -v mysql &> /dev/null; then
        echo -e "${YELLOW}mysql client not found. Install with: brew install mysql-client${NC}"
        exit 1
    fi
    
    if ! command -v psql &> /dev/null; then
        echo -e "${YELLOW}psql client not found. Install with: brew install libpq${NC}"
        exit 1
    fi
    
    if ! command -v sqlite3 &> /dev/null; then
        echo -e "${YELLOW}sqlite3 not found. Install with: brew install sqlite3${NC}"
        exit 1
    fi
    
    mkdir -p "$OUTPUT_DIR"
    
    check_docker
    
    # Populate databases
    populate_mysql
    populate_mariadb
    populate_postgres
    generate_sqlite_db
    
    # Generate dumps
    dump_mysql
    dump_mariadb
    dump_postgres
    dump_sqlite
    
    # Summary
    generate_summary
    
    print_header "Done!"
    echo "Generated dumps are in: $OUTPUT_DIR"
    echo ""
    echo "To clean up Docker containers:"
    echo "  docker compose -f docker/docker-compose.databases.yml down -v"
}

main "$@"
