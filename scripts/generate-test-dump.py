#!/usr/bin/env python3
"""
Generate realistic mysqldump-format SQL files for benchmarking.

Creates files that all SQL splitter tools can parse, with proper:
- mysqldump headers and footers
- "Table structure for table" comments
- Varied table structures (different column types, sizes)
- Realistic data patterns
- Mixed INSERT styles (single-row and multi-row)
"""

import argparse
import random
import string
import sys
from datetime import datetime, timedelta


# Table definitions with varied structures
TABLE_DEFINITIONS = [
    {
        "name": "users",
        "columns": [
            ("id", "bigint unsigned NOT NULL AUTO_INCREMENT"),
            ("email", "varchar(255) NOT NULL"),
            ("name", "varchar(100) DEFAULT NULL"),
            ("password_hash", "varchar(255) NOT NULL"),
            ("avatar_url", "varchar(500) DEFAULT NULL"),
            ("bio", "text"),
            ("created_at", "datetime DEFAULT NULL"),
            ("updated_at", "datetime DEFAULT NULL"),
        ],
        "primary_key": "id",
        "insert_batch_size": 50,  # rows per INSERT
    },
    {
        "name": "posts",
        "columns": [
            ("id", "bigint unsigned NOT NULL AUTO_INCREMENT"),
            ("user_id", "bigint unsigned NOT NULL"),
            ("title", "varchar(255) NOT NULL"),
            ("slug", "varchar(300) NOT NULL"),
            ("content", "longtext"),
            ("excerpt", "text"),
            ("status", "enum('draft','published','archived') DEFAULT 'draft'"),
            ("published_at", "datetime DEFAULT NULL"),
            ("created_at", "datetime DEFAULT NULL"),
            ("updated_at", "datetime DEFAULT NULL"),
        ],
        "primary_key": "id",
        "insert_batch_size": 20,
    },
    {
        "name": "comments",
        "columns": [
            ("id", "bigint unsigned NOT NULL AUTO_INCREMENT"),
            ("post_id", "bigint unsigned NOT NULL"),
            ("user_id", "bigint unsigned DEFAULT NULL"),
            ("parent_id", "bigint unsigned DEFAULT NULL"),
            ("body", "text NOT NULL"),
            ("author_name", "varchar(100) DEFAULT NULL"),
            ("author_email", "varchar(255) DEFAULT NULL"),
            ("is_approved", "tinyint(1) DEFAULT '0'"),
            ("created_at", "datetime DEFAULT NULL"),
        ],
        "primary_key": "id",
        "insert_batch_size": 100,
    },
    {
        "name": "products",
        "columns": [
            ("id", "bigint unsigned NOT NULL AUTO_INCREMENT"),
            ("sku", "varchar(50) NOT NULL"),
            ("name", "varchar(255) NOT NULL"),
            ("description", "text"),
            ("price", "decimal(10,2) NOT NULL"),
            ("cost", "decimal(10,2) DEFAULT NULL"),
            ("quantity", "int DEFAULT '0'"),
            ("weight", "decimal(8,3) DEFAULT NULL"),
            ("is_active", "tinyint(1) DEFAULT '1'"),
            ("metadata", "json DEFAULT NULL"),
            ("created_at", "datetime DEFAULT NULL"),
            ("updated_at", "datetime DEFAULT NULL"),
        ],
        "primary_key": "id",
        "insert_batch_size": 30,
    },
    {
        "name": "orders",
        "columns": [
            ("id", "bigint unsigned NOT NULL AUTO_INCREMENT"),
            ("user_id", "bigint unsigned NOT NULL"),
            ("order_number", "varchar(50) NOT NULL"),
            ("status", "enum('pending','processing','shipped','delivered','cancelled') DEFAULT 'pending'"),
            ("subtotal", "decimal(12,2) NOT NULL"),
            ("tax", "decimal(10,2) DEFAULT '0.00'"),
            ("shipping", "decimal(10,2) DEFAULT '0.00'"),
            ("total", "decimal(12,2) NOT NULL"),
            ("currency", "char(3) DEFAULT 'USD'"),
            ("notes", "text"),
            ("shipped_at", "datetime DEFAULT NULL"),
            ("created_at", "datetime DEFAULT NULL"),
            ("updated_at", "datetime DEFAULT NULL"),
        ],
        "primary_key": "id",
        "insert_batch_size": 40,
    },
    {
        "name": "order_items",
        "columns": [
            ("id", "bigint unsigned NOT NULL AUTO_INCREMENT"),
            ("order_id", "bigint unsigned NOT NULL"),
            ("product_id", "bigint unsigned NOT NULL"),
            ("quantity", "int NOT NULL"),
            ("unit_price", "decimal(10,2) NOT NULL"),
            ("total_price", "decimal(12,2) NOT NULL"),
            ("created_at", "datetime DEFAULT NULL"),
        ],
        "primary_key": "id",
        "insert_batch_size": 100,
    },
    {
        "name": "sessions",
        "columns": [
            ("id", "varchar(255) NOT NULL"),
            ("user_id", "bigint unsigned DEFAULT NULL"),
            ("ip_address", "varchar(45) DEFAULT NULL"),
            ("user_agent", "text"),
            ("payload", "longtext NOT NULL"),
            ("last_activity", "int NOT NULL"),
        ],
        "primary_key": "id",
        "insert_batch_size": 50,
    },
    {
        "name": "activity_logs",
        "columns": [
            ("id", "bigint unsigned NOT NULL AUTO_INCREMENT"),
            ("log_name", "varchar(100) DEFAULT NULL"),
            ("description", "text NOT NULL"),
            ("subject_type", "varchar(255) DEFAULT NULL"),
            ("subject_id", "bigint unsigned DEFAULT NULL"),
            ("causer_type", "varchar(255) DEFAULT NULL"),
            ("causer_id", "bigint unsigned DEFAULT NULL"),
            ("properties", "json DEFAULT NULL"),
            ("created_at", "datetime DEFAULT NULL"),
        ],
        "primary_key": "id",
        "insert_batch_size": 80,
    },
    {
        "name": "cache",
        "columns": [
            ("key", "varchar(255) NOT NULL"),
            ("value", "mediumtext NOT NULL"),
            ("expiration", "int NOT NULL"),
        ],
        "primary_key": "key",
        "insert_batch_size": 100,
    },
    {
        "name": "media",
        "columns": [
            ("id", "bigint unsigned NOT NULL AUTO_INCREMENT"),
            ("model_type", "varchar(255) NOT NULL"),
            ("model_id", "bigint unsigned NOT NULL"),
            ("collection_name", "varchar(255) NOT NULL"),
            ("name", "varchar(255) NOT NULL"),
            ("file_name", "varchar(255) NOT NULL"),
            ("mime_type", "varchar(100) DEFAULT NULL"),
            ("disk", "varchar(50) DEFAULT 'public'"),
            ("size", "bigint unsigned DEFAULT NULL"),
            ("manipulations", "json DEFAULT NULL"),
            ("custom_properties", "json DEFAULT NULL"),
            ("responsive_images", "json DEFAULT NULL"),
            ("created_at", "datetime DEFAULT NULL"),
            ("updated_at", "datetime DEFAULT NULL"),
        ],
        "primary_key": "id",
        "insert_batch_size": 30,
    },
]

# Sample data generators
FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
               "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
              "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]
WORDS = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "sed", "do", "eiusmod",
         "tempor", "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua", "enim", "ad", "minim", "veniam"]
DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com", "test.org", "company.io"]


def random_string(length):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


def random_email():
    name = f"{random.choice(FIRST_NAMES).lower()}.{random.choice(LAST_NAMES).lower()}{random.randint(1, 999)}"
    return f"{name}@{random.choice(DOMAINS)}"


def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_text(min_words=10, max_words=50):
    count = random.randint(min_words, max_words)
    return ' '.join(random.choices(WORDS, k=count)).capitalize() + '.'


def random_datetime(start_year=2020):
    start = datetime(start_year, 1, 1)
    end = datetime.now()
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime('%Y-%m-%d %H:%M:%S')


def random_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def escape_sql(s):
    """Escape string for SQL."""
    if s is None:
        return "NULL"
    s = str(s)
    return s.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")


def generate_row_value(table_name, col_name, col_type, row_id):
    """Generate a realistic value for a column."""
    
    # Handle NULLable columns randomly
    if "DEFAULT NULL" in col_type and random.random() < 0.1:
        return None
    
    col_lower = col_name.lower()
    type_lower = col_type.lower()
    
    # ID columns
    if col_name == "id" and "auto_increment" in type_lower:
        return row_id
    
    # Foreign keys and references
    if col_name.endswith("_id"):
        return random.randint(1, max(1, row_id // 2))
    
    # Email
    if "email" in col_lower:
        return random_email()
    
    # Name columns
    if col_lower in ("name", "author_name"):
        return random_name()
    if col_lower == "first_name":
        return random.choice(FIRST_NAMES)
    if col_lower == "last_name":
        return random.choice(LAST_NAMES)
    
    # URL columns
    if "url" in col_lower:
        return f"https://example.com/{random_string(10)}"
    
    # IP address
    if "ip" in col_lower:
        return random_ip()
    
    # Datetime columns
    if "datetime" in type_lower or col_lower.endswith("_at"):
        return random_datetime()
    
    # Boolean/tinyint
    if "tinyint" in type_lower:
        return random.randint(0, 1)
    
    # JSON columns
    if "json" in type_lower:
        return '{"key": "value", "count": ' + str(random.randint(1, 100)) + '}'
    
    # Decimal/price columns
    if "decimal" in type_lower:
        return round(random.uniform(1.0, 999.99), 2)
    
    # Integer columns
    if "int" in type_lower and "bigint" not in type_lower:
        return random.randint(1, 10000)
    if "bigint" in type_lower:
        return random.randint(1, 1000000)
    
    # Enum columns
    if "enum" in type_lower:
        # Extract enum values
        import re
        match = re.search(r"enum\(([^)]+)\)", type_lower)
        if match:
            values = [v.strip("'\"") for v in match.group(1).split(",")]
            return random.choice(values)
    
    # Text columns (long content)
    if "longtext" in type_lower:
        return random_text(100, 500)
    if "mediumtext" in type_lower:
        return random_text(50, 200)
    if "text" in type_lower:
        return random_text(20, 100)
    
    # VARCHAR columns
    if "varchar" in type_lower:
        match = __import__('re').search(r"varchar\((\d+)\)", type_lower)
        max_len = int(match.group(1)) if match else 100
        if col_lower in ("title", "slug"):
            return ' '.join(random.choices(WORDS, k=random.randint(3, 8))).title()[:max_len]
        if col_lower == "sku":
            return f"SKU-{random_string(6).upper()}-{random.randint(1000, 9999)}"
        if col_lower == "order_number":
            return f"ORD-{random.randint(100000, 999999)}"
        if col_lower == "password_hash":
            return f"$2y$10${random_string(50)}"
        if col_lower == "user_agent":
            return f"Mozilla/5.0 (compatible; Bot/{random.randint(1, 10)}.0)"
        return random_string(min(max_len, random.randint(10, 50)))
    
    # CHAR columns
    if "char" in type_lower:
        match = __import__('re').search(r"char\((\d+)\)", type_lower)
        length = int(match.group(1)) if match else 3
        return random_string(length).upper()
    
    return random_string(20)


def write_header(f):
    """Write mysqldump header."""
    f.write(f"""-- MySQL dump 10.13  Distrib 8.0.40, for Linux (x86_64)
--
-- Host: localhost    Database: benchmark
-- ------------------------------------------------------
-- Server version	8.0.40

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

""")


def write_footer(f):
    """Write mysqldump footer."""
    f.write("""
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n")


def write_table_structure(f, table_def):
    """Write CREATE TABLE statement with proper headers."""
    name = table_def["name"]
    columns = table_def["columns"]
    pk = table_def["primary_key"]
    
    f.write(f"""--
-- Table structure for table `{name}`
--

DROP TABLE IF EXISTS `{name}`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `{name}` (
""")
    
    col_defs = []
    for col_name, col_type in columns:
        col_defs.append(f"  `{col_name}` {col_type}")
    col_defs.append(f"  PRIMARY KEY (`{pk}`)")
    
    f.write(",\n".join(col_defs))
    f.write("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n")
    f.write("/*!40101 SET character_set_client = @saved_cs_client */;\n\n")


def write_table_data(f, table_def, num_rows, progress_callback=None):
    """Write INSERT statements for a table."""
    name = table_def["name"]
    columns = table_def["columns"]
    batch_size = table_def["insert_batch_size"]
    
    f.write(f"""--
-- Dumping data for table `{name}`
--

LOCK TABLES `{name}` WRITE;
/*!40000 ALTER TABLE `{name}` DISABLE KEYS */;
""")
    
    col_names = [c[0] for c in columns]
    rows_written = 0
    
    while rows_written < num_rows:
        # Determine batch size for this INSERT
        remaining = num_rows - rows_written
        current_batch = min(batch_size, remaining)
        
        f.write(f"INSERT INTO `{name}` (`{'`,`'.join(col_names)}`) VALUES\n")
        
        row_strs = []
        for i in range(current_batch):
            row_id = rows_written + i + 1
            values = []
            for col_name, col_type in columns:
                val = generate_row_value(name, col_name, col_type, row_id)
                if val is None:
                    values.append("NULL")
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                else:
                    values.append(f"'{escape_sql(val)}'")
            row_strs.append(f"({','.join(values)})")
        
        f.write(",\n".join(row_strs))
        f.write(";\n")
        
        rows_written += current_batch
        if progress_callback:
            progress_callback(rows_written)
    
    f.write(f"/*!40000 ALTER TABLE `{name}` ENABLE KEYS */;\n")
    f.write("UNLOCK TABLES;\n\n")


def generate_dump(output_file, target_size_mb, verbose=True):
    """Generate a mysqldump file of approximately target_size_mb."""
    target_bytes = target_size_mb * 1024 * 1024
    
    # Estimate: ~750 bytes per row on average (more realistic given our varied content)
    rows_per_table = max(100, (target_bytes // len(TABLE_DEFINITIONS)) // 750)
    
    if verbose:
        print(f"Generating {target_size_mb}MB dump file: {output_file}")
        print(f"  Tables: {len(TABLE_DEFINITIONS)}")
        print(f"  Rows per table: ~{rows_per_table:,}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        write_header(f)
        
        for table_def in TABLE_DEFINITIONS:
            write_table_structure(f, table_def)
        
        for i, table_def in enumerate(TABLE_DEFINITIONS):
            if verbose:
                print(f"  [{i+1}/{len(TABLE_DEFINITIONS)}] Generating {table_def['name']}...", end=' ', flush=True)
            
            write_table_data(f, table_def, rows_per_table)
            
            if verbose:
                print("done")
        
        write_footer(f)
    
    actual_size = __import__('os').path.getsize(output_file)
    if verbose:
        print(f"Generated: {actual_size / 1024 / 1024:.1f}MB")
    
    return actual_size


def main():
    parser = argparse.ArgumentParser(
        description='Generate realistic mysqldump-format SQL files for benchmarking'
    )
    parser.add_argument('size', type=int, help='Target size in MB')
    parser.add_argument('-o', '--output', help='Output file path')
    parser.add_argument('-q', '--quiet', action='store_true', help='Quiet mode')
    
    args = parser.parse_args()
    
    output = args.output or f"/tmp/benchmark_{args.size}mb.sql"
    generate_dump(output, args.size, verbose=not args.quiet)
    print(f"Output: {output}")


if __name__ == '__main__':
    main()
