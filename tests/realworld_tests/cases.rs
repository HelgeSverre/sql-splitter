//! Test case definitions for real-world SQL dumps.
//!
//! Each test case references a public SQL dump file that can be downloaded
//! and used to verify sql-splitter functionality.

/// A test case definition
#[derive(Debug, Clone)]
pub struct TestCase {
    /// Unique name for the test case
    pub name: &'static str,
    /// Expected SQL dialect: mysql, postgres, sqlite
    pub dialect: &'static str,
    /// URL to download the SQL file or archive
    pub url: &'static str,
    /// Command to extract archive (None if direct SQL file)
    pub unzip_cmd: Option<&'static str>,
    /// Path to SQL file within archive (or filename for direct downloads)
    pub sql_file: &'static str,
    /// Description of the test case
    pub notes: &'static str,
}

impl TestCase {
    pub const fn new(
        name: &'static str,
        dialect: &'static str,
        url: &'static str,
        unzip_cmd: Option<&'static str>,
        sql_file: &'static str,
        notes: &'static str,
    ) -> Self {
        Self {
            name,
            dialect,
            url,
            unzip_cmd,
            sql_file,
            notes,
        }
    }

    /// Returns true if this is a MySQL test case
    pub fn is_mysql(&self) -> bool {
        self.dialect == "mysql"
    }

    /// Returns true if this is a PostgreSQL test case
    pub fn is_postgres(&self) -> bool {
        self.dialect == "postgres"
    }

    /// Returns true if this is a SQLite test case
    pub fn is_sqlite(&self) -> bool {
        self.dialect == "sqlite"
    }
}

/// All test cases from the original bash script
pub static TEST_CASES: &[TestCase] = &[
    // MySQL/MariaDB dumps
    TestCase::new(
        "mysql-classicmodels",
        "mysql",
        "https://www.mysqltutorial.org/wp-content/uploads/2023/10/mysqlsampledatabase.zip",
        Some("unzip -o"),
        "mysqlsampledatabase.sql",
        "MySQL Tutorial sample DB",
    ),
    TestCase::new(
        "mysql-sakila-schema",
        "mysql",
        "https://downloads.mysql.com/docs/sakila-db.zip",
        Some("unzip -o"),
        "sakila-db/sakila-schema.sql",
        "Official MySQL Sakila schema",
    ),
    TestCase::new(
        "mysql-sakila-data",
        "mysql",
        "https://downloads.mysql.com/docs/sakila-db.zip",
        Some("unzip -o"),
        "sakila-db/sakila-data.sql",
        "Official MySQL Sakila data",
    ),
    TestCase::new(
        "mysql-employees",
        "mysql",
        "https://github.com/datacharmer/test_db/raw/master/employees.sql",
        None,
        "employees.sql",
        "MySQL Employees test DB",
    ),
    TestCase::new(
        "mysql-world",
        "mysql",
        "https://downloads.mysql.com/docs/world-db.zip",
        Some("unzip -o"),
        "world-db/world.sql",
        "Official MySQL World DB",
    ),
    // PostgreSQL dumps
    TestCase::new(
        "postgres-pagila-schema",
        "postgres",
        "https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql",
        None,
        "pagila-schema.sql",
        "Pagila PostgreSQL port of Sakila",
    ),
    TestCase::new(
        "postgres-pagila-data",
        "postgres",
        "https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql",
        None,
        "pagila-data.sql",
        "Pagila data with COPY statements",
    ),
    TestCase::new(
        "postgres-airlines-small",
        "postgres",
        "https://edu.postgrespro.com/demo-small-en.zip",
        Some("unzip -o"),
        "demo-small-en-20170815.sql",
        "PostgresPro Airlines demo (small)",
    ),
    TestCase::new(
        "postgres-northwind",
        "postgres",
        "https://raw.githubusercontent.com/pthom/northwind_psql/master/northwind.sql",
        None,
        "northwind.sql",
        "Northwind PostgreSQL port",
    ),
    TestCase::new(
        "postgres-periodic",
        "postgres",
        "https://raw.githubusercontent.com/sdrahmath/PeriodicTableDatabase/main/periodic_table.sql",
        None,
        "periodic_table.sql",
        "Periodic table elements DB",
    ),
    TestCase::new(
        "postgres-ecommerce",
        "postgres",
        "https://raw.githubusercontent.com/larbisahli/e-commerce-database-schema/main/init.sql",
        None,
        "init.sql",
        "E-commerce schema with UUIDs",
    ),
    TestCase::new(
        "postgres-sakila-schema",
        "postgres",
        "https://raw.githubusercontent.com/jOOQ/sakila/main/postgres-sakila-db/postgres-sakila-schema.sql",
        None,
        "postgres-sakila-schema.sql",
        "jOOQ Sakila PostgreSQL schema",
    ),
    TestCase::new(
        "postgres-sakila-data",
        "postgres",
        "https://raw.githubusercontent.com/jOOQ/sakila/main/postgres-sakila-db/postgres-sakila-insert-data.sql",
        None,
        "postgres-sakila-insert-data.sql",
        "jOOQ Sakila PostgreSQL data",
    ),
    TestCase::new(
        "postgres-adventureworks",
        "postgres",
        "https://raw.githubusercontent.com/morenoh149/postgresDBSamples/master/adventureworks/install.sql",
        None,
        "install.sql",
        "AdventureWorks PostgreSQL port",
    ),
    // Chinook database - multi-dialect
    TestCase::new(
        "chinook-postgres",
        "postgres",
        "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql",
        None,
        "Chinook_PostgreSql.sql",
        "Chinook DB PostgreSQL version",
    ),
    TestCase::new(
        "chinook-sqlite",
        "sqlite",
        "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sql",
        None,
        "Chinook_Sqlite.sql",
        "Chinook DB SQLite version",
    ),
    TestCase::new(
        "chinook-mysql",
        "mysql",
        "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_MySql.sql",
        None,
        "Chinook_MySql.sql",
        "Chinook DB MySQL version",
    ),
    // WordPress database dumps
    TestCase::new(
        "wordpress-films",
        "mysql",
        "https://raw.githubusercontent.com/chamathis/WordPress-Test/master/wp_films.sql",
        None,
        "wp_films.sql",
        "WordPress Films site (2017)",
    ),
    // Northwind MySQL port
    TestCase::new(
        "mysql-northwind-data",
        "mysql",
        "https://raw.githubusercontent.com/dalers/mywind/master/northwind-data.sql",
        None,
        "northwind-data.sql",
        "Northwind MySQL data",
    ),
    // Geographic/reference data
    TestCase::new(
        "mysql-countries",
        "mysql",
        "https://gist.githubusercontent.com/adhipg/1600028/raw/countries.sql",
        None,
        "countries.sql",
        "Countries with phone codes",
    ),
    TestCase::new(
        "mysql-wilayah",
        "mysql",
        "https://raw.githubusercontent.com/cahyadsn/wilayah/master/db/wilayah.sql",
        None,
        "wilayah.sql",
        "Indonesian administrative regions (large)",
    ),
    // Tutorial databases
    TestCase::new(
        "mysql-coffeeshop",
        "mysql",
        "https://raw.githubusercontent.com/mochen862/full-sql-database-course/main/create_insert.sql",
        None,
        "create_insert.sql",
        "Coffee shop tutorial DB",
    ),
    // WordPress WooCommerce
    TestCase::new(
        "wordpress-woocommerce",
        "mysql",
        "https://raw.githubusercontent.com/GoldenOwlAsia/wordpress-woocommerce-demo/master/demowordpress.sql",
        None,
        "demowordpress.sql",
        "WooCommerce demo with products/orders",
    ),
    TestCase::new(
        "wordpress-woo-replica",
        "mysql",
        "https://raw.githubusercontent.com/GoldenOwlAsia/wordpress-woocommerce-demo/master/demowordpress_replica.sql",
        None,
        "demowordpress_replica.sql",
        "WooCommerce replica DB",
    ),
    TestCase::new(
        "wordpress-plugin-test",
        "mysql",
        "https://raw.githubusercontent.com/WPBP/WordPress-Plugin-Boilerplate-Powered/master/plugin-name/tests/_data/dump.sql",
        None,
        "dump.sql",
        "WordPress plugin test fixture",
    ),
];

/// Get a test case by name
pub fn get_case(name: &str) -> Option<&'static TestCase> {
    TEST_CASES.iter().find(|c| c.name == name)
}

/// Get all MySQL test cases
#[allow(dead_code)]
pub fn mysql_cases() -> impl Iterator<Item = &'static TestCase> {
    TEST_CASES.iter().filter(|c| c.is_mysql())
}

/// Get all PostgreSQL test cases
#[allow(dead_code)]
pub fn postgres_cases() -> impl Iterator<Item = &'static TestCase> {
    TEST_CASES.iter().filter(|c| c.is_postgres())
}

/// Get all SQLite test cases
#[allow(dead_code)]
pub fn sqlite_cases() -> impl Iterator<Item = &'static TestCase> {
    TEST_CASES.iter().filter(|c| c.is_sqlite())
}
