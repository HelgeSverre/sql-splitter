//! Data generator that produces row data for all tables.
//!
//! Generates deterministic, FK-consistent data at various scales.

use crate::fake::FakeData;
// Note: schema module contains the type definitions, but generator uses its own TableData structure
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;

/// Generation scale presets
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Scale {
    /// ~500 total rows, 3 tenants
    Small,
    /// ~10,000 total rows, 10 tenants
    Medium,
    /// ~200,000 total rows, 50 tenants
    Large,
    /// ~1,000,000 total rows, 100 tenants (for memory stress testing)
    XLarge,
}

impl Scale {
    pub fn tenants(&self) -> usize {
        match self {
            Scale::Small => 3,
            Scale::Medium => 10,
            Scale::Large => 50,
            Scale::XLarge => 100,
        }
    }

    pub fn users_per_tenant(&self) -> usize {
        match self {
            Scale::Small => 5,
            Scale::Medium => 50,
            Scale::Large => 200,
            Scale::XLarge => 500,
        }
    }

    pub fn orders_per_tenant(&self) -> usize {
        match self {
            Scale::Small => 10,
            Scale::Medium => 100,
            Scale::Large => 500,
            Scale::XLarge => 1500,
        }
    }

    pub fn products_per_tenant(&self) -> usize {
        match self {
            Scale::Small => 10,
            Scale::Medium => 50,
            Scale::Large => 200,
            Scale::XLarge => 500,
        }
    }

    pub fn categories_per_tenant(&self) -> usize {
        match self {
            Scale::Small => 5,
            Scale::Medium => 15,
            Scale::Large => 30,
            Scale::XLarge => 50,
        }
    }

    pub fn projects_per_tenant(&self) -> usize {
        match self {
            Scale::Small => 3,
            Scale::Medium => 20,
            Scale::Large => 100,
            Scale::XLarge => 200,
        }
    }

    pub fn tasks_per_project(&self) -> usize {
        match self {
            Scale::Small => 5,
            Scale::Medium => 10,
            Scale::Large => 20,
            Scale::XLarge => 30,
        }
    }

    pub fn folders_per_tenant(&self) -> usize {
        match self {
            Scale::Small => 5,
            Scale::Medium => 20,
            Scale::Large => 50,
            Scale::XLarge => 100,
        }
    }

    pub fn comments_per_tenant(&self) -> usize {
        match self {
            Scale::Small => 10,
            Scale::Medium => 50,
            Scale::Large => 200,
            Scale::XLarge => 500,
        }
    }
}

impl std::str::FromStr for Scale {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "small" | "s" => Ok(Scale::Small),
            "medium" | "m" => Ok(Scale::Medium),
            "large" | "l" => Ok(Scale::Large),
            "xlarge" | "xl" | "x" => Ok(Scale::XLarge),
            _ => Err(format!(
                "Unknown scale: {}. Use small, medium, large, or xlarge",
                s
            )),
        }
    }
}

/// SQL value representation
#[derive(Debug, Clone)]
pub enum SqlValue {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

impl SqlValue {
    /// Format for MySQL INSERT statement
    pub fn to_mysql(&self) -> String {
        match self {
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Int(n) => n.to_string(),
            SqlValue::Float(n) => format!("{:.2}", n),
            SqlValue::String(s) => format!("'{}'", escape_mysql_string(s)),
            SqlValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        }
    }

    /// Format for PostgreSQL INSERT/COPY statement
    pub fn to_postgres(&self) -> String {
        match self {
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Int(n) => n.to_string(),
            SqlValue::Float(n) => format!("{:.2}", n),
            SqlValue::String(s) => format!("'{}'", escape_postgres_string(s)),
            SqlValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        }
    }

    /// Format for PostgreSQL COPY (tab-separated)
    pub fn to_postgres_copy(&self) -> String {
        match self {
            SqlValue::Null => "\\N".to_string(),
            SqlValue::Int(n) => n.to_string(),
            SqlValue::Float(n) => format!("{:.2}", n),
            SqlValue::String(s) => escape_postgres_copy(s),
            SqlValue::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        }
    }

    /// Format for SQLite INSERT statement
    pub fn to_sqlite(&self) -> String {
        match self {
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Int(n) => n.to_string(),
            SqlValue::Float(n) => format!("{:.2}", n),
            SqlValue::String(s) => format!("'{}'", escape_sqlite_string(s)),
            SqlValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        }
    }

    /// Format for MSSQL INSERT statement
    pub fn to_mssql(&self) -> String {
        match self {
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Int(n) => n.to_string(),
            SqlValue::Float(n) => format!("{:.2}", n),
            SqlValue::String(s) => format!("N'{}'", escape_mssql_string(s)),
            SqlValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        }
    }
}

fn escape_mssql_string(s: &str) -> String {
    s.replace('\'', "''")
}

fn escape_mysql_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn escape_postgres_string(s: &str) -> String {
    s.replace('\'', "''")
}

fn escape_postgres_copy(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn escape_sqlite_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// A row of generated data
pub type Row = Vec<SqlValue>;

/// Generated data for a single table
#[derive(Debug, Clone)]
pub struct TableData {
    pub table_name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

/// All generated data
#[derive(Debug, Clone)]
pub struct GeneratedData {
    pub tables: Vec<TableData>,
}

/// ID tracking for FK relationships
#[derive(Debug, Default)]
struct IdTracker {
    /// Table name -> list of generated IDs
    ids: HashMap<String, Vec<i64>>,
    /// Table name -> next ID to assign
    next_id: HashMap<String, i64>,
}

impl IdTracker {
    fn new() -> Self {
        Self::default()
    }

    fn next_id(&mut self, table: &str) -> i64 {
        let id = self.next_id.entry(table.to_string()).or_insert(1);
        let current = *id;
        *id += 1;
        self.ids.entry(table.to_string()).or_default().push(current);
        current
    }

    fn get_ids(&self, table: &str) -> &[i64] {
        self.ids.get(table).map(|v| v.as_slice()).unwrap_or(&[])
    }

    #[allow(dead_code)]
    fn get_ids_for_tenant(
        &self,
        table: &str,
        tenant_id: i64,
        tenant_mapping: &HashMap<String, HashMap<i64, Vec<i64>>>,
    ) -> Vec<i64> {
        tenant_mapping
            .get(table)
            .and_then(|m| m.get(&tenant_id))
            .cloned()
            .unwrap_or_default()
    }
}

/// Main data generator
pub struct Generator {
    #[allow(dead_code)]
    rng: ChaCha8Rng,
    scale: Scale,
    fake: FakeData<ChaCha8Rng>,
}

impl Generator {
    pub fn new(seed: u64, scale: Scale) -> Self {
        let rng = ChaCha8Rng::seed_from_u64(seed);
        let fake_rng = ChaCha8Rng::seed_from_u64(seed.wrapping_add(1));
        Self {
            rng,
            scale,
            fake: FakeData::new(fake_rng),
        }
    }

    /// Generate all data for the standard multi-tenant schema
    pub fn generate(&mut self) -> GeneratedData {
        let mut ids = IdTracker::new();
        let mut tenant_mapping: HashMap<String, HashMap<i64, Vec<i64>>> = HashMap::new();

        // Generate global tables first (no tenant association)
        let mut tables = vec![
            self.generate_permissions(&mut ids),
            self.generate_roles_global(&mut ids),
            self.generate_role_permissions(&ids),
            self.generate_currencies(&mut ids),
        ];

        // Generate tenants
        tables.push(self.generate_tenants(&mut ids));
        let tenant_ids: Vec<i64> = ids.get_ids("tenants").to_vec();

        // Generate per-tenant data
        for &tenant_id in &tenant_ids {
            // Users
            let user_data = self.generate_users(&mut ids, tenant_id);
            let user_ids: Vec<i64> = user_data
                .rows
                .iter()
                .map(|r| {
                    if let SqlValue::Int(id) = &r[0] {
                        *id
                    } else {
                        0
                    }
                })
                .collect();
            tenant_mapping
                .entry("users".to_string())
                .or_default()
                .insert(tenant_id, user_ids.clone());
            tables.push(user_data);

            // User roles junction
            tables.push(self.generate_user_roles(&ids, tenant_id, &user_ids));

            // Categories (hierarchical)
            let cat_data = self.generate_categories(&mut ids, tenant_id);
            let cat_ids: Vec<i64> = cat_data
                .rows
                .iter()
                .map(|r| {
                    if let SqlValue::Int(id) = &r[0] {
                        *id
                    } else {
                        0
                    }
                })
                .collect();
            tenant_mapping
                .entry("categories".to_string())
                .or_default()
                .insert(tenant_id, cat_ids.clone());
            tables.push(cat_data);

            // Products
            let prod_data = self.generate_products(&mut ids, tenant_id, &cat_ids);
            let prod_ids: Vec<i64> = prod_data
                .rows
                .iter()
                .map(|r| {
                    if let SqlValue::Int(id) = &r[0] {
                        *id
                    } else {
                        0
                    }
                })
                .collect();
            tenant_mapping
                .entry("products".to_string())
                .or_default()
                .insert(tenant_id, prod_ids.clone());
            tables.push(prod_data);

            // Customers
            let cust_data = self.generate_customers(&mut ids, tenant_id);
            let cust_ids: Vec<i64> = cust_data
                .rows
                .iter()
                .map(|r| {
                    if let SqlValue::Int(id) = &r[0] {
                        *id
                    } else {
                        0
                    }
                })
                .collect();
            tenant_mapping
                .entry("customers".to_string())
                .or_default()
                .insert(tenant_id, cust_ids.clone());
            tables.push(cust_data);

            // Orders
            let order_data = self.generate_orders(&mut ids, tenant_id, &cust_ids);
            let order_ids: Vec<i64> = order_data
                .rows
                .iter()
                .map(|r| {
                    if let SqlValue::Int(id) = &r[0] {
                        *id
                    } else {
                        0
                    }
                })
                .collect();
            tenant_mapping
                .entry("orders".to_string())
                .or_default()
                .insert(tenant_id, order_ids.clone());
            tables.push(order_data);

            // Order items (no tenant_id - FK chain only)
            tables.push(self.generate_order_items(&mut ids, &order_ids, &prod_ids));

            // Projects
            let proj_data = self.generate_projects(&mut ids, tenant_id, &user_ids);
            let proj_ids: Vec<i64> = proj_data
                .rows
                .iter()
                .map(|r| {
                    if let SqlValue::Int(id) = &r[0] {
                        *id
                    } else {
                        0
                    }
                })
                .collect();
            tenant_mapping
                .entry("projects".to_string())
                .or_default()
                .insert(tenant_id, proj_ids.clone());
            tables.push(proj_data);

            // Tasks
            let task_data = self.generate_tasks(&mut ids, tenant_id, &proj_ids, &user_ids);
            let task_ids: Vec<i64> = task_data
                .rows
                .iter()
                .map(|r| {
                    if let SqlValue::Int(id) = &r[0] {
                        *id
                    } else {
                        0
                    }
                })
                .collect();
            tenant_mapping
                .entry("tasks".to_string())
                .or_default()
                .insert(tenant_id, task_ids.clone());
            tables.push(task_data);

            // Folders (self-referential)
            tables.push(self.generate_folders(&mut ids, tenant_id));

            // Comments (self-referential + polymorphic)
            tables
                .push(self.generate_comments(&mut ids, tenant_id, &user_ids, &task_ids, &proj_ids));
        }

        // Consolidate tables with same name (from different tenants)
        let consolidated = self.consolidate_tables(tables);

        GeneratedData {
            tables: consolidated,
        }
    }

    fn consolidate_tables(&self, tables: Vec<TableData>) -> Vec<TableData> {
        let mut result: HashMap<String, TableData> = HashMap::new();

        for table in tables {
            result
                .entry(table.table_name.clone())
                .and_modify(|existing| {
                    existing.rows.extend(table.rows.clone());
                })
                .or_insert(table);
        }

        // Return in a stable order
        let order = [
            "permissions",
            "roles",
            "role_permissions",
            "currencies",
            "tenants",
            "users",
            "user_roles",
            "categories",
            "products",
            "customers",
            "orders",
            "order_items",
            "projects",
            "tasks",
            "folders",
            "comments",
        ];

        order
            .iter()
            .filter_map(|name| result.remove(*name))
            .collect()
    }

    fn generate_permissions(&mut self, ids: &mut IdTracker) -> TableData {
        let perms = FakeData::<ChaCha8Rng>::all_permissions();
        let rows: Vec<Row> = perms
            .iter()
            .map(|name| {
                let id = ids.next_id("permissions");
                vec![
                    SqlValue::Int(id),
                    SqlValue::String(name.to_string()),
                    SqlValue::String(format!("Permission to {}", name.replace('.', " "))),
                ]
            })
            .collect();

        TableData {
            table_name: "permissions".to_string(),
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "description".to_string(),
            ],
            rows,
        }
    }

    fn generate_roles_global(&mut self, ids: &mut IdTracker) -> TableData {
        let roles = FakeData::<ChaCha8Rng>::all_roles();
        let rows: Vec<Row> = roles
            .iter()
            .map(|name| {
                let id = ids.next_id("roles");
                let created = self.fake.datetime(2020, 2024);
                vec![
                    SqlValue::Int(id),
                    SqlValue::Null, // tenant_id is NULL for global roles
                    SqlValue::String(name.to_string()),
                    SqlValue::Bool(true), // is_system
                    SqlValue::String(created.clone()),
                    SqlValue::String(created),
                ]
            })
            .collect();

        TableData {
            table_name: "roles".to_string(),
            columns: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "name".to_string(),
                "is_system".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ],
            rows,
        }
    }

    fn generate_role_permissions(&mut self, ids: &IdTracker) -> TableData {
        let role_ids = ids.get_ids("roles");
        let perm_ids = ids.get_ids("permissions");

        let mut rows = Vec::new();
        // Admin role gets all permissions
        if let Some(&admin_role) = role_ids.first() {
            for &perm_id in perm_ids {
                rows.push(vec![SqlValue::Int(admin_role), SqlValue::Int(perm_id)]);
            }
        }
        // Other roles get subset of permissions
        for &role_id in role_ids.iter().skip(1) {
            let count = self.fake.int_range(2, 5) as usize;
            for &perm_id in perm_ids.iter().take(count.min(perm_ids.len())) {
                rows.push(vec![SqlValue::Int(role_id), SqlValue::Int(perm_id)]);
            }
        }

        TableData {
            table_name: "role_permissions".to_string(),
            columns: vec!["role_id".to_string(), "permission_id".to_string()],
            rows,
        }
    }

    fn generate_currencies(&mut self, ids: &mut IdTracker) -> TableData {
        let currencies = [
            ("USD", "US Dollar", "$"),
            ("EUR", "Euro", "€"),
            ("GBP", "British Pound", "£"),
            ("JPY", "Japanese Yen", "¥"),
            ("CAD", "Canadian Dollar", "C$"),
        ];

        let rows: Vec<Row> = currencies
            .iter()
            .map(|(code, name, symbol)| {
                let id = ids.next_id("currencies");
                vec![
                    SqlValue::Int(id),
                    SqlValue::String(code.to_string()),
                    SqlValue::String(name.to_string()),
                    SqlValue::String(symbol.to_string()),
                ]
            })
            .collect();

        TableData {
            table_name: "currencies".to_string(),
            columns: vec![
                "id".to_string(),
                "code".to_string(),
                "name".to_string(),
                "symbol".to_string(),
            ],
            rows,
        }
    }

    fn generate_tenants(&mut self, ids: &mut IdTracker) -> TableData {
        let count = self.scale.tenants();
        let rows: Vec<Row> = (0..count)
            .map(|_| {
                let id = ids.next_id("tenants");
                let name = self.fake.company_name();
                let slug = self.fake.slug(&name);
                let created = self.fake.datetime(2020, 2024);
                vec![
                    SqlValue::Int(id),
                    SqlValue::String(name),
                    SqlValue::String(slug),
                    SqlValue::String(created.clone()),
                    SqlValue::String(created),
                ]
            })
            .collect();

        TableData {
            table_name: "tenants".to_string(),
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "slug".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ],
            rows,
        }
    }

    fn generate_users(&mut self, ids: &mut IdTracker, tenant_id: i64) -> TableData {
        let count = self.scale.users_per_tenant();
        let rows: Vec<Row> = (0..count)
            .map(|_| {
                let id = ids.next_id("users");
                let first = self.fake.first_name();
                let last = self.fake.last_name();
                let email = self.fake.email(first, last, "example.com");
                let name = format!("{} {}", first, last);
                let role = self.fake.role();
                let active = self.fake.bool_with_probability(0.9);
                let created = self.fake.datetime(2020, 2024);
                let deleted = if self.fake.bool_with_probability(0.05) {
                    SqlValue::String(self.fake.datetime(2023, 2024))
                } else {
                    SqlValue::Null
                };

                vec![
                    SqlValue::Int(id),
                    SqlValue::Int(tenant_id),
                    SqlValue::String(email),
                    SqlValue::String(name),
                    SqlValue::String(role.to_string()),
                    SqlValue::Bool(active),
                    SqlValue::String(created.clone()),
                    SqlValue::String(created),
                    deleted,
                ]
            })
            .collect();

        TableData {
            table_name: "users".to_string(),
            columns: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "email".to_string(),
                "name".to_string(),
                "role".to_string(),
                "active".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
                "deleted_at".to_string(),
            ],
            rows,
        }
    }

    fn generate_user_roles(
        &mut self,
        ids: &IdTracker,
        _tenant_id: i64,
        user_ids: &[i64],
    ) -> TableData {
        let role_ids = ids.get_ids("roles");
        let mut rows = Vec::new();

        for &user_id in user_ids {
            // Each user gets 1-3 roles
            let role_count = self.fake.int_range(1, 3) as usize;
            for &role_id in role_ids.iter().take(role_count) {
                rows.push(vec![SqlValue::Int(user_id), SqlValue::Int(role_id)]);
            }
        }

        TableData {
            table_name: "user_roles".to_string(),
            columns: vec!["user_id".to_string(), "role_id".to_string()],
            rows,
        }
    }

    fn generate_categories(&mut self, ids: &mut IdTracker, tenant_id: i64) -> TableData {
        let count = self.scale.categories_per_tenant();
        let mut rows = Vec::new();
        let mut category_ids = Vec::new();

        for i in 0..count {
            let id = ids.next_id("categories");
            category_ids.push(id);

            // First few categories are root (no parent)
            let parent_id = if i < 3 || category_ids.len() <= 1 {
                SqlValue::Null
            } else {
                // Pick a random earlier category as parent
                let parent_idx = self.fake.int_range(0, (category_ids.len() - 2) as i64) as usize;
                SqlValue::Int(category_ids[parent_idx])
            };

            let level = if matches!(parent_id, SqlValue::Null) {
                0
            } else {
                1
            };
            let name = self.fake.category();
            let created = self.fake.datetime(2020, 2024);

            rows.push(vec![
                SqlValue::Int(id),
                SqlValue::Int(tenant_id),
                parent_id,
                SqlValue::String(name.to_string()),
                SqlValue::Int(level),
                SqlValue::String(created.clone()),
                SqlValue::String(created),
            ]);
        }

        TableData {
            table_name: "categories".to_string(),
            columns: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "parent_id".to_string(),
                "name".to_string(),
                "level".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ],
            rows,
        }
    }

    fn generate_products(
        &mut self,
        ids: &mut IdTracker,
        tenant_id: i64,
        category_ids: &[i64],
    ) -> TableData {
        let count = self.scale.products_per_tenant();
        let rows: Vec<Row> = (0..count)
            .map(|_| {
                let id = ids.next_id("products");
                let cat_id = if category_ids.is_empty() {
                    SqlValue::Null
                } else {
                    SqlValue::Int(self.fake.pick_id(category_ids))
                };
                let name = self.fake.product_name();
                let sku = self.fake.sku();
                let price = self.fake.price(5.0, 500.0);
                let active = self.fake.bool_with_probability(0.85);
                let created = self.fake.datetime(2020, 2024);

                vec![
                    SqlValue::Int(id),
                    SqlValue::Int(tenant_id),
                    cat_id,
                    SqlValue::String(sku),
                    SqlValue::String(name),
                    SqlValue::Float(price),
                    SqlValue::Bool(active),
                    SqlValue::String(created.clone()),
                    SqlValue::String(created),
                ]
            })
            .collect();

        TableData {
            table_name: "products".to_string(),
            columns: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "category_id".to_string(),
                "sku".to_string(),
                "name".to_string(),
                "price".to_string(),
                "active".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ],
            rows,
        }
    }

    fn generate_customers(&mut self, ids: &mut IdTracker, tenant_id: i64) -> TableData {
        let count = self.scale.orders_per_tenant() / 2; // Fewer customers than orders
        let rows: Vec<Row> = (0..count.max(5))
            .map(|_| {
                let id = ids.next_id("customers");
                let first = self.fake.first_name();
                let last = self.fake.last_name();
                let name = format!("{} {}", first, last);
                let email = self.fake.email(first, last, "customer.example.com");
                let phone = self.fake.phone();
                let created = self.fake.datetime(2020, 2024);

                vec![
                    SqlValue::Int(id),
                    SqlValue::Int(tenant_id),
                    SqlValue::String(name),
                    SqlValue::String(email),
                    SqlValue::String(phone),
                    SqlValue::String(created.clone()),
                    SqlValue::String(created),
                ]
            })
            .collect();

        TableData {
            table_name: "customers".to_string(),
            columns: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "name".to_string(),
                "email".to_string(),
                "phone".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ],
            rows,
        }
    }

    fn generate_orders(
        &mut self,
        ids: &mut IdTracker,
        tenant_id: i64,
        customer_ids: &[i64],
    ) -> TableData {
        let count = self.scale.orders_per_tenant();
        let rows: Vec<Row> = (0..count)
            .map(|_| {
                let id = ids.next_id("orders");
                let cust_id = if customer_ids.is_empty() {
                    SqlValue::Null
                } else {
                    SqlValue::Int(self.fake.pick_id(customer_ids))
                };
                let order_num = self.fake.order_number();
                let status = self.fake.order_status();
                let total = self.fake.price(20.0, 2000.0);
                let created = self.fake.datetime(2023, 2024);

                vec![
                    SqlValue::Int(id),
                    SqlValue::Int(tenant_id),
                    cust_id,
                    SqlValue::String(order_num),
                    SqlValue::String(status.to_string()),
                    SqlValue::Float(total),
                    SqlValue::String(created.clone()),
                    SqlValue::String(created),
                ]
            })
            .collect();

        TableData {
            table_name: "orders".to_string(),
            columns: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "customer_id".to_string(),
                "order_number".to_string(),
                "status".to_string(),
                "total".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ],
            rows,
        }
    }

    fn generate_order_items(
        &mut self,
        ids: &mut IdTracker,
        order_ids: &[i64],
        product_ids: &[i64],
    ) -> TableData {
        let mut rows = Vec::new();

        for &order_id in order_ids {
            // Each order has 1-5 items
            let item_count = self.fake.int_range(1, 5) as usize;
            for _ in 0..item_count {
                let id = ids.next_id("order_items");
                let prod_id = if product_ids.is_empty() {
                    continue;
                } else {
                    self.fake.pick_id(product_ids)
                };
                let qty = self.fake.int_range(1, 10);
                let price = self.fake.price(5.0, 200.0);

                rows.push(vec![
                    SqlValue::Int(id),
                    SqlValue::Int(order_id),
                    SqlValue::Int(prod_id),
                    SqlValue::Int(qty),
                    SqlValue::Float(price),
                ]);
            }
        }

        TableData {
            table_name: "order_items".to_string(),
            columns: vec![
                "id".to_string(),
                "order_id".to_string(),
                "product_id".to_string(),
                "quantity".to_string(),
                "unit_price".to_string(),
            ],
            rows,
        }
    }

    fn generate_projects(
        &mut self,
        ids: &mut IdTracker,
        tenant_id: i64,
        user_ids: &[i64],
    ) -> TableData {
        let count = self.scale.projects_per_tenant();
        let rows: Vec<Row> = (0..count)
            .map(|_| {
                let id = ids.next_id("projects");
                let owner_id = if user_ids.is_empty() {
                    SqlValue::Null
                } else {
                    SqlValue::Int(self.fake.pick_id(user_ids))
                };
                let name = format!("Project {}", self.fake.product_name());
                let status = self.fake.project_status();
                let created = self.fake.datetime(2022, 2024);

                vec![
                    SqlValue::Int(id),
                    SqlValue::Int(tenant_id),
                    owner_id,
                    SqlValue::String(name),
                    SqlValue::String(status.to_string()),
                    SqlValue::String(created.clone()),
                    SqlValue::String(created),
                ]
            })
            .collect();

        TableData {
            table_name: "projects".to_string(),
            columns: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "owner_id".to_string(),
                "name".to_string(),
                "status".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ],
            rows,
        }
    }

    fn generate_tasks(
        &mut self,
        ids: &mut IdTracker,
        tenant_id: i64,
        project_ids: &[i64],
        user_ids: &[i64],
    ) -> TableData {
        let mut rows = Vec::new();

        for &project_id in project_ids {
            let task_count = self.scale.tasks_per_project();
            for _ in 0..task_count {
                let id = ids.next_id("tasks");
                let assignee = if user_ids.is_empty() || self.fake.bool_with_probability(0.2) {
                    SqlValue::Null
                } else {
                    SqlValue::Int(self.fake.pick_id(user_ids))
                };
                let title = self.fake.sentence(4);
                let priority = self.fake.task_priority();
                let completed = self.fake.bool_with_probability(0.3);
                let created = self.fake.datetime(2023, 2024);

                rows.push(vec![
                    SqlValue::Int(id),
                    SqlValue::Int(tenant_id),
                    SqlValue::Int(project_id),
                    assignee,
                    SqlValue::String(title),
                    SqlValue::Int(priority as i64),
                    SqlValue::Bool(completed),
                    SqlValue::String(created.clone()),
                    SqlValue::String(created),
                ]);
            }
        }

        TableData {
            table_name: "tasks".to_string(),
            columns: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "project_id".to_string(),
                "assignee_id".to_string(),
                "title".to_string(),
                "priority".to_string(),
                "completed".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ],
            rows,
        }
    }

    fn generate_folders(&mut self, ids: &mut IdTracker, tenant_id: i64) -> TableData {
        let count = self.scale.folders_per_tenant();
        let mut rows = Vec::new();
        let mut folder_ids = Vec::new();

        for i in 0..count {
            let id = ids.next_id("folders");
            folder_ids.push(id);

            // First folders are root
            let parent_id = if i < 2 || folder_ids.len() <= 1 {
                SqlValue::Null
            } else {
                let parent_idx = self.fake.int_range(0, (folder_ids.len() - 2) as i64) as usize;
                SqlValue::Int(folder_ids[parent_idx])
            };

            let name = self.fake.path_segment();
            let path = format!("/{}", name);
            let created = self.fake.datetime(2022, 2024);

            rows.push(vec![
                SqlValue::Int(id),
                SqlValue::Int(tenant_id),
                parent_id,
                SqlValue::String(name),
                SqlValue::String(path),
                SqlValue::String(created.clone()),
                SqlValue::String(created),
            ]);
        }

        TableData {
            table_name: "folders".to_string(),
            columns: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "parent_id".to_string(),
                "name".to_string(),
                "path".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ],
            rows,
        }
    }

    fn generate_comments(
        &mut self,
        ids: &mut IdTracker,
        tenant_id: i64,
        user_ids: &[i64],
        task_ids: &[i64],
        project_ids: &[i64],
    ) -> TableData {
        let count = self.scale.comments_per_tenant();
        let mut rows = Vec::new();
        let mut comment_ids = Vec::new();

        for i in 0..count {
            let id = ids.next_id("comments");
            comment_ids.push(id);

            // Self-referential parent
            let parent_id =
                if i < 3 || comment_ids.len() <= 1 || self.fake.bool_with_probability(0.6) {
                    SqlValue::Null
                } else {
                    let parent_idx =
                        self.fake.int_range(0, (comment_ids.len() - 2) as i64) as usize;
                    SqlValue::Int(comment_ids[parent_idx])
                };

            let user_id = if user_ids.is_empty() {
                SqlValue::Null
            } else {
                SqlValue::Int(self.fake.pick_id(user_ids))
            };

            // Polymorphic: comment on task or project
            let (commentable_type, commentable_id) =
                if self.fake.bool_with_probability(0.7) && !task_ids.is_empty() {
                    ("task", self.fake.pick_id(task_ids))
                } else if !project_ids.is_empty() {
                    ("project", self.fake.pick_id(project_ids))
                } else {
                    continue;
                };

            let body = self.fake.paragraph(2);
            let created = self.fake.datetime(2023, 2024);

            rows.push(vec![
                SqlValue::Int(id),
                SqlValue::Int(tenant_id),
                parent_id,
                user_id,
                SqlValue::String(commentable_type.to_string()),
                SqlValue::Int(commentable_id),
                SqlValue::String(body),
                SqlValue::String(created.clone()),
                SqlValue::String(created),
            ]);
        }

        TableData {
            table_name: "comments".to_string(),
            columns: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "parent_id".to_string(),
                "user_id".to_string(),
                "commentable_type".to_string(),
                "commentable_id".to_string(),
                "body".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ],
            rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generator_deterministic() {
        let mut gen1 = Generator::new(42, Scale::Small);
        let mut gen2 = Generator::new(42, Scale::Small);

        let data1 = gen1.generate();
        let data2 = gen2.generate();

        assert_eq!(data1.tables.len(), data2.tables.len());
        for (t1, t2) in data1.tables.iter().zip(data2.tables.iter()) {
            assert_eq!(t1.table_name, t2.table_name);
            assert_eq!(t1.rows.len(), t2.rows.len());
        }
    }

    #[test]
    fn test_scale_small() {
        let mut gen = Generator::new(42, Scale::Small);
        let data = gen.generate();

        // Check we have all expected tables
        let table_names: Vec<&str> = data.tables.iter().map(|t| t.table_name.as_str()).collect();
        assert!(table_names.contains(&"tenants"));
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"order_items"));

        // Check tenant count
        let tenants = data
            .tables
            .iter()
            .find(|t| t.table_name == "tenants")
            .unwrap();
        assert_eq!(tenants.rows.len(), Scale::Small.tenants());
    }

    #[test]
    fn test_fk_consistency() {
        let mut gen = Generator::new(42, Scale::Small);
        let data = gen.generate();

        // Get all order IDs
        let orders = data
            .tables
            .iter()
            .find(|t| t.table_name == "orders")
            .unwrap();
        let order_ids: Vec<i64> = orders
            .rows
            .iter()
            .map(|r| {
                if let SqlValue::Int(id) = &r[0] {
                    *id
                } else {
                    panic!()
                }
            })
            .collect();

        // Check all order_items reference valid orders
        let order_items = data
            .tables
            .iter()
            .find(|t| t.table_name == "order_items")
            .unwrap();
        for row in &order_items.rows {
            if let SqlValue::Int(order_id) = &row[1] {
                assert!(
                    order_ids.contains(order_id),
                    "Order item references non-existent order"
                );
            }
        }
    }
}
