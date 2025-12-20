//! Unit tests for schema module, extracted from inline tests.

use sql_splitter::schema::{
    Column, ColumnId, ColumnType, ForeignKey, Schema, SchemaBuilder, SchemaGraph, TableId,
    TableSchema,
};

mod mod_tests {
    use super::*;

    #[test]
    fn test_column_type_parsing() {
        assert_eq!(ColumnType::from_mysql_type("INT"), ColumnType::Int);
        assert_eq!(ColumnType::from_mysql_type("int(11)"), ColumnType::Int);
        assert_eq!(ColumnType::from_mysql_type("BIGINT"), ColumnType::BigInt);
        assert_eq!(
            ColumnType::from_mysql_type("VARCHAR(255)"),
            ColumnType::Text
        );
        assert_eq!(ColumnType::from_mysql_type("TEXT"), ColumnType::Text);
        assert_eq!(
            ColumnType::from_mysql_type("DATETIME"),
            ColumnType::DateTime
        );
        assert_eq!(
            ColumnType::from_mysql_type("DECIMAL(10,2)"),
            ColumnType::Decimal
        );
    }

    #[test]
    fn test_schema_table_lookup() {
        let mut schema = Schema::new();
        let table = TableSchema::new("users".to_string(), TableId(0));
        let id = schema.add_table(table);

        assert_eq!(schema.get_table_id("users"), Some(id));
        assert_eq!(schema.get_table_id("USERS"), Some(id)); // case-insensitive
        assert_eq!(schema.get_table_id("nonexistent"), None);
    }

    #[test]
    fn test_table_schema_column_lookup() {
        let mut table = TableSchema::new("users".to_string(), TableId(0));
        table.columns.push(Column {
            name: "id".to_string(),
            col_type: ColumnType::Int,
            ordinal: ColumnId(0),
            is_primary_key: true,
            is_nullable: false,
        });
        table.columns.push(Column {
            name: "email".to_string(),
            col_type: ColumnType::Text,
            ordinal: ColumnId(1),
            is_primary_key: false,
            is_nullable: true,
        });
        table.primary_key = vec![ColumnId(0)];

        assert!(table.get_column("id").is_some());
        assert!(table.get_column("ID").is_some()); // case-insensitive
        assert_eq!(table.get_column_id("email"), Some(ColumnId(1)));
        assert!(table.is_pk_column(ColumnId(0)));
        assert!(!table.is_pk_column(ColumnId(1)));
    }
}

mod ddl_tests {
    use super::*;
    use sql_splitter::schema::{extract_alter_table_name, extract_create_table_name};

    #[test]
    fn test_extract_create_table_name() {
        assert_eq!(
            extract_create_table_name("CREATE TABLE users (id INT);"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_create_table_name("CREATE TABLE `my_table` (id INT);"),
            Some("my_table".to_string())
        );
        assert_eq!(
            extract_create_table_name("CREATE TABLE IF NOT EXISTS `users` (id INT);"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_extract_alter_table_name() {
        assert_eq!(
            extract_alter_table_name("ALTER TABLE users ADD COLUMN email VARCHAR(255);"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_alter_table_name("ALTER TABLE `orders` ADD CONSTRAINT ..."),
            Some("orders".to_string())
        );
    }

    #[test]
    fn test_parse_create_table_simple() {
        let mut builder = SchemaBuilder::new();
        let stmt = r#"CREATE TABLE `users` (
            `id` int NOT NULL AUTO_INCREMENT,
            `email` varchar(255) DEFAULT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB;"#;

        let id = builder.parse_create_table(stmt);
        assert!(id.is_some());

        let schema = builder.build();
        let table = schema.table(id.unwrap()).unwrap();

        assert_eq!(table.name, "users");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.columns[0].col_type, ColumnType::Int);
        assert!(table.columns[0].is_primary_key);
        assert!(!table.columns[0].is_nullable);
        assert_eq!(table.columns[1].name, "email");
        assert!(table.columns[1].is_nullable);
        assert_eq!(table.primary_key.len(), 1);
    }

    #[test]
    fn test_parse_create_table_with_fk() {
        let mut builder = SchemaBuilder::new();

        // First create the referenced table
        builder.parse_create_table("CREATE TABLE `companies` (`id` int PRIMARY KEY);");

        let stmt = r#"CREATE TABLE `users` (
            `id` int NOT NULL AUTO_INCREMENT,
            `company_id` int DEFAULT NULL,
            PRIMARY KEY (`id`),
            CONSTRAINT `fk_company` FOREIGN KEY (`company_id`) REFERENCES `companies` (`id`)
        ) ENGINE=InnoDB;"#;

        let id = builder.parse_create_table(stmt);
        let schema = builder.build();
        let table = schema.table(id.unwrap()).unwrap();

        assert_eq!(table.foreign_keys.len(), 1);
        let fk = &table.foreign_keys[0];
        assert_eq!(fk.name, Some("fk_company".to_string()));
        assert_eq!(fk.column_names, vec!["company_id".to_string()]);
        assert_eq!(fk.referenced_table, "companies");
        assert_eq!(fk.referenced_columns, vec!["id".to_string()]);
        assert!(fk.referenced_table_id.is_some());
    }

    #[test]
    fn test_parse_foreign_key_without_constraint_name() {
        let mut builder = SchemaBuilder::new();
        builder.parse_create_table("CREATE TABLE `orders` (`id` int PRIMARY KEY);");

        let stmt = r#"CREATE TABLE `order_items` (
            `id` int NOT NULL AUTO_INCREMENT,
            `order_id` int NOT NULL,
            PRIMARY KEY (`id`),
            FOREIGN KEY (`order_id`) REFERENCES `orders` (`id`)
        );"#;

        let id = builder.parse_create_table(stmt);
        let schema = builder.build();
        let table = schema.table(id.unwrap()).unwrap();

        assert_eq!(table.foreign_keys.len(), 1);
        let fk = &table.foreign_keys[0];
        assert!(fk.name.is_none());
        assert_eq!(fk.referenced_table, "orders");
    }

    #[test]
    fn test_parse_composite_primary_key() {
        let mut builder = SchemaBuilder::new();
        let stmt = r#"CREATE TABLE `order_items` (
            `order_id` int NOT NULL,
            `product_id` int NOT NULL,
            `quantity` int DEFAULT 1,
            PRIMARY KEY (`order_id`, `product_id`)
        );"#;

        let id = builder.parse_create_table(stmt);
        let schema = builder.build();
        let table = schema.table(id.unwrap()).unwrap();

        assert_eq!(table.primary_key.len(), 2);
        assert!(table.columns[0].is_primary_key);
        assert!(table.columns[1].is_primary_key);
        assert!(!table.columns[2].is_primary_key);
    }

    #[test]
    fn test_parse_inline_primary_key() {
        let mut builder = SchemaBuilder::new();
        let stmt = "CREATE TABLE `simple` (`id` int PRIMARY KEY, `name` varchar(100));";

        let id = builder.parse_create_table(stmt);
        let schema = builder.build();
        let table = schema.table(id.unwrap()).unwrap();

        assert_eq!(table.primary_key.len(), 1);
        assert!(table.columns[0].is_primary_key);
    }

    #[test]
    fn test_split_table_body() {
        use sql_splitter::schema::split_table_body;

        let body = "`id` int, `name` varchar(255), PRIMARY KEY (`id`)";
        let parts = split_table_body(body);
        assert_eq!(parts.len(), 3);
        assert!(parts[0].contains("id"));
        assert!(parts[1].contains("name"));
        assert!(parts[2].contains("PRIMARY KEY"));
    }

    #[test]
    fn test_parse_column_list() {
        use sql_splitter::schema::parse_column_list;

        assert_eq!(
            parse_column_list("`id`, `name`"),
            vec!["id".to_string(), "name".to_string()]
        );
        assert_eq!(
            parse_column_list("id,name,email"),
            vec!["id".to_string(), "name".to_string(), "email".to_string()]
        );
    }
}

mod graph_tests {
    use super::*;

    fn create_test_schema() -> Schema {
        let mut schema = Schema::new();

        // companies (root)
        let companies = TableSchema::new("companies".to_string(), TableId(0));
        schema.add_table(companies);

        // users -> companies
        let mut users = TableSchema::new("users".to_string(), TableId(0));
        users.columns.push(Column {
            name: "company_id".to_string(),
            col_type: ColumnType::Int,
            ordinal: ColumnId(1),
            is_primary_key: false,
            is_nullable: true,
        });
        users.foreign_keys.push(ForeignKey {
            name: None,
            columns: vec![ColumnId(1)],
            column_names: vec!["company_id".to_string()],
            referenced_table: "companies".to_string(),
            referenced_columns: vec!["id".to_string()],
            referenced_table_id: Some(TableId(0)),
        });
        schema.add_table(users);

        // orders -> users
        let mut orders = TableSchema::new("orders".to_string(), TableId(0));
        orders.foreign_keys.push(ForeignKey {
            name: None,
            columns: vec![ColumnId(1)],
            column_names: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            referenced_table_id: Some(TableId(1)),
        });
        schema.add_table(orders);

        schema
    }

    #[test]
    fn test_graph_construction() {
        let schema = create_test_schema();
        let graph = SchemaGraph::from_schema(schema);

        assert_eq!(graph.len(), 3);

        // companies (0) has no parents
        assert!(graph.parents[0].is_empty());
        // companies (0) has users (1) as child
        assert_eq!(graph.children[0], vec![TableId(1)]);

        // users (1) has companies (0) as parent
        assert_eq!(graph.parents[1], vec![TableId(0)]);
        // users (1) has orders (2) as child
        assert_eq!(graph.children[1], vec![TableId(2)]);

        // orders (2) has users (1) as parent
        assert_eq!(graph.parents[2], vec![TableId(1)]);
        // orders (2) has no children
        assert!(graph.children[2].is_empty());
    }

    #[test]
    fn test_topo_sort() {
        let schema = create_test_schema();
        let graph = SchemaGraph::from_schema(schema);
        let result = graph.topo_sort();

        assert!(result.cyclic_tables.is_empty());
        assert_eq!(result.order.len(), 3);

        // companies must come before users
        let companies_pos = result
            .order
            .iter()
            .position(|&id| id == TableId(0))
            .unwrap();
        let users_pos = result
            .order
            .iter()
            .position(|&id| id == TableId(1))
            .unwrap();
        let orders_pos = result
            .order
            .iter()
            .position(|&id| id == TableId(2))
            .unwrap();

        assert!(companies_pos < users_pos);
        assert!(users_pos < orders_pos);
    }

    #[test]
    fn test_cycle_detection() {
        let mut schema = Schema::new();

        // Create a cycle: A -> B -> A
        let mut table_a = TableSchema::new("table_a".to_string(), TableId(0));
        table_a.foreign_keys.push(ForeignKey {
            name: None,
            columns: vec![],
            column_names: vec![],
            referenced_table: "table_b".to_string(),
            referenced_columns: vec![],
            referenced_table_id: Some(TableId(1)),
        });
        schema.add_table(table_a);

        let mut table_b = TableSchema::new("table_b".to_string(), TableId(0));
        table_b.foreign_keys.push(ForeignKey {
            name: None,
            columns: vec![],
            column_names: vec![],
            referenced_table: "table_a".to_string(),
            referenced_columns: vec![],
            referenced_table_id: Some(TableId(0)),
        });
        schema.add_table(table_b);

        let graph = SchemaGraph::from_schema(schema);
        let result = graph.topo_sort();

        assert!(result.order.is_empty());
        assert_eq!(result.cyclic_tables.len(), 2);
    }

    #[test]
    fn test_self_reference() {
        let mut schema = Schema::new();

        // Create a self-referential table (e.g., categories with parent_id)
        let mut categories = TableSchema::new("categories".to_string(), TableId(0));
        categories.foreign_keys.push(ForeignKey {
            name: None,
            columns: vec![ColumnId(1)],
            column_names: vec!["parent_id".to_string()],
            referenced_table: "categories".to_string(),
            referenced_columns: vec!["id".to_string()],
            referenced_table_id: Some(TableId(0)), // Self-reference
        });
        schema.add_table(categories);

        let graph = SchemaGraph::from_schema(schema);

        // Self-references should be detected
        assert!(graph.has_self_reference(TableId(0)));
        assert_eq!(graph.self_referential_tables(), vec![TableId(0)]);

        // Self-references should NOT create cycles in the graph
        // (we filter them out during graph construction)
        let result = graph.topo_sort();
        assert!(result.cyclic_tables.is_empty());
        assert_eq!(result.order.len(), 1);
    }

    #[test]
    fn test_root_and_leaf_tables() {
        let schema = create_test_schema();
        let graph = SchemaGraph::from_schema(schema);

        let roots = graph.root_tables();
        assert_eq!(roots, vec![TableId(0)]); // companies

        let leaves = graph.leaf_tables();
        assert_eq!(leaves, vec![TableId(2)]); // orders
    }

    #[test]
    fn test_ancestors_and_descendants() {
        let schema = create_test_schema();
        let graph = SchemaGraph::from_schema(schema);

        // orders' ancestors: users, companies
        let order_ancestors = graph.ancestors(TableId(2));
        assert!(order_ancestors.contains(&TableId(1))); // users
        assert!(order_ancestors.contains(&TableId(0))); // companies

        // companies' descendants: users, orders
        let company_descendants = graph.descendants(TableId(0));
        assert!(company_descendants.contains(&TableId(1))); // users
        assert!(company_descendants.contains(&TableId(2))); // orders
    }

    #[test]
    fn test_is_ancestor() {
        let schema = create_test_schema();
        let graph = SchemaGraph::from_schema(schema);

        assert!(graph.is_ancestor(TableId(0), TableId(2))); // companies is ancestor of orders
        assert!(graph.is_ancestor(TableId(1), TableId(2))); // users is ancestor of orders
        assert!(!graph.is_ancestor(TableId(2), TableId(0))); // orders is not ancestor of companies
    }
}
