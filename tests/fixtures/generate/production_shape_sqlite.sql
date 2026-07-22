-- SQLite .dump-style variant of production_shape. Standard SQL identifiers,
-- '' string escaping, integers for booleans.
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE users (
  id INTEGER NOT NULL PRIMARY KEY,
  email TEXT NOT NULL,
  password TEXT NOT NULL,
  api_key TEXT,
  token TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  balance NUMERIC NOT NULL DEFAULT 0,
  metadata TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
INSERT INTO users (id, email, password, api_key, token, is_active, balance, metadata, created_at, updated_at) VALUES
(1,'alice@example.com','$2y$10$abcdefghijk','key_00000001','tok_a',1,10.50,'{"tier":"gold"}','2024-01-01 10:00:00','2024-01-02 10:00:00'),
(2,'bob@example.com','$2y$10$abcdefghijk',NULL,NULL,1,20.00,'{"tier":"silver"}','2024-01-03 10:00:00','2024-01-04 10:00:00'),
(3,'carol@example.com','$2y$10$abcdefghijk','key_00000003',NULL,0,0.00,'{broken json','2024-02-01 10:00:00','2024-02-02 10:00:00'),
(4,'dave@example.com','$2y$10$abcdefghijk',NULL,'tok_d',1,5.25,'{"tier":"gold"}','2024-03-01 10:00:00','2024-03-02 10:00:00'),
(5,'erin@example.com','$2y$10$abcdefghijk','key_00000005','tok_e',1,100.00,'{"tier":"gold"}','2024-04-01 10:00:00','2024-04-02 10:00:00'),
(6,'frank@example.com','$2y$10$abcdefghijk','key_00000006','tok_f',0,1000.00,'{"tier":"silver"}','2024-05-01 10:00:00','2024-05-02 10:00:00');
CREATE TABLE orders (
  id INTEGER NOT NULL PRIMARY KEY,
  user_id INTEGER NOT NULL,
  total NUMERIC NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users (id)
);
INSERT INTO orders (id, user_id, total, status, created_at, updated_at) VALUES
(1,1,10.50,'paid','2024-01-05 10:00:00','2024-01-06 10:00:00'),
(2,1,20.00,'paid','2024-01-07 10:00:00','2024-01-08 10:00:00'),
(3,2,5.25,'pending','2024-02-05 10:00:00','2024-02-06 10:00:00'),
(4,3,100.00,'paid','2024-04-05 10:00:00','2024-04-06 10:00:00'),
(5,5,1000.00,'paid','2024-05-05 10:00:00','2024-05-06 10:00:00');
COMMIT;
