-- MySQL mysqldump-style dump exercising the shapes the profiler must observe:
-- multiple tables, multi-row INSERTs, NULLs, value skew, duplicates, declared
-- foreign keys, a composite primary key, timestamps, JSON, and credential-like
-- column names password, api_key, token. Reused by Task 20 inference and the
-- Phase 2 checkpoint, so keep it representative and dialect-clean with no inline
-- comments between value tuples.

CREATE TABLE `users` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `email` VARCHAR(255) NOT NULL,
  `password` VARCHAR(255) NOT NULL,
  `api_key` VARCHAR(64) DEFAULT NULL,
  `token` VARCHAR(64) DEFAULT NULL,
  `is_active` TINYINT(1) NOT NULL DEFAULT 1,
  `balance` DECIMAL(12,2) NOT NULL DEFAULT 0.00,
  `metadata` JSON DEFAULT NULL,
  `created_at` DATETIME NOT NULL,
  `updated_at` DATETIME NOT NULL,
  PRIMARY KEY (`id`)
);

INSERT INTO `users` (`id`, `email`, `password`, `api_key`, `token`, `is_active`, `balance`, `metadata`, `created_at`, `updated_at`) VALUES
(1,'alice@example.com','$2y$10$abcdefghijk','key_00000001','tok_a',1,10.50,'{"tier":"gold","seats":3}','2024-01-01 10:00:00','2024-01-02 10:00:00'),
(2,'bob@example.com','$2y$10$abcdefghijk',NULL,NULL,1,20.00,'{"tier":"silver","seats":1}','2024-01-03 10:00:00','2024-01-04 10:00:00'),
(3,'carol@example.com','$2y$10$abcdefghijk','key_00000003',NULL,0,0.00,'{broken json','2024-02-01 10:00:00','2024-02-02 10:00:00'),
(4,'dave@example.com','$2y$10$abcdefghijk',NULL,'tok_d',1,5.25,'{"tier":"gold","seats":9}','2024-03-01 10:00:00','2024-03-02 10:00:00'),
(5,'erin@example.com','$2y$10$abcdefghijk','key_00000005','tok_e',1,100.00,'{"tier":"gold","seats":2}','2024-04-01 10:00:00','2024-04-02 10:00:00'),
(6,'frank@example.com','$2y$10$abcdefghijk','key_00000006','tok_f',0,1000.00,'{"tier":"silver","seats":4}','2024-05-01 10:00:00','2024-05-02 10:00:00');

CREATE TABLE `orders` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `user_id` INT NOT NULL,
  `total` DECIMAL(12,2) NOT NULL,
  `status` VARCHAR(32) NOT NULL,
  `created_at` DATETIME NOT NULL,
  `updated_at` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
);

INSERT INTO `orders` (`id`, `user_id`, `total`, `status`, `created_at`, `updated_at`) VALUES
(1,1,10.50,'paid','2024-01-05 10:00:00','2024-01-06 10:00:00'),
(2,1,20.00,'paid','2024-01-07 10:00:00','2024-01-08 10:00:00'),
(3,2,5.25,'pending','2024-02-05 10:00:00','2024-02-06 10:00:00'),
(4,3,100.00,'paid','2024-04-05 10:00:00','2024-04-06 10:00:00'),
(5,5,1000.00,'paid','2024-05-05 10:00:00','2024-05-06 10:00:00');

CREATE TABLE `order_items` (
  `order_id` INT NOT NULL,
  `product_id` INT NOT NULL,
  `qty` INT NOT NULL DEFAULT 1,
  PRIMARY KEY (`order_id`, `product_id`),
  FOREIGN KEY (`order_id`) REFERENCES `orders` (`id`)
);

INSERT INTO `order_items` (`order_id`, `product_id`, `qty`) VALUES
(1,10,2),
(1,11,1),
(2,10,5),
(3,12,1);
