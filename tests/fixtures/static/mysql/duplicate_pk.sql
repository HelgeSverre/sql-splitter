-- Test fixture: Duplicate primary key
-- Should trigger DUPLICATE_PK error

CREATE TABLE `users` (
  `id` INT PRIMARY KEY,
  `name` VARCHAR(255)
);

INSERT INTO `users` (`id`, `name`) VALUES (1, 'Alice');
INSERT INTO `users` (`id`, `name`) VALUES (2, 'Bob');
INSERT INTO `users` (`id`, `name`) VALUES (1, 'Charlie');
