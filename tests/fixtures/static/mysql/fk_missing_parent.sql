-- Test fixture: FK referencing missing parent
-- Should trigger FK_MISSING_PARENT error

CREATE TABLE `departments` (
  `id` INT PRIMARY KEY,
  `name` VARCHAR(255)
);

CREATE TABLE `employees` (
  `id` INT PRIMARY KEY,
  `name` VARCHAR(255),
  `department_id` INT,
  CONSTRAINT `fk_department` FOREIGN KEY (`department_id`) REFERENCES `departments` (`id`)
);

INSERT INTO `departments` (`id`, `name`) VALUES (1, 'Engineering');
INSERT INTO `employees` (`id`, `name`, `department_id`) VALUES (1, 'Alice', 1);
INSERT INTO `employees` (`id`, `name`, `department_id`) VALUES (2, 'Bob', 99);
