-- Test fixture: INSERT into missing table
-- Should trigger DDL_MISSING_TABLE error

INSERT INTO `nonexistent_table` (`id`, `name`) VALUES (1, 'test');
