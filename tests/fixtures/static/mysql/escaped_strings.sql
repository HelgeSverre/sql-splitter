-- Test fixture: MySQL string escaping edge cases
-- Used to verify parser handles escaped quotes, newlines, etc.

CREATE TABLE `test_escapes` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `data` VARCHAR(255)
);

INSERT INTO `test_escapes` (`id`, `data`) VALUES
(1, 'Simple string'),
(2, 'String with ''double quotes'''),
(3, 'String with \'backslash quotes\''),
(4, 'Line1\nLine2'),
(5, 'Tab\there'),
(6, 'Backslash\\here'),
(7, 'Mixed: it\'s a \"test\"\nwith\ttabs');
