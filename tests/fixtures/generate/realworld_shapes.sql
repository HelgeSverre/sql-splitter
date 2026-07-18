-- Synthetic regression fixture for real-world MySQL inference shapes.
--
-- Every name and value here is invented; nothing is copied from any source
-- dump. It reproduces three inference/compile shapes that real MySQL dumps
-- exposed, each of which previously made `generate --input <dump>` fail:
--
--   1. `bigint/int/tinyint unsigned` columns. MySQL 8 omits the display width,
--      so a modern dump writes `bigint unsigned` (no parens). These must
--      classify as Integer/BigInteger, not collapse into the `Other` fallback
--      (which then mis-assigns a `sequence`/`string` generator).
--   2. A 0/1 `tinyint(1)` boolean-by-convention column. The profiler infers a
--      `boolean` generator, which must compile and render as `0`/`1` on the
--      integer-family column rather than being rejected outright.
--   3. A `binary(16)` hash column whose name matches a semantic text rule
--      (`filename`). It is UUID-family, so it must fall back to the `uuid`
--      generator instead of a Text-only semantic generator.
--
-- `retry_count` deliberately carries values other than 0/1 to prove an
-- unsigned integer that is NOT boolean-by-convention still generates as an
-- integer.

DROP TABLE IF EXISTS `widgets`;
CREATE TABLE `widgets` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `owner_id` int unsigned NOT NULL,
  `retry_count` tinyint unsigned NOT NULL,
  `is_active` tinyint(1) NOT NULL,
  `document_filename` binary(16) NOT NULL,
  `label` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `widgets` (`id`,`owner_id`,`retry_count`,`is_active`,`document_filename`,`label`) VALUES
(1,10,0,1,0x0102030405060708090a0b0c0d0e0f10,'alpha'),
(2,11,2,0,0x1112131415161718191a1b1c1d1e1f20,'bravo'),
(3,10,1,1,0x2122232425262728292a2b2c2d2e2f30,'charlie'),
(4,12,0,0,0x3132333435363738393a3b3c3d3e3f40,'delta'),
(5,11,3,1,0x4142434445464748494a4b4c4d4e4f50,'echo'),
(6,13,0,1,0x5152535455565758595a5b5c5d5e5f60,'foxtrot');
