-- Test fixture: PostgreSQL COPY with NULL values
-- Used to verify COPY block parsing handles \N correctly

CREATE TABLE copy_test (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100),
  value INTEGER
);

COPY copy_test (id, name, value) FROM stdin;
1	Alice	100
2	\N	200
3	Carol	\N
4	\N	\N
5	Eve	500
\.

-- Additional INSERT for comparison
INSERT INTO copy_test (id, name, value) VALUES (6, 'Frank', NULL);
