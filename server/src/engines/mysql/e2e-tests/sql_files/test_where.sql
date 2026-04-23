-- WHERE and Comparison
CREATE TABLE test_where (id INT, val INT, name TEXT);
INSERT INTO test_where VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, NULL, 'd');
SELECT * FROM test_where WHERE val > 10 AND val < 30;
SELECT * FROM test_where WHERE val = 10 OR val = 30;
SELECT * FROM test_where WHERE val IS NULL;
SELECT * FROM test_where WHERE val != 20;
SELECT * FROM test_where WHERE val BETWEEN 10 AND 30;
SELECT * FROM test_where WHERE name IN ('a', 'b');
