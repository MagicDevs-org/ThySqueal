-- Subqueries
CREATE TABLE test_sub (id INT, val INT);
INSERT INTO test_sub VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
SELECT * FROM test_sub WHERE val > (SELECT AVG(val) FROM test_sub);
SELECT * FROM test_sub WHERE val IN (SELECT val FROM test_sub WHERE id > 2);
SELECT * FROM test_sub WHERE EXISTS (SELECT 1 FROM test_sub WHERE val > 100);
SELECT (SELECT MAX(val) FROM test_sub), (SELECT MIN(val) FROM test_sub);
