-- Test LEFT JOIN
CREATE TABLE test_j1 (id INT, name TEXT);
INSERT INTO test_j1 VALUES (1, 'a');
INSERT INTO test_j1 VALUES (2, 'b');
INSERT INTO test_j1 VALUES (3, 'c');
CREATE TABLE test_j2 (id INT, val INT);
INSERT INTO test_j2 VALUES (1, 100);
INSERT INTO test_j2 VALUES (2, 200);
INSERT INTO test_j2 VALUES (4, 400);
SELECT * FROM test_j1 LEFT JOIN test_j2 ON test_j1.id = test_j2.id ORDER BY test_j1.id
