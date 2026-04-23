-- JOIN Variants
CREATE TABLE test_j1 (id INT, name TEXT);
INSERT INTO test_j1 VALUES (1, 'a'), (2, 'b'), (3, 'c');
CREATE TABLE test_j2 (id INT, val INT);
INSERT INTO test_j2 VALUES (1, 100), (2, 200), (4, 400);
SELECT * FROM test_j1 INNER JOIN test_j2 ON test_j1.id = test_j2.id;
SELECT * FROM test_j1 LEFT JOIN test_j2 ON test_j1.id = test_j2.id;
SELECT * FROM test_j1 RIGHT JOIN test_j2 ON test_j1.id = test_j2.id;
