-- UNION / SET Operations
CREATE TABLE test_union1 (id INT, name TEXT);
INSERT INTO test_union1 VALUES (1, 'a'), (2, 'b');
CREATE TABLE test_union2 (id INT, name TEXT);
INSERT INTO test_union2 VALUES (2, 'b'), (3, 'c');
SELECT * FROM test_union1 UNION SELECT * FROM test_union2 ORDER BY id;
SELECT * FROM test_union1 UNION ALL SELECT * FROM test_union2 ORDER BY id;
