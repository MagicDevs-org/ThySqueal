CREATE TABLE test_tx (id INT, name TEXT);
INSERT INTO test_tx VALUES (1, 'a');
BEGIN;
INSERT INTO test_tx VALUES (2, 'b');
COMMIT;
BEGIN;
INSERT INTO test_tx VALUES (3, 'c');
ROLLBACK;
SELECT * FROM test_tx ORDER BY id;
