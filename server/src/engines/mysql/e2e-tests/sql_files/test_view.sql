-- Views
CREATE TABLE test_v (id INT, name TEXT);
INSERT INTO test_v VALUES (1, 'a'), (2, 'b');
CREATE VIEW test_view AS SELECT * FROM test_v WHERE id > 1;
SELECT * FROM test_view;
DROP VIEW test_view;
