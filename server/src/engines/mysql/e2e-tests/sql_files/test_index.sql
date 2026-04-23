-- Indexes
CREATE TABLE test_idx (id INT, name TEXT);
INSERT INTO test_idx VALUES (1, 'a'), (2, 'b'), (3, 'c');
CREATE INDEX idx_name ON test_idx(name);
SHOW INDEX FROM test_idx;
DROP INDEX idx_name ON test_idx;
