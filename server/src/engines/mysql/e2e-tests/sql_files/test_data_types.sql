-- Data Types (basic)
CREATE TABLE test_types (
    id INT PRIMARY KEY,
    vchr VARCHAR(100),
    txt TEXT,
    dt DATETIME,
    dt2 DATE,
    tm TIME
);
INSERT INTO test_types VALUES (1, 'hello', 'world', NOW(), '2024-01-01', '12:30:00');
SELECT id, vchr FROM test_types
