DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, created_at DATETIME);
INSERT INTO users VALUES (1, 'alice', NOW());
INSERT INTO users VALUES (2, 'bob', NOW());
SELECT name, created_at FROM users
