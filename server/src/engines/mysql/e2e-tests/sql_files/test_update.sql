DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'alice');
INSERT INTO users VALUES (2, 'bob');
UPDATE users SET name = 'alice_updated' WHERE id = 1;
SELECT * FROM users WHERE id = 1
