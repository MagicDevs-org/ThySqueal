DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'alice');
INSERT INTO users VALUES (2, 'bob');
SELECT * FROM users ORDER BY id
