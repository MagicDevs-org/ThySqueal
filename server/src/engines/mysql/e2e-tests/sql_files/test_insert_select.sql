DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT, name TEXT, age INT);
INSERT INTO users VALUES (1, 'alice', 25);
INSERT INTO users VALUES (2, 'bob', 30);
INSERT INTO users VALUES (3, 'charlie', 25);
SELECT * FROM users ORDER BY id
