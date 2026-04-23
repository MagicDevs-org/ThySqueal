DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
INSERT INTO users VALUES (1, 'alice', 25);
INSERT INTO users VALUES (2, 'bob', 30);
INSERT INTO users VALUES (3, 'charlie', 25);
DELETE FROM users WHERE age = 25;
SELECT * FROM users ORDER BY id
