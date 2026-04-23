DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);
INSERT INTO users VALUES (1, 'alice', 100);
INSERT INTO users VALUES (2, 'bob', 85);
INSERT INTO users VALUES (3, 'charlie', 95);
SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) as rn FROM users ORDER BY score DESC
