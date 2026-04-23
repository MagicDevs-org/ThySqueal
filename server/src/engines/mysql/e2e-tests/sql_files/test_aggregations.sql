DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);
INSERT INTO users VALUES (1, 'alice', 100);
INSERT INTO users VALUES (2, 'bob', 85);
INSERT INTO users VALUES (3, 'charlie', 95);
SELECT AVG(score) as avg_score, SUM(score) as total_score, COUNT(*) as cnt FROM users
