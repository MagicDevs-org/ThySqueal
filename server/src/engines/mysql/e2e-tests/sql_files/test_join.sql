-- Test JOINs
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'alice');
INSERT INTO users VALUES (2, 'bob');
CREATE TABLE orders (id INT, user_id INT, amount INT);
INSERT INTO orders VALUES (1, 1, 100);
INSERT INTO orders VALUES (2, 1, 200);
INSERT INTO orders VALUES (3, 2, 150);
SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.name, o.amount
