-- GROUP BY and HAVING
CREATE TABLE test_group (dept TEXT, emp TEXT, sal INT);
INSERT INTO test_group VALUES ('sales', 'alice', 1000), ('sales', 'bob', 2000), ('it', 'charlie', 3000), ('it', 'david', 2500);
SELECT dept, COUNT(*), SUM(sal), AVG(sal) FROM test_group GROUP BY dept;
SELECT dept, SUM(sal) as total FROM test_group GROUP BY dept HAVING total > 3000;
