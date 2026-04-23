-- String Functions
CREATE TABLE test_str (id INT, name TEXT);
INSERT INTO test_str VALUES (1, 'hello'), (2, 'world'), (3, 'HELLO');
SELECT LENGTH(name), UPPER(name), LOWER(name), LEFT(name, 2), RIGHT(name, 3), SUBSTRING(name, 2, 3) FROM test_str;
SELECT CONCAT(name, '!') FROM test_str;
SELECT REPLACE('hello', 'l', 'r');
SELECT TRIM('  hello  '), LTRIM('  hello'), RTRIM('hello  ');
SELECT LPAD('hi', 5, '*'), RPAD('hi', 5, '*');
SELECT INSTR('hello', 'l'), LOCATE('l', 'hello', 1);
SELECT REVERSE('hello');
