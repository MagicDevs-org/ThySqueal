-- NULL handling
SELECT NULL IS NULL, NULL IS NOT NULL;
SELECT IFNULL(NULL, 'default'), NULLIF(1, 1), COALESCE(NULL, NULL, 'fallback');
SELECT IF(1=1, 'yes', 'no'), IFNULL(NULL, 'yes');
