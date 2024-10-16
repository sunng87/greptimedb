CREATE TABLE integers(i BIGINT TIME INDEX);

INSERT INTO integers VALUES (1), (2), (3);

SELECT DISTINCT i%2 FROM integers ORDER BY 1;

SELECT DISTINCT i % 2 FROM integers WHERE i<3 ORDER BY i;

SELECT DISTINCT ON (1) i % 2, i FROM integers WHERE i<3 ORDER BY i;

SELECT DISTINCT integers.i FROM integers ORDER BY i DESC;

SELECT DISTINCT i FROM integers ORDER BY integers.i DESC;

SELECT DISTINCT integers.i FROM integers ORDER BY integers.i DESC;

DROP TABLE integers;
