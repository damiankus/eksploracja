SELECT c.COUNTRY, COUNT(*) AS patents_no from countries AS c JOIN patents as p ON c.CODE = p.COUNTRY
GROUP BY(c.COUNTRY, p.COUNTRY)
ORDER BY patents_no DESC
LIMIT 25

SELECT s.STATE, COUNT(*) AS patents_no FROM states AS s JOIN patents AS p ON p.POSTATE = s.CODE
GROUP BY(s.STATE, p.POSTATE)
ORDER BY patents_no DESC;

SELECT CITED, COUNT(CITING) AS referenced_by FROM cite
GROUP BY(CITED)
ORDER BY referenced_by DESC
LIMIT 25;

SELECT c.COUNTRY, p.GYEAR, COUNT(*) as patents_granted FROM patents AS p 
JOIN countries AS c ON c.CODE = p.COUNTRY
GROUP BY(p.GYEAR, c.COUNTRY)
ORDER BY c.COUNTRY, p.GYEAR

SELECT p_cited.COUNTRY AS cited_country, p_citing.COUNTRY AS citing_country, COUNT(p_citing.COUNTRY) AS citations_no  
FROM (SELECT * FROM cite LIMIT 100000) AS c
JOIN patents AS p_cited ON p_cited.PATENT = c.CITED
JOIN patents AS p_citing ON p_citing.PATENT = c.CITING
GROUP BY(citing_country, cited_country)
ORDER BY cited_country, citations_no DESC

SELECT * FROM patents 
WHERE gdate BETWEEN DATE_PART('day', '1990-01-01'::timestamp - '1960-01-01'::timestamp)
AND DATE_PART('day', '1999-01-01'::timestamp - '1960-01-01'::timestamp);
