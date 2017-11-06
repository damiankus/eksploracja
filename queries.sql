COPY(
	SELECT c.COUNTRY, COUNT(*) AS patents_no from countries AS c JOIN patents as p ON c.CODE = p.COUNTRY
	GROUP BY(c.COUNTRY, p.COUNTRY)
	ORDER BY patents_no DESC
) TO '/tmp/patents_per_country.csv' WITH CSV DELIMITER ',' HEADER;

COPY(
	SELECT s.STATE, COUNT(*) AS patents_no FROM states AS s JOIN patents AS p ON p.POSTATE = s.CODE
	GROUP BY(s.STATE, p.POSTATE)
	ORDER BY patents_no DESC
) TO '/tmp/patents_per_us_state.csv' WITH CSV DELIMITER ',' HEADER;

COPY(
	SELECT CITED, COUNT(CITING) AS referenced_by FROM citations
	GROUP BY(CITED)
	ORDER BY referenced_by DESC
) TO '/tmp/citations_per_patent.csv' WITH CSV DELIMITER ',' HEADER;	

COPY(
	SELECT c.COUNTRY, p.GYEAR, COUNT(*) as patents_granted FROM patents AS p 
	JOIN countries AS c ON c.CODE = p.COUNTRY
	GROUP BY(p.GYEAR, c.COUNTRY)
	ORDER BY c.COUNTRY, p.GYEAR
) TO '/tmp/patents_per_country_per_year.csv' WITH CSV DELIMITER ',' HEADER;

COPY(
	SELECT c_cited.COUNTRY AS cited_country, c_citing.COUNTRY AS citing_country, COUNT(p_citing.COUNTRY) AS citations_no  
	FROM citations AS c
	JOIN patents AS p_cited ON p_cited.PATENT = c.CITED
	JOIN patents AS p_citing ON p_citing.PATENT = c.CITING
	JOIN countries AS c_cited ON c_cited.CODE = p_cited.COUNTRY
	JOIN countries AS c_citing ON c_citing.CODE = p_citing.COUNTRY
	GROUP BY(citing_country, cited_country)
	ORDER BY cited_country, citations_no DESC
) TO '/tmp/citations_per_other_country.csv' WITH CSV DELIMITER ',' HEADER;

COPY(
	SELECT ppi.granted_patents, COUNT(ppi.INVENTOR_ID) as inventors_no 
	FROM (SELECT INVENTOR_ID, COUNT(PATENT_ID) as granted_patents
		FROM patent_inventor
		GROUP BY INVENTOR_ID) AS ppi
	GROUP BY granted_patents
	ORDER BY inventors_no DESC
) TO '/tmp/patents_per_inventor_histogram.csv' WITH CSV DELIMITER ',' HEADER;

COPY(
	SELECT referenced_by, COUNT(CITED) as patents_no
	FROM (
		SELECT CITED, COUNT(CITING) AS referenced_by FROM citations
		GROUP BY(CITED)
	) AS citations_per_patent
	GROUP BY referenced_by
	ORDER BY patents_no DESC
) TO '/tmp/citations_histogram.csv' WITH CSV DELIMITER ',' HEADER;