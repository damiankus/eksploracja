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

COPY(
    SELECT c1.catnamelong AS citing, c2.catnamelong AS cited, COUNT(*) AS numberOfCitations
    FROM citations
    INNER JOIN patents AS p1 ON citations.citing = p1.patent
    INNER JOIN patents AS p2 ON citations.cited = p2.patent
    INNER JOIN categories AS c1 ON c1.cat = p1.cat
    INNER JOIN categories AS c2 ON c2.cat = p2.cat
    GROUP BY c1.catnamelong, c2.catnamelong
) TO '/tmp/citations_between_categories.csv' WITH CSV DELIMITER ',' HEADER;

COPY(
	SELECT pi_citing.inventor_id AS Source, pi_cited.inventor_id AS Target, COUNT(*) AS Weight
	FROM citations as ci
	INNER JOIN patent_inventor AS pi_cited ON pi_cited.patent_id = ci.cited
	INNER JOIN patent_inventor AS pi_citing ON pi_citing.patent_id = ci.citing
	INNER JOIN patents as p_cited ON p_cited.patent = ci.cited
	INNER JOIN patents as p_citing ON p_citing.patent = ci.citing
	WHERE p_cited.gyear = p_citing.gyear
	GROUP BY Target, Source
	HAVING COUNT(*) > 1
	ORDER BY Source, Target
) TO '/tmp/citations_same_year.csv' WITH CSV DELIMITER ',' HEADER;


CREATE OR REPLACE FUNCTION save_citations_within_period(delta integer, threshold integer)
RETURNS VOID AS $$
DECLARE
	min_year integer;
	max_year integer;
	stat text;
BEGIN
	SELECT MIN(gyear) INTO min_year FROM patents;
	SELECT MAX(gyear) INTO max_year FROM patents;
	FOR y in (min_year)..(max_year - delta) LOOP
		stat := format('COPY(
			SELECT pi_citing.inventor_id AS Source, pi_cited.inventor_id AS Target, COUNT(*) AS Weight
			FROM citations as ci
			INNER JOIN patent_inventor AS pi_cited ON pi_cited.patent_id = ci.cited
			INNER JOIN patent_inventor AS pi_citing ON pi_citing.patent_id = ci.citing
			INNER JOIN patents as p_cited ON p_cited.patent = ci.cited
			INNER JOIN patents as p_citing ON p_citing.patent = ci.citing
			WHERE p_cited.gyear = %s AND p_citing.gyear BETWEEN %s AND %s
			GROUP BY Target, Source
			HAVING COUNT(*) > %s
			ORDER BY Source, Target
		) TO ''/tmp/inv_threshold_within_%s_years_from_%s.csv'' WITH CSV DELIMITER '','' HEADER', y::text, y::text, (y + delta)::text, threshold::text, delta::text, y::text);
		RAISE NOTICE '%', stat;
		EXECUTE stat;
	END LOOP;
END; $$ LANGUAGE plpgsql


DROP FUNCTION save_citations_with_countries(integer, integer, integer, integer)
CREATE OR REPLACE FUNCTION save_inventors_with_countries(start_year integer, delta integer, threshold integer, step integer)
RETURNS VOID AS $$
DECLARE
	max_year integer;
	cit_stat text;
	copy_nodes_stat text;
	copy_edges_stat text;
BEGIN
	SELECT MAX(gyear) INTO max_year FROM patents;
	
	FOR y in (start_year)..(max_year - delta) BY step LOOP
		CREATE TEMP TABLE citations_within_year AS 
		(
			SELECT pi_citing.inventor_id AS Source, pi_cited.inventor_id AS Target, COUNT(*) AS Weight,
			c_citing.country AS citing_country, c_cited.country AS cited_country
			FROM citations as ci
			INNER JOIN patent_inventor AS pi_cited ON pi_cited.patent_id = ci.cited
			INNER JOIN patent_inventor AS pi_citing ON pi_citing.patent_id = ci.citing
			INNER JOIN patents as p_cited ON p_cited.patent = ci.cited
			INNER JOIN patents as p_citing ON p_citing.patent = ci.citing
			INNER JOIN inventors as i_cited ON i_cited.id = pi_cited.inventor_id
			INNER JOIN inventors as i_citing ON i_citing.id = pi_citing.inventor_id
			INNER JOIN countries as c_cited ON c_cited.code = i_cited.country
			INNER JOIN countries as c_citing ON c_citing.code = i_citing.country
			WHERE p_cited.gyear = y AND p_citing.gyear BETWEEN y AND (y + delta)
			GROUP BY cited_country, citing_country, Target, Source
			HAVING COUNT(*) >= threshold
			ORDER BY Source, Target
		);
		
		copy_edges_stat := format(
		'COPY(
		SELECT Source, Target, Weight AS Label, Weight
		FROM citations_within_year
		) TO ''/tmp/citations_within_%s_years_from_%s.csv'' WITH CSV DELIMITER '','' HEADER', delta::text, y::text);
		EXECUTE copy_edges_stat;

		copy_nodes_stat := format(
		'COPY(
		SELECT DISTINCT idc.id as Id, idc.country AS Label
		FROM
		(SELECT Source AS Id, citing_country AS country
		FROM citations_within_year
		UNION 
		SELECT Target AS Id, cited_country AS country
		FROM citations_within_year) AS idc
		) TO ''/tmp/inventor_country_%s.csv'' WITH CSV DELIMITER '','' HEADER', y::text);
		EXECUTE copy_nodes_stat;
		RAISE NOTICE '%', copy_nodes_stat;
		DROP TABLE citations_within_year;
	END LOOP;
END; $$ LANGUAGE plpgsql

SELECT save_inventors_with_countries(1993, 2, 4, 1);


DROP FUNCTION save_citations_graph(integer, integer, integer, integer)
CREATE OR REPLACE FUNCTION save_citations_graph(start_year integer, delta integer, threshold integer, step integer)
RETURNS VOID AS $$
DECLARE
	max_year integer;
	cit_stat text;
	copy_nodes_stat text;
	copy_edges_stat text;
BEGIN
	SELECT MAX(gyear) INTO max_year FROM patents;
	CREATE TEMP TABLE patent_max_inventor AS 
	(
		SELECT patent_id, MAX(inventor_id) as inventor_id
		FROM patent_inventor
		GROUP BY patent_id
	);
	
	FOR y in (start_year)..(max_year - delta) BY step LOOP
		CREATE TEMP TABLE citations_within_year AS 
		(
			SELECT ci.citing AS Source, ci.cited AS Target, 1 AS Weight,
			c_citing.country AS citing_country, c_cited.country AS cited_country
			FROM citations as ci
			INNER JOIN patent_max_inventor AS pi_cited ON pi_cited.patent_id = ci.cited
			INNER JOIN patent_max_inventor AS pi_citing ON pi_citing.patent_id = ci.citing
			INNER JOIN patents as p_cited ON p_cited.patent = ci.cited
			INNER JOIN patents as p_citing ON p_citing.patent = ci.citing
			INNER JOIN inventors as i_cited ON i_cited.id = pi_cited.inventor_id
			INNER JOIN inventors as i_citing ON i_citing.id = pi_citing.inventor_id
			INNER JOIN countries as c_cited ON c_cited.code = i_cited.country
			INNER JOIN countries as c_citing ON c_citing.code = i_citing.country
			WHERE p_cited.gyear = y AND p_citing.gyear BETWEEN y AND (y + delta)
		);
		CREATE TEMP TABLE frequently_cited AS 
		(
			SELECT Source, c.Target, Weight, citing_country, cited_country
			FROM citations_within_year as c
			JOIN (
				SELECT Target
				FROM citations_within_year
				GROUP BY Target
				HAVING COUNT(Source) > threshold
			) AS fc ON fc.Target = c.Target OR fc.Target = c.Source
		);
		copy_edges_stat := format(
		'COPY(
			SELECT Source, Target, Weight, Weight AS Label
			FROM frequently_cited
		) TO ''/tmp/edges_citations_within_%s_years_from_%s_tresh_%s.csv'' WITH CSV DELIMITER '','' HEADER', delta::text, y::text, threshold::text);
		EXECUTE copy_edges_stat;

		copy_nodes_stat := format(
		'COPY(
		SELECT DISTINCT Idc.id as Id, idc.country AS Label
		FROM
		(SELECT Source AS id, citing_country AS country
		FROM frequently_cited
		UNION 
		SELECT Target AS Id, cited_country AS country
		FROM frequently_cited) AS idc
		) TO ''/tmp/nodes_inventor_country_%s_tresh_%s.csv'' WITH CSV DELIMITER '','' HEADER', y::text, threshold::text);
		EXECUTE copy_nodes_stat;
		RAISE NOTICE '%', copy_nodes_stat;
		DROP TABLE frequently_cited;
		DROP TABLE citations_within_year;
	END LOOP;
	DROP TABLE patent_max_inventor;
END; $$ LANGUAGE plpgsql;
-- save_citations_graph(start_year integer, delta integer, threshold integer, step integer)
SELECT save_citations_graph(1993, 2, 15, 1);


DROP FUNCTION save_most_cited(Integer)
CREATE OR REPLACE FUNCTION save_most_cited(top Integer)
RETURNS VOID AS $$
DECLARE
	copy_nodes_stat text;
	copy_edges_stat text;
BEGIN
	CREATE TEMP TABLE patent_max_inventor AS 
	(
		SELECT patent_id, MAX(inventor_id) as inventor_id
		FROM patent_inventor
		GROUP BY patent_id
	);
	CREATE TEMP TABLE most_cited AS 
	(
		SELECT cited, COUNT(citing) AS CitingNo
		FROM citations
		GROUP BY cited
		ORDER BY CitingNo DESC
		LIMIT top
	);
	
	CREATE TEMP TABLE cited_countries AS 
	(
		SELECT ci.citing AS Source, ci.cited AS Target, 1 AS Weight,
		c_citing.country AS citing_country, c_cited.country AS cited_country
		FROM citations as ci
		INNER JOIN most_cited AS mc ON mc.cited = ci.cited
		INNER JOIN patent_max_inventor AS pi_cited ON pi_cited.patent_id = ci.cited
		INNER JOIN patent_max_inventor AS pi_citing ON pi_citing.patent_id = ci.citing
		INNER JOIN patents as p_cited ON p_cited.patent = ci.cited
		INNER JOIN patents as p_citing ON p_citing.patent = ci.citing
		INNER JOIN inventors as i_cited ON i_cited.id = pi_cited.inventor_id
		INNER JOIN inventors as i_citing ON i_citing.id = pi_citing.inventor_id
		INNER JOIN countries as c_cited ON c_cited.code = i_cited.country
		INNER JOIN countries as c_citing ON c_citing.code = i_citing.country
	);

	COPY (
		SELECT * FROM most_cited
	) TO '/tmp/most_cited.csv' WITH CSV DELIMITER ',' HEADER;
	RAISE NOTICE 'Saved most cited patents ranking';
	
	copy_edges_stat := format(
	'COPY(
		SELECT Source, Target, Weight, Weight AS Label
		FROM cited_countries
	) TO ''/tmp/edges_most_cited_top%s.csv'' WITH CSV DELIMITER '','' HEADER', top::text);
	EXECUTE copy_edges_stat;

	copy_nodes_stat := format(
		'COPY(
		SELECT DISTINCT Idc.id as Id, idc.country AS Label
		FROM
		(SELECT Source AS id, citing_country AS country
		FROM cited_countries
		UNION 
		SELECT Target AS Id, cited_country AS country
		FROM cited_countries) AS idc
		) TO ''/tmp/nodes_most_cited_top_%s.csv'' WITH CSV DELIMITER '','' HEADER', top::text);
	EXECUTE copy_nodes_stat;
	RAISE NOTICE '%', copy_nodes_stat;

	DROP TABLE most_cited;
	DROP TABLE cited_countries;
	DROP TABLE patent_max_inventor;
	
END; $$ LANGUAGE plpgsql;
-- save_citations_graph(start_year integer, delta integer, threshold integer, step integer)
SELECT save_most_cited(15);

