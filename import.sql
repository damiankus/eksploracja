DROP TABLE inv_tmp;
DROP TABLE unique_inventors;
DROP TABLE patent_inventor;
DROP TABLE citations;
DROP TABLE inventors;
DROP TABLE patents;
DROP TABLE class_match;
DROP TABLE subcategories;
DROP TABLE classes;
DROP TABLE states;
DROP TABLE countries;
DROP TABLE cusip_match;
DROP TABLE companies;

CREATE DATABASE patents;

CREATE TABLE IF NOT EXISTS companies (
    ASSIGNEE NUMERIC(12) PRIMARY KEY,
    COMPNAME CHAR(80)
);

CREATE TABLE IF NOT EXISTS cusip_match (
    ASSIGNEE BIGINT REFERENCES companies(ASSIGNEE),
    ASSNAME CHAR(128),
    CNAME CHAR(128),
    CUSIP CHAR(64),
    OWN NUMERIC(12),
    PNAME CHAR(128),
    SNAME CHAR(128)
);

CREATE TABLE IF NOT EXISTS countries (
    CODE CHAR(2) PRIMARY KEY,
    COUNTRY VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS states (
    CODE CHAR(2) PRIMARY KEY,
    STATE CHAR(128)
);

CREATE TABLE IF NOT EXISTS classes (
    CLASS NUMERIC(12) PRIMARY KEY,
    TITLE CHAR(256)
);

CREATE TABLE IF NOT EXISTS subcategories (
--  CAT SUBCAT NUMERIC(12),
    SUBCAT NUMERIC(12) PRIMARY KEY, 
    SUBCATNAME VARCHAR(128),
    CATNAMESHORT VARCHAR(64),
    CATNAMELONG VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS class_match (
    CLASS NUMERIC(12) REFERENCES classes(CLASS),
    SUBCAT NUMERIC(12) REFERENCES subcategories(SUBCAT), 
    CAT NUMERIC(12)
);

CREATE TABLE IF NOT EXISTS patents (
    PATENT NUMERIC(7) PRIMARY KEY,
    GYEAR NUMERIC(12),
    GDATE NUMERIC(12),
    APPYEAR NUMERIC(12),
    COUNTRY CHAR(2) REFERENCES countries(CODE),
    POSTATE CHAR(2),
    ASSIGNEE NUMERIC(12) REFERENCES companies(ASSIGNEE),
    ASSCODE NUMERIC(12),
    CLAIMS NUMERIC(12),
    NCLASS NUMERIC(12) REFERENCES classes(CLASS), -- DELETE
    CAT NUMERIC(12), -- DELETE
    SUBCAT NUMERIC(12) REFERENCES subcategories(SUBCAT), --
    CMADE NUMERIC(12),
    CRECEIVE NUMERIC(12),
    RATIOCIT NUMERIC(6),
    GENERAL NUMERIC(6),
    ORIGINAL NUMERIC(6),
    FWDAPLAG NUMERIC(7),
    BCKGTLAG NUMERIC(8),
    SELFCTUB NUMERIC(6),
    SELFCTLB NUMERIC(6),
    SECDUPBD NUMERIC(6),
    SECDLWBD NUMERIC(6)
);

CREATE TABLE IF NOT EXISTS inventors(
--  REPLACE PATENT WITH ID
--  ADD TABLE 1:n inventor ID -> n x patents.PATENT

    ID SERIAL PRIMARY KEY,
    PATENT NUMERIC(7) REFERENCES patents(PATENT),
    LASTNAM CHAR(20),
    FIRSTNAME CHAR(15),
    MIDNAM CHAR(9),
    MODIFNAM CHAR(3),
    STREET CHAR(30),
    CITY CHAR(20),
    POSTATE CHAR(2),
    COUNTRY CHAR(2),
    ZIP CHAR(5),
    INVSEQ NUMERIC(12) 
);

CREATE INDEX inventors_lastname_hash_idx ON inventors USING hash (lastnam);

CREATE TABLE IF NOT EXISTS citations (
 CITING NUMERIC(7),
 CITED NUMERIC(7)
);

CREATE TABLE IF NOT EXISTS patent_inventor (
  ID SERIAL PRIMARY KEY,
  PATENT_ID NUMERIC(7) REFERENCES patents(PATENT),
  INVENTOR_ID INT REFERENCES inventors(ID),
  FIRSTNAME CHAR(15),
  LASTNAM CHAR(20),
  CITY CHAR(20)
);

-- IN CASE OF DENIAL OF ACCESS TO FILES RUN chmod a+rX filename
-- ERRORS IN companies FILE : 703089, 722814, 

COPY companies FROM '/home/damian/Dokumenty/eksploracja/dataset/csv/aconame.csv' WITH CSV HEADER;
COPY cusip_match FROM '/home/damian/Dokumenty/eksploracja/dataset/csv/match.csv' WITH CSV HEADER;
COPY countries FROM '/home/damian/Dokumenty/eksploracja/dataset/csv/list_of_countries.csv' WITH CSV HEADER;
COPY states FROM '/home/damian/Dokumenty/eksploracja/dataset/csv/list_of_states.csv' WITH CSV HEADER;
COPY classes FROM '/home/damian/Dokumenty/eksploracja/dataset/csv/list_of_classes.csv' WITH CSV HEADER;
COPY subcategories FROM '/home/damian/Dokumenty/eksploracja/dataset/csv/subcategories.csv' WITH CSV HEADER;
COPY class_match FROM '/home/damian/Dokumenty/eksploracja/dataset/csv/class_match.csv' WITH CSV HEADER;
COPY patents FROM '/home/damian/Dokumenty/eksploracja/dataset/csv/apat63_99.csv' WITH CSV HEADER;
COPY inventors (PATENT, LASTNAM, FIRSTNAME, MIDNAM, MODIFNAM, STREET, CITY, POSTATE, COUNTRY, ZIP, INVSEQ) FROM '/home/damian/Dokumenty/eksploracja/dataset/csv/ainventor.csv' WITH CSV HEADER;
COPY citations FROM '/home/damian/Dokumenty/eksploracja/dataset/csv/cite75_99.csv' WITH CSV HEADER;

SELECT MAX(id) as id, lastnam, firstname, city
INTO unique_inventors 
FROM inventors
GROUP BY lastnam, firstname, city;

INSERT INTO patent_inventor (PATENT_ID, INVENTOR_ID, FIRSTNAME, LASTNAM, CITY)
SELECT i.PATENT, u.ID, i.FIRSTNAME, i.LASTNAM, i.CITY 
FROM inventors as i
JOIN unique_inventors AS u 
ON (i.lastnam = u.lastnam AND i.firstname = u.firstname AND i.city = u.city);

select * from inventors limit 10;
select * from patent_inventor limit 1000;
select * from patent_inventor where patent_id = 3858241;

SELECT i.* INTO inv_tmp FROM inventors AS i
JOIN unique_inventors AS u
ON i.id = u.id;

ALTER TABLE patent_inventor DROP CONSTRAINT patent_inventor_inventor_id_fkey;
DROP TABLE inventors;
ALTER TABLE inv_tmp RENAME TO inventors;
ALTER TABLE inventors DROP COLUMN PATENT;
ALTER TABLE inventors ADD PRIMARY KEY (id);
ALTER TABLE patent_inventor ADD CONSTRAINT patent_inventor_inventor_id_fkey FOREIGN KEY (inventor_id) REFERENCES inventors(id);
DROP TABLE unique_inventors;

