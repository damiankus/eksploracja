DROP TABLE cite;
DROP TABLE ainventor;
DROP TABLE patents;
DROP TABLE class_match;
DROP TABLE subcategories;
DROP TABLE classes;
DROP TABLE states;
DROP TABLE countries;
DROP TABLE cusip_match;
DROP TABLE aconame;

CREATE DATABASE patents;

CREATE TABLE IF NOT EXISTS aconame (
    ASSIGNEE BIGINT UNIQUE,
    COMPNAME VARCHAR(256),
    PRIMARY KEY(ASSIGNEE)
);

CREATE TABLE IF NOT EXISTS cusip_match (
    ASSIGNEE BIGINT REFERENCES aconame(ASSIGNEE),
    ASSNAME VARCHAR(128),
    CNAME VARCHAR(128),
    CUSIP VARCHAR(64),
    OWN BIGINT,
    PNAME VARCHAR(128),
    SNAME VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS countries (
    CODE CHAR(2) UNIQUE,
    COUNTRY VARCHAR(128),
    PRIMARY KEY(CODE)
);

CREATE TABLE IF NOT EXISTS states (
    CODE CHAR(2) UNIQUE,
    STATE VARCHAR(128),
    PRIMARY KEY(CODE)
);

CREATE TABLE IF NOT EXISTS classes (
    CLASS INT UNIQUE,
    TITLE VARCHAR(256),
    PRIMARY KEY(CLASS)
);

CREATE TABLE IF NOT EXISTS subcategories (
    CAT INT,
    SUBCAT INT UNIQUE, 
    SUBCATNAME VARCHAR(128),
    CATNAMESHORT VARCHAR(64),
    CATNAMELONG VARCHAR(256),
    PRIMARY KEY(SUBCAT)
);

CREATE TABLE IF NOT EXISTS class_match (
    CLASS INT REFERENCES classes(CLASS),
    SUBCAT INT REFERENCES subcategories(SUBCAT), 
    CAT INT
);

CREATE TABLE IF NOT EXISTS patents (
    PATENT BIGINT,
    GYEAR INT,
    GDATE INT,
    APPYEAR INT,
    COUNTRY CHAR(2) REFERENCES countries(CODE),
    POSTATE CHAR(2),
    ASSIGNEE BIGINT REFERENCES aconame(ASSIGNEE),
    ASSCODE BIGINT,
    CLAIMS VARCHAR(32),
    NCLASS INT REFERENCES classes(CLASS),
    CAT INT,
    SUBCAT INT REFERENCES subcategories(SUBCAT),
    CMADE VARCHAR(32),
    CRECEIVE INT,
    RATIOCIT VARCHAR(32),
    GENERAL VARCHAR(32),
    ORIGINAL VARCHAR(32),
    FWDAPLAG VARCHAR(32),
    BCKGTLAG VARCHAR(32),
    SELFCTUB VARCHAR(32),
    SELFCTLB VARCHAR(32),
    SECDUPBD VARCHAR(32),
    SECDLWBD VARCHAR(32),

    PRIMARY KEY(PATENT)
);

CREATE TABLE IF NOT EXISTS ainventor(
    PATENT BIGINT REFERENCES patents(PATENT),
    LASTNAM VARCHAR(128),
    FIRSTNAME VARCHAR(128),
    MIDNAM VARCHAR(128),
    MODIFNAM VARCHAR(128),
    STREET VARCHAR(256),
    CITY VARCHAR(128),
    POSTATE VARCHAR(2),
    COUNTRY CHAR(2),
    ZIP VARCHAR(64),
    INVSEQ INT
);

CREATE TABLE IF NOT EXISTS cite (
 CITING BIGINT,
 CITED BIGINT
);


-- IN CASE OF DENIAL OF ACCESS TO FILES RUN chmod a+rX filename
-- ERRORS IN ACONAME FILE : 703089, 722814, 

COPY aconame FROM '/home/damian/Dokumenty/studia/eksploracja/dataset/csv/aconame.csv' WITH CSV HEADER;
COPY cusip_match FROM '/home/damian/Dokumenty/studia/eksploracja/dataset/csv/match.csv' WITH CSV HEADER;
COPY countries FROM '/home/damian/Dokumenty/studia/eksploracja/dataset/csv/list_of_countries.csv' WITH CSV HEADER;
COPY states FROM '/home/damian/Dokumenty/studia/eksploracja/dataset/csv/list_of_states.csv' WITH CSV HEADER;
COPY classes FROM '/home/damian/Dokumenty/studia/eksploracja/dataset/csv/list_of_classes.csv' WITH CSV HEADER;
COPY subcategories FROM '/home/damian/Dokumenty/studia/eksploracja/dataset/csv/subcategories.csv' WITH CSV HEADER;
COPY class_match FROM '/home/damian/Dokumenty/studia/eksploracja/dataset/csv/class_match.csv' WITH CSV HEADER;
COPY patents FROM '/home/damian/Dokumenty/studia/eksploracja/dataset/csv/apat63_99.csv' WITH CSV HEADER;
COPY ainventor FROM '/home/damian/Dokumenty/studia/eksploracja/dataset/csv/ainventor.csv' WITH CSV HEADER;
COPY cite FROM '/home/damian/Dokumenty/studia/eksploracja/dataset/csv/cite75_99.csv' WITH CSV HEADER;

select * from patents limit 100;
select * from cite limit 100;