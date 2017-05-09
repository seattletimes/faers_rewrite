DROP TABLE IF EXISTS country_code;
CREATE TABLE country_code
(
  country_name TEXT,
  country_code TEXT
);
TRUNCATE country_code;

\COPY country_code FROM 'ISO_3166-1_country_codes.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'"';

--# Insert non-standard country names to country code mappings based on analysis of the reporter_country field in the legacy and current data
INSERT INTO country_code VALUES('ALAND ISLANDS', 'AX');
INSERT INTO country_code VALUES('BOLIVIA','BO');
INSERT INTO country_code VALUES('BOSNIA AND HERZEGOWINA','BA');
INSERT INTO country_code VALUES('CAPE VERDE','CV');
INSERT INTO country_code VALUES('CONGO, THE DEMOCRATIC REPUBLIC OF THE','CD');
INSERT INTO country_code VALUES('COTE D''IVOIRE','CI');
INSERT INTO country_code VALUES('CROATIA (local name: Hrvatska)','HR');
INSERT INTO country_code VALUES('CURACAO','CW');
INSERT INTO country_code VALUES('European Union','??');
INSERT INTO country_code VALUES('FRANCE, METROPOLITAN', 'FR');
INSERT INTO country_code VALUES('KOREA, DEMOCRATIC PEOPLE''S REPUBLIC OF','KP');
INSERT INTO country_code VALUES('KOREA, REPUBLIC OF','KR');
INSERT INTO country_code VALUES('LIBYAN ARAB JAMAHIRIYA','LY');
INSERT INTO country_code VALUES('MACAU','MO');
INSERT INTO country_code VALUES('MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF','MK');
INSERT INTO country_code VALUES('MICRONESIA, FEDERATED STATES OF','FM');
INSERT INTO country_code VALUES('MOLDOVA, REPUBLIC OF','MD');
INSERT INTO country_code VALUES('NETHERLANDS ANTILLES','AN');
INSERT INTO country_code VALUES('NETHERLANDS ANTILLES (retired code)','AN');
INSERT INTO country_code VALUES('PALESTINIAN TERRITORY, OCCUPIED','PS');
INSERT INTO country_code VALUES('REUNION','RE');
INSERT INTO country_code VALUES('SERBIA AND MONTENEGRO','CS');
INSERT INTO country_code VALUES('SERBIA AND MONTENEGRO (see individual countries)','CS');
INSERT INTO country_code VALUES('SLOVAKIA (Slovak Republic)','SK');
INSERT INTO country_code VALUES('SVALBARD AND JAN MAYEN ISLANDS','SJ');
INSERT INTO country_code VALUES('UNITED KINGDOM','GB');
INSERT INTO country_code VALUES('UNITED STATES','US');
INSERT INTO country_code VALUES('VATICAN CITY STATE (HOLY SEE)','VA');
INSERT INTO country_code VALUES('VENEZUELA','VE');
INSERT INTO country_code VALUES('WALLIS AND FUTUNA ISLANDS', 'WF');
INSERT INTO country_code VALUES('YUGOSLAVIA','YU');
INSERT INTO country_code VALUES('ZAIRE','CD');
