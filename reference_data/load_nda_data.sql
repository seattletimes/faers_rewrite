DROP TABLE IF EXISTS nda;

CREATE TABLE nda
(
  ingredient TEXT,
  dfroute TEXT,
  trade_name TEXT,
  applicant TEXT,
  strength TEXT,
  appl_type TEXT,
  appl_no TEXT,
  product_no TEXT,
  te_code TEXT,
  approval_date TEXT,
  rld TEXT,
  rs TEXT,
  type TEXT,
  applicant_full_name TEXT
);

TRUNCATE nda;

CREATE INDEX ingredient_ix ON nda(appl_no, ingredient);

\COPY nda FROM 'products.txt' WITH DELIMITER E'~' CSV HEADER QUOTE E'\b' ;

ALTER TABLE nda ADD COLUMN drug_form TEXT;
ALTER TABLE nda ADD COLUMN route TEXT;
UPDATE nda SET drug_form = substring(dfroute FROM '(.*);');
UPDATE nda SET route = substring(dfroute FROM ';(.*)');