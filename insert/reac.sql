DROP TABLE IF EXISTS reac;

CREATE TABLE reac (
  primaryid TEXT,
  caseid TEXT,
  pt TEXT,
  drug_rec_act TEXT
);

\COPY reac FROM 'scratch/combined/reac.csv' WITH CSV;