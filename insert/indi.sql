DROP TABLE IF EXISTS indi;

CREATE TABLE indi (
  primaryid TEXT,
  caseid TEXT,
  indi_drug_seq TEXT,
  indi_pt TEXT
);

\COPY indi FROM 'scratch/combined/indi.csv' WITH CSV;