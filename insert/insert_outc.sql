DROP TABLE IF EXISTS outc;

CREATE TABLE outc (
  primaryid TEXT,
  caseid TEXT,
  outc_cod TEXT
);

\COPY outc FROM 'scratch/combined/outc.csv' WITH CSV;