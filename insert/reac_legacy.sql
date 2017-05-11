DROP TABLE IF EXISTS reac_legacy;

CREATE TABLE reac_legacy (
  isr TEXT,
  pt TEXT
);

\COPY reac_legacy FROM 'scratch/combined/reac_legacy.csv' WITH CSV;