DROP TABLE IF EXISTS indi_legacy;

CREATE TABLE indi_legacy (
  isr TEXT,
  drug_seq TEXT,
  indi_pt TEXT
);

\COPY indi_legacy FROM 'scratch/combined/indi_legacy.csv' WITH CSV;