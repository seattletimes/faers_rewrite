DROP TABLE IF EXISTS ther_legacy;

CREATE TABLE ther_legacy (
  isr TEXT,
  drug_seq TEXT,
  start_dt TEXT,
  end_dt TEXT,
  dur TEXT,
  dur_cod TEXT
);

\COPY ther_legacy FROM 'scratch/combined/ther_legacy.csv' WITH CSV;