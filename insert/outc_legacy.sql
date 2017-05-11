DROP TABLE outc_legacy;

CREATE TABLE outc_legacy (
  isr TEXT,
  outc_cod TEXT
);

\COPY outc_legacy FROM 'scratch/combined/outc_legacy.csv' WITH CSV;