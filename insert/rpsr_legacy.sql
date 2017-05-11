DROP TABLE IF EXISTS rpsr_legacy;

CREATE TABLE rpsr_legacy (
  isr TEXT,
  rpsr_cod TEXT
);

\COPY rpsr_legacy FROM 'scratch/combined/rpsr_legacy.csv' WITH CSV;