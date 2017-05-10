DROP TABLE IF EXISTS rpsr;

CREATE TABLE rpsr (
  primaryid TEXT,
  caseid TEXT,
  rpsr_cod TEXT
);

\COPY rpsr FROM 'scratch/combined/rpsr.csv' WITH CSV;