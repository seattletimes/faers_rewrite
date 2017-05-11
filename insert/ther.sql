DROP TABLE IF EXISTS ther;

CREATE TABLE ther (
  primaryid TEXT,
  caseid TEXT,
  dsg_drug_seq TEXT,
  start_dt TEXT,
  end_dt TEXT,
  dur TEXT,
  dur_cod TEXT
);

\COPY ther FROM 'scratch/combined/ther.csv' WITH CSV;