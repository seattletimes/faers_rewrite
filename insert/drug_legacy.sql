DROP TABLE IF EXISTS drug_legacy;

CREATE TABLE drug_legacy (
  isr TEXT,
  drug_seq TEXT,
  role_cod TEXT,
  drugname TEXT,
  val_vbm TEXT,
  route TEXT,
  dose_vbm TEXT,
  dechal TEXT,
  rechal TEXT,
  lot_num TEXT,
  exp_dt TEXT,
  nda_num TEXT
);

\COPY drug_legacy FROM 'scratch/combined/drug_legacy.csv' WITH CSV;