DROP TABLE IF EXISTS drug;

CREATE TABLE drug (
  primaryid TEXT,
  caseid TEXT,
  drug_seq TEXT,
  role_cod TEXT,
  drugname TEXT,
  prod_ai TEXT,
  val_vbm TEXT,
  route TEXT,
  dose_vbm TEXT,
  cum_dose_chr TEXT,
  cum_dose_unit TEXT,
  dechal TEXT,
  rechal TEXT,
  lot_num TEXT,
  exp_dt TEXT,
  nda_num TEXT,
  dose_amt TEXT,
  dose_unit TEXT,
  dose_form TEXT,
  dose_freq TEXT
);

\COPY drug FROM 'scratch/combined/drug.csv' WITH CSV;