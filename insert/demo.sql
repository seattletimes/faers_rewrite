DROP TABLE IF EXISTS demo;

CREATE TABLE demo (
  primaryid TEXT,
  caseid TEXT,
  caseversion TEXT,
  i_f_code TEXT,
  event_dt TEXT,
  mfr_dt TEXT,
  init_fda_dt TEXT,
  fda_dt TEXT,
  rept_cod TEXT,
  auth_num TEXT,
  mfr_num TEXT,
  mfr_sndr TEXT,
  lit_ref TEXT,
  age TEXT,
  age_cod TEXT,
  age_grp TEXT,
  sex TEXT,
  e_sub TEXT,
  wt TEXT,
  wt_cod TEXT,
  rept_dt TEXT,
  to_mfr TEXT,
  occp_cod TEXT,
  reporter_country TEXT,
  occr_country TEXT
);

\COPY demo FROM 'scratch/combined/demo.csv' WITH CSV;