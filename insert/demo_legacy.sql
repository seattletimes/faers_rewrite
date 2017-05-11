DROP TABLE IF EXISTS demo_legacy;

CREATE TABLE demo_legacy (
  isr TEXT,
  "case" TEXT,
  i_f_cod TEXT,
  foll_seq TEXT,
  image TEXT,
  event_dt TEXT,
  mfr_dt TEXT,
  fda_dt TEXT,
  rept_cod TEXT,
  mfr_num TEXT,
  mfr_sndr TEXT,
  age TEXT,
  age_cod TEXT,
  gndr_cod TEXT,
  e_sub TEXT,
  wt TEXT,
  wt_cod TEXT,
  rept_dt TEXT,
  occp_cod TEXT,
  death_dt TEXT,
  to_mfr TEXT,
  confid TEXT,
  reporter_country TEXT
);

\COPY demo_legacy FROM 'scratch/combined/demo_legacy.csv' WITH CSV;