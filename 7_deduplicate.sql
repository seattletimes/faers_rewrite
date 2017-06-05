/*

This file is based on work by LTS Computing, available from
https://github.com/ltscomputingllc/faersdbstats

Ideally, at this point in the FAERS process, we should be able to use their
code (previous steps did much of the normalization in Node, instead of via
shell scripts and SQL). For this reason, I've preserved their table names
(although I do reserve the right to refactor into more descriptive names in
the future). However, I've taken the liberties of adding comments and cleaning
up some of the odder "features" of the LTS code, such as the use of a
separate, non-public schema for all tables.

*/

/*

These next few queries create a set of lookup tables for the current data
mapping primary IDs to drug names and reactions, with the mapped list combined
into a pipe-delimited string. Those tables are then used to combine the drug
names and reactions into the demographic data, keyed on the case ID.

*/

\echo 'Creating drug/reaction name lists...'

DROP TABLE IF EXISTS drugname_list;
CREATE TABLE drugname_list AS (
  SELECT
    primaryid,
    UPPER(STRING_AGG(drugname, '|' ORDER BY drugname)) AS drugname_list
  FROM drug
  GROUP BY primaryid
);

DROP TABLE IF EXISTS reac_pt_list;
CREATE TABLE reac_pt_list AS (
  SELECT
    primaryid,
    UPPER(STRING_AGG(pt, '|' ORDER BY pt)) AS reac_pt_list
  FROM reac
  GROUP BY primaryid
);

DROP TABLE IF EXISTS casedemo;
CREATE TABLE casedemo AS (
  SELECT
    caseid,
    caseversion,
    i_f_code,
    event_dt,
    age,
    sex,
    reporter_country,
    demo.primaryid,
    drugname_list,
    reac_pt_list,
    fda_dt
  FROM demo
  LEFT OUTER JOIN drugname_list ON demo.primaryid = drugname_list.primaryid
  LEFT OUTER JOIN reac_pt_list ON demo.primaryid = reac_pt_list.primaryid
);

/*

Now let's do the same thing for the legacy database, but using ISR (Individual
Safety Report) number instead of primary ID or case ID.

FDA FAQ on the legacy->current transition:

https://www.fda.gov/downloads/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM350394.pdf

*/

\echo 'Creating drugname/reaction lists for legacy data...'

DROP TABLE IF EXISTS drugname_legacy_list;
CREATE TABLE drugname_legacy_list AS (
  SELECT
    isr,
    UPPER(STRING_AGG(drugname, '|' ORDER BY drugname)) AS drugname_list
  FROM drug_legacy
  GROUP BY isr
);

DROP TABLE IF EXISTS reac_pt_legacy_list;
CREATE TABLE reac_pt_legacy_list AS (
  SELECT
    isr,
    UPPER(STRING_AGG(pt, '|' ORDER BY pt)) AS reac_pt_list
  FROM reac_legacy
  GROUP BY isr
);

DROP TABLE IF EXISTS casedemo_legacy;
CREATE TABLE casedemo_legacy AS (
  SELECT
    "case", --quoted because it's a keyword, thanks FDA
    i_f_cod,
    event_dt,
    age,
    gndr_cod,
    reporter_country,
    demo_legacy.isr,
    drugname_list,
    reac_pt_list,
    fda_dt
  FROM demo_legacy
  LEFT OUTER JOIN drugname_legacy_list ON demo_legacy.isr = drugname_legacy_list.isr
  LEFT OUTER JOIN reac_pt_legacy_list ON demo_legacy.isr = reac_pt_legacy_list.isr
);

/*

Now we combine the legacy and current casedemo tables into a single table,
normalizing the column names between the two. We add the country code to the
legacy data using the lookup table created during step one of this long
process.

*/

\echo 'Combining legacy and current data...'

DROP TABLE IF EXISTS all_casedemo;
CREATE TABLE all_casedemo AS (
  SELECT
    'FAERS' AS database,
    caseid,
    CAST(null AS TEXT) AS isr,
    caseversion,
    i_f_code,
    event_dt,
    age,
    sex,
    reporter_country,
    primaryid,
    drugname_list,
    reac_pt_list,
    fda_dt,
    null AS imputed_field_name
  FROM casedemo
) UNION (
  SELECT
    'LAERS' AS database,
    "case" AS caseid,
    isr,
    CAST(0 AS TEXT) AS caseversion,
    i_f_cod AS i_f_code,
    event_dt,
    age,
    gndr_cod AS sex,
    country_code.country_code AS reporter_country,
    cast("case" || 0 AS TEXT) AS primaryid,
    drugname_list,
    reac_pt_list,
    fda_dt,
    null AS imputed_field_name
    FROM casedemo_legacy
    LEFT OUTER JOIN country_code ON UPPER(casedemo_legacy.reporter_country) = UPPER(country_code.country_name)
);


/*

Now we're going to start filling in information where it may be missing from
other reports (imputation). Basically, we create a table containing the rows
where all the information is filled in. Then, we find the rows where they have
all but one field filled in, look for a row that matches all the fields in the
completed table, and use that to fill in the single missing value. In theory,
this ensures that we're only completing data when we have sufficient
information to disambiguate a row convincingly. 

All told, this process involves two queries per imputation: one to create the
table of complete values with the most recent version of the missing field,
and one to fill in from that table. Each of the filler queries also sets an
`imputed_field` column, so that we can track which updates came from where. It
might be a good idea to filter out non-null `imputed_field` rows from the
first query, so that we don't use imputed rows to fill in further rows, but
the original script doesn't do that and I'm not sure how much it matters.

Needless to say, this part of the script is really repetitive, and ideally
should be machine generated in the future.

Start with the `event_dt` column:

*/

\echo 'Imputing on event_dt...'

DROP TABLE IF EXISTS default_all_casedemo_event_dt_keys;
CREATE TABLE default_all_casedemo_event_dt_keys AS (
  SELECT
    caseid,
    age,
    sex,
    reporter_country,
    MAX(event_dt) AS default_event_dt
  FROM all_casedemo
  WHERE
    caseid IS NOT NULL AND
    event_dt IS NOT NULL AND
    age IS NOT NULL AND
    sex IS NOT NULL AND
    reporter_country IS NOT NULL
  GROUP BY caseid, age, sex, reporter_country
);

UPDATE all_casedemo
  SET
    event_dt = default_event_dt,
    imputed_field_name = 'event_dt'
  FROM default_all_casedemo_event_dt_keys AS d
  WHERE 
    all_casedemo.caseid = d.caseid AND
    all_casedemo.age = d.age AND
    all_casedemo.sex = d.sex AND
    all_casedemo.reporter_country = d.reporter_country AND
    all_casedemo.caseid IS NOT NULL AND
    all_casedemo.age IS NOT NULL AND
    all_casedemo.sex IS NOT NULL AND
    all_casedemo.reporter_country IS NOT NULL AND
    all_casedemo.event_dt IS NULL;

/*

Age column:

*/

\echo 'Imputing on age...'

DROP TABLE IF EXISTS default_all_casedemo_age_keys;
CREATE TABLE default_all_casedemo_age_keys AS (
  SELECT
    caseid,
    event_dt,
    sex,
    reporter_country,
    MAX(age) AS default_age
  FROM all_casedemo
  WHERE
    caseid IS NOT NULL AND
    event_dt IS NOT NULL AND
    age IS NOT NULL AND
    sex IS NOT NULL AND
    reporter_country IS NOT NULL
  GROUP BY caseid, event_dt, sex, reporter_country
);

UPDATE all_casedemo
  SET
    age = default_age,
    imputed_field_name = 'age'
  FROM default_all_casedemo_age_keys AS d
  WHERE
    all_casedemo.caseid = d.caseid AND
    all_casedemo.event_dt = d.event_dt AND
    all_casedemo.sex = d.sex AND
    all_casedemo.reporter_country = d.reporter_country AND
    all_casedemo.caseid IS NOT NULL AND
    all_casedemo.event_dt IS NOT NULL AND
    all_casedemo.sex IS NOT NULL AND
    all_casedemo.reporter_country IS NOT NULL AND
    all_casedemo.age IS NULL;

/*

Sex column:

*/

\echo 'Imputing on sex...'

DROP TABLE IF EXISTS default_all_casedemo_sex_keys;
CREATE TABLE default_all_casedemo_sex_keys AS (
  SELECT
    caseid,
    event_dt,
    age,
    reporter_country,
    MAX(sex) AS default_sex --max? Seems odd.
  FROM all_casedemo
  WHERE
    caseid IS NOT NULL AND
    event_dt IS NOT NULL AND
    age IS NOT NULL AND
    sex IS NOT NULL AND
    reporter_country IS NOT NULL
  GROUP BY caseid, event_dt, age, reporter_country
);

UPDATE all_casedemo
  SET
    sex = default_sex,
    imputed_field_name = 'sex'
  FROM default_all_casedemo_sex_keys d
  WHERE
    all_casedemo.caseid = d.caseid AND
    all_casedemo.event_dt = d.event_dt AND
    all_casedemo.age = d.age AND
    all_casedemo.reporter_country = d.reporter_country AND
    all_casedemo.caseid IS NOT NULL AND
    all_casedemo.event_dt IS NOT NULL AND
    all_casedemo.age IS NOT NULL AND
    all_casedemo.reporter_country IS NOT NULL AND
    all_casedemo.sex IS NULL;

/*

Reporter country column:

*/

\echo 'Imputing on country'

DROP TABLE IF EXISTS default_all_casedemo_reporter_country_keys;
CREATE TABLE default_all_casedemo_reporter_country_keys AS (
  SELECT
    caseid,
    event_dt,
    age,
    sex,
    MAX(reporter_country) AS default_reporter_country
  FROM all_casedemo
  WHERE
    caseid IS NOT NULL AND
    event_dt IS NOT NULL AND
    age IS NOT NULL AND
    sex IS NOT NULL AND
    reporter_country IS NOT NULL
  GROUP BY caseid, event_dt, age, sex
);

UPDATE all_casedemo
  SET
    reporter_country = default_reporter_country,
    imputed_field_name = 'reporter_country'
  FROM default_all_casedemo_reporter_country_keys AS d
  WHERE
    all_casedemo.caseid = d.caseid AND
    all_casedemo.event_dt = d.event_dt AND
    all_casedemo.age = d.age AND
    all_casedemo.sex = d.sex AND
    all_casedemo.caseid IS NOT NULL AND
    all_casedemo.event_dt IS NOT NULL AND
    all_casedemo.age IS NOT NULL AND
    all_casedemo.sex IS NOT NULL AND
    all_casedemo.reporter_country IS NULL;

/*

Finally, let's deduplicate the data. We'll create a table containing the
latest row for each caseid from all_casedemo using the partition subquery.
Then we'll take that table, partition on the combination of all fields for
completed rows, select the top row from those partitions (to get the subset of
unique completed rows) and merge that with the remaining partly-null rows.

The resulting table is a list of all the unique records by case ID, primary
ID, and ISR, but no data otherwise. In order to use it, we must match it up
against the other tables to actually get reporting data.

*/

\echo 'Deduplicating data...'

DROP TABLE IF EXISTS unique_all_casedemo;
CREATE TABLE unique_all_casedemo AS (
  SELECT
    --get just the data, not partition row number from the subquery
    database,
    caseid,
    isr,
    caseversion,
    i_f_code,
    event_dt,
    age,
    sex,
    reporter_country,
    primaryid,
    drugname_list,
    reac_pt_list,
    fda_dt
  FROM (
    --split the data into caseid partitions and identify the row number for each one
    SELECT
      *,
      ROW_NUMBER() OVER(
        PARTITION BY caseid
        ORDER BY primaryid DESC, database DESC, fda_dt DESC, i_f_code, isr DESC
      ) AS row_num
    FROM all_casedemo
  ) AS a
  -- get the topmost row number for each caseid partition
  WHERE a.row_num = 1
);

DROP TABLE IF EXISTS unique_all_case;
CREATE TABLE unique_all_case AS (
  SELECT
    caseid,
    CASE 
      WHEN isr IS NOT NULL THEN null ELSE primaryid END
      AS primaryid,
    isr
  FROM (
    SELECT
      caseid,
      primaryid,
      isr,
      ROW_NUMBER() OVER(
        PARTITION BY event_dt, age, sex, reporter_country, drugname_list, reac_pt_list
        ORDER BY primaryid DESC, database DESC, fda_dt DESC, i_f_code, isr DESC
      ) AS row_num
    FROM unique_all_casedemo
    WHERE
      caseid IS NOT NULL AND
      event_dt IS NOT NULL AND
      age IS NOT NULL AND
      sex IS NOT NULL AND
      reporter_country IS NOT NULL AND
      drugname_list IS NOT NULL AND
      reac_pt_list IS NOT NULL
  ) AS a
  WHERE a.row_num = 1
  UNION (
    SELECT
      caseid,
      CASE
        WHEN isr IS NOT NULL THEN null ELSE primaryid END
        AS primaryid,
      isr
    FROM unique_all_casedemo
    WHERE
      caseid IS NULL OR
      event_dt IS NULL OR
      age IS NULL OR
      sex IS NULL OR
      reporter_country IS NULL OR
      drugname_list IS NULL OR
      reac_pt_list IS NULL
  )
);