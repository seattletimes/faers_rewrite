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

------------------------------

-- perform single imputation of missing 'key' demographic fields for multiple reports within the same case across all the legacy and current data

-- create table of default demo event_dt key value for each case where all the key fields are populated on at least one report for that case
drop table if exists default_all_casedemo_event_dt_keys; 
create table default_all_casedemo_event_dt_keys as 
select caseid, age, sex, reporter_country, max(event_dt) as default_event_dt
from all_casedemo 
where caseid is not null and event_dt is not null and age is not null and sex is not null and reporter_country is not null
group by caseid, age, sex, reporter_country;

-- single imputation of missing event_dt 
update all_casedemo a
set event_dt = default_event_dt, imputed_field_name = 'event_dt' 
from default_all_casedemo_event_dt_keys d
where a.caseid = d.caseid and a.age = d.age and a.sex = d.sex and a.reporter_country = d.reporter_country
and a.caseid is not null and a.event_dt is null and a.age is not null and a.sex is not null and a.reporter_country is not null;

-- create table of default demo age key value for each case where all the key fields are populated on at least one report for that case
drop table if exists default_all_casedemo_age_keys; 
create table default_all_casedemo_age_keys as 
select caseid, event_dt, sex, reporter_country, max(age) as default_age
from all_casedemo 
where caseid is not null and event_dt is not null and age is not null and sex is not null and reporter_country is not null
group by caseid, event_dt, sex, reporter_country;

-- single imputation of missing age 
update all_casedemo a 
set age = default_age, imputed_field_name = 'age'  
from default_all_casedemo_age_keys d
where a.caseid = d.caseid and a.event_dt = d.event_dt and a.sex = d.sex and a.reporter_country = d.reporter_country
and a.caseid is not null and a.event_dt is not null and a.age is null and a.sex is not null and a.reporter_country is not null;

-- create table of default demo gender key value for each case where all the key fields are populated on at least one report for that case
drop table if exists default_all_casedemo_sex_keys; 
create table default_all_casedemo_sex_keys as 
select caseid, event_dt, age, reporter_country, max(sex) as default_sex
from all_casedemo 
where caseid is not null and event_dt is not null and age is not null and sex is not null and reporter_country is not null
group by caseid, event_dt, age, reporter_country;

-- single imputation of missing gender
update all_casedemo a 
set sex = default_sex, imputed_field_name = 'sex' 
from default_all_casedemo_sex_keys d
where a.caseid = d.caseid and a.event_dt = d.event_dt and a.age = d.age and a.reporter_country = d.reporter_country
and a.caseid is not null and a.event_dt is not null and a.age is not null and a.sex is null and a.reporter_country is not null;

-- create table of default demo reporter_country key value for each case where all the key fields are populated on at least one report for that case
drop table if exists default_all_casedemo_reporter_country_keys; 
create table default_all_casedemo_reporter_country_keys as 
select caseid, event_dt, age, sex, max(reporter_country) as default_reporter_country
from all_casedemo 
where caseid is not null and event_dt is not null and age is not null and sex is not null and reporter_country is not null
group by caseid, event_dt, age, sex;

-- single imputation of missing reporter_country
update all_casedemo a
set reporter_country = default_reporter_country, imputed_field_name = 'reporter_country'  
from default_all_casedemo_reporter_country_keys d
where a.caseid = d.caseid and a.event_dt = d.event_dt and a.age = d.age and a.sex = d.sex
and a.caseid is not null and a.event_dt is not null and a.age is not null and a.sex is not null and a.reporter_country is null;

------------------------------

-- get the latest case row for each case across both the legacy LAERS and current FAERS data based on CASE ID
drop table if exists unique_all_casedemo;
create table unique_all_casedemo as
select database, caseid, isr, caseversion, i_f_code, event_dt, age, sex, reporter_country, primaryid, drugname_list, reac_pt_list, fda_dt
from (
select *, 
row_number() over(partition by caseid order by primaryid desc, database desc, fda_dt desc, i_f_code, isr desc) as row_num 
from all_casedemo 
) a where a.row_num = 1;

-- remove any duplicates based on fully populated matching demographic key fields and exact match on list of drugs and list of outcomes (FAERS reactions)
-- NOTE. when using this table for subsequent joins in the ETL process, join to FAERS data using primaryid and join to LAERS data using isr
drop table if exists unique_all_case;   
create table unique_all_case as
select caseid, case when isr is not null then null else primaryid end as primaryid, isr 
from (
  select caseid, primaryid,isr, 
  row_number() over(partition by event_dt, age, sex, reporter_country, drugname_list, reac_pt_list order by primaryid desc, database desc, fda_dt desc, i_f_code, isr desc) as row_num 
  from unique_all_casedemo 
  where caseid is not null and event_dt is not null and age is not null and sex is not null and reporter_country is not null and drugname_list is not null and reac_pt_list is not null
) a where a.row_num = 1
union 
select caseid, case when isr is not null then null else primaryid end as primaryid, isr 
from unique_all_casedemo 
where caseid is null or event_dt is null or age is null or sex is null or reporter_country is null or drugname_list is null or reac_pt_list is null;
*/