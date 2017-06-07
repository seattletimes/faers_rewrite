DROP TABLE IF EXISTS all_drug;
CREATE TABLE all_drug AS (
  SELECT
    drug.*,
    null AS isr
  FROM drug
  UNION (
    SELECT
      drug_legacy.*,
      null AS primaryid,
      null AS caseid,
      null AS prod_ai,
      null AS cum_dose_chr,
      null AS cum_dose_unit,
      null AS dose_amt,
      null AS dose_unit,
      null AS dose_form,
      null AS dose_freq
    FROM drug_legacy
  )
);

DROP TABLE IF EXISTS all_outc;
CREATE TABLE all_outc AS (
  SELECT
    outc.*,
    null AS isr
  FROM outc
  UNION (
    SELECT
      outc_legacy.*,
      null AS primaryid,
      null AS caseid
    FROM outc_legacy
  )
);