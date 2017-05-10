var transform = require("stream").Transform;

var identity = function(columns) {
  var f = row => row;
  f.columns = columns.split("$");
  return f;
};

var translator = function(translate, columns) {
  var f = function(row) {
    for (var key in translate) {
      row[key] = row[key] || row[translate[key]];
    }
    return row;
  };
  f.columns = columns.split("$");
  return f;
};

var demo = translator(
  { sex: "gndr_cod" },
  "primaryid$caseid$caseversion$i_f_code$event_dt$mfr_dt$init_fda_dt$fda_dt$rept_cod$auth_num$mfr_num$mfr_sndr$lit_ref$age$age_cod$age_grp$sex$e_sub$wt$wt_cod$rept_dt$to_mfr$occp_cod$reporter_country$occr_country"
);

var drug = translator(
  { log_num: "lot_nbr" },
  "primaryid$caseid$drug_seq$role_cod$drugname$prod_ai$val_vbm$route$dose_vbm$cum_dose_chr$cum_dose_unit$dechal$rechal$lot_num$exp_dt$nda_num$dose_amt$dose_unit$dose_form$dose_freq"
);

var indi = identity("primaryid$caseid$indi_drug_seq$indi_pt");

var outc = identity("primaryid$caseid$outc_cod");

var reac = identity("primaryid$caseid$pt$drug_rec_act");

var rpsr = identity("primaryid$caseid$rpsr_cod");

var ther = identity("primaryid$caseid$dsg_drug_seq$start_dt$end_dt$dur$dur_cod");

// the legacy cleaners

var demo_legacy = identity("ISR$CASE$I_F_COD$FOLL_SEQ$IMAGE$EVENT_DT$MFR_DT$FDA_DT$REPT_COD$MFR_NUM$MFR_SNDR$AGE$AGE_COD$GNDR_COD$E_SUB$WT$WT_COD$REPT_DT$OCCP_COD$DEATH_DT$TO_MFR$CONFID$REPORTER_COUNTRY".toLowerCase());

var drug_legacy = identity("ISR$DRUG_SEQ$ROLE_COD$DRUGNAME$VAL_VBM$ROUTE$DOSE_VBM$DECHAL$RECHAL$LOT_NUM$EXP_DT$NDA_NUM".toLowerCase());

var indi_legacy = identity("ISR$DRUG_SEQ$INDI_PT".toLowerCase());

var outc_legacy = identity("ISR$OUTC_COD".toLowerCase());

var reac_legacy = identity("ISR$PT".toLowerCase());

var rpsr_legacy = identity("ISR$RPSR_COD".toLowerCase());

var ther_legacy = identity("ISR$DRUG_SEQ$START_DT$END_DT$DUR$DUR_COD".toLowerCase());

module.exports = {
  //current
  demo, drug, indi, outc, reac, rpsr, ther,
  //legacy
  demo_legacy, drug_legacy, indi_legacy, outc_legacy, reac_legacy, rpsr_legacy, ther_legacy
};