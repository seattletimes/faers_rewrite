var identity = function(columns) {
  var f = row => row;
  f.columns = columns.split("$");
  return f;
};

var translator = function(translate) {
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

module.exports = { demo };