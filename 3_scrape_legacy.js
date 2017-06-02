var async = require("async");
var request = require("request");
var shell = require("shelljs");

var fs = require("fs");
var path = require("path");
var url = require("url");

var noop = function() {};

/*

The Legacy Adverse Effects Reporting System (LAERS) data is available here:

https://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm083765.htm

However, since this is legacy data, instead of scraping the page like we do
with the current setup, we will just download it from an array of filenames
culled from the original download script (which, to its shame, contained the
same basic download block copy/pasted for each archive file). In theory, these
URLs will not change. They are, of course, also not consistent with each
other, which reminds us that truly, nobody is without sin in this transaction.

Many of the inconsistencies in these files come from earlier years, such as
2010 and 2011, and we're only looking at 2012 or later. So for the purposes of
simplifying prep, I've commented out earlier years in the URL hash below.

*/

var base = "http://www.fda.gov/downloads/Drugs/";

var archives = [
  "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM337616.zip",
  "GuidanceComplianceRegulatoryInformation/Surveillance/UCM319844.zip",
  "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM307572.zip" //,
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM300452.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM287845.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM278762.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM270803.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM251745.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM244691.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM232595.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM220802.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM213485.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM197917.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM186488.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM173888.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/UCM150381.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm085780.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm085785.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm085793.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm085809.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm083816.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm083983.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084042.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084161.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084206.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084217.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084242.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084259.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084277.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084916.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084918.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084920.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084922.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084924.zip",
  // "GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm084927.zip"
];

var getLegacy = function(done = noop) {
  shell.mkdir("-p", "scratch/legacy");
  async.eachLimit(archives, 4, function(zip, c) {
    var filename = path.basename(zip);
    if (fs.existsSync(`scratch/legacy/${filename}`)) return c();
    console.log(`Downloading legacy report data: ${filename}`);
    var save = fs.createWriteStream(`scratch/legacy/${filename}`);
    var r = request(url.resolve(base, zip));
    r.pipe(save);
    r.on("end", c);
  }, done);
};

var unzipLegacy = function(done = noop) {
  shell.mkdir("-p", "scratch/ascii_legacy");
  var files = fs.readdirSync("scratch/legacy");
  async.eachSeries(files, function(file, c) {
    shell.exec(`unzip -oCj ${file} -d ../ascii_legacy a*/*.txt`, { cwd: "scratch/legacy" }, c)
  }, done);
};

async.series([getLegacy, unzipLegacy]);