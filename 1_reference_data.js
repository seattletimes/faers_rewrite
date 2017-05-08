var async = require("async");
var cheerio = require("cheerio");
var request = require("request");
var shell = require("shelljs");

var fs = require("fs");
var path = require("path");
var url = require("url");

/*

Get the "Orange Book" from the FDA, which details drug products, patents, and
exclusivity.

I'm not sure how reliable the FDA's URLs are, long-term. They use one of these
horrible UCM systems that Oracle's apparently peddling now, and gives me
flashbacks to being at the World Bank. Theoretically, they should be stable.
So we load the Orange Book page, and grab the first file that contains "UCM"
and is a zip file. Then we unzip it. If this works, you should have three
files: exclusivity.txt, patent.txt, and products.txt. These are used to build
up the reference database.

We'll check in the reference files, just in case. But they're updated monthly,
so you should refresh if possible.

*/

var getOrangeBook = function(done) {

  var orangePage = "https://www.fda.gov/Drugs/InformationOnDrugs/ucm129689.htm";
  request(orangePage, function(err, response, body) {
    var $ = cheerio.load(body);
    var link = $(`[href*="UCM"][href*="zip"]`).attr("href");
    var resolved = url.resolve(orangePage, link);

    console.log(`Loading Orange Book data from ${resolved}`);
    var r = request(resolved);
    var zip = fs.createWriteStream("reference_data/orangebook.zip")
    r.pipe(zip);
    zip.on("close", function() {
      shell.exec("unzip -o orangebook.zip", { cwd: "reference_data" }, function(exit) {
        if (exit) console.log(exit);
        if (
          fs.existsSync("reference_data/exclusivity.txt") &&
          fs.existsSync("reference_data/products.txt") &&
          fs.existsSync("reference_data/patent.txt")
        ) {
          console.log("Orange book data downloaded.");
          if (done) done();
        } else {
          console.log("Could not verify existence of orange book files.")
        }
      });
    });
  });

};

var createRefDB = function(done) {
  shell.exec("createdb faers", function(exit) {
    console.log(exit);
    if (done) done();
  });
};

var runLoadScript = function(done) {
  shell.exec("psql -d faers -f load_nda_data.sql", { cwd: "reference_data" }, function(exit) {
    if (exit) console.log(exit);
    if (done) done();
  })
}

async.waterfall([getOrangeBook, createRefDB, runLoadScript]);