var async = require("async");
var cheerio = require("cheerio");
var request = require("request");
var shell = require("shelljs");

var fs = require("fs");
var path = process.platform == "win32" ? require("path").win32 : require("path");
var url = require("url");

var noop = function() {};

/*

Previously, the LTS scripts would download current and legacy FAERS data from
a series of shell scripts, each of which was hard-coded and manually updated
with each new release. This is Not Great practice if you plan on doing this
over a large amount of time, such as an investigation. So in this script, we
actually scrape the pages for each release, sending it through a processing
chain on the way.

*/

var currentPage = "https://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm";
var getCurrentFaers = function(done = noop) {
  shell.mkdir("-p", "scratch/current");

  request(currentPage, function(err, response, body) {
    var $ = cheerio.load(body);
    var links = $(`[href*="UCM"][href*="zip"]`)
      .filter((_, el) => $(el).text().indexOf("ASCII") > -1)
      .toArray()
      .map(el => url.resolve(currentPage, el.attribs.href));

    async.eachLimit(links, 4, function(href, c) {
      var name = path.basename(url.parse(href).pathname);
      var dest = path.join("scratch/current", name);
      if (fs.existsSync(dest)) return c();
      console.log(`Downloading current FAERS data: ${name}`);
      var r = request(href);
      var zip = fs.createWriteStream(dest);
      r.pipe(zip);
      zip.on("close", c);
    }, done);
  });
};

/*

Current FAERS archives contain the text files in an "ascii" directory, unless
they have an "asii" or "asci" directory instead, because oh god what have I
done with my life. Unzip them in this step.

*/

var unzipCurrent = function(done = noop) {
  fs.readdir("scratch/current", function(err, list) {
    async.eachSeries(list.filter(n => n.match(/\.zip$/)), function(file, c) {
      // unzip -o overwrite -C case insensitive -j flatten directories -d destination
      shell.exec(`unzip -oCj ${file} -d ../ascii a*/*.txt`, { cwd: "scratch/current" }, c);
    }, done);
  });
};

async.series([getCurrentFaers, unzipCurrent]);