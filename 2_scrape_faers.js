var async = require("async");
var cheerio = require("cheerio");
var csv = require("csv");
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
  request(currentPage, function(err, response, body) {
    var $ = cheerio.load(body);
    var links = $(`[href*="UCM"][href*="zip"]`)
      .filter((_, el) => $(el).text().indexOf("ASCII") > -1)
      .toArray()
      .map(el => url.resolve(currentPage, el.attribs.href));

    async.eachLimit(links, 4, function(href, c) {
      var name = path.basename(url.parse(href).pathname);
      var dest = path.join("scratch", name);
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
  fs.readdir("scratch", function(err, list) {
    async.eachSeries(list.filter(n => n.match(/\.zip$/)), function(file, c) {
      // unzip -o overwrite -C case insensitive -j flatten directories -d destination
      shell.exec(`unzip -oCj ${file} -d ascii a\\*/\\*.txt`, { cwd: "scratch" }, c);
    }, done);
  });
};

/*

Now we need to merge the files together into one big file per table: DEMO,
DRUG, INDI, OUTC, REAC, RPSR, and THER.

Fun problems: the filenames are not consistently cased up or down. There are
multiple formats per individual file type, which we will want to sniff out
from the header. These files are too large to want to load and process in
their entirety. So we'll use a stream to load them and then serialize them
back out into our single-data files.

Stream handlers are loaded from clean_current and clean_legacy, and should
take the form of a transform function that takes the input from each file and
converts it into an object with the expected columns. The function should also
have an attached `columns` property containing an array with the order of the
columns for output.

*/

var processCurrent = function() {
  var cleaners = require("./utils/cleaners");

  var files = fs.readdirSync("scratch/ascii");

  async.eachSeries(files, function(f, nextFile) {

    var prefix = f.slice(0, 4).toLowerCase();
    var basename = path.basename(f, path.extname(f)).toUpperCase();
    var cleaner = cleaners[prefix];

    //create the output for the normalized file
    shell.mkdir("-p", `scratch/normalized/${prefix}`);
    var fileStream = fs.createWriteStream(`scratch/normalized/${prefix}/${basename}.csv`);
    var csvStream = csv.stringify({ headers: true, columns: cleaner.columns });
    csvStream.pipe(fileStream);

    var parser = csv.parse({
      columns: true,
      delimiter: "$",
      relax: true
    });

    console.time(f);

    parser.on("error", err => console.log(err));

    parser.on("data", row => csvStream.write(cleaner(row)));

    parser.on("finish", function(event) {
      console.log(`Processed ${parser.count} rows`);
      console.timeEnd(f);
      if (global.gc) global.gc();
      csvStream.end();
      fileStream.end();
      nextFile();
    });

    console.log(`Processing ${f} using type "${prefix}"...`);
    var input = fs.createReadStream(path.join("scratch/ascii", f));
    input.pipe(parser);

  }, function() {

    console.log("All files done");

  });

};

var combineCurrent = function(done = noop) {
  var normalized = fs.readdirSync("scratch/normalized");
  async.eachSeries(normalized, function(n, c) {
    var files = fs.readdirSync(`scratch/normalized/${n}`);
    var command = files.join(" ");
    shell.exec(`cat ${command} > ../combined/${n}.csv`, { cwd: "scratch/normalized" }, c);
  }, done);
}

async.series([/*getCurrentFaers, unzipCurrent,*/ processCurrent, combineCurrent]);