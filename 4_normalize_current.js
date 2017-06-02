var async = require("async");
var csv = require("csv");
var shell = require("shelljs");

var cleaners = require("./cleaners");

var fs = require("fs");
var path = process.platform == "win32" ? require("path").win32 : require("path");

var noop = function() {};

/*

Now we need to merge the files together into one big file per table: DEMO,
DRUG, INDI, OUTC, REAC, RPSR, and THER.

Fun problems: the filenames are not consistently cased up or down. There are
multiple formats per individual file type as the FDA has changed them over
time (including the shift between the current system and the legacy data).
These files are too large to want to load and process in their entirety. So
we'll use a stream to load them and then serialize them back out into our
single-data files, this time as a standard CSV instead of a $-delimited file.

Stream handlers are loaded from `cleaners.js`, and should take the form of a
transform function that takes the input from each file and converts it into an
object with the expected columns. The function should also have an attached
`columns` property containing an array with the order of the columns for
output.

*/

var processAscii = function(done = noop) {

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

    var written = 0;

    parser.on("data", row => {
      csvStream.write(cleaner(row));
      written++;
    });

    parser.on("finish", function(event) {
      console.log(`Processed ${parser.count} rows`);
      console.log(`Wrote ${written} rows`);
      console.timeEnd(f);
      if (global.gc) global.gc();
      csvStream.end();

      fileStream.on("close", nextFile);
    });

    console.log(`Processing ${f} to ${basename} using type "${prefix}"...`);
    var input = fs.createReadStream(path.join("scratch/ascii", f));
    input.pipe(parser);

  }, done);

};

var combineAscii = function(done = noop) {
  var normalized = fs.readdirSync("scratch/normalized");
  async.eachSeries(normalized, function(n, c) {
    var from = `scratch/normalized/${n}`;
    var files = fs.readdirSync(from);
    var listing = files.join(" ");
    shell.exec(`cat ${listing} > ../../combined/${n}.csv`, { cwd: from }, c);
  }, done);
}

async.series([processAscii, combineAscii]);