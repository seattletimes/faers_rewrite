var async = require("async");
var csv = require("csv");
var shell = require("shelljs");

var cleaners = require("./cleaners");

var fs = require("fs");
var path = process.platform == "win32" ? require("path").win32 : require("path");

var noop = function() {};

/*

There are three quarters of legacy data that we have to handle. This is
similar to the process from the current file set, but uses its own set of
cleaners and reads from a different set of data, since the columns are almost
all different for some reason. We'll merge current and legacy data in SQL, as
the original LTS does, for lack of any better solution.

*/

var processAscii = function(done = noop) {

  var files = fs.readdirSync("scratch/ascii_legacy");

  async.eachSeries(files, function(f, nextFile) {

    var prefix = f.slice(0, 4).toLowerCase();
    if (prefix == "stat") return nextFile();
    var basename = path.basename(f, path.extname(f)).toUpperCase();
    var cleaner = cleaners[prefix + "_legacy"];

    //create the output for the normalized file
    shell.mkdir("-p", `scratch/normalized_legacy/${prefix}`);
    var fileStream = fs.createWriteStream(`scratch/normalized_legacy/${prefix}/${basename}.csv`);
    var csvStream = csv.stringify({ headers: true, columns: cleaner.columns });
    csvStream.pipe(fileStream);

    var parser = csv.parse({
      columns: cleaner.columns, //we only have 3 legacy quarters, this should be fine.
      delimiter: "$",
      relax: true,
      relax_column_count: true
    });

    console.time(f);

    parser.on("error", err => console.log(err));

    parser.on("data", row => csvStream.write(cleaner(row)));

    parser.on("finish", function(event) {
      console.log(`Processed ${parser.lines} rows`);
      console.timeEnd(f);
      if (global.gc) global.gc();
      csvStream.end();
      fileStream.end();
      nextFile();
    });

    console.log(`Processing ${f} using type "${prefix}"...`);
    var input = fs.createReadStream(path.join("scratch/ascii_legacy", f));
    input.pipe(parser);

  }, done);

};

var combineAscii = function(done = noop) {
  var normalized = fs.readdirSync("scratch/normalized_legacy");
  async.eachSeries(normalized, function(n, c) {
    var from = `scratch/normalized_legacy/${n}`;
    var files = fs.readdirSync(from);
    var listing = files.join(" ");
    shell.exec(`cat ${listing} > ../../combined/${n}_legacy.csv`, { cwd: from }, c);
  }, done);
}

async.series([processAscii, combineAscii]);