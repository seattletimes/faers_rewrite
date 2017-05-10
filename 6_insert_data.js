var async = require("async");
var shell = require("shelljs");

var fs = require("fs");
var path = require("path");

var noop = function() {};

/*

Some notes on the tables we're about to create, which are defined and
populated using the scripts in the `insert` directory:

At least at the start of the process, all columns are defined as the TEXT
type. This is inefficient and we'll probably regret it later, but since we're
going to be concatenating a lot of values together to create keys for
deduplication, it makes sense to effectively treat our database as loosely-
typed. It also keeps us from worrying about crashing due to messy or
unexpected data, at least at this stage of the game.

Based on our conversation with LTS, the data should be clean enough that we
can de-dupe without running OpenRefine or some other process on it first. I'm
a little wary of that, and reserve the right to go back and add additional
logic to step 3 that cleans up the data while we're normalizing the column
names. If you're reading this in the future, should we all survive that long,
it may be a fun exercise to check the git history on `cleaners.js`.

As mentioned, we're currently running the scripts to do insertion from the
`insert` directory. But in the future, it may make sense to auto-generate the
SQL from the headers we've already created for doing column normalization (all
the table setup becomes pretty formulaic once the format variations are
smoothed out). I just don't feel like learning how to pipe stuff from Node
into the shell at this point in my life.

*/

var loadCurrent = function(done = noop) {
  var scripts = fs.readdirSync("insert");
  async.eachSeries(scripts, function(script, c) {
    console.log(`Running insert/${script}`);
    shell.exec(`psql -d faers -f insert/${script}`, c);
  }, done);
};

var loadLegacy = function(done = noop) {

};

async.series([loadCurrent]);