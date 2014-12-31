var addon = require('../build/Release/cassandra-native');

function Client(options) {
    this.client = new addon.Client();
}

Client.prototype.connect = function() {
    this.client.connect();
};

// Utility to parse the query function args
function parseArgs() {
    var args = Array.prototype.slice.call(arguments);

    var ret = {
        query: args[0],
        params: [],
        options: {},
        callback: undefined,
        endCallback: undefined
    };

    args.shift();

    // If specified, params must be an array
    if (args[0] instanceof Array) {
        ret.params = args[0];
        args.shift();
    }

    // If specified, options must be a non-Function object
    if (! (args[0] instanceof Function)) {
        ret.options = args[0];
        args.shift();
    }

    ret.callback = args[0];
    ret.endCallback = args[1];

    if (ret.query === undefined || ret.callback === undefined) {
        throw new Error('query and callback are required');
    }

    return ret;
}

// Execute the given query in pages.
//
// * query: (required) the CQL query
// * params: (optional) data to bind to the query
// * options: (optional) options for the query
// * callback: (required) page callback function
//
// For each batch of rows, calls the callback with arguments (err, data, more).
// If err is non-null, then data contains the batch of rows and more is a
// boolean indicating whether there is more data to read.
Client.prototype.query = function(query, params, options, callback) {
    var args = parseArgs(query, params, options, callback);
    console.log(args);
    this.client.query(args.query, args.params, args.options, args.callback);
};

// Execute the given query and return all the results.
//
// * query: (required) the CQL query
// * params: (optional) data to bind to the query
// * options: (optional) options for the query
// * callback: (required) page callback function
Client.prototype.execute = function(query, params, options, callback) {
    var rows = [];
    function pageCallback(err, data, more) {
        if (err) { return callback(err); }
        rows = rows.concat(data);
        if (! more) {
            callback(null, rows);
        }
    }
    this.query(query, params, options, pageCallback);
};

// Execute the given query and call the given callback one row at a time,
// calling the final callback when complete.
//
// * query: (required) the CQL query
// * params: (optional) data to bind to the query
// * options: (optional) options for the query
// * callback: (required) page callback function
Client.prototype.eachRow = function(query, params, options, rowCallback, endCallback) {
    function pageCallback(err, data, more) {
        if (err) { return endCallback(err); }
        data.forEach(function(row) {
            rowCallback(row);
        });
        if (! more) {
            endCallback(null);
        }
    }
    this.query(query, params, options, pageCallback);
};

module.exports = Client;
