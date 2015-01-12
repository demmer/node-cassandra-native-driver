var addon = require('../build/Release/cassandra-native');
//var addon = require('../build/Debug/cassandra-native');

function Client(options) {
    this.client = new addon.Client();
}

Client.prototype.connect = function(address, cb) {
    this.client.connect(address, cb);
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
// * endCallback: (required) end callback function
//
// If successful, calls the callback with the batch of rows.
// If autoPage is enabled, then it repeatedly queries each batch of rows.
// When the operation completes or has an error, calls the endCallback.
Client.prototype.query = function(query, params, options, callback, endCallback) {
    var args = parseArgs(query, params, options, callback, endCallback);
    var self = this;

    function pageCallback(err, data) {
        if (err) { return args.endCallback(err); }

        var result = {
            rows: data.rows,
            meta: {}
        };

        if (data.more) {
            result.meta.pageState = data.query;
        }
        args.callback(result);

        if (data.more && args.options.autoPage) {
            args.options.query = data.query;
            self.client.query(args.query, args.params, args.options, pageCallback);
        } else {
            args.endCallback();
        }
    }

    args.options.query = args.options.pageState;
    this.client.query(args.query, args.params, args.options, pageCallback);
};

// Execute the given query and return all the results.
//
// * query: (required) the CQL query
// * params: (optional) data to bind to the query
// * options: (optional) options for the query
// * callback: (required) page callback function
Client.prototype.execute = function(query, params, options, callback) {
    var args = parseArgs(query, params, options, callback);

    var data = {
        rows: [],
        meta: {}
    };

    function pageCallback(results) {
        data.rows = data.rows.concat(results.rows);
        data.meta = results.meta;
    }

    function endCallback(err) {
        if (err) { return args.callback(err); }
        args.callback(null, data);
    }
    this.query(args.query, args.params, args.options, pageCallback, endCallback);
};

// Execute the given query and call the given callback one row at a time,
// calling the final callback when complete.
//
// * query: (required) the CQL query
// * params: (optional) data to bind to the query
// * options: (optional) options for the query
// * rowCallback: (required) row callback function
// * endCallback: (required) operation callback function
Client.prototype.eachRow = function(query, params, options, rowCallback, endCallback) {
    var args = parseArgs(query, params, options, rowCallback, endCallback);

    function pageCallback(results) {
        results.rows.forEach(function(row) {
            args.callback(row);
        });
    }

    this.query(args.query, args.params, args.options, pageCallback, endCallback);
};

module.exports = Client;
