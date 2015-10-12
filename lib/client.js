var addon = require('../addon');

var debug = require('debug')('cassandra');

function Client(options) {
    this.client = new addon.Client(options || {});
}

Client.prototype.connect = function(options, cb) {
    this.client.connect(options, cb);
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

// Internal execution function
Client.prototype._execute = function(query, params, options, callback, endCallback) {
    // Args should have been parsed previously
    if (!query || !params || !options || !callback || !endCallback) {
        throw new Error("invalid arguments");
    }

    var self = this;
    var q;

    function pageCallback(err, data) {
        if (err) { return endCallback(err); }

        var result = {
            rows: data.rows
        };

        if (data.more) {
            result.pageState = q;
        }
        callback(result);

        if (data.more && options.autoPage) {
            debug('autoPage: calling execute', options);
            q.execute(options, pageCallback);
        } else {
            endCallback(null, {pageState: result.pageState});
        }
    }

    // Handle manual pageState in which the query object is passed in from the
    // application as options.pageState.
    if (options.pageState) {
        q = options.pageState;
        if (q.query != query) {
            throw new Error('pageState mismatch: query ' + query + ' != bound ' + q.query);
        }
        delete options.pageState;
    } else {
        q = this.client.new_query();
    }

    debug('calling bind', query, params);
    q.parse(query, params, options);

    debug('calling execute', options);
    q.execute(options, pageCallback);

    return q;
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
        rows: []
    };

    function pageCallback(results) {
        data.rows = data.rows.concat(results.rows);
        data.pageState = results.pageState;
    }

    function endCallback(err) {
        if (err) { return args.callback(err); }
        args.callback(null, data);
    }
    this._execute(args.query, args.params, args.options, pageCallback, endCallback);
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

    this._execute(args.query, args.params, args.options, pageCallback, endCallback);
};

Client.prototype.new_query = function(query, callback) {
    return this.client.new_query();
};

Client.prototype.prepare = function(query, callback) {
    var p = this.client.new_prepared_query();
    return p.prepare(query, callback);
};

Client.prototype.new_batch = function(type) {
    return this.client.new_batch(type);
};

Client.prototype.metrics = function(reset) {
    return this.client.metrics(reset);
};

module.exports = Client;
