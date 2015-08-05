var Base = require('extendable-base');
var cassandra = require('../index');
var Promise = require('bluebird');
var util = require('util');
var _ = require('underscore');

var types = cassandra.types;

// Test wrapper around the cassandra client.
//
// Wraps a promisified client object and exposes additional helper methods for
// writing tests.
var TestClient = Base.extend({
    initialize: function(options) {
        this.client = new cassandra.Client(options);
        Promise.promisifyAll(this.client);
        this.keyspaces = [];
    },

    connect: function (options) {
        return this.client.connectAsync(options);
    },

    execute: function() {
        return this.client.executeAsync.apply(this.client, arguments);
    },

    eachRow: function() {
        return this.client.eachRowAsync.apply(this.client, arguments);
    },

    prepare: function() {
        return this.client.prepareAsync.apply(this.client, arguments);
    },

    new_batch: function(style) {
        return this.client.new_batch(style);
    },

    metrics: function (reset) {
        return this.client.metrics(reset);
    },

    createKeyspace: function(name, replication) {
        replication = replication || 1;
        this.keyspaces.push(name);
        var cql = util.format("CREATE KEYSPACE %s WITH replication = " +
        "{'class': 'SimpleStrategy', 'replication_factor' : %d};",
        name, replication);

        return this.client.executeAsync(cql);
    },

    cleanKeyspace: function(name) {
        return this.client.executeAsync('DROP KEYSPACE ' + name)
        .catch(function(err) {
            if (/non existing keyspace/.test(err.message)) {
                return;
            }
            throw err;
        });
    },

    cleanup: function() {
        var self = this;
        return Promise.map(this.keyspaces, function(ks) {
            return self.client.executeAsync('DROP KEYSPACE ' + ks);
        }).then(function() {
            self.client = null;
        });
    },

    // Create a table with the given name in the current keyspace. Fields is an
    // object mapping the field name to the type.
    createTable: function(name, fields, key, opts) {
        opts = opts || "";
        var columns = _.map(fields, function(type, column) {
            return column + " " + type;
        });
        return this.execute(util.format("CREATE TABLE %s (%s, PRIMARY KEY(%s)) %s;",
            name, columns, key, opts));
    },

    _getInsertQuery: function(table, data, options) {
        var count = data.length;
        var keys = _.keys(data[0]);
        var cols = keys.join(',');

        var vars = _.map(keys, function(k) {
            return '?';
        }).join(',');

        var cql = util.format('INSERT INTO %s (%s) VALUES (%s)', table, cols, vars);

        var hints = options.hints || {};
        var hintsArray = _.map(keys, function(k) {
            return hints[k] || types.CASS_VALUE_TYPE_UNKNOWN;
        });

        if (options.timestamp && options.ttl) {
            cql = cql + ' USING TIMESTAMP ? AND TTL ?';
            hintsArray.push(types.CASS_VALUE_TYPE_TIMESTAMP);
            hintsArray.push(types.CASS_VALUE_TYPE_INT);

        } else if (options.timestamp || options.ttl) {
            throw new Error('xxx not implemented');
        }

        return {cql: cql, hints: hintsArray};
    },

    // Insert n rows of data into the given table using the supplied generator
    // function.
    insertRows: function(table, data, options) {
        var self = this;
        options = options || {};
        var keys = _.keys(data[0]);
        var query = this._getInsertQuery(table, data, options);

        function insert(d) {
            var vals = _.map(keys, function(k) { return d[k]; });
            if (options.timestamp && options.ttl) {
                vals.push(options.timestamp);
                vals.push(options.ttl);
            }

            return self.execute(query.cql, vals, {hints: query.hints});
        }
        return Promise.map(data, insert, {concurrency: options.concurrency});
    },

    insertRowsPrepared: function(table, data, options) {
        var self = this;
        var prepared;
        var count = data.length;
        var keys = _.keys(data[0]);

        var query = this._getInsertQuery(table, data, options);

        function prepare() {
            return self.client.prepareAsync(query.cql)
            .then(function(p) {
                prepared = p;
            });
        }

        function _insert(d, i, n, cb) {
            var vals = _.map(keys, function(k) { return d[k]; });
            if (options.timestamp && options.ttl) {
                vals.push(options.timestamp);
                vals.push(options.ttl);
            }

            var q = prepared.query();
            q.bind(vals, {hints: query.hints});
            q.execute({}, cb);
        }

        var insert = Promise.promisify(_insert);
        return prepare()
        .then(function() {
            return Promise.map(data, insert, {concurrency: options.concurrency});
        });
    },

    insertRowsPreparedBatch: function(table, data, options) {
        var self = this;
        var prepared;

        var count = data.length;
        var keys = _.keys(data[0]);
        var batch_size = options.batch_size || 1;

        var query = this._getInsertQuery(table, data, options);
        function prepare() {
            return self.client.prepareAsync(query.cql)
            .then(function(p) {
                prepared = p;
            });
        }

        function _insert_batch(x, batch_i, n, cb) {
            var batch = self.client.new_batch("unlogged");

            _.times(batch_size, function(i) {
                var d = data[(batch_i * batch_size) + i];

                // The last batch might not be full
                if (d === undefined) {
                    return;
                }

                var vals = _.map(keys, function(k) { return d[k]; });
                if (options.timestamp && options.ttl) {
                    vals.push(options.timestamp);
                    vals.push(options.ttl);
                }
                batch.add_prepared(prepared, vals, {hints: query.hints});
            });
            batch.execute({}, cb);
        }

        // Create an array with a dummy entry for each batch just to be able to
        // use Promise.map
        var batches = _.times(Math.ceil(count / batch_size), _.noop);

        var insert_batch = Promise.promisify(_insert_batch);
        return prepare()
        .then(function() {
            return Promise.map(batches, insert_batch, {concurrency: options.concurrency});
        });
    }
});

TestClient.types = types;

module.exports = TestClient;
