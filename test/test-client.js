var Base = require('extendable-base');
var cassandra = require('../index');
var Promise = require('bluebird');
var util = require('util');
var _ = require('underscore');
var request = Promise.promisifyAll(require('request'));

// Test wrapper around the cassandra client.
//
// Wraps a promisified client object and exposes additional helper methods for
// writing tests.
var TestClient = Base.extend({
    initialize: function() {
        this.client = new cassandra.Client();
        Promise.promisifyAll(this.client);
        this.keyspaces = [];
    },

    connect: function (address) {
        return this.client.connectAsync(address);
    },

    execute: function() {
        return this.client.executeAsync.apply(this.client, arguments);
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

    // Insert n rows of data into the given table using the supplied generator
    // function.
    insertRows: function(table, data, concurrent) {
        var self = this;
        var count = data.length;
        var keys = _.keys(data[0]);
        var cols = keys.join(',');
        var vars = _.map(keys, function() { return '?'; }).join(',');
        function insert(d) {
            var vals = _.map(keys, function(k) { return d[k]; });
            return self.execute(util.format('INSERT INTO %s (%s) VALUES (%s)', table, cols, vars),
                vals, {});
        }
        return Promise.map(data, insert, {concurrency: concurrent});
    },

    insertRowsPrepared: function(table, data, concurrent) {
        var self = this;
        var prepared;

        var count = data.length;
        var keys = _.keys(data[0]);
        var cols = keys.join(',');
        var vars = _.map(keys, function() { return '?'; }).join(',');

        function prepare() {
            var cql = util.format('INSERT INTO %s (%s) VALUES (%s)', table, cols, vars);
            return self.client.prepareAsync(cql)
            .then(function(p) {
                console.log('got prepared', p);
                prepared = p;
            });
        }

        function _insert(d, i, n, cb) {
            var vals = _.map(keys, function(k) { return d[k]; });
            var query = prepared.query();
            query.bind(vals);
            query.execute({}, cb);
        }

        var insert = Promise.promisify(_insert);
        return prepare()
        .then(function() {
            return Promise.map(data, insert, {concurrency: concurrent});
        });
    },

    insertRowsPreparedBatch: function(table, data, batch_size, concurrent, with_metadata) {
        var self = this;
        var prepared;

        var count = data.length;
        var keys = _.keys(data[0]);
        var cols = keys.join(',');
        var vars = _.map(keys, function() { return '?'; }).join(',');

        function prepare() {
            var cql = util.format('INSERT INTO %s (%s) VALUES (%s)', table, cols, vars);
            return self.client.prepareAsync(cql)
            .then(function(p) {
                console.log('got prepared', p);
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
                var query = prepared.query();
                query.bind(vals);

                batch.add(query);
            });
            batch.execute({}, cb);
        }

        function insert_metadata() {
            var metadata = data.map(function(pt) {
                return _.omit(pt, 'value', 'time');
            });

            var requestBody = metadata.map(function(datum) {
                var updateCmd = JSON.stringify({
                    update: {
                        _index: 'metadata',
                        _type: datum.name,
                        _id: 1
                    }
                });
                var doc = {};
                _.each(datum, function(value, key) {
                    doc[key + ':' + value] = 1;
                });
                var requestLine = JSON.stringify({
                    doc: doc,
                    doc_as_upsert: true,
                    detect_noop: true
                });
                return updateCmd + '\n' + requestLine + '\n';
            });
            var start = new Date();
            console.log('sending points over to ES');
            return request.postAsync({
                url: 'http://localhost:9200/_bulk',
                body: requestBody
            })
            .spread(function(res, body) {
                var end = new Date();
                console.log('status code for meta-point insert', res.statusCode);
                console.log('done inserting ', data.length, ' meta-points, took', (end - start) / 1000, 'seconds');
            });
        }

        // Create an array with a dummy entry for each batch just to be able to
        // use Promise.map
        var batches = _.times(Math.ceil(count / batch_size), _.noop);

        var insert_batch = Promise.promisify(_insert_batch);
        return prepare()
        .then(function() {
            var promises = [Promise.map(batches, insert_batch, {concurrency: concurrent}).then(function() { console.log('done inserting real points'); })];
            if (with_metadata) {
                promises.push(insert_metadata());
            }
            return Promise.all(promises);
        });
    }
});

module.exports = TestClient;
