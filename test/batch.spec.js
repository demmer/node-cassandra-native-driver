var TestClient = require('./test-client');
var Promise = require('bluebird');
var expect = require('chai').expect;
var ks = 'batch_test';
var table = 'test';
var _ = require('underscore');
var util = require('util');
var test_utils = require('./test-utils');

var fields = {
    'row': 'varchar',
    'col': 'int',
    'val': 'int'
};

var key = 'row, col';
var data = test_utils.generate(100);
var client;

describe('batch queries', function() {
    before(function() {
        client = new TestClient();
        return test_utils.setup_environment(client)
            .then(function() {
                return client.createTable(table, fields, key);
            });
    });

    it('inserts data using a batch of prepared queries', function() {
        var rows = _.times(10, function (i) { return 'row-' + i; });

        var cql = util.format('INSERT INTO %s (row, col, val) VALUES (?, ?, ?)', table);
        return client.prepare(cql)
            .then(function(prepared) {
                var batch = client.new_batch('unlogged');
                _.each(rows, function(row) {
                    batch.add_prepared(prepared, [row, 10, 1000000]);
                });

                Promise.promisifyAll(batch);
                return batch.executeAsync({});
            })
            .then(function(results) {
                return client.execute('SELECT * FROM ' + table, [], {});
            })
            .then(function(results) {
                expect(results.rows.length).equal(10);
            });
    });

    it('inserts data using a batch with a bad prepared query', function() {
        var rows = _.times(10, function (i) { return 'row-' + i; });

        var cql = util.format('INSERT INTO %s (row, col, val) VALUES (?, ?, ?)', table);
        return client.prepare(cql)
            .then(function(prepared) {
                var batch = client.new_batch('unlogged');
                batch.add_prepared(prepared, ["foo", "bar", "baz"]);
                Promise.promisifyAll(batch);
                return batch.executeAsync({});
            })
            .then(function(results) {
                return client.execute('SELECT * FROM ' + table, [], {});
            })
            .then(function() {
                throw new Error('unexpected success');
            })
            .catch(function(err) {
                expect(err).match(/Invalid unset value/);
            });
    });

    it('errors properly if a bogus query is added to a batch', function() {
        var bogus = [null, undefined, {}, 1, "foo"];
        return Promise.map(bogus, function(bad) {
            return Promise.try(function() {
                var batch = client.new_batch('unlogged');
                batch.add(bad);
            })
            .then(function() {
                throw new Error('unexpected success');
            })
            .catch(function(err) {
                expect(err).match(/requires a valid query object/);
            });
        });
    });

    it('errors properly if a bogus prepared is added to a batch', function() {
        var bogus = [null, undefined, {}, 1, "foo"];
        return Promise.map(bogus, function(bad) {
            return Promise.try(function() {
                var batch = client.new_batch('unlogged');
                batch.add_prepared(bad, []);
            })
            .then(function() {
                throw new Error('unexpected success');
            })
            .catch(function(err) {
                expect(err).match(/requires a valid prepared object/);
            });
        });
    });

    it('handles errors in a prepared query with a bogus table', function() {
        var cql = util.format('SELECT * FROM %s where ROW = ? and col < ?', 'bogus');
        return client.prepare(cql)
        .then(function() {
            throw new Error('unexpected success');
        })
        .catch(function(err) {
            expect(err.toString()).match(/unconfigured table/);
        });
    });

    after(function() {
        return client.cleanup();
    });
});
