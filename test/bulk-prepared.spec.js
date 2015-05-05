var TestClient = require('./test-client');
var Promise = require('bluebird');
var expect = require('chai').expect;
var ks = 'bulk_test';
var table = 'test';
var table2 = 'test2';
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

describe('bulk queries', function() {
    before(function() {
        client = new TestClient();
        return test_utils.setup_environment(client)
            .then(function() {
                return client.createTable(table, fields, key);
            })
            .then(function() {
                return client.createTable(table2, fields, key);
            });
    });

    it('inserts data using prepared queries', function() {
        var rows = _.times(10, function (i) { return 'row-' + i; });

        var cql = util.format('INSERT INTO %s (row, col, val) VALUES (?, ?, ?)', table);
        return client.prepare(cql)
            .then(function(prepared) {
                var bulk = client.new_bulk_prepared();
                _.each(rows, function(row) {
                    bulk.add(prepared, [row, 10, 1000000]);
                });

                Promise.promisifyAll(bulk);
                return bulk.doneAsync();
            })
            .then(function(results) {
                expect(Object.keys(results)).deep.equal(['success']);
                expect(results.success).equal(10);
                return client.execute('SELECT * FROM ' + table, [], {});
            })
            .then(function(results) {
                expect(results.rows.length).equal(10);
            });
    });

    it('supports different prepared queries in the same bulk operation', function() {
        var rows = _.times(10, function (i) { return 'row-' + i; });

        var cqls = [
            util.format('INSERT INTO %s (row, col, val) VALUES (?, ?, ?)', table),
            util.format('INSERT INTO %s (row, col, val) VALUES (?, ?, ?)', table2)
        ];

        return Promise.map(cqls, function(cql) {
            return client.prepare(cql);
        })
        .then(function(prepareds) {
            var bulk = client.new_bulk_prepared();
            _.each(rows, function(row, i) {
                bulk.add(prepareds[i % 2], [row, 10, 1000000]);
            });

            Promise.promisifyAll(bulk);
            return bulk.doneAsync();
        })
        .then(function(results) {
            expect(Object.keys(results)).deep.equal(['success']);
            expect(results.success).equal(10);
            return Promise.map([table, table2], function(table) {
                return client.execute('SELECT * FROM ' + table, [], {});
            })
        })
        .then(function(results) {
            expect(results.length).equal(2);
            expect(results[0].rows.length).equal(10);
            expect(results[1].rows.length).equal(5);
        });
    });

    it('inserts data using a bulk with a bad prepared query', function() {
        var rows = _.times(10, function (i) { return 'row-' + i; });

        var cql = util.format('INSERT INTO %s (row, col, val) VALUES (?, ?, ?)', table);
        return client.prepare(cql)
            .then(function(prepared) {
                var bulk = client.new_bulk_prepared();
                bulk.add(prepared, ["foo", "bar", "baz"]);
                Promise.promisifyAll(bulk);
                return bulk.doneAsync();
            })
            .then(function(results) {
                expect(results.success).equal(0);
                expect(results.errors[0]).match(/Expected 4 or 0 byte int/);
            });
    });

    it('errors properly if a bogus prepared is added to a bulk', function() {
        var bogus = [null, undefined, {}, 1, "foo"];
        return Promise.map(bogus, function(bad) {
            return Promise.try(function() {
                var bulk = client.new_bulk_prepared();
                bulk.add(bad, []);
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
            expect(err).match(/unconfigured columnfamily/);
        });
    });

    after(function() {
        return client.cleanup();
    });
});
