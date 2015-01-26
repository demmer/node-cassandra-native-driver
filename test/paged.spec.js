var TestClient = require('./test-client');
var Promise = require('bluebird');
var expect = require('chai').expect;
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

describe('paged queries', function() {
    before(function() {
        client = new TestClient();
        return test_utils.setup_environment(client)
            .then(function() {
                return client.createTable(table, fields, key);
            })
            .then(function() {
                return client.insertRows(table, data);
            });
    });

    it('autoPage fetches all data', function() {
        return client.execute('select * from ' + table, [],
        {fetchSize: 2, autoPage: true})
            .then(function(results) {
                expect(results.rows.length).equal(data.length);
                var rows = _.sortBy(results.rows, 'col');
                for (i = 0; i < rows.length; ++i) {
                    expect(rows[i].col).equal(i);
                }
            });
    });

    it('manual paging returns only fetchSize points at a time', function() {
        var cols = [];
        return client.execute('select * from ' + table, [], {fetchSize: data.length / 2})
        .then(function(results) {
            expect(results.rows.length).equal(data.length / 2);
            cols = cols.concat(_.pluck(results.rows, 'col'));
            expect(results.meta.pageState).not.null();
            return client.execute('select * from ' + table, [],
                {fetchSize: data.length / 2, pageState: results.meta.pageState});
        })
        .then(function(results) {
            expect(results.rows.length).equal(data.length / 2);
            cols = cols.concat(_.pluck(results.rows, 'col'));

            cols.sort(function(a,b) { return a - b; });
            for (i = 0; i < cols.length; ++i) {
                expect(cols[i]).equal(i);
            }
        });
    });

    after(function() {
        return client.cleanup();
    });
});
