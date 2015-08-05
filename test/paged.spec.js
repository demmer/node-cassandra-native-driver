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

describe('paging', function() {
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

    it('execute can page through results', function() {
        var cols = [];
        return client.execute('select * from ' + table, [], {fetchSize: data.length / 2})
        .then(function(results) {
            expect(results.rows.length).equal(data.length / 2);
            cols = cols.concat(_.pluck(results.rows, 'col'));
            expect(results.pageState).not.null();
            return client.execute('select * from ' + table, [],
                {fetchSize: data.length, pageState: results.pageState});
        })
        .then(function(results) {
            expect(results.rows.length).equal(data.length / 2);
            expect(results.pageState).is.undefined();
            cols = cols.concat(_.pluck(results.rows, 'col'));

            cols.sort(function(a,b) { return a - b; });
            for (i = 0; i < cols.length; ++i) {
                expect(cols[i]).equal(i);
            }
        });
    });

    it('execute autoPage fetches all data', function() {
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

    it('eachRow returns only one chunk at a time', function() {
        var rows = [];
        function addRow(row) {
            rows.push(row);
        }
        return client.eachRow('select * from ' + table, [],
            {fetchSize: data.length / 2}, addRow)
        .then(function(results) {
            expect(results.pageState).not.null();
            return client.eachRow('select * from ' + table, [],
                {fetchSize: data.length, pageState: results.pageState}, addRow);
        })
        .then(function(results) {
            expect(results.pageState).undefined();
            expect(rows.length).equal(data.length);
            var cols = _.pluck(results.rows, 'col');
            cols.sort(function(a,b) { return a - b; });
            for (i = 0; i < cols.length; ++i) {
                expect(cols[i]).equal(i);
            }
        });
    });

    it('eachRow autoPage fetches all data', function() {
        var rows = [];
        return client.eachRow('select * from ' + table, [],
            {fetchSize: 2, autoPage: true},
            function(row) {
                rows.push(row);
            })
            .then(function(results) {
                expect(rows.length).equal(data.length);
                rows = _.sortBy(rows, 'col');
                for (i = 0; i < rows.length; ++i) {
                    expect(rows[i].col).equal(i);
                }
            });
    });

    after(function() {
        return client.cleanup();
    });
});
