var TestClient = require('./test-client');
var Promise = require('bluebird');
var expect = require('chai').expect;
var ks = 'prepared_test';
var table = 'prepared_test';
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

describe('prepared queries', function() {
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

    it('retrieves data using a prepared query', function() {
        var rows = _.times(10, function (i) { return 'row-' + i; });

        var cql = util.format('SELECT * FROM %s where ROW = ? and col < ?', table);
        return client.prepare(cql)
            .then(function(prepared) {
                return Promise.map(rows, function(row) {
                    var q = prepared.query();
                    q.bind([row, 1000000000], {});
                    Promise.promisifyAll(q);
                    return q.executeAsync({});
                });
            })
            .then(function(results) {
                var allResults = _.reduce(results, function(memo, result) {
                    memo.rows = memo.rows.concat(result.rows);
                    return memo;
                }, {rows: []});

                expect(allResults.rows.length).equal(data.length);

                var cols = _.pluck(allResults.rows, 'col');

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
